context("Blob client interface, extra")

tenant <- Sys.getenv("AZ_TEST_TENANT_ID")
app <- Sys.getenv("AZ_TEST_APP_ID")
password <- Sys.getenv("AZ_TEST_PASSWORD")
subscription <- Sys.getenv("AZ_TEST_SUBSCRIPTION")

if(tenant == "" || app == "" || password == "" || subscription == "")
    skip("Authentication tests skipped: ARM credentials not set")

rgname <- Sys.getenv("AZ_TEST_STORAGE_RG")
storname <- Sys.getenv("AZ_TEST_STORAGE_NOHNS")

if(rgname == "" || storname == "")
    skip("Blob client tests skipped: resource names not set")

sub <- AzureRMR::az_rm$new(tenant=tenant, app=app, password=password)$get_subscription(subscription)
stor <- sub$get_resource_group(rgname)$get_storage_account(storname)
bl <- stor$get_blob_endpoint()
options(azure_storage_progress_bar=FALSE)

srcdir <- tempfile(pattern="ul")
destdir <- tempfile(pattern="dl")
destdir2 <- tempfile(pattern="dl")
destdir3 <- tempfile(pattern="dl")
destdir4 <- tempfile(pattern="dl")
dir.create(srcdir)
dir.create(file.path(srcdir, "subdir"))

srcs <- replicate(5, write_file(srcdir))
srcs_sub <- replicate(5, write_file(file.path(srcdir, "subdir")))


test_that("Blob vector multitransfer works",
{
    contname <- paste0(sample(letters, 10, TRUE), collapse="")
    cont <- create_blob_container(bl, contname)

    # test that extra args passed to nodes
    expect_error(multiupload_blob(cont, file.path(srcdir, srcs), type="foo"))

    multiupload_blob(cont, file.path(srcdir, srcs), srcs)
    multidownload_blob(cont, srcs, file.path(destdir, srcs))

    expect_identical(AzureRMR::pool_size(), 2L)
    expect_true(files_identical(file.path(srcdir, srcs), file.path(destdir, srcs)))

    # vector src needs vector dest
    expect_error(multiupload_blob(cont, file.path(srcdir, srcs), "/"))
    expect_error(multidownload_blob(cont, srcs, destdir))
})


test_that("Blob wildcard multitransfer works",
{
    contname <- paste0(sample(letters, 10, TRUE), collapse="")
    cont <- create_blob_container(bl, contname)

    multiupload_blob(cont, file.path(srcdir, "*"), "/")
    multidownload_blob(cont, "*", destdir2)

    expect_true(files_identical(file.path(srcdir, srcs), file.path(destdir2, srcs)))

    # wildcard src needs directory dest
    expect_error(multiupload_blob(cont, file.path(srcdir, "*"), srcs))
    expect_error(multidownload_blob(cont, "*", file.path(destdir2, srcs)))
})


test_that("Blob wildcard multitransfer from subdir works",
{
    contname <- paste0(sample(letters, 10, TRUE), collapse="")
    cont <- create_blob_container(bl, contname)

    multiupload_blob(cont, file.path(srcdir, "*"), "/", recursive=TRUE)
    multidownload_blob(cont, "subdir/*", destdir3)

    expect_true(files_identical(file.path(srcdir, "subdir", srcs_sub), file.path(destdir3, srcs_sub)))
})


test_that("Blob recursive wildcard multitransfer works",
{
    contname <- paste0(sample(letters, 10, TRUE), collapse="")
    cont <- create_blob_container(bl, contname)

    multiupload_blob(cont, file.path(srcdir, "*"), "/", recursive=TRUE)
    multidownload_blob(cont, "*", destdir4, recursive=TRUE)

    expect_true(files_identical(file.path(srcdir, srcs), file.path(destdir4, srcs)))

    # wildcard src needs directory dest
    expect_error(multiupload_blob(cont, file.path(srcdir, "*"), srcs))
    expect_error(multidownload_blob(cont, "*", file.path(destdir4, srcs)))
})


test_that("Default multitransfer destination works",
{
    contname <- paste0(sample(letters, 10, TRUE), collapse="")
    cont <- create_blob_container(bl, contname)

    multiupload_blob(cont, file.path(srcdir, srcs))
    multidownload_blob(cont, srcs)

    expect_true(files_identical(file.path(srcdir, srcs), srcs))
    file.remove(srcs)
})


test_that("Default multitransfer destination works with wildcard src",
{
    contname <- paste0(sample(letters, 10, TRUE), collapse="")
    cont <- create_blob_container(bl, contname)

    multiupload_blob(cont, file.path(srcdir, "subdir/*"))
    multidownload_blob(cont, "*")

    expect_true(files_identical(file.path(srcdir, "subdir", srcs_sub), srcs_sub))
    file.remove(srcs_sub)
})


test_that("Blob multicopy from URL works",
{
    contname <- paste0(sample(letters, 10, TRUE), collapse="")
    cont <- create_blob_container(bl, contname)

    fnames <- c("LICENSE", "DESCRIPTION", "NAMESPACE")
    src_urls <- paste0("https://raw.githubusercontent.com/AzureRSDK/AzureStor/master/", fnames)
    origs <- replicate(3, tempfile())
    dests <- replicate(3, tempfile())

    multicopy_url_to_blob(cont, src_urls, fnames, async=FALSE)
    multidownload_blob(cont, fnames, dests)
    pool_map(download.file, src_urls, origs, mode="wb")

    expect_true(files_identical(origs, dests))
})


teardown(
{
    conts <- list_blob_containers(bl)
    lapply(conts, delete_blob_container, confirm=FALSE)
})
