context("Blob/ADLS interop 1")

tenant <- Sys.getenv("AZ_TEST_TENANT_ID")
app <- Sys.getenv("AZ_TEST_APP_ID")
password <- Sys.getenv("AZ_TEST_PASSWORD")
subscription <- Sys.getenv("AZ_TEST_SUBSCRIPTION")

if(tenant == "" || app == "" || password == "" || subscription == "")
    skip("Authentication tests skipped: ARM credentials not set")

rgname <- Sys.getenv("AZ_TEST_STORAGE_RG")
storname <- Sys.getenv("AZ_TEST_STORAGE_HNS")

if(rgname == "" || storname == "")
    skip("Blob/ADLS interop tests skipped: resource names not set")

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


test_that("Blob API on HNS-enabled account works",
{
    expect_is(bl, "blob_endpoint")

    contname <- paste0(sample(letters, 10, TRUE), collapse="")
    cont <- create_blob_container(bl, contname)
    expect_is(cont, "blob_container")

    lst <- list_blob_containers(bl)
    expect_true(is.list(lst) && inherits(lst[[1]], "blob_container"))

    expect_true(is_empty(list_blobs(cont)))
    orig_file <- "../resources/iris.csv"
    new_file <- file.path(tempdir(), "iris.csv")
    upload_blob(cont, orig_file, "iris.csv")

    expect_is(list_blobs(cont), "data.frame")
    expect_is(list_blobs(cont, info="name"), "character")
    expect_is(list_blobs(cont, info="all"), "data.frame")

    suppressWarnings(file.remove(new_file))
    download_blob(cont, "iris.csv", new_file)
    expect_identical(readBin(orig_file, "raw", n=1e5), readBin(new_file, "raw", n=1e5))

    expect_is(get_storage_properties(cont, "iris.csv"), "list")
    set_storage_metadata(cont, "iris.csv", key1="value1")
    expect_identical(get_storage_metadata(cont, "iris.csv"), list(key1="value1"))
})


test_that("Blob vector multitransfer works",
{
    contname <- paste0(sample(letters, 10, TRUE), collapse="")
    cont <- create_blob_container(bl, contname)

    # test that extra args passed to nodes: append blobs not (yet) supported on HNS accts
    expect_error(multiupload_blob(cont, file.path(srcdir, srcs), paste0(srcs, "_bad"), type="AppendBlob"))

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


teardown(
{
    conts <- list_blob_containers(bl)
    lapply(conts, delete_blob_container, confirm=FALSE)
})
