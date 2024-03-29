context("File client interface, extra")

tenant <- Sys.getenv("AZ_TEST_TENANT_ID")
app <- Sys.getenv("AZ_TEST_APP_ID")
password <- Sys.getenv("AZ_TEST_PASSWORD")
subscription <- Sys.getenv("AZ_TEST_SUBSCRIPTION")

if(tenant == "" || app == "" || password == "" || subscription == "")
    skip("Authentication tests skipped: ARM credentials not set")

rgname <- Sys.getenv("AZ_TEST_STORAGE_RG")
storname <- Sys.getenv("AZ_TEST_STORAGE_NOHNS")

if(rgname == "" || storname == "")
    skip("File client tests skipped: resource names not set")

sub <- AzureRMR::az_rm$new(tenant=tenant, app=app, password=password)$get_subscription(subscription)
stor <- sub$get_resource_group(rgname)$get_storage_account(storname)
fl <- stor$get_file_endpoint()
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


test_that("File vector multitransfer works",
{
    contname <- paste0(sample(letters, 10, TRUE), collapse="")
    cont <- create_file_share(fl, contname)

    # test that extra args passed to nodes
    expect_error(multiupload_azure_file(cont, file.path(srcdir, srcs), type="AppendBlob"))

    multiupload_azure_file(cont, file.path(srcdir, srcs), srcs)
    multidownload_azure_file(cont, srcs, file.path(destdir, srcs))

    expect_identical(AzureRMR::pool_size(), 10L)
    expect_true(files_identical(file.path(srcdir, srcs), file.path(destdir, srcs)))

    # vector src needs vector dest
    expect_error(multiupload_azure_file(cont, file.path(srcdir, srcs), "/"))
    expect_error(multidownload_azure_file(cont, srcs, destdir))
})


test_that("File wildcard multitransfer works",
{
    contname <- paste0(sample(letters, 10, TRUE), collapse="")
    cont <- create_file_share(fl, contname)

    multiupload_azure_file(cont, file.path(srcdir, "*"), "/")
    multidownload_azure_file(cont, "*", destdir2)

    expect_true(files_identical(file.path(srcdir, srcs), file.path(destdir2, srcs)))

    # wildcard src needs directory dest
    expect_error(multiupload_azure_file(cont, file.path(srcdir, "*"), srcs))
    expect_error(multidownload_azure_file(cont, "*", file.path(destdir2, srcs)))
})


test_that("File wildcard multitransfer from subdir works",
{
    contname <- paste0(sample(letters, 10, TRUE), collapse="")
    cont <- create_file_share(fl, contname)

    multiupload_azure_file(cont, file.path(srcdir, "*"), "/", recursive=TRUE)
    multidownload_azure_file(cont, "subdir/*", destdir3)

    expect_true(files_identical(file.path(srcdir, "subdir", srcs_sub), file.path(destdir3, srcs_sub)))
})


test_that("File recursive wildcard multitransfer works",
{
    contname <- paste0(sample(letters, 10, TRUE), collapse="")
    cont <- create_file_share(fl, contname)

    multiupload_azure_file(cont, file.path(srcdir, "*"), "/", recursive=TRUE)
    multidownload_azure_file(cont, "*", destdir4, recursive=TRUE)

    expect_true(files_identical(file.path(srcdir, srcs), file.path(destdir4, srcs)))

    # wildcard src needs directory dest
    expect_error(multiupload_azure_file(cont, file.path(srcdir, "*"), srcs))
    expect_error(multidownload_azure_file(cont, "*", file.path(destdir4, srcs)))
})


test_that("Default multitransfer destination works",
{
    contname <- paste0(sample(letters, 10, TRUE), collapse="")
    cont <- create_file_share(fl, contname)

    multiupload_azure_file(cont, file.path(srcdir, srcs))
    multidownload_azure_file(cont, srcs)

    expect_true(files_identical(file.path(srcdir, srcs), srcs))
    file.remove(srcs)
})


test_that("Default multitransfer destination works with wildcard src",
{
    contname <- paste0(sample(letters, 10, TRUE), collapse="")
    cont <- create_file_share(fl, contname)

    multiupload_azure_file(cont, file.path(srcdir, "subdir/*"))
    multidownload_azure_file(cont, "*")

    expect_true(files_identical(file.path(srcdir, "subdir", srcs_sub), srcs_sub))
    file.remove(srcs_sub)
})


teardown(
{
    conts <- list_file_shares(fl)
    lapply(conts, delete_file_share, confirm=FALSE)
})
