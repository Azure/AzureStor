context("File directory existence check")

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

opts <- options(azure_storage_progress_bar=FALSE)

basedir <- tempfile()
dirs <- file.path(basedir, c("dir1", "dir1/dir2", "dir1/dir2/dir3"))
files <- sapply(dirs, function(d)
{
    dir.create(d, recursive=TRUE)
    file.path(d, write_file(d))
})

test_that("Azure file direxist check works",
{
    cont <- create_file_share(fl, make_name())

    upload_azure_file(cont, files[1], "/dir1/file1", create_dir=TRUE)
    upload_azure_file(cont, files[2], "/dir1/dir2/file2", create_dir=TRUE)
    upload_azure_file(cont, files[3], "/dir1/dir2/dir3/file3", create_dir=TRUE)

    expect_true(azure_dir_exists(cont, "/"))
    expect_true(azure_dir_exists(cont, "dir1"))
    expect_true(azure_dir_exists(cont, "dir1/dir2"))

    expect_false(azure_dir_exists(cont, "dir1/file1"))
    expect_false(azure_dir_exists(cont, "dir1/dir2/file2"))
    expect_false(azure_dir_exists(cont, "badfile"))
    expect_false(azure_dir_exists(cont, "dir1/badfile"))
})


teardown(
{
    unlink(basedir, recursive=TRUE)
    options(opts)
    conts <- list_file_shares(fl)
    lapply(conts, delete_file_share, confirm=FALSE)
})
