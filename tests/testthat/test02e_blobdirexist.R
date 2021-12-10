context("Blob directory existence check")

tenant <- Sys.getenv("AZ_TEST_TENANT_ID")
app <- Sys.getenv("AZ_TEST_APP_ID")
password <- Sys.getenv("AZ_TEST_PASSWORD")
subscription <- Sys.getenv("AZ_TEST_SUBSCRIPTION")

if(tenant == "" || app == "" || password == "" || subscription == "")
    skip("Authentication tests skipped: ARM credentials not set")

rgname <- Sys.getenv("AZ_TEST_STORAGE_RG")
storname1 <- Sys.getenv("AZ_TEST_STORAGE_NOHNS")
storname2 <- Sys.getenv("AZ_TEST_STORAGE_HNS")

if(rgname == "" || storname1 == "" || storname2 == "")
    skip("Blob recursive deletion tests skipped: resource names not set")

sub <- AzureRMR::az_rm$new(tenant=tenant, app=app, password=password)$get_subscription(subscription)
stor1 <- sub$get_resource_group(rgname)$get_storage_account(storname1)
stor2 <- sub$get_resource_group(rgname)$get_storage_account(storname2)

bl1 <- stor1$get_blob_endpoint()
bl2 <- stor2$get_blob_endpoint()

opts <- options(azure_storage_progress_bar=FALSE)

basedir <- tempfile()
dirs <- file.path(basedir, c("dir1", "dir1/dir2", "dir1/dir2/dir3"))
files <- sapply(dirs, function(d)
{
    dir.create(d, recursive=TRUE)
    file.path(d, write_file(d))
})

test_that("Blob direxist check works for non-HNS account",
{
    cont <- create_blob_container(bl1, make_name())

    upload_blob(cont, files[1], "/dir1/file1")
    upload_blob(cont, files[2], "/dir1/dir2/file2")
    upload_blob(cont, files[3], "/dir1/dir2/dir3/file3")

    expect_true(blob_dir_exists(cont, "/"))
    expect_true(blob_dir_exists(cont, "dir1"))
    expect_true(blob_dir_exists(cont, "dir1/dir2"))

    expect_false(blob_dir_exists(cont, "dir1/file1"))
    expect_false(blob_dir_exists(cont, "dir1/dir2/file2"))
    expect_false(blob_dir_exists(cont, "badfile"))
    expect_false(blob_dir_exists(cont, "dir1/badfile"))
})


test_that("Blob direxist check works for HNS account",
{
    cont <- create_blob_container(bl2, make_name())

    upload_blob(cont, files[1], "/dir1/file1")
    upload_blob(cont, files[2], "/dir1/dir2/file2")
    upload_blob(cont, files[3], "/dir1/dir2/dir3/file3")

    expect_true(blob_dir_exists(cont, "/"))
    expect_true(blob_dir_exists(cont, "dir1"))
    expect_true(blob_dir_exists(cont, "dir1/dir2"))

    expect_false(blob_dir_exists(cont, "dir1/file1"))
    expect_false(blob_dir_exists(cont, "dir1/dir2/file2"))
    expect_false(blob_dir_exists(cont, "badfile"))
    expect_false(blob_dir_exists(cont, "dir1/badfile"))
})


teardown(
{
    unlink(basedir, recursive=TRUE)
    options(opts)
    conts <- list_blob_containers(bl1)
    lapply(conts, delete_blob_container, confirm=FALSE)

    conts <- list_blob_containers(bl2)
    lapply(conts, delete_blob_container, confirm=FALSE)
})
