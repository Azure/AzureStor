context("Blob snapshots")

tenant <- Sys.getenv("AZ_TEST_TENANT_ID")
app <- Sys.getenv("AZ_TEST_APP_ID")
password <- Sys.getenv("AZ_TEST_PASSWORD")
subscription <- Sys.getenv("AZ_TEST_SUBSCRIPTION")

if(tenant == "" || app == "" || password == "" || subscription == "")
    skip("Authentication tests skipped: ARM credentials not set")

rgname <- Sys.getenv("AZ_TEST_STORAGE_RG")
storname <- Sys.getenv("AZ_TEST_STORAGE_NOHNS")

if(rgname == "" || storname == "")
    skip("Blob snapshot tests skipped: resource names not set")

sub <- AzureRMR::az_rm$new(tenant=tenant, app=app, password=password)$get_subscription(subscription)
stor1 <- sub$get_resource_group(rgname)$get_storage_account(storname)

bl1 <- stor1$get_blob_endpoint()

opts <- options(azure_storage_progress_bar=FALSE)

src1 <- file.path(tempdir(), write_file(tempdir()))
src2 <- file.path(tempdir(), write_file(tempdir()))

test_that("Blob versioning works",
{
    cont <- create_blob_container(bl1, make_name())

    upload_blob(cont, src1, "file1")
    expect_true(blob_exists(cont, "file1"))

    Sys.sleep(2)

    upload_blob(cont, src2, "file1")

    v <- list_blob_versions(cont, "file1")

    expect_type(v, "character")
    expect_identical(length(v), 2L)

    dest1 <- tempfile()
    download_blob(cont, "file1", dest1, version=v[1])
    expect_true(files_identical(src1, dest1))
})


teardown(
{
    options(opts)
    conts <- list_blob_containers(bl1)
    lapply(conts, delete_blob_container, confirm=FALSE)
})
