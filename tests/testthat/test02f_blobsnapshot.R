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

test_that("Blob snapshotting works",
{
    cont <- create_blob_container(bl1, make_name())

    upload_blob(cont, src1, "file1")
    expect_true(blob_exists(cont, "file1"))

    set_storage_metadata(cont, "file1", tag1="value1")
    expect_identical(get_storage_metadata(cont, "file1"), list(tag1="value1"))

    sn1 <- create_storage_snapshot(cont, "file1")
    expect_type(sn1, "character")
    expect_identical(get_storage_metadata(cont, "file1", snapshot=sn1), list(tag1="value1"))

    expect_type(get_storage_properties(cont, "file1", snapshot=sn1), "list")

    Sys.sleep(2)

    upload_blob(cont, src2, "file1")

    sn2 <- create_storage_snapshot(cont, "file1", tag2="value2")
    expect_type(sn2, "character")
    expect_identical(get_storage_metadata(cont, "file1", snapshot=sn2), list(tag2="value2"))

    sns <- list_storage_snapshots(cont, "file1")
    expect_identical(length(sns), 2L)
    expect_identical(sns[1], sn1)
    expect_identical(sns[2], sn2)

    dest0 <- tempfile()
    download_blob(cont, "file1", dest0)
    expect_true(files_identical(src2, dest0))

    dest1 <- tempfile()
    download_blob(cont, "file1", dest1, snapshot=sn1)
    expect_true(files_identical(src1, dest1))

    dest2 <- tempfile()
    download_blob(cont, "file1", dest2, snapshot=sn2)
    expect_true(files_identical(src2, dest2))

    delete_storage_snapshot(cont, "file1", sn1, confirm=FALSE)
    expect_identical(length(list_storage_snapshots(cont, "file1")), 1L)

    expect_silent(delete_blob(cont, "file1", confirm=FALSE))
})


teardown(
{
    options(opts)
    conts <- list_blob_containers(bl1)
    lapply(conts, delete_blob_container, confirm=FALSE)
})
