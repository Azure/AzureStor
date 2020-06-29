context("Blob client interface, directories")

tenant <- Sys.getenv("AZ_TEST_TENANT_ID")
app <- Sys.getenv("AZ_TEST_APP_ID")
password <- Sys.getenv("AZ_TEST_PASSWORD")
subscription <- Sys.getenv("AZ_TEST_SUBSCRIPTION")

if(tenant == "" || app == "" || password == "" || subscription == "")
    skip("Authentication tests skipped: ARM credentials not set")

rgname <- Sys.getenv("AZ_TEST_STORAGE_RG")
storname <- Sys.getenv("AZ_TEST_STORAGE_HNS")

if(rgname == "" || storname == "")
    skip("Blob client tests skipped: resource names not set")

sub <- AzureRMR::az_rm$new(tenant=tenant, app=app, password=password)$get_subscription(subscription)
stor <- sub$get_resource_group(rgname)$get_storage_account(storname)
options(azure_storage_progress_bar=FALSE)


test_that("Blob recursive file listing works",
{
    bl <- stor$get_blob_endpoint()
    cont <- create_blob_container(bl, make_name())

    dirs <- file.path(tempdir(), c("dir1", "dir1/dir2", "dir1/dir2/dir3"))
    files <- sapply(dirs, function(d)
    {
        dir.create(d, recursive=TRUE)
        file.path(d, write_file(d))
    })
    expect_silent(upload_blob(cont, files[1], "/dir1/file1"))
    expect_silent(upload_blob(cont, files[2], "/dir1/dir2/file2"))
    expect_silent(upload_blob(cont, files[3], "/dir1/dir2/dir3/file3"))

    # this is for hierarchical namespace enabled
    l <- list_blobs(cont)
    expect_is(l, "data.frame")
    expect_identical(nrow(l), 6L)

    l0 <- list_blobs(cont, recursive=FALSE)
    expect_is(l0, "data.frame")
    expect_identical(nrow(l0), 1L)

    l0n <- list_blobs(cont, recursive=FALSE, info="name")
    expect_is(l0n, "character")
    expect_identical(l0n, "dir1/")

    l1 <- list_blobs(cont, "dir1/", recursive=FALSE)
    expect_identical(nrow(l1), 2L)
    expect_identical(l1$name, c("dir1/dir2/", "dir1/file1"))

    l1n <- list_blobs(cont, "dir1/", recursive=FALSE, info="name")
    expect_identical(l1n, c("dir1/dir2/", "dir1/file1"))

    l1rec <- list_blobs(cont, "dir1/", recursive=TRUE)
    expect_identical(nrow(l1rec), 5L)

    l1noslash <- list_blobs(cont, "dir1", recursive=FALSE)
    expect_identical(nrow(l1noslash), 2L)
    expect_identical(l1noslash$name, c("dir1/dir2/", "dir1/file1"))
})


teardown(
{
    unlink(file.path(tempdir(), "dir1"), recursive=TRUE)
    bl <- stor$get_blob_endpoint()
    conts <- list_blob_containers(bl)
    lapply(conts, delete_blob_container, confirm=FALSE)
})
