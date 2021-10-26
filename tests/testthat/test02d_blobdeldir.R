context("Blob recursive directory delete")

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


test_that("Recursive deletion works for non-HNS account",
{
    cont_name <- paste0(sample(letters, 10, TRUE), collapse="")
    cont <- create_blob_container(bl1, cont_name)

    upload_blob(cont, "../resources/iris.csv", "iris.csv")
    upload_blob(cont, "../resources/iris.csv", "dir1/iris.csv")
    upload_blob(cont, "../resources/iris.csv", "dir1/dir2/iris.csv")
    upload_blob(cont, "../resources/iris.csv", "dir1/dir2/dir3/iris.csv")

    delete_blob_dir(cont, "dir1", recursive=TRUE, confirm=FALSE)
    expect_false(blob_exists(cont, "dir1"))
    expect_true(blob_exists(cont, "iris.csv"))

    delete_blob_dir(cont, "/", recursive=TRUE, confirm=FALSE)
    expect_false(blob_exists(cont, "iris.csv"))
})


test_that("Recursive deletion works for HNS account",
{
    cont_name <- paste0(sample(letters, 10, TRUE), collapse="")
    cont <- create_blob_container(bl2, cont_name)

    upload_blob(cont, "../resources/iris.csv", "iris.csv")
    upload_blob(cont, "../resources/iris.csv", "dir1/iris.csv")
    upload_blob(cont, "../resources/iris.csv", "dir1/dir2/iris.csv")
    upload_blob(cont, "../resources/iris.csv", "dir1/dir2/dir3/iris.csv")

    delete_blob_dir(cont, "dir1", recursive=TRUE, confirm=FALSE)
    expect_false(blob_exists(cont, "dir1"))
    expect_true(blob_exists(cont, "iris.csv"))

    delete_blob_dir(cont, "/", recursive=TRUE, confirm=FALSE)
    expect_false(blob_exists(cont, "iris.csv"))
})


teardown(
{
    options(opts)
    conts <- list_blob_containers(bl1)
    lapply(conts, delete_blob_container, confirm=FALSE)

    conts <- list_blob_containers(bl2)
    lapply(conts, delete_blob_container, confirm=FALSE)
})
