context("ADLSgen2 directory delete")

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

ad1 <- stor1$get_adls_endpoint()
ad2 <- stor2$get_adls_endpoint()

opts <- options(azure_storage_progress_bar=FALSE)


test_that("Recursive deletion works for non-HNS account",
{
    cont_name <- paste0(sample(letters, 10, TRUE), collapse="")
    cont <- create_adls_filesystem(ad1, cont_name)

    upload_adls_file(cont, "../resources/iris.csv", "iris.csv")
    upload_adls_file(cont, "../resources/iris.csv", "dir1/iris.csv")
    upload_adls_file(cont, "../resources/iris.csv", "dir1/dir2/iris.csv")
    upload_adls_file(cont, "../resources/iris.csv", "dir1/dir2/dir3/iris.csv")

    delete_adls_dir(cont, "dir1", recursive=TRUE, confirm=FALSE)
    expect_false(adls_file_exists(cont, "dir1"))
    expect_true(adls_file_exists(cont, "iris.csv"))

    delete_adls_dir(cont, "/", recursive=TRUE, confirm=FALSE)
    expect_false(adls_file_exists(cont, "iris.csv"))
})


test_that("Recursive deletion works for HNS account",
{
    cont_name <- paste0(sample(letters, 10, TRUE), collapse="")
    cont <- create_adls_filesystem(ad2, cont_name)

    upload_adls_file(cont, "../resources/iris.csv", "iris.csv")
    upload_adls_file(cont, "../resources/iris.csv", "dir1/iris.csv")
    upload_adls_file(cont, "../resources/iris.csv", "dir1/dir2/iris.csv")
    upload_adls_file(cont, "../resources/iris.csv", "dir1/dir2/dir3/iris.csv")

    delete_adls_dir(cont, "dir1", recursive=TRUE, confirm=FALSE)
    expect_false(adls_file_exists(cont, "dir1"))
    expect_true(adls_file_exists(cont, "iris.csv"))

    delete_adls_dir(cont, "/", recursive=TRUE, confirm=FALSE)
    expect_false(adls_file_exists(cont, "iris.csv"))
})


teardown(
{
    options(opts)
    conts <- list_adls_filesystems(ad1)
    lapply(conts, delete_adls_filesystem, confirm=FALSE)

    conts <- list_adls_filesystems(ad2)
    lapply(conts, delete_adls_filesystem, confirm=FALSE)
})

