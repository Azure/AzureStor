context("File recursive directory delete")

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


test_that("Recursive directory deletion works",
{
    contname <- paste0(sample(letters, 10, TRUE), collapse="")
    cont <- create_file_share(fl, contname)

    create_azure_dir(cont, "dir1")
    create_azure_dir(cont, "dir1/dir2")
    create_azure_dir(cont, "dir1/dir2/dir3")
    upload_azure_file(cont, "../resources/iris.csv", "iris.csv")
    upload_azure_file(cont, "../resources/iris.csv", "dir1/iris.csv")
    upload_azure_file(cont, "../resources/iris.csv", "dir1/dir2/iris.csv")
    upload_azure_file(cont, "../resources/iris.csv", "dir1/dir2/dir3/iris.csv")

    delete_azure_dir(cont, "dir1", recursive=TRUE, confirm=FALSE)
    expect_false(azure_file_exists(cont, "dir1"))
    expect_true(azure_file_exists(cont, "iris.csv"))
})


teardown(
{
    conts <- list_file_shares(fl)
    lapply(conts, delete_file_share, confirm=FALSE)
})
