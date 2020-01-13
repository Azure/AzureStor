context("Blob client interface, extra")

tenant <- Sys.getenv("AZ_TEST_TENANT_ID")
app <- Sys.getenv("AZ_TEST_APP_ID")
cli_app <- Sys.getenv("AZ_TEST_NATIVE_APP_ID")
password <- Sys.getenv("AZ_TEST_PASSWORD")
subscription <- Sys.getenv("AZ_TEST_SUBSCRIPTION")

if(tenant == "" || app == "" || password == "" || subscription == "")
    skip("Authentication tests skipped: ARM credentials not set")

rgname <- Sys.getenv("AZ_TEST_STORAGE_RG")
storname <- Sys.getenv("AZ_TEST_STORAGE_NOHNS")

if(rgname == "" || storname == "")
    skip("Azcopy client tests skipped: resource names not set")

set_azcopy_path()
if(is.null(.AzureStor$azcopy) || is.na(.AzureStor$azcopy))
    skip("Azcopy tests skipped: not detected")

stor <- AzureRMR::az_rm$new(tenant=tenant, app=app, password=password)$
    get_subscription(subscription)$
    get_resource_group(rgname)$
    get_storage_account(storname)

token_svc <- AzureRMR::get_azure_token("https://storage.azure.com/", tenant=tenant, app=app, password=password)
token_usr <- AzureRMR::get_azure_token("https://storage.azure.com/", tenant=tenant, app=cli_app)
sas <- stor$get_account_sas(permissions="rw")
key <- stor$list_keys()[1]

bl_svc <- stor$get_blob_endpoint(key=NULL, sas=NULL, token=token_svc)
bl_usr <- stor$get_blob_endpoint(key=NULL, sas=NULL, token=token_usr)
bl_sas <- stor$get_blob_endpoint(key=NULL, sas=sas, token=NULL)
ad_key <- stor$get_adls_endpoint(key=key, sas=NULL, token=NULL)

options(azure_storage_progress_bar=FALSE)


test_that("call_azcopy works",
{
    expect_output(azc1 <- call_azcopy(ad_key))
    expect_output(azc2 <- call_azcopy(ad_key, "help"))
    expect_identical(substr(azc1$stdout, 1, 200), substr(azc2$stdout, 1, 200))
})


test_that("azcopy works with key",
{
    contname <- paste0(sample(letters, 10, TRUE), collapse="")
    cont <- create_storage_container(ad_key, "contname")
    expect_type(call_azcopy(ad_key, "list"), "list")
    storage_upload(cont, "../resources/iris.csv", "iris.csv", use_azcopy=TRUE)
    storage_download(cont, "iris.csv", tempfile(), use_azcopy=TRUE)
})


test_that("azcopy works with service token",
{
    contname <- paste0(sample(letters, 10, TRUE), collapse="")
    cont <- create_storage_container(bl_svc, "contname")
    expect_type(call_azcopy(bl_svc, "list"), "list")
    storage_upload(cont, "../resources/iris.csv", "iris.csv", use_azcopy=TRUE)
    storage_download(cont, "iris.csv", tempfile(), use_azcopy=TRUE)
})


test_that("azcopy works with user token",
{
    contname <- paste0(sample(letters, 10, TRUE), collapse="")
    cont <- create_storage_container(bl_usr, "contname")
    expect_type(call_azcopy(bl_usr, "list"), "list")
    storage_upload(cont, "../resources/iris.csv", "iris.csv", use_azcopy=TRUE)
    storage_download(cont, "iris.csv", tempfile(), use_azcopy=TRUE)
})


test_that("azcopy works with sas",
{
    contname <- paste0(sample(letters, 10, TRUE), collapse="")
    cont <- create_storage_container(bl_sas, "contname")
    expect_type(call_azcopy(bl_sas, "list"), "list")
    storage_upload(cont, "../resources/iris.csv", "iris.csv", use_azcopy=TRUE)
    storage_download(cont, "iris.csv", tempfile(), use_azcopy=TRUE)
})


teardown(
{
    conts <- list_blob_containers(bl_svc)
    lapply(conts, delete_blob_container, confirm=FALSE)
    conts <- list_adls_filesystems(ad_key)
    lapply(conts, delete_adls_filesystem, confirm=FALSE)
})
