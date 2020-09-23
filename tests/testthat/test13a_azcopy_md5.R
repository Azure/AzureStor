context("Azcopy with MD5")

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

if(Sys.getenv("_R_CHECK_CRAN_INCOMING_") != "")
    skip("Azcopy tests skipped: tests being run from devtools::check")

opt_sil <- getOption("azure_storage_azcopy_silent")
options(azure_storage_azcopy_silent="TRUE")

stor <- AzureRMR::az_rm$new(tenant=tenant, app=app, password=password)$
    get_subscription(subscription)$
    get_resource_group(rgname)$
    get_storage_account(storname)

sas <- stor$get_account_sas(permissions="rwdla")
bl <- stor$get_blob_endpoint(key=NULL, sas=sas, token=NULL)

test_that("azcopy works with put-md5 and check-md5",
{
    contname <- make_name()
    cont <- create_blob_container(bl, contname)

    expect_silent(upload_blob(cont, "../resources/iris.csv", put_md5=TRUE, use_azcopy=TRUE))
    md5 <- encode_md5(file("../resources/iris.csv"))
    lst <- list_blobs(cont, info="all")
    expect_identical(lst[["Content-MD5"]], md5)

    dl_file <- file.path(tempdir(), make_name())
    expect_silent(download_blob(cont, "iris.csv", dl_file, check_md5=TRUE, use_azcopy=TRUE))
    dl_md5 <- encode_md5(file(dl_file))
    expect_identical(md5, dl_md5)
})

teardown(
{
    options(azure_storage_azcopy_silent=opt_sil)
    conts <- list_blob_containers(bl)
    lapply(conts, delete_blob_container, confirm=FALSE)
})

