context("Blob client interface, extra")

tenant <- Sys.getenv("AZ_TEST_TENANT_ID")
app <- Sys.getenv("AZ_TEST_APP_ID")
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

sub <- AzureRMR::az_rm$new(tenant=tenant, app=app, password=password)$get_subscription(subscription)
stor <- sub$get_resource_group(rgname)$get_storage_account(storname)
bl <- stor$get_blob_endpoint()

token <- AzureRMR::get_azure_token("https://storage.azure.com/", tenant=tenant, app=app, password=password)
bl2 <- blob_endpoint(bl$url, token=token)

options(azure_storage_progress_bar=FALSE)


test_that("call_azcopy works",
{
    expect_output(azc1 <- call_azcopy())
    expect_output(azc2 <- call_azcopy("help"))
    expect_identical(substr(azc1$stdout, 1, 200), substr(azc2$stdout, 1, 200))
})


test_that("azcopy works with key",
{
    key_env <- azcopy_key_creds(bl)
    expect_type(key_env, "character")
    expect_named(key_env)

    call_azcopy("list", bl$url, env=key_env)
})


test_that("azcopy works with token",
{
    tok_env <- azcopy_token_creds(bl2)
    expect_type(tok_env, "character")
    expect_named(tok_env)
    conts <- jsonlite::fromJSON(tok_env)
    print(conts)
    expect_false(is.null(conts$access_token))
    expect_false(is.null(conts$refresh_token))

    call_azcopy("list", bl2$url, env=tok_env)
})


teardown(
{
    conts <- list_blob_containers(bl)
    lapply(conts, delete_blob_container, confirm=FALSE)
})
