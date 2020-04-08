context("Account and user SAS")

tenant <- Sys.getenv("AZ_TEST_TENANT_ID")
app <- Sys.getenv("AZ_TEST_APP_ID")
cliapp <- Sys.getenv("AZ_TEST_NATIVE_APP_ID")
password <- Sys.getenv("AZ_TEST_PASSWORD")
subscription <- Sys.getenv("AZ_TEST_SUBSCRIPTION")

if(tenant == "" || app == "" || password == "" || subscription == "" || cliapp == "")
    skip("SAS tests skipped: ARM credentials not set")

rgname <- Sys.getenv("AZ_TEST_STORAGE_RG")
storname <- Sys.getenv("AZ_TEST_STORAGE_HNS")

if(rgname == "" || storname == "")
    skip("SAS tests skipped: resource names not set")

sub <- AzureRMR::az_rm$new(tenant=tenant, app=app, password=password)$get_subscription(subscription)
stor <- sub$get_resource_group(rgname)$get_storage_account(storname)
options(azure_storage_progress_bar=FALSE)

dates <- c(Sys.Date() - 1, Sys.Date() + 5)
token <- AzureAuth::get_azure_token("https://storage.azure.com", tenant, app=cliapp)

test_that("Account SAS works 0",
{
    key <- stor$list_keys()[1]
    sas <- get_account_sas(storname, key, permissions="rwlc")
    bl <- stor$get_blob_endpoint(key=NULL, sas=sas)
    expect_silent(list_storage_containers(bl))
    expect_silent(cont <- create_storage_container(bl, make_name()))
    expect_silent(storage_upload(cont, "../resources/iris.csv"))
})


test_that("Account SAS works 1",
{
    sas <- stor$get_account_sas(permissions="rwlc")
    expect_type(sas, "character")
    bl <- stor$get_blob_endpoint(key=NULL, sas=sas)
    expect_silent(list_storage_containers(bl))
    expect_silent(cont <- create_storage_container(bl, make_name()))
    expect_silent(storage_upload(cont, "../resources/iris.csv"))
})


test_that("Account SAS works 2",
{
    bl0 <- stor$get_blob_endpoint(key=NULL, sas=NULL, token=NULL)
    expect_error(get_account_sas(bl0, permissions="rwlc"))

    bl0 <- stor$get_blob_endpoint()
    sas <- get_account_sas(bl0, permissions="rwlc")
    bl <- stor$get_blob_endpoint(key=NULL, sas=sas)
    expect_silent(list_storage_containers(bl))
    expect_silent(cont <- create_storage_container(bl, make_name()))
    expect_silent(storage_upload(cont, "../resources/iris.csv"))
})


test_that("User delegation key works 1",
{
    ukey <- stor$get_user_delegation_key(key_start=dates[1], key_expiry=dates[2], token=token)
    expect_is(ukey, "user_delegation_key")
    expect_type(ukey$Value, "character")
})


test_that("User delegation key works 2",
{
    bl <- stor$get_blob_endpoint(key=NULL, token=token)
    ukey <- get_user_delegation_key(bl, key_start=dates[1], key_expiry=dates[2])
    expect_is(ukey, "user_delegation_key")
    expect_type(ukey$Value, "character")
})


test_that("User delegation SAS works 1",
{
    contname <- make_name()
    bl0 <- stor$get_blob_endpoint(key=NULL, token=token)
    expect_silent(create_storage_container(bl0, contname))
    ukey <- get_user_delegation_key(bl0, key_start=dates[1], key_expiry=dates[2])

    usas <- get_user_delegation_sas(bl0, ukey, resource=contname, start=dates[1], expiry=dates[2], permissions="rcwl")
    expect_type(usas, "character")

    Sys.sleep(30)

    bl <- stor$get_blob_endpoint(key=NULL, sas=usas)
    cont <- storage_container(bl, contname)
    expect_silent(storage_upload(cont, "../resources/iris.csv"))
})


test_that("User delegation SAS works 2",
{
    contname <- make_name()
    bl0 <- stor$get_blob_endpoint(key=NULL, token=token)
    expect_silent(create_storage_container(bl0, contname))
    ukey <- get_user_delegation_key(bl0, key_start=dates[1], key_expiry=dates[2])

    usas <- get_user_delegation_sas(storname, ukey, resource=contname, start=dates[1], expiry=dates[2],
                                    permissions="rcwl")
    expect_type(usas, "character")

    Sys.sleep(30)

    bl <- stor$get_blob_endpoint(key=NULL, sas=usas)
    cont <- storage_container(bl, contname)
    expect_silent(storage_upload(cont, "../resources/iris.csv"))
})


teardown({
    stor$revoke_user_delegation_keys()
    bl <- stor$get_blob_endpoint()
    blconts <- list_storage_containers(bl)
    lapply(blconts, delete_storage_container, confirm=FALSE)
})

