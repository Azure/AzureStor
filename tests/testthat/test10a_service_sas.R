context("Service SAS")

tenant <- Sys.getenv("AZ_TEST_TENANT_ID")
app <- Sys.getenv("AZ_TEST_APP_ID")
#cliapp <- Sys.getenv("AZ_TEST_NATIVE_APP_ID")
password <- Sys.getenv("AZ_TEST_PASSWORD")
subscription <- Sys.getenv("AZ_TEST_SUBSCRIPTION")

if(tenant == "" || app == "" || password == "" || subscription == "")
    skip("SAS tests skipped: ARM credentials not set")

rgname <- Sys.getenv("AZ_TEST_STORAGE_RG")
storname <- Sys.getenv("AZ_TEST_STORAGE_HNS")

if(rgname == "" || storname == "")
    skip("SAS tests skipped: resource names not set")

sub <- AzureRMR::az_rm$new(tenant=tenant, app=app, password=password)$get_subscription(subscription)
stor <- sub$get_resource_group(rgname)$get_storage_account(storname)
options(azure_storage_progress_bar=FALSE)

bl0 <- stor$get_blob_endpoint()
fl0 <- stor$get_file_endpoint()

test_that("Service SAS works 0",
{
    contname <- make_name()
    key <- stor$list_keys()[1]

    bsas <- get_service_sas(storname, contname, key, service="blob", permissions="rcwl", resource_type="c")
    bl <- stor$get_blob_endpoint(key=NULL, sas=bsas)
    expect_silent(cont <- create_storage_container(bl0, contname))
    cont <- storage_container(bl, contname)
    expect_silent(storage_upload(cont, "../resources/iris.csv"))

    fsas <- get_service_sas(storname, contname, key, service="file", permissions="rcwl", resource_type="s")
    fl <- stor$get_file_endpoint(key=NULL, sas=fsas)
    expect_silent(create_storage_container(fl0, contname))
    share <- storage_container(fl, contname)
    expect_silent(storage_upload(share, "../resources/iris.csv"))
})


test_that("Service SAS works 1",
{
    contname <- make_name()

    bsas <- stor$get_service_sas(resource=contname, service="blob", permissions="rcwl", resource_type="c")
    bl <- stor$get_blob_endpoint(key=NULL, sas=bsas)
    expect_silent(cont <- create_storage_container(bl0, contname))
    cont <- storage_container(bl, contname)
    expect_silent(storage_upload(cont, "../resources/iris.csv"))

    fsas <- stor$get_service_sas(resource=contname, service="file", permissions="rcwl", resource_type="s")
    fl <- stor$get_file_endpoint(key=NULL, sas=fsas)
    expect_silent(create_storage_container(fl0, contname))
    share <- storage_container(fl, contname)
    expect_silent(storage_upload(share, "../resources/iris.csv"))
})


test_that("Service SAS works 2",
{
    contname <- make_name()

    bsas <- get_service_sas(bl0, resource=contname, permissions="rcwl", resource_type="c")
    bl <- stor$get_blob_endpoint(key=NULL, sas=bsas)
    expect_silent(create_storage_container(bl0, contname))
    cont <- storage_container(bl, contname)
    expect_silent(storage_upload(cont, "../resources/iris.csv"))

    fsas <- get_service_sas(fl0, resource=contname, permissions="rcwl", resource_type="s")
    fl <- stor$get_file_endpoint(key=NULL, sas=fsas)
    expect_silent(create_storage_container(fl0, contname))
    share <- storage_container(fl, contname)
    expect_silent(storage_upload(share, "../resources/iris.csv"))
})


test_that("Service SAS works with dir resource",
{
    contname <- make_name()
    dirname <- make_name()

    bsas <- get_service_sas(bl0, resource=file.path(contname, dirname), permissions="rcwl", resource_type="d",
                            directory_depth=1)
    bl <- stor$get_blob_endpoint(key=NULL, sas=bsas)
    expect_silent(cont <- create_storage_container(bl0, contname))
    expect_silent(create_storage_dir(cont, dirname))
    cont <- storage_container(bl, contname)
    expect_silent(storage_upload(cont, "../resources/iris.csv", file.path(dirname, "iris.csv")))
})


teardown({
    bl <- stor$get_blob_endpoint()
    blconts <- list_storage_containers(bl)
    lapply(blconts, delete_storage_container, confirm=FALSE)

    fl <- stor$get_file_endpoint()
    flshares <- list_storage_containers(fl)
    lapply(flshares, delete_storage_container, confirm=FALSE)
})

