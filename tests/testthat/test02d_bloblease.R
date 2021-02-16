context("Blob leases")

tenant <- Sys.getenv("AZ_TEST_TENANT_ID")
app <- Sys.getenv("AZ_TEST_APP_ID")
password <- Sys.getenv("AZ_TEST_PASSWORD")
subscription <- Sys.getenv("AZ_TEST_SUBSCRIPTION")

if(tenant == "" || app == "" || password == "" || subscription == "")
    skip("Authentication tests skipped: ARM credentials not set")

rgname <- Sys.getenv("AZ_TEST_STORAGE_RG")
storname <- Sys.getenv("AZ_TEST_STORAGE_NOHNS")

if(rgname == "" || storname == "")
    skip("Blob lease tests skipped: resource names not set")

sub <- AzureRMR::az_rm$new(tenant=tenant, app=app, password=password)$get_subscription(subscription)
stor <- sub$get_resource_group(rgname)$get_storage_account(storname)

bl <- stor$get_blob_endpoint()

opts <- options(azure_storage_progress_bar=FALSE)


test_that("Blob container leasing works",
{
    cont_name <- make_name(10)
    cont <- create_blob_container(bl, cont_name)

    lease1 <- acquire_lease(cont, duration=15)
    expect_type(lease1, "character")

    Sys.sleep(1)

    lease2 <- change_lease(cont, lease=lease1, new_lease=uuid::UUIDgenerate())
    expect_type(lease2, "character")

    Sys.sleep(1)
    expect_silent(renew_lease(cont, lease=lease2))
    expect_error(delete_blob_container(cont, confirm=FALSE))
    expect_silent(break_lease(cont))
    expect_error(acquire_lease(cont, duration=15))

    Sys.sleep(15)
    expect_silent(lease3 <- acquire_lease(cont, duration=15))
    expect_silent(release_lease(cont, lease=lease3))

    Sys.sleep(2)
    expect_silent(delete_blob_container(cont, confirm=FALSE))
})


test_that("Blob leasing works",
{
    cont_name <- make_name(10)
    cont <- create_blob_container(bl, cont_name)

    expect_silent(upload_blob(cont, "../resources/iris.csv"))

    blname <- "iris.csv"
    lease1 <- acquire_lease(cont, blname, duration=15)
    expect_type(lease1, "character")

    Sys.sleep(1)
    expect_error(upload_blob(cont, "../resources/iris.csv"))

    lease2 <- change_lease(cont, blname, lease=lease1, new_lease=uuid::UUIDgenerate())
    expect_type(lease2, "character")

    Sys.sleep(1)
    expect_silent(upload_blob(cont, "../resources/iris.csv", lease=lease2))
    expect_silent(renew_lease(cont, blname, lease=lease2))
    expect_error(delete_blob(cont, blname, confirm=FALSE))
    expect_silent(break_lease(cont, blname))
    expect_error(acquire_lease(cont, blname, duration=15))

    Sys.sleep(15)
    expect_silent(lease3 <- acquire_lease(cont, blname, duration=15))
    expect_silent(release_lease(cont, blname, lease=lease3))

    Sys.sleep(2)
    expect_silent(delete_blob(cont, blname, confirm=FALSE))
})


teardown(
{
    options(opts)
    conts <- list_blob_containers(bl)
    lapply(conts, delete_blob_container, confirm=FALSE)
})
