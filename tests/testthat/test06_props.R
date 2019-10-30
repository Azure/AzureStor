context("Property getters")

tenant <- Sys.getenv("AZ_TEST_TENANT_ID")
app <- Sys.getenv("AZ_TEST_APP_ID")
password <- Sys.getenv("AZ_TEST_PASSWORD")
subscription <- Sys.getenv("AZ_TEST_SUBSCRIPTION")

if(tenant == "" || app == "" || password == "" || subscription == "")
    skip("ARM credentials not set")

rgname <- Sys.getenv("AZ_TEST_STORAGE_RG")
stornamenohns <- Sys.getenv("AZ_TEST_STORAGE_NOHNS")
stornamehns <- Sys.getenv("AZ_TEST_STORAGE_HNS")

if(rgname == "" || stornamenohns == "" || stornamehns == "")
    skip("Property getter tests skipped: resource names not set")

sub <- AzureRMR::az_rm$new(tenant=tenant, app=app, password=password)$get_subscription(subscription)
stornohns <- sub$get_resource_group(rgname)$get_storage_account(stornamenohns)
storhns <- sub$get_resource_group(rgname)$get_storage_account(stornamehns)

options(azure_storage_progress_bar=FALSE)

bl <- stornohns$get_blob_endpoint()
cont <- create_storage_container(bl, paste0(sample(letters, 10, TRUE), collapse=""))
storage_upload(cont, "../resources/iris.csv", "iris.csv")

fl <- stornohns$get_file_endpoint()
share <- create_storage_container(fl, paste0(sample(letters, 10, TRUE), collapse=""))
create_storage_dir(share, "dir")
storage_upload(share, "../resources/iris.csv", "iris.csv")

ad <- storhns$get_adls_endpoint()
fs <- create_storage_container(ad, paste0(sample(letters, 10, TRUE), collapse=""))
create_storage_dir(fs, "dir")
storage_upload(fs, "../resources/iris.csv", "iris.csv")


test_that("Blob property getters work",
{
    expect_is(cont, "blob_container")

    prop1 <- get_storage_properties(bl)
    expect_true(is.list(prop1) && !is_empty(prop1))

    prop2 <- get_storage_properties(cont)
    expect_true(is.list(prop2) && !is_empty(prop2))

    prop3 <- get_storage_properties(cont, "iris.csv")
    expect_true(is.list(prop3) && !is_empty(prop3))
})


test_that("File property getters work",
{
    expect_is(share, "file_share")

    prop1 <- get_storage_properties(fl)
    expect_true(is.list(prop1) && !is_empty(prop1))

    prop2 <- get_storage_properties(share)
    expect_true(is.list(prop2) && !is_empty(prop2))

    prop3 <- get_storage_properties(share, "iris.csv")
    expect_true(is.list(prop3) && !is_empty(prop3))

    prop4 <- get_storage_properties(share, "dir")
    expect_true(is.list(prop4) && !is_empty(prop4))
})


test_that("ADLS property getters work",
{
    expect_is(fs, "adls_filesystem")

    # no filesystem method for ADLS
    expect_error(get_storage_properties(ad))

    prop2 <- get_storage_properties(fs)
    expect_true(is.list(prop2) && !is_empty(prop2))

    prop3 <- get_storage_properties(fs, "iris.csv")
    expect_true(is.list(prop3) && !is_empty(prop3))

    prop4 <- get_storage_properties(fs, "dir")
    expect_true(is.list(prop4) && !is_empty(prop4))

    # ACLs
    expect_type(get_adls_file_acl(fs, "iris.csv"), "character")

    # status
    stat <- get_adls_file_status(fs, "iris.csv")
    expect_true(is.list(stat) && !is_empty(stat))
})


teardown(
{
    blconts <- list_storage_containers(bl)
    lapply(blconts, delete_storage_container, confirm=FALSE)

    flconts <- list_storage_containers(fl)
    lapply(flconts, delete_storage_container, confirm=FALSE)

    adconts <- list_storage_containers(ad)
    lapply(adconts, delete_storage_container, confirm=FALSE)
})
