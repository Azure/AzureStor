context("Metadata getters/setters")

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
    skip("Metadata getter/setter tests skipped: resource names not set")

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


test_that("Blob metadata getters/setters work",
{
    expect_is(cont, "blob_container")

    meta0 <- get_storage_metadata(cont, "iris.csv")
    expect_true(is_empty(meta0))

    meta1set <- set_storage_metadata(cont, "iris.csv", name1="value1")
    meta1get <- get_storage_metadata(cont, "iris.csv")
    expect_identical(meta1set, meta1get)
    expect_identical(meta1get, list(name1="value1"))

    set_storage_metadata(cont, "iris.csv", name2="value2")
    meta2get <- get_storage_metadata(cont, "iris.csv")
    expect_identical(meta2get, c(meta1get, list(name2="value2")))

    set_storage_metadata(cont, "iris.csv", name3="value3", keep_existing=FALSE)
    meta3get <- get_storage_metadata(cont, "iris.csv")
    expect_identical(meta3get, list(name3="value3"))

    set_storage_metadata(cont, "iris.csv", keep_existing=FALSE)
    meta4get <- get_storage_metadata(cont, "iris.csv")
    expect_true(is_empty(meta4get))
})


test_that("File metadata getters/setters work",
{
    expect_is(share, "file_share")

    meta0 <- get_storage_metadata(share, "iris.csv")
    expect_true(is_empty(meta0))

    meta1set <- set_storage_metadata(share, "iris.csv", name1="value1")
    meta1get <- get_storage_metadata(share, "iris.csv")
    expect_identical(meta1set, meta1get)
    expect_identical(meta1get, list(name1="value1"))

    set_storage_metadata(share, "iris.csv", name2="value2")
    meta2get <- get_storage_metadata(share, "iris.csv")
    expect_identical(meta2get, c(meta1get, list(name2="value2")))

    set_storage_metadata(share, "iris.csv", name3="value3", keep_existing=FALSE)
    meta3get <- get_storage_metadata(share, "iris.csv")
    expect_identical(meta3get, list(name3="value3"))

    set_storage_metadata(share, "iris.csv", keep_existing=FALSE)
    meta4get <- get_storage_metadata(share, "iris.csv")
    expect_true(is_empty(meta4get))
})


test_that("File metadata getters/setters work for directory",
{
    expect_is(share, "file_share")

    meta0 <- get_storage_metadata(share, "dir")
    expect_true(is_empty(meta0))

    meta1set <- set_storage_metadata(share, "dir", name1="value1")
    meta1get <- get_storage_metadata(share, "dir")
    expect_identical(meta1set, meta1get)
    expect_identical(meta1get, list(name1="value1"))

    set_storage_metadata(share, "dir", name2="value2")
    meta2get <- get_storage_metadata(share, "dir")
    expect_identical(meta2get, c(meta1get, list(name2="value2")))

    set_storage_metadata(share, "dir", name3="value3", keep_existing=FALSE)
    meta3get <- get_storage_metadata(share, "dir")
    expect_identical(meta3get, list(name3="value3"))

    set_storage_metadata(share, "dir", keep_existing=FALSE)
    meta4get <- get_storage_metadata(share, "dir")
    expect_true(is_empty(meta4get))
})


test_that("ADLS metadata getters/setters work",
{
    expect_is(fs, "adls_filesystem")

    meta0 <- get_storage_metadata(fs, "iris.csv")
    expect_true(is_empty(meta0))

    meta1set <- set_storage_metadata(fs, "iris.csv", name1="value1")
    meta1get <- get_storage_metadata(fs, "iris.csv")
    expect_identical(meta1set, meta1get)
    expect_identical(meta1get, list(name1="value1"))

    set_storage_metadata(fs, "iris.csv", name2="value2")
    meta2get <- get_storage_metadata(fs, "iris.csv")
    expect_identical(meta2get, c(meta1get, list(name2="value2")))

    set_storage_metadata(fs, "iris.csv", name3="value3", keep_existing=FALSE)
    meta3get <- get_storage_metadata(fs, "iris.csv")
    expect_identical(meta3get, list(name3="value3"))

    set_storage_metadata(fs, "iris.csv", keep_existing=FALSE)
    meta4get <- get_storage_metadata(fs, "iris.csv")
    expect_true(is_empty(meta4get))
})


test_that("ADLS metadata getters/setters work for directory",
{
    expect_is(fs, "adls_filesystem")

    meta0 <- get_storage_metadata(fs, "dir")
    expect_true(is_empty(meta0))

    meta1set <- set_storage_metadata(fs, "dir", name1="value1")
    meta1get <- get_storage_metadata(fs, "dir")
    expect_identical(meta1set, meta1get)
    expect_identical(meta1get, list(name1="value1"))

    set_storage_metadata(fs, "dir", name2="value2")
    meta2get <- get_storage_metadata(fs, "dir")
    expect_identical(meta2get, c(meta1get, list(name2="value2")))

    set_storage_metadata(fs, "dir", name3="value3", keep_existing=FALSE)
    meta3get <- get_storage_metadata(fs, "dir")
    expect_identical(meta3get, list(name3="value3"))

    set_storage_metadata(fs, "dir", keep_existing=FALSE)
    meta4get <- get_storage_metadata(fs, "dir")
    expect_true(is_empty(meta4get))
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
