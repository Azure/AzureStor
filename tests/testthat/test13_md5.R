context("MD5 hashes")

tenant <- Sys.getenv("AZ_TEST_TENANT_ID")
app <- Sys.getenv("AZ_TEST_APP_ID")
password <- Sys.getenv("AZ_TEST_PASSWORD")
subscription <- Sys.getenv("AZ_TEST_SUBSCRIPTION")

if(tenant == "" || app == "" || password == "" || subscription == "")
    skip("Authentication tests skipped: ARM credentials not set")

rgname <- Sys.getenv("AZ_TEST_STORAGE_RG")
storname <- Sys.getenv("AZ_TEST_STORAGE_HNS")

if(rgname == "" || storname == "")
    skip("MD5 tests skipped: resource names not set")

sub <- AzureRMR::az_rm$new(tenant=tenant, app=app, password=password)$get_subscription(subscription)
stor <- sub$get_resource_group(rgname)$get_storage_account(storname)
bl <- stor$get_blob_endpoint()
ad <- stor$get_adls_endpoint()
opts <- options(azure_storage_progress_bar=FALSE)


test_that("Blob upload works with MD5 hash",
{
    contname <- make_name()
    cont <- create_blob_container(bl, contname)

    expect_silent(upload_blob(cont, "../resources/iris.csv"))
    lst <- list_blobs(cont, info="all")
    expect_true(all(is.na(lst[["Content-MD5"]])))

    expect_silent(upload_blob(cont, "../resources/iris.csv", put_md5=TRUE))
    md5 <- encode_md5(file("../resources/iris.csv"))
    lst <- list_blobs(cont, info="all")
    expect_identical(lst[["Content-MD5"]], md5)
})


test_that("ADLS upload works with MD5 hash",
{
    contname <- make_name()
    fs <- create_adls_filesystem(ad, contname)

    expect_silent(upload_adls_file(fs, "../resources/iris.csv"))
    props <- get_storage_properties(fs, "iris.csv")
    expect_null(props$`content-md5`)

    expect_silent(upload_adls_file(fs, "../resources/iris.csv", put_md5=TRUE))
    md5 <- encode_md5(file("../resources/iris.csv"))
    props <- get_storage_properties(fs, "iris.csv")
    expect_identical(props$`content-md5`, md5)
})


teardown(
{
    conts <- list_blob_containers(bl)
    lapply(conts, delete_blob_container, confirm=FALSE)
    fss <- list_adls_filesystems(ad)
    lapply(fss, delete_adls_filesystem, confirm=FALSE)
    options(opts)
})
