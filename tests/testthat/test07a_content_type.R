context("Content-type set on upload")

tenant <- Sys.getenv("AZ_TEST_TENANT_ID")
app <- Sys.getenv("AZ_TEST_APP_ID")
password <- Sys.getenv("AZ_TEST_PASSWORD")
subscription <- Sys.getenv("AZ_TEST_SUBSCRIPTION")

if(tenant == "" || app == "" || password == "" || subscription == "")
    skip("Authentication tests skipped: ARM credentials not set")

rgname <- Sys.getenv("AZ_TEST_STORAGE_RG")
storname <- Sys.getenv("AZ_TEST_STORAGE_HNS")

if(rgname == "" || storname == "")
    skip("Content-type upload tests skipped: resource names not set")

sub <- AzureRMR::az_rm$new(tenant=tenant, app=app, password=password)$get_subscription(subscription)
stor <- sub$get_resource_group(rgname)$get_storage_account(storname)
options(azure_storage_progress_bar=FALSE)

ad <- stor$get_adls_endpoint()
bl <- stor$get_blob_endpoint()
fl <- stor$get_file_endpoint()

test_that("ADLSgen2 content-type setting works",
{
    fsname <- paste0(sample(letters, 20, TRUE), collapse="")
    fs <- create_adls_filesystem(ad, fsname)

    upload_adls_file(fs, "../resources/iris.csv")
    props <- get_storage_properties(fs, "iris.csv")
    expect_identical(props[["content-type"]], "text/csv")
})


test_that("Blob content-type setting works",
{
    contname <- paste0(sample(letters, 20, TRUE), collapse="")
    cont <- create_blob_container(bl, contname)

    upload_blob(cont, "../resources/iris.csv")
    props <- get_storage_properties(cont, "iris.csv")
    expect_identical(props[["content-type"]], "text/csv")
})


test_that("File content-type setting works",
{
    sharename <- paste0(sample(letters, 20, TRUE), collapse="")
    share <- create_file_share(fl, sharename)

    upload_azure_file(share, "../resources/iris.csv")
    props <- get_storage_properties(share, "iris.csv")
    expect_identical(props[["content-type"]], "text/csv")
})


teardown({
    blconts <- list_storage_containers(bl)
    lapply(blconts, delete_storage_container, confirm=FALSE)

    flconts <- list_storage_containers(fl)
    lapply(flconts, delete_storage_container, confirm=FALSE)

    adconts <- list_storage_containers(ad)
    lapply(adconts, delete_storage_container, confirm=FALSE)
})
