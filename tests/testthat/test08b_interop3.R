context("Blob/ADLS interop 3")

tenant <- Sys.getenv("AZ_TEST_TENANT_ID")
app <- Sys.getenv("AZ_TEST_APP_ID")
password <- Sys.getenv("AZ_TEST_PASSWORD")
subscription <- Sys.getenv("AZ_TEST_SUBSCRIPTION")

if(tenant == "" || app == "" || password == "" || subscription == "")
    skip("Authentication tests skipped: ARM credentials not set")

rgname <- Sys.getenv("AZ_TEST_STORAGE_RG")
storname <- Sys.getenv("AZ_TEST_STORAGE_HNS")

if(rgname == "" || storname == "")
    skip("Blob/ADLS interop tests skipped: resource names not set")

sub <- AzureRMR::az_rm$new(tenant=tenant, app=app, password=password)$get_subscription(subscription)
stor <- sub$get_resource_group(rgname)$get_storage_account(storname)

ad <- stor$get_adls_endpoint()
bl <- stor$get_blob_endpoint()

options(azure_storage_progress_bar=FALSE)


test_that("Blob and ADLS mixed updating works on blob container in HNS-enabled account",
{
    cont_name <- paste0(sample(letters, 10, TRUE), collapse="")
    cont <- create_blob_container(bl, cont_name)
    fs_from_cont <- adls_filesystem(ad, cont_name)

    upload_blob(cont, "../resources/iris.csv", "blobfoo/bar/iris.csv")
    upload_adls_file(fs_from_cont, "../resources/iris.csv", "adlsfoo/bar/iris.csv")

    bloblst <- list_blobs(cont, info="all")
    expect_is(bloblst, "data.frame")
    expect_true(bloblst$isdir[bloblst$name == "adlsfoo"])
    expect_true(bloblst$isdir[bloblst$name == "adlsfoo/bar"])

    adlslst <- list_adls_files(fs_from_cont, recursive=TRUE)
    expect_is(adlslst, "data.frame")
    expect_true(adlslst$isdir[bloblst$name == "adlsfoo"])
    expect_true(adlslst$isdir[bloblst$name == "adlsfoo/bar"])

    destdir <- tempfile(pattern="dl")
    expect_silent(multidownload_blob(cont, "*", destdir, recursive=TRUE))
    expect_true(file.size(file.path(destdir, "adlsfoo/bar/iris.csv")) > 0)
    expect_true(file.size(file.path(destdir, "blobfoo/bar/iris.csv")) > 0)

    destdir2 <- tempfile(pattern="dl")
    expect_silent(multidownload_adls_file(fs_from_cont, "*", destdir2, recursive=TRUE))
    expect_true(file.size(file.path(destdir2, "adlsfoo/bar/iris.csv")) > 0)
    expect_true(file.size(file.path(destdir2, "blobfoo/bar/iris.csv")) > 0)
})


test_that("Blob and ADLS mixed updating works on ADLS filesystem in HNS-enabled account",
{
    cont_name <- paste0(sample(letters, 10, TRUE), collapse="")
    fs <- create_adls_filesystem(ad, cont_name)
    cont_from_fs <- blob_container(bl, cont_name)

    upload_blob(cont_from_fs, "../resources/iris.csv", "blobfoo/bar/iris.csv")
    upload_adls_file(fs, "../resources/iris.csv", "adlsfoo/bar/iris.csv")

    bloblst <- list_blobs(cont_from_fs, info="all")
    expect_is(bloblst, "data.frame")
    expect_true(bloblst$isdir[bloblst$name == "adlsfoo"])
    expect_true(bloblst$isdir[bloblst$name == "adlsfoo/bar"])

    adlslst <- list_adls_files(fs, recursive=TRUE)
    expect_is(adlslst, "data.frame")
    expect_true(adlslst$isdir[bloblst$name == "adlsfoo"])
    expect_true(adlslst$isdir[bloblst$name == "adlsfoo/bar"])

    destdir <- tempfile(pattern="dl")
    expect_silent(multidownload_blob(cont_from_fs, "*", destdir, recursive=TRUE))
    expect_true(file.size(file.path(destdir, "adlsfoo/bar/iris.csv")) > 0)
    expect_true(file.size(file.path(destdir, "blobfoo/bar/iris.csv")) > 0)

    destdir2 <- tempfile(pattern="dl")
    expect_silent(multidownload_adls_file(fs, "*", destdir2, recursive=TRUE))
    expect_true(file.size(file.path(destdir2, "adlsfoo/bar/iris.csv")) > 0)
    expect_true(file.size(file.path(destdir2, "blobfoo/bar/iris.csv")) > 0)
})


teardown(
{
    conts <- list_adls_filesystems(ad)
    lapply(conts, delete_adls_filesystem, confirm=FALSE)
})
