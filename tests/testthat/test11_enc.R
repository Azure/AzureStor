context("Filename encoding")

tenant <- Sys.getenv("AZ_TEST_TENANT_ID")
app <- Sys.getenv("AZ_TEST_APP_ID")
password <- Sys.getenv("AZ_TEST_PASSWORD")
subscription <- Sys.getenv("AZ_TEST_SUBSCRIPTION")

if(tenant == "" || app == "" || password == "" || subscription == "")
    skip("Authentication tests skipped: ARM credentials not set")

rgname <- Sys.getenv("AZ_TEST_STORAGE_RG")
storname <- Sys.getenv("AZ_TEST_STORAGE_HNS")

if(rgname == "" || storname == "")
    skip("Filename encoding tests skipped: resource names not set")

sub <- AzureRMR::az_rm$new(tenant=tenant, app=app, password=password)$get_subscription(subscription)
stor <- sub$get_resource_group(rgname)$get_storage_account(storname)
options(azure_storage_progress_bar=FALSE)

fname <- paste0(paste0(sample(letters, 10, replace=TRUE), collapse=""), "srcáé.txt")
pathname <- file.path(tempdir(), fname)  # don't use basename() because that sets encoding to UTF-8
writeLines(letters, pathname)


test_that("Blob transfer works with non-ASCII filename",
{
    bl <- stor$get_blob_endpoint()
    contname <- paste0(sample(letters, 10, TRUE), collapse="")
    cont <- create_blob_container(bl, contname)

    expect_silent(upload_blob(cont, pathname))
    expect_silent(upload_blob(cont, pathname, fname))
    expect_silent(upload_blob(cont, pathname, "srcáé2.txt"))

    destname <- paste0(paste0(sample(letters, 10, replace=TRUE), collapse=""), "destáé.txt")
    expect_silent(download_blob(cont, fname, file.path(tempdir(), destname)))
})


test_that("File transfer works with non-ASCII filename",
{
    fl <- stor$get_file_endpoint()
    sharename <- paste0(sample(letters, 10, TRUE), collapse="")
    share <- create_file_share(fl, sharename)

    expect_silent(upload_azure_file(share, pathname))
    expect_silent(upload_azure_file(share, pathname, fname))
    expect_silent(upload_azure_file(share, pathname, "srcáé2.txt"))

    destname <- paste0(paste0(sample(letters, 10, replace=TRUE), collapse=""), "destáé.txt")
    expect_silent(download_azure_file(share, fname, file.path(tempdir(), destname)))

})


test_that("ADLS2 transfer works with non-ASCII filename",
{
    ad <- stor$get_adls_endpoint()
    fsname <- paste0(sample(letters, 10, TRUE), collapse="")
    fs <- create_adls_filesystem(ad, fsname)

    expect_silent(upload_adls_file(fs, pathname))
    expect_silent(upload_adls_file(fs, pathname, fname))
    expect_silent(upload_adls_file(fs, pathname, "srcáé2.txt"))

    destname <- paste0(paste0(sample(letters, 10, replace=TRUE), collapse=""), "destáé.txt")
    expect_silent(download_adls_file(fs, fname, file.path(tempdir(), destname)))
})


teardown(
{
    ad <- stor$get_adls_endpoint()
    fss <- list_adls_filesystems(ad)
    lapply(fss, delete_adls_filesystem, confirm=FALSE)

    bl <- stor$get_blob_endpoint()
    conts <- list_blob_containers(bl)
    lapply(conts, delete_blob_container, confirm=FALSE)

    fl <- stor$get_file_endpoint()
    shares <- list_file_shares(fl)
    lapply(shares, delete_file_share, confirm=FALSE)
})
