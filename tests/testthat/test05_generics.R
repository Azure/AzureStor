context("Client generics")

tenant <- Sys.getenv("AZ_TEST_TENANT_ID")
app <- Sys.getenv("AZ_TEST_APP_ID")
password <- Sys.getenv("AZ_TEST_PASSWORD")
subscription <- Sys.getenv("AZ_TEST_SUBSCRIPTION")

if(tenant == "" || app == "" || password == "" || subscription == "")
    skip("Authentication tests skipped: ARM credentials not set")

rgname <- Sys.getenv("AZ_TEST_STORAGE_RG")
storname1 <- Sys.getenv("AZ_TEST_STORAGE_HNS")
storname2 <- Sys.getenv("AZ_TEST_STORAGE_NOHNS")

if(rgname == "" || storname1 == "" || storname2 == "")
    skip("Blob client tests skipped: resource names not set")

sub <- AzureRMR::az_rm$new(tenant=tenant, app=app, password=password)$get_subscription(subscription)
stor1 <- sub$get_resource_group(rgname)$get_storage_account(storname1)
stor2 <- sub$get_resource_group(rgname)$get_storage_account(storname2)

options(azure_storage_progress_bar=FALSE)


test_that("Blob dispatch works, HNS",
{
    endpname <- stor1$properties$primaryEndpoints$blob
    expect_type(endpname, "character")
    key <- stor1$list_keys()[[1]]

    contname <- paste(sample(letters, 10, TRUE), collapse="")
    dirname <- "newdir"
    filename <- "iris.csv"

    # working with a container
    expect_is(endp <- storage_endpoint(endpname, key=key), "blob_endpoint")
    expect_silent(cont <- storage_container(endp, contname))
    expect_silent(create_storage_container(cont))

    # working with objects within container
    expect_silent(list_storage_files(cont))
    expect_silent(create_storage_dir(cont, dirname))

    # file transfer
    expect_silent(storage_upload(cont, file.path("../resources", filename), filename))
    expect_silent(storage_download(cont, filename, tempfile()))

    # file existence
    expect_false(storage_file_exists(cont, "nonexistent"))
    expect_true(storage_file_exists(cont, filename))

    # delete the objects
    expect_silent(delete_storage_file(cont, filename, confirm=FALSE))
    expect_silent(delete_storage_dir(cont, dirname, confirm=FALSE))
    expect_silent(delete_storage_container(cont, confirm=FALSE))
})


test_that("Blob dispatch works, no HNS",
{
    endpname <- stor2$properties$primaryEndpoints$blob
    expect_type(endpname, "character")
    key <- stor2$list_keys()[[1]]

    contname <- paste(sample(letters, 10, TRUE), collapse="")
    dirname <- "newdir"
    filename <- "iris.csv"

    # working with a container
    expect_is(endp <- storage_endpoint(endpname, key=key), "blob_endpoint")
    expect_silent(cont <- storage_container(endp, contname))
    expect_silent(create_storage_container(cont))

    # working with objects within container
    expect_silent(list_storage_files(cont))
    expect_silent(create_storage_dir(cont, dirname))

    # file transfer
    expect_silent(storage_upload(cont, file.path("../resources", filename), filename))
    expect_silent(storage_download(cont, filename, tempfile()))

    # file existence
    expect_false(storage_file_exists(cont, "nonexistent"))
    expect_true(storage_file_exists(cont, filename))

    # delete the objects
    expect_silent(delete_storage_file(cont, filename, confirm=FALSE))
    expect_error(delete_storage_dir(cont, dirname, confirm=FALSE)) # deleting object also deletes dir
    expect_silent(delete_storage_container(cont, confirm=FALSE))
})


test_that("File dispatch works",
{
    endpname <- stor2$properties$primaryEndpoints$file
    expect_type(endpname, "character")
    key <- stor2$list_keys()[[1]]

    contname <- paste(sample(letters, 10, TRUE), collapse="")
    dirname <- "newdir"
    filename <- "iris.csv"

    # working with a container
    expect_is(endp <- storage_endpoint(endpname, key=key), "file_endpoint")
    expect_silent(cont <- storage_container(endp, contname))
    expect_silent(create_storage_container(cont))

    # working with objects within container
    expect_silent(create_storage_dir(cont, dirname))
    expect_silent(list_storage_files(cont, dirname))

    # file transfer
    expect_silent(storage_upload(cont, file.path("../resources", filename), file.path(dirname, filename)))
    expect_silent(storage_download(cont, file.path(dirname, filename), tempfile()))

    # file existence
    expect_false(storage_file_exists(cont, "nonexistent"))
    expect_true(storage_file_exists(cont, file.path(dirname, filename)))

    # delete the objects
    expect_silent(delete_storage_file(cont, file.path(dirname, filename), confirm=FALSE))
    expect_silent(delete_storage_dir(cont, dirname, confirm=FALSE))
    expect_silent(delete_storage_container(cont, confirm=FALSE))
})


test_that("ADLSgen2 dispatch works",
{
    endpname <- stor1$properties$primaryEndpoints$dfs
    expect_type(endpname, "character")
    key <- stor1$list_keys()[[1]]

    contname <- paste(sample(letters, 10, TRUE), collapse="")
    dirname <- "newdir"
    filename <- "iris.csv"

    # working with a container
    expect_is(endp <- storage_endpoint(endpname, key=key), "adls_endpoint")
    expect_silent(cont <- storage_container(endp, contname))
    expect_silent(create_storage_container(cont))

    # working with objects within container
    expect_silent(create_storage_dir(cont, dirname))
    expect_silent(list_storage_files(cont, dirname))

    # file transfer
    expect_silent(storage_upload(cont, file.path("../resources", filename), file.path(dirname, filename)))
    expect_silent(storage_download(cont, file.path(dirname, filename), tempfile()))

    # file existence
    expect_false(storage_file_exists(cont, "nonexistent"))
    expect_true(storage_file_exists(cont, file.path(dirname, filename)))

    # delete the objects
    expect_silent(delete_storage_file(cont, file.path(dirname, filename), confirm=FALSE))
    expect_silent(delete_storage_dir(cont, dirname, confirm=FALSE))
    expect_silent(delete_storage_container(cont, confirm=FALSE))
})


test_that("Blob copy from URL works",
{
    bl <- stor2$get_blob_endpoint()
    contname <- paste0(sample(letters, 10, TRUE), collapse="")
    cont <- create_blob_container(bl, contname)

    # copy from GitHub repo
    src_url <- "https://raw.githubusercontent.com/AzureRSDK/AzureStor/master/tests/resources/iris.csv"
    orig_file <- tempfile()
    new_file <- tempfile()

    copy_url_to_storage(cont, src_url, "iris.csv", async=FALSE)
    storage_download(cont, "iris.csv", new_file)
    download.file(src_url, orig_file, mode="wb", quiet=TRUE)

    expect_true(files_identical(orig_file, new_file))

    fnames <- c("LICENSE", "DESCRIPTION", "NAMESPACE")
    src_urls <- paste0("https://raw.githubusercontent.com/AzureRSDK/AzureStor/master/", fnames)
    origs <- replicate(3, tempfile())
    dests <- replicate(3, tempfile())

    multicopy_url_to_storage(cont, src_urls, fnames, async=FALSE)
    storage_multidownload(cont, fnames, dests)
    pool_map(download.file, src_urls, origs, mode="wb")

    expect_true(files_identical(origs, dests))
})


teardown(
{
    bl <- stor2$get_blob_endpoint()
    blconts <- list_blob_containers(bl)
    lapply(blconts, delete_blob_container, confirm=FALSE)

    fl <- stor2$get_file_endpoint()
    flconts <- list_file_shares(fl)
    lapply(flconts, delete_file_share, confirm=FALSE)

    ad <- stor1$get_adls_endpoint()
    adconts <- list_adls_filesystems(ad)
    lapply(adconts, delete_adls_filesystem, confirm=FALSE)
})

