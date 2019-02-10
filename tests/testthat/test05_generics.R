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

test_that("Blob dispatch works",
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
    expect_error(create_storage_dir(cont, dirname))

    # file transfer
    expect_silent(storage_upload(cont, file.path("../resources", filename), filename))
    expect_silent(storage_download(cont, filename, tempfile()))

    # delete the objects
    expect_silent(delete_storage_file(cont, filename, confirm=FALSE))
    expect_error(delete_storage_dir(cont, dirname, confirm=FALSE))
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

    # delete the objects
    expect_silent(delete_storage_file(cont, file.path(dirname, filename), confirm=FALSE))
    expect_silent(delete_storage_dir(cont, dirname, confirm=FALSE))
    expect_silent(delete_storage_container(cont, confirm=FALSE))
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

