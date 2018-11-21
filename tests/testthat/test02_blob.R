context("Blob client interface")

tenant <- Sys.getenv("AZ_TEST_TENANT_ID")
app <- Sys.getenv("AZ_TEST_APP_ID")
password <- Sys.getenv("AZ_TEST_PASSWORD")
subscription <- Sys.getenv("AZ_TEST_SUBSCRIPTION")

if(tenant == "" || app == "" || password == "" || subscription == "")
    skip("Authentication tests skipped: ARM credentials not set")


sub <- az_rm$new(tenant=tenant, app=app, password=password)$get_subscription(subscription)
rgname <- paste(sample(letters, 20, replace=TRUE), collapse="")
rg <- sub$create_resource_group(rgname, location="australiaeast")

test_that("Blob client interface works",
{

    storname <- paste(sample(letters, 20, replace=TRUE), collapse="")
    stor <- rg$create_storage_account(storname)

    # wait until provisioning is complete
    for(i in 1:100)
    {
        Sys.sleep(5)
        state <- stor$sync_fields()
        if(state == "Succeeded")
            break
    }
    if(state != "Succeeded")
        stop("Unable to create storage account")

    bl <- stor$get_blob_endpoint()
    bl2 <- blob_endpoint(stor$properties$primaryEndpoints$blob, key=stor$list_keys()[1])
    expect_is(bl, "blob_endpoint")
    expect_identical(bl, bl2)

    expect_true(is_empty(list_blob_containers(bl)))

    # listing blobs in a nonexistent container
    expect_error(list_blobs(blob_container(bl, "newcontainer")))

    # ways of creating a container
    cont <- blob_container(bl, "newcontainer1")
    create_blob_container(cont)
    create_blob_container(bl, "newcontainer2", public_access="container")
    create_blob_container(paste0(bl$url, "newcontainer3"), key=bl$key)

    lst <- list_blob_containers(bl)
    expect_true(is.list(lst) && inherits(lst[[1]], "blob_container") && length(lst) == 3)

    expect_identical(cont, lst[["newcontainer1"]])

    expect_true(is_empty(list_blobs(cont)))
    orig_file <- "../resources/iris.csv"
    new_file <- file.path(tempdir(), "iris.csv")
    upload_blob(cont, orig_file, "iris.csv", blocksize=1000) # small blocksize to force blocked upload

    expect_is(list_blobs(cont), "data.frame")
    expect_is(list_blobs(cont, info="name"), "character")

    # download with and without overwrite
    suppressWarnings(file.remove(new_file))
    download_blob(cont, "iris.csv", new_file)
    expect_error(download_blob(cont, "iris.csv", new_file, overwrite=FALSE))
    writeLines("foo", new_file)
    expect_silent(download_blob(cont, "iris.csv", new_file, overwrite=TRUE))
    expect_identical(readBin(orig_file, "raw", n=1e5), readBin(new_file, "raw", n=1e5))

    # public container upload/download
    public_cont <- lst[["newcontainer2"]]
    upload_blob(public_cont, orig_file, "iris_public.csv")
    public_url <- paste0(public_cont$endpoint$url, "newcontainer2/iris_public.csv")
    public_dl <- file.path(tempdir(), "iris_public.csv")
    suppressWarnings(file.remove(public_dl))
    download_from_url(public_url, public_dl)
    expect_identical(readBin(orig_file, "raw", n=1e5), readBin(public_dl, "raw", n=1e5))
    suppressWarnings(file.remove(public_dl))
    download.file(public_url, public_dl, mode="wb", quiet=TRUE)
    expect_identical(readBin(orig_file, "raw", n=1e5), readBin(public_dl, "raw", n=1e5))

    # exact-100k upload: check content-size header works
    file_100k <- "../resources/100k.out"
    upload_blob(cont, file_100k, "100k_single.out")
    upload_blob(cont, file_100k, "100k_blocked.out", blocksize=1e4)
    single_dl <- file.path(tempdir(), "100k_single.out")
    blocked_dl <- file.path(tempdir(), "100k_blocked.out")
    suppressWarnings(file.remove(single_dl, blocked_dl))
    download_blob(cont, "100k_single.out", single_dl)
    download_blob(cont, "100k_blocked.out", blocked_dl)
    expect_identical(readBin(file_100k, "raw", n=2e5), readBin(single_dl, "raw", n=2e5))
    expect_identical(readBin(file_100k, "raw", n=2e5), readBin(blocked_dl, "raw", n=2e5))

    # ways of deleting a container
    delete_blob_container(cont, confirm=FALSE)
    delete_blob_container(bl, "newcontainer2", confirm=FALSE)
    delete_blob_container(paste0(bl$url, "newcontainer3"), key=bl$key, confirm=FALSE)
    Sys.sleep(5)
    expect_true(is_empty(list_blob_containers(bl)))
})

rg$delete(confirm=FALSE)

