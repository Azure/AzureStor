context("Blob client interface")

tenant <- Sys.getenv("AZ_TEST_TENANT_ID")
app <- Sys.getenv("AZ_TEST_APP_ID")
password <- Sys.getenv("AZ_TEST_PASSWORD")
subscription <- Sys.getenv("AZ_TEST_SUBSCRIPTION")

if(tenant == "" || app == "" || password == "" || subscription == "")
    skip("Authentication tests skipped: ARM credentials not set")


sub <- AzureRMR::az_rm$new(tenant=tenant, app=app, password=password)$get_subscription(subscription)
rgname <- paste(sample(letters, 20, replace=TRUE), collapse="")
rg <- sub$create_resource_group(rgname, location="australiaeast")

test_that("Blob client interface works",
{

    storname <- paste(sample(letters, 20, replace=TRUE), collapse="")
    stor <- rg$create_storage_account(storname, hierarchical_namespace_enabled=FALSE)

    # wait until provisioning is complete
    for(i in 1:100)
    {
        Sys.sleep(5)
        state <- stor$sync_fields()
        if(state %in% c("Succeeded", "Error", "Failed"))
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

    # download from url
    suppressWarnings(file.remove(new_file))
    url <- file.path(bl$url, cont$name, "iris.csv")
    expect_silent(download_from_url(url, new_file, key=bl$key))

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

    # upload/download with SAS
    sas <- stor$get_account_sas(permissions="rw")
    blsas <- blob_endpoint(stor$properties$primaryEndpoints$blob, sas=sas)
    contsas <- create_blob_container(blsas, "contsas")
    upload_blob(contsas, orig_file, "iris.csv")

    sas_dl <- file.path(tempdir(), "iris_sas.csv")
    suppressWarnings(file.remove(sas_dl))
    download_blob(cont, "iris.csv", sas_dl)
    expect_identical(readBin(orig_file, "raw", n=1e5), readBin(sas_dl, "raw", n=1e5))

    # upload from connection
    json <- jsonlite::toJSON(iris, dataframe="columns", auto_unbox=TRUE, pretty=TRUE)
    con <- textConnection(json)
    upload_blob(cont, con, "iris.json")

    con_dl1 <- file.path(tempdir(), "iris.json")
    suppressWarnings(file.remove(con_dl1))
    download_blob(cont, "iris.json", con_dl1)
    expect_identical(readBin("../resources/iris.json", "raw", n=1e5), readBin(con_dl1, "raw", n=1e5))

    rds <- serialize(iris, NULL)
    con <- rawConnection(rds)
    upload_blob(cont, con, "iris.rds")

    con_dl2 <- file.path(tempdir(), "iris.rds")
    suppressWarnings(file.remove(con_dl2))
    download_blob(cont, "iris.rds", con_dl2)
    expect_identical(readBin("../resources/iris.rds", "raw", n=1e5), readBin(con_dl2, "raw", n=1e5))

    # download to memory
    rawvec <- download_blob(cont, "iris.rds", NULL)
    iris2 <- unserialize(rawvec)
    expect_identical(iris, iris2)

    con <- rawConnection(raw(0), open="r+")
    download_blob(cont, "iris.json", con)
    iris3 <- as.data.frame(jsonlite::fromJSON(con))
    expect_identical(iris, iris3)

    # multiple file transfers
    files <- lapply(1:10, function(f) paste0(sample(letters, 1000, replace=TRUE), collapse=""))
    filenames <- sapply(1:10, function(n) file.path(tempdir(), sprintf("multitransfer_%d", n)))
    suppressWarnings(file.remove(filenames))
    mapply(writeLines, files, filenames)

    multiupload_blob(cont, file.path(tempdir(), "multitransfer_*"))
    expect_warning(multiupload_blob(cont, file.path(tempdir(), "multitransfer_*"), "newnames"))

    dest_dir <- file.path(tempdir(), "blob_multitransfer")
    suppressWarnings(unlink(dest_dir, recursive=TRUE))
    dir.create(dest_dir)
    multidownload_blob(cont, "multitransfer_*", dest_dir, overwrite=TRUE)

    expect_true(all(sapply(filenames, function(f)
    {
        src <- readBin(f, "raw", n=1e5)
        dest <- readBin(file.path(dest_dir, basename(f)), "raw", n=1e5)
        identical(src, dest)
    })))

    # ways of deleting a container
    delete_blob_container(cont, confirm=FALSE)
    delete_blob_container(bl, "newcontainer2", confirm=FALSE)
    delete_blob_container(bl, "contsas", confirm=FALSE)
    delete_blob_container(paste0(bl$url, "newcontainer3"), key=bl$key, confirm=FALSE)
    Sys.sleep(5)
    expect_true(is_empty(list_blob_containers(bl)))

    close(con)
})

rg$delete(confirm=FALSE)

