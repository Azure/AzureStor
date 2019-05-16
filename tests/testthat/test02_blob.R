context("Blob client interface")

tenant <- Sys.getenv("AZ_TEST_TENANT_ID")
app <- Sys.getenv("AZ_TEST_APP_ID")
password <- Sys.getenv("AZ_TEST_PASSWORD")
subscription <- Sys.getenv("AZ_TEST_SUBSCRIPTION")

if(tenant == "" || app == "" || password == "" || subscription == "")
    skip("Authentication tests skipped: ARM credentials not set")

rgname <- Sys.getenv("AZ_TEST_STORAGE_RG")
storname <- Sys.getenv("AZ_TEST_STORAGE_NOHNS")

if(rgname == "" || storname == "")
    skip("Blob client tests skipped: resource names not set")

sub <- AzureRMR::az_rm$new(tenant=tenant, app=app, password=password)$get_subscription(subscription)
stor <- sub$get_resource_group(rgname)$get_storage_account(storname)
options(azure_dl_progress_bar=FALSE)

test_that("Blob client interface works",
{
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
    Sys.sleep(2)  # deal with synchronisation issues
    blsas <- blob_endpoint(stor$properties$primaryEndpoints$blob, sas=sas)
    contsas <- create_blob_container(blsas, "contsas")
    upload_blob(contsas, orig_file, "iris.csv")

    sas_dl <- file.path(tempdir(), "iris_sas.csv")
    suppressWarnings(file.remove(sas_dl))
    download_blob(cont, "iris.csv", sas_dl)
    expect_identical(readBin(orig_file, "raw", n=1e5), readBin(sas_dl, "raw", n=1e5))

    # upload from connection
    json <- jsonlite::toJSON(iris, dataframe="columns", auto_unbox=TRUE, pretty=TRUE)

    base_json <- file.path(tempdir(), "iris_base.json")
    writeBin(charToRaw(json), base_json)

    con <- textConnection(json)
    upload_blob(cont, con, "iris.json")

    con_dl1 <- file.path(tempdir(), "iris.json")
    suppressWarnings(file.remove(con_dl1))
    download_blob(cont, "iris.json", con_dl1)
    expect_identical(readBin(base_json, "raw", n=1e5), readBin(con_dl1, "raw", n=1e5))

    base_rds <- file.path(tempdir(), "iris_base.rds")
    saveRDS(iris, base_rds, compress=FALSE)

    rds <- serialize(iris, NULL)
    con <- rawConnection(rds)
    upload_blob(cont, con, "iris.rds")

    con_dl2 <- file.path(tempdir(), "iris.rds")
    suppressWarnings(file.remove(con_dl2))
    download_blob(cont, "iris.rds", con_dl2)
    expect_identical(readBin(base_rds, "raw", n=1e5), readBin(con_dl2, "raw", n=1e5))

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

    # multiple file transfer with directory names
    files <- lapply(1:10, function(f) paste0(sample(letters, 1000, replace=TRUE), collapse=""))
    filenames <- sapply(1:10, function(n) file.path(tempdir(), sprintf("multitransfer2_%d", n)))
    suppressWarnings(file.remove(filenames))
    mapply(writeLines, files, filenames)

    multiupload_blob(cont, file.path(tempdir(), "multitransfer2_*"), "new/dir")
    dest_dir <- file.path(tempdir(), "blob_multitransfer")
    suppressWarnings(unlink(dest_dir, recursive=TRUE))
    dir.create(dest_dir)
    multidownload_blob(cont, "new/dir/multitransfer2_*", dest_dir, overwrite=TRUE)

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


test_that("AAD authentication works",
{
    url <- stor$get_blob_endpoint()$url 
    token <- AzureRMR::get_azure_token("https://storage.azure.com/", tenant=tenant, app=app, password=password)
    bl <- blob_endpoint(url, token=token)
    cont <- create_blob_container(bl, "newcontainer4")

    # upload and download
    orig_file <- "../resources/iris.csv"
    upload_blob(cont, orig_file, "iris.csv")
    tok_dl <- file.path(tempdir(), "iris_tok.csv")
    suppressWarnings(file.remove(tok_dl))
    download_blob(cont, "iris.csv", tok_dl)
    expect_identical(readBin(orig_file, "raw", n=1e5), readBin(tok_dl, "raw", n=1e5))

    # multiple upload and download
    files <- lapply(1:10, function(f) paste0(sample(letters, 1000, replace=TRUE), collapse=""))
    filenames <- sapply(1:10, function(n) file.path(tempdir(), sprintf("multitransfer_%d", n)))
    suppressWarnings(file.remove(filenames))
    mapply(writeLines, files, filenames)

    multiupload_blob(cont, file.path(tempdir(), "multitransfer_*"))

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

    delete_blob_container(cont, confirm=FALSE)
    expect_true(is_empty(list_blob_containers(bl)))
})


test_that("Invalid transfers handled correctly",
{
    ## nonexistent endpoint
    badname <- paste0(sample(letters, 20, TRUE), collapse="")
    endp <- blob_endpoint(sprintf("https://%s.blob.core.windows.net", badname), key="foo")
    cont <- blob_container(endp, "nocontainer")

    # uploading
    expect_error(upload_blob(cont, "../resources/iris.csv", "iris.csv"))

    json <- jsonlite::toJSON(iris)
    con <- textConnection(json)
    expect_error(upload_blob(cont, con, "iris.json"))

    rds <- serialize(iris, NULL)
    con <- rawConnection(rds)
    expect_error(upload_blob(cont, con, "iris.rds"))

    # downloading
    expect_error(download_blob(cont, "nofile", tempfile()))
    expect_error(download_blob(cont, "nofile", NULL))

    con <- rawConnection(raw(0), "r+")
    expect_error(download_blob(cont, "nofile", con))


    ## nonexistent container
    bl <- stor$get_blob_endpoint()
    cont <- blob_container(bl, "nocontainer")

    # uploading
    expect_error(upload_blob(cont, "../resources/iris.csv", "iris.csv"))

    json <- jsonlite::toJSON(iris)
    con <- textConnection(json)
    expect_error(upload_blob(cont, con, "iris.json"))

    rds <- serialize(iris, NULL)
    con <- rawConnection(rds)
    expect_error(upload_blob(cont, con, "iris.rds"))

    # downloading
    expect_error(download_blob(cont, "nofile", tempfile()))
    expect_error(download_blob(cont, "nofile", NULL))

    con <- rawConnection(raw(0), "r+")
    expect_error(download_blob(cont, "nofile", con))


    ## bad auth
    bl$key <- "badkey"

    cont <- blob_container(bl, "nocontainer")

    # uploading
    expect_error(upload_blob(cont, "../resources/iris.csv", "iris.csv"))

    json <- jsonlite::toJSON(iris)
    con <- textConnection(json)
    expect_error(upload_blob(cont, con, "iris.json"))

    rds <- serialize(iris, NULL)
    con <- rawConnection(rds)
    expect_error(upload_blob(cont, con, "iris.rds"))

    # downloading
    expect_error(download_blob(cont, "nofile", tempfile()))
    expect_error(download_blob(cont, "nofile", NULL))

    con <- rawConnection(raw(0), "r+")
    expect_error(download_blob(cont, "nofile", con))

    close(con)
})


test_that("chunked downloading works",
{
    bl <- stor$get_blob_endpoint()
    cont <- create_blob_container(bl, "chunkdl")

    orig_file <- "../resources/iris.csv"
    new_file <- tempfile()
    upload_blob(cont, orig_file, "iris.csv")

    download_blob(cont, "iris.csv", new_file, overwrite=TRUE, blocksize=100)
    expect_identical(readBin(orig_file, "raw", n=1e5), readBin(new_file, "raw", n=1e5))

    con <- rawConnection(raw(0), open="r+")
    download_blob(cont, "iris.csv", con, blocksize=130)
    expect_identical(readBin(orig_file, "raw", n=1e5), readBin(con, "raw", n=1e5))

    con <- download_blob(cont, "iris.csv", NULL, blocksize=150)
    expect_identical(readBin(orig_file, "raw", n=1e5), readBin(con, "raw", n=1e5))
})


teardown(
{
    bl <- stor$get_blob_endpoint()
    conts <- list_blob_containers(bl)
    lapply(conts, delete_blob_container, confirm=FALSE)
})
