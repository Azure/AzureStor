context("ADLSgen2 client interface")

tenant <- Sys.getenv("AZ_TEST_TENANT_ID")
app <- Sys.getenv("AZ_TEST_APP_ID")
password <- Sys.getenv("AZ_TEST_PASSWORD")
subscription <- Sys.getenv("AZ_TEST_SUBSCRIPTION")

if(tenant == "" || app == "" || password == "" || subscription == "")
    skip("Authentication tests skipped: ARM credentials not set")

rgname <- Sys.getenv("AZ_TEST_STORAGE_RG")
storname <- Sys.getenv("AZ_TEST_STORAGE_HNS")

if(rgname == "" || storname == "")
    skip("ADLSgen2 client tests skipped: resource names not set")

sub <- AzureRMR::az_rm$new(tenant=tenant, app=app, password=password)$get_subscription(subscription)
stor <- sub$get_resource_group(rgname)$get_storage_account(storname)
options(azure_dl_progress_bar=FALSE)

test_that("ADLSgen2 client interface works",
{
    ad <- stor$get_adls_endpoint()
    ad2 <- adls_endpoint(stor$properties$primaryEndpoints$dfs, key=stor$list_keys()[1])
    expect_is(ad, "adls_endpoint")
    expect_identical(ad, ad2)

    expect_true(is_empty(list_adls_filesystems(ad)))

    # listing files in a nonexistent filesystem
    expect_error(list_adls_files(adls_filesystem(ad, "newfs")))

    # ways of creating a filesystem
    fs <- adls_filesystem(ad, "newfs1")
    create_adls_filesystem(fs)
    create_adls_filesystem(ad, "newfs2")
    create_adls_filesystem(paste0(ad$url, "newfs3"), key=ad$key)

    lst <- list_adls_filesystems(ad)
    expect_true(is.list(lst) && inherits(lst[[1]], "adls_filesystem") && length(lst) == 3)

    expect_identical(fs, lst[["newfs1"]])

    expect_true(is_empty(list_adls_files(fs, "/", info="name")))
    orig_file <- "../resources/iris.csv"
    new_file <- file.path(tempdir(), "iris.csv")
    upload_adls_file(fs, orig_file, "iris.csv", blocksize=1000)

    expect_is(list_adls_files(fs, "/"), "data.frame")
    expect_is(list_adls_files(fs, "/", info="name"), "character")

    # download with and without overwrite
    suppressWarnings(file.remove(new_file))
    download_adls_file(fs, "iris.csv", new_file)
    expect_error(download_adls_file(fs, "iris.csv", new_file, overwrite=FALSE))
    writeLines("foo", new_file)
    expect_silent(download_adls_file(fs, "iris.csv", new_file, overwrite=TRUE))
    expect_identical(readBin(orig_file, "raw", n=1e5), readBin(new_file, "raw", n=1e5))

    # download from url
    suppressWarnings(file.remove(new_file))
    url <- file.path(ad$url, fs$name, "iris.csv")
    download_from_url(url, new_file, key=ad$key)

    # directory manipulation
    create_adls_dir(fs, "dir1")
    create_adls_dir(fs, "/dir_with_root")
    create_adls_dir(fs, "dir1/dir2")

    upload_adls_file(fs, orig_file, "dir1/iris.csv")
    upload_adls_file(fs, orig_file, "/dir_with_root/iris.csv")
    upload_adls_file(fs, orig_file, "/dir1/dir2/iris.csv")

    suppressWarnings(file.remove(new_file))
    download_adls_file(fs, "/dir1/iris.csv", new_file)
    suppressWarnings(file.remove(new_file))
    download_adls_file(fs, "dir_with_root/iris.csv", new_file)
    suppressWarnings(file.remove(new_file))
    download_adls_file(fs, "dir1/dir2/iris.csv", new_file)
    expect_identical(readBin(orig_file, "raw", n=1e5), readBin(new_file, "raw", n=1e5))

    # exact-100k upload: check content-size header works
    file_100k <- "../resources/100k.out"
    upload_adls_file(fs, file_100k, "100k_single.out")
    upload_adls_file(fs, file_100k, "100k_blocked.out", blocksize=1e4)
    single_dl <- file.path(tempdir(), "100k_single.out")
    blocked_dl <- file.path(tempdir(), "100k_blocked.out")
    suppressWarnings(file.remove(single_dl, blocked_dl))
    download_adls_file(fs, "100k_single.out", single_dl)
    download_adls_file(fs, "100k_blocked.out", blocked_dl)
    expect_identical(readBin(file_100k, "raw", n=2e5), readBin(single_dl, "raw", n=2e5))
    expect_identical(readBin(file_100k, "raw", n=2e5), readBin(blocked_dl, "raw", n=2e5))

    # upload from connection
    json <- jsonlite::toJSON(iris, dataframe="columns", auto_unbox=TRUE, pretty=TRUE)

    base_json <- file.path(tempdir(), "iris_base.json")
    writeBin(charToRaw(json), base_json)

    con <- textConnection(json)
    upload_adls_file(fs, con, "iris.json")

    con_dl1 <- file.path(tempdir(), "iris.json")
    suppressWarnings(file.remove(con_dl1))
    download_adls_file(fs, "iris.json", con_dl1)
    expect_identical(readBin(base_json, "raw", n=1e5), readBin(con_dl1, "raw", n=1e5))

    base_rds <- file.path(tempdir(), "iris_base.rds")
    saveRDS(iris, base_rds, compress=FALSE)

    rds <- serialize(iris, NULL)
    con <- rawConnection(rds)
    upload_adls_file(fs, con, "iris.rds")

    con_dl2 <- file.path(tempdir(), "iris.rds")
    suppressWarnings(file.remove(con_dl2))
    download_adls_file(fs, "iris.rds", con_dl2)
    expect_identical(readBin(base_rds, "raw", n=1e5), readBin(con_dl2, "raw", n=1e5))

    # download to memory
    rawvec <- download_adls_file(fs, "iris.rds", NULL)
    iris2 <- unserialize(rawvec)
    expect_identical(iris, iris2)

    con <- rawConnection(raw(0), open="r+")
    download_adls_file(fs, "iris.json", con)
    iris3 <- as.data.frame(jsonlite::fromJSON(con))
    expect_identical(iris, iris3)

    # multiple file transfers
    files <- lapply(1:10, function(f) paste0(sample(letters, 1000, replace=TRUE), collapse=""))
    filenames <- sapply(1:10, function(n) file.path(tempdir(), sprintf("multitransfer_%d", n)))
    suppressWarnings(file.remove(filenames))
    mapply(writeLines, files, filenames)

    create_adls_dir(fs, "multi")
    multiupload_adls_file(fs, file.path(tempdir(), "multitransfer_*"), "multi")

    dest_dir <- file.path(tempdir(), "adls_multitransfer")
    suppressWarnings(unlink(dest_dir, recursive=TRUE))
    dir.create(dest_dir)
    multidownload_adls_file(fs, "multi/multitransfer_*", dest_dir, overwrite=TRUE)

    expect_true(all(sapply(filenames, function(f)
    {
        src <- readBin(f, "raw", n=1e5)
        dest <- readBin(file.path(dest_dir, basename(f)), "raw", n=1e5)
        identical(src, dest)
    })))

    # ways of deleting a filesystem
    delete_adls_filesystem(fs, confirm=FALSE)
    delete_adls_filesystem(ad, "newfs2", confirm=FALSE)
    delete_adls_filesystem(paste0(ad$url, "newfs3"), key=ad$key, confirm=FALSE)
    Sys.sleep(5)
    expect_true(is_empty(list_adls_filesystems(ad)))

    close(con)
})


test_that("AAD authentication works",
{
    url <- stor$get_adls_endpoint()$url 
    token <- AzureRMR::get_azure_token("https://storage.azure.com/", tenant=tenant, app=app, password=password)
    ad <- adls_endpoint(url, token=token)
    fs <- create_adls_filesystem(ad, "newfs4")

    # upload and download
    orig_file <- "../resources/iris.csv"
    upload_adls_file(fs, orig_file, "iris.csv")
    tok_dl <- file.path(tempdir(), "iris_tok.csv")
    suppressWarnings(file.remove(tok_dl))
    download_adls_file(fs, "iris.csv", tok_dl)
    expect_identical(readBin(orig_file, "raw", n=1e5), readBin(tok_dl, "raw", n=1e5))

    # multiple upload and download
    files <- lapply(1:10, function(f) paste0(sample(letters, 1000, replace=TRUE), collapse=""))
    filenames <- sapply(1:10, function(n) file.path(tempdir(), sprintf("multitransfer_%d", n)))
    suppressWarnings(file.remove(filenames))
    mapply(writeLines, files, filenames)

    multiupload_adls_file(fs, file.path(tempdir(), "multitransfer_*"), "/")

    dest_dir <- file.path(tempdir(), "adls_multitransfer")
    suppressWarnings(unlink(dest_dir, recursive=TRUE))
    dir.create(dest_dir)
    multidownload_adls_file(fs, "multitransfer_*", dest_dir, overwrite=TRUE)

    expect_true(all(sapply(filenames, function(f)
    {
        src <- readBin(f, "raw", n=1e5)
        dest <- readBin(file.path(dest_dir, basename(f)), "raw", n=1e5)
        identical(src, dest)
    })))

    delete_adls_filesystem(fs, confirm=FALSE)
    expect_true(is_empty(list_adls_filesystems(ad)))
})


test_that("Invalid transfers handled correctly",
{
    ## nonexistent endpoint
    badname <- paste0(sample(letters, 20, TRUE), collapse="")
    endp <- adls_endpoint(sprintf("https://%s.dfs.core.windows.net", badname), key="foo")
    fs <- adls_filesystem(endp, "nocontainer")

    # uploading
    expect_error(upload_adls_file(fs, "../resources/iris.csv", "iris.csv"))

    json <- jsonlite::toJSON(iris)
    con <- textConnection(json)
    expect_error(upload_adls_file(fs, con, "iris.json"))

    rds <- serialize(iris, NULL)
    con <- rawConnection(rds)
    expect_error(upload_adls_file(fs, con, "iris.rds"))

    # downloading
    expect_error(download_adls_file(fs, "nofile", tempfile()))
    expect_error(download_adls_file(fs, "nofile", NULL))

    con <- rawConnection(raw(0), "r+")
    expect_error(download_adls_file(fs, "nofile", con))


    ## nonexistent container
    ad <- stor$get_adls_endpoint()
    fs <- adls_filesystem(ad, "nocontainer")

    # uploading
    expect_error(upload_adls_file(fs, "../resources/iris.csv", "iris.csv"))

    json <- jsonlite::toJSON(iris)
    con <- textConnection(json)
    expect_error(upload_adls_file(fs, con, "iris.json"))

    rds <- serialize(iris, NULL)
    con <- rawConnection(rds)
    expect_error(upload_adls_file(fs, con, "iris.rds"))

    # downloading
    expect_error(download_adls_file(fs, "nofile", tempfile()))
    expect_error(download_adls_file(fs, "nofile", NULL))

    con <- rawConnection(raw(0), "r+")
    expect_error(download_adls_file(fs, "nofile", con))


    ## bad auth
    ad$key <- "badkey"

    fs <- adls_filesystem(ad, "nocontainer")

    # uploading
    expect_error(upload_adls_file(fs, "../resources/iris.csv", "iris.csv"))

    json <- jsonlite::toJSON(iris)
    con <- textConnection(json)
    expect_error(upload_adls_file(fs, con, "iris.json"))

    rds <- serialize(iris, NULL)
    con <- rawConnection(rds)
    expect_error(upload_adls_file(fs, con, "iris.rds"))

    # downloading
    expect_error(download_adls_file(fs, "nofile", tempfile()))
    expect_error(download_adls_file(fs, "nofile", NULL))

    con <- rawConnection(raw(0), "r+")
    expect_error(download_adls_file(fs, "nofile", con))

    close(con)
})


test_that("chunked downloading works",
{
    ad <- stor$get_adls_endpoint()
    fs <- create_adls_filesystem(ad, "chunkdl")

    orig_file <- "../resources/iris.csv"
    new_file <- tempfile()
    upload_adls_file(fs, orig_file, "iris.csv")

    download_adls_file(fs, "iris.csv", new_file, overwrite=TRUE, blocksize=100)
    expect_identical(readBin(orig_file, "raw", n=1e5), readBin(new_file, "raw", n=1e5))

    con <- rawConnection(raw(0), open="r+")
    download_adls_file(fs, "iris.csv", con, blocksize=130)
    expect_identical(readBin(orig_file, "raw", n=1e5), readBin(con, "raw", n=1e5))

    con <- download_adls_file(fs, "iris.csv", NULL, blocksize=150)
    expect_identical(readBin(orig_file, "raw", n=1e5), readBin(con, "raw", n=1e5))
})


teardown(
{
    ad <- stor$get_adls_endpoint()
    conts <- list_adls_filesystems(ad)
    lapply(conts, delete_adls_filesystem, confirm=FALSE)
})
