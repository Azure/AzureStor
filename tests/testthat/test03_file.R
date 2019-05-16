context("File client interface")

tenant <- Sys.getenv("AZ_TEST_TENANT_ID")
app <- Sys.getenv("AZ_TEST_APP_ID")
password <- Sys.getenv("AZ_TEST_PASSWORD")
subscription <- Sys.getenv("AZ_TEST_SUBSCRIPTION")

if(tenant == "" || app == "" || password == "" || subscription == "")
    skip("Authentication tests skipped: ARM credentials not set")

rgname <- Sys.getenv("AZ_TEST_STORAGE_RG")
storname <- Sys.getenv("AZ_TEST_STORAGE_NOHNS")

if(rgname == "" || storname == "")
    skip("File client tests skipped: resource names not set")

sub <- AzureRMR::az_rm$new(tenant=tenant, app=app, password=password)$get_subscription(subscription)
stor <- sub$get_resource_group(rgname)$get_storage_account(storname)
options(azure_dl_progress_bar=FALSE)

test_that("File client interface works",
{
    fl <- stor$get_file_endpoint()
    fl2 <- file_endpoint(stor$properties$primaryEndpoints$file, key=stor$list_keys()[1])
    expect_is(fl, "file_endpoint")
    expect_identical(fl, fl2)

    expect_true(is_empty(list_file_shares(fl)))

    # listing files in a nonexistent share
    expect_error(list_azure_files(file_share(fl, "newshare")))

    # ways of creating a share
    share <- file_share(fl, "newshare1")
    create_file_share(share)
    create_file_share(fl, "newshare2")
    create_file_share(paste0(fl$url, "newshare3"), key=fl$key)

    lst <- list_file_shares(fl)
    expect_true(is.list(lst) && inherits(lst[[1]], "file_share") && length(lst) == 3)

    expect_identical(share, lst[["newshare1"]])

    expect_true(is_empty(list_azure_files(share, "/", info="name")))
    orig_file <- "../resources/iris.csv"
    new_file <- file.path(tempdir(), "iris.csv")
    upload_azure_file(share, orig_file, "iris.csv", blocksize=1000)

    expect_is(list_azure_files(share, "/"), "data.frame")
    expect_is(list_azure_files(share, "/", info="name"), "character")

    # download with and without overwrite
    suppressWarnings(file.remove(new_file))
    download_azure_file(share, "iris.csv", new_file)
    expect_error(download_azure_file(share, "iris.csv", new_file, overwrite=FALSE))
    writeLines("foo", new_file)
    expect_silent(download_azure_file(share, "iris.csv", new_file, overwrite=TRUE))
    expect_identical(readBin(orig_file, "raw", n=1e5), readBin(new_file, "raw", n=1e5))

    # download from url
    suppressWarnings(file.remove(new_file))
    url <- file.path(fl$url, share$name, "iris.csv")
    download_from_url(url, new_file, key=fl$key)

    # directory manipulation
    create_azure_dir(share, "dir1")
    create_azure_dir(share, "/dir_with_root")
    create_azure_dir(share, "dir1/dir2")

    upload_azure_file(share, orig_file, "dir1/iris.csv")
    upload_azure_file(share, orig_file, "/dir_with_root/iris.csv")
    upload_azure_file(share, orig_file, "/dir1/dir2/iris.csv")

    suppressWarnings(file.remove(new_file))
    download_azure_file(share, "/dir1/iris.csv", new_file)
    suppressWarnings(file.remove(new_file))
    download_azure_file(share, "dir_with_root/iris.csv", new_file)
    suppressWarnings(file.remove(new_file))
    download_azure_file(share, "dir1/dir2/iris.csv", new_file)
    expect_identical(readBin(orig_file, "raw", n=1e5), readBin(new_file, "raw", n=1e5))

    # exact-100k upload: check content-size header works
    file_100k <- "../resources/100k.out"
    upload_azure_file(share, file_100k, "100k_single.out")
    upload_azure_file(share, file_100k, "100k_blocked.out", blocksize=1e4)
    single_dl <- file.path(tempdir(), "100k_single.out")
    blocked_dl <- file.path(tempdir(), "100k_blocked.out")
    suppressWarnings(file.remove(single_dl, blocked_dl))
    download_azure_file(share, "100k_single.out", single_dl)
    download_azure_file(share, "100k_blocked.out", blocked_dl)
    expect_identical(readBin(file_100k, "raw", n=2e5), readBin(single_dl, "raw", n=2e5))
    expect_identical(readBin(file_100k, "raw", n=2e5), readBin(blocked_dl, "raw", n=2e5))

    # upload/download with SAS
    sas <- stor$get_account_sas(permissions="rw")
    Sys.sleep(2)  # deal with synchronisation issues
    flsas <- file_endpoint(stor$properties$primaryEndpoints$file, sas=sas)
    sharesas <- create_file_share(flsas, "sharesas")
    upload_azure_file(sharesas, orig_file, "iris.csv")

    sas_dl <- file.path(tempdir(), "iris_sas.csv")
    suppressWarnings(file.remove(sas_dl))
    download_azure_file(sharesas, "iris.csv", sas_dl)
    expect_identical(readBin(orig_file, "raw", n=1e5), readBin(sas_dl, "raw", n=1e5))

    # upload from connection
    json <- jsonlite::toJSON(iris, dataframe="columns", auto_unbox=TRUE, pretty=TRUE)

    base_json <- file.path(tempdir(), "iris_base.json")
    writeBin(charToRaw(json), base_json)

    con <- textConnection(json)
    upload_azure_file(share, con, "iris.json")

    con_dl1 <- file.path(tempdir(), "iris.json")
    suppressWarnings(file.remove(con_dl1))
    download_azure_file(share, "iris.json", con_dl1)
    expect_identical(readBin(base_json, "raw", n=1e5), readBin(con_dl1, "raw", n=1e5))

    base_rds <- file.path(tempdir(), "iris_base.rds")
    saveRDS(iris, base_rds, compress=FALSE)

    rds <- serialize(iris, NULL)
    con <- rawConnection(rds)
    upload_azure_file(share, con, "iris.rds")

    con_dl2 <- file.path(tempdir(), "iris.rds")
    suppressWarnings(file.remove(con_dl2))
    download_azure_file(share, "iris.rds", con_dl2)
    expect_identical(readBin(base_rds, "raw", n=1e5), readBin(con_dl2, "raw", n=1e5))

    # download to memory
    rawvec <- download_azure_file(share, "iris.rds", NULL)
    iris2 <- unserialize(rawvec)
    expect_identical(iris, iris2)

    con <- rawConnection(raw(0), open="r+")
    download_azure_file(share, "iris.json", con)
    iris3 <- as.data.frame(jsonlite::fromJSON(con))
    expect_identical(iris, iris3)

    # multiple file transfers
    files <- lapply(1:10, function(f) paste0(sample(letters, 1000, replace=TRUE), collapse=""))
    filenames <- sapply(1:10, function(n) file.path(tempdir(), sprintf("multitransfer_%d", n)))
    suppressWarnings(file.remove(filenames))
    mapply(writeLines, files, filenames)

    create_azure_dir(share, "multi")
    multiupload_azure_file(share, file.path(tempdir(), "multitransfer_*"), "multi")

    dest_dir <- file.path(tempdir(), "file_multitransfer")
    suppressWarnings(unlink(dest_dir, recursive=TRUE))
    dir.create(dest_dir)
    multidownload_azure_file(share, "multi/multitransfer_*", dest_dir, overwrite=TRUE)

    expect_true(all(sapply(filenames, function(f)
    {
        src <- readBin(f, "raw", n=1e5)
        dest <- readBin(file.path(dest_dir, basename(f)), "raw", n=1e5)
        identical(src, dest)
    })))

    # ways of deleting a share
    delete_file_share(share, confirm=FALSE)
    delete_file_share(fl, "newshare2", confirm=FALSE)
    delete_file_share(fl, "sharesas", confirm=FALSE)
    delete_file_share(paste0(fl$url, "newshare3"), key=fl$key, confirm=FALSE)
    Sys.sleep(5)
    expect_true(is_empty(list_file_shares(fl)))

    close(con)
})


test_that("Invalid transfers handled correctly",
{
    ## nonexistent endpoint
    badname <- paste0(sample(letters, 20, TRUE), collapse="")
    endp <- file_endpoint(sprintf("https://%s.file.core.windows.net", badname), key="foo")
    share <- file_share(endp, "nocontainer")

    # uploading
    expect_error(upload_azure_file(share, "../resources/iris.csv", "iris.csv"))

    json <- jsonlite::toJSON(iris)
    con <- textConnection(json)
    expect_error(upload_azure_file(share, con, "iris.json"))

    rds <- serialize(iris, NULL)
    con <- rawConnection(rds)
    expect_error(upload_azure_file(share, con, "iris.rds"))

    # downloading
    expect_error(download_azure_file(share, "nofile", tempfile()))
    expect_error(download_azure_file(share, "nofile", NULL))

    con <- rawConnection(raw(0), "r+")
    expect_error(download_azure_file(share, "nofile", con))


    ## nonexistent container
    fl <- stor$get_file_endpoint()
    share <- file_share(fl, "nocontainer")

    # uploading
    expect_error(upload_azure_file(share, "../resources/iris.csv", "iris.csv"))

    json <- jsonlite::toJSON(iris)
    con <- textConnection(json)
    expect_error(upload_azure_file(share, con, "iris.json"))

    rds <- serialize(iris, NULL)
    con <- rawConnection(rds)
    expect_error(upload_azure_file(share, con, "iris.rds"))

    # downloading
    expect_error(download_azure_file(share, "nofile", tempfile()))
    expect_error(download_azure_file(share, "nofile", NULL))

    con <- rawConnection(raw(0), "r+")
    expect_error(download_azure_file(share, "nofile", con))


    ## bad auth
    fl$key <- "badkey"

    share <- file_share(fl, "nocontainer")

    # uploading
    expect_error(upload_azure_file(share, "../resources/iris.csv", "iris.csv"))

    json <- jsonlite::toJSON(iris)
    con <- textConnection(json)
    expect_error(upload_azure_file(share, con, "iris.json"))

    rds <- serialize(iris, NULL)
    con <- rawConnection(rds)
    expect_error(upload_azure_file(share, con, "iris.rds"))

    # downloading
    expect_error(download_azure_file(share, "nofile", tempfile()))
    expect_error(download_azure_file(share, "nofile", NULL))

    con <- rawConnection(raw(0), "r+")
    expect_error(download_azure_file(share, "nofile", con))

    close(con)
})


test_that("chunked downloading works",
{
    fl <- stor$get_file_endpoint()
    share <- create_file_share(fl, "chunkdl")

    orig_file <- "../resources/iris.csv"
    new_file <- tempfile()
    upload_azure_file(share, orig_file, "iris.csv")

    download_azure_file(share, "iris.csv", new_file, overwrite=TRUE, blocksize=100)
    expect_identical(readBin(orig_file, "raw", n=1e5), readBin(new_file, "raw", n=1e5))

    con <- rawConnection(raw(0), open="r+")
    download_azure_file(share, "iris.csv", con, blocksize=130)
    expect_identical(readBin(orig_file, "raw", n=1e5), readBin(con, "raw", n=1e5))

    con <- download_azure_file(share, "iris.csv", NULL, blocksize=150)
    expect_identical(readBin(orig_file, "raw", n=1e5), readBin(con, "raw", n=1e5))
})


teardown(
{
    fl <- stor$get_file_endpoint()
    conts <- list_file_shares(fl)
    lapply(conts, delete_file_share, confirm=FALSE)
})
