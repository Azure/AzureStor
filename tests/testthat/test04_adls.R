context("ADLSgen2 client interface")

tenant <- Sys.getenv("AZ_TEST_TENANT_ID")
app <- Sys.getenv("AZ_TEST_APP_ID")
password <- Sys.getenv("AZ_TEST_PASSWORD")
subscription <- Sys.getenv("AZ_TEST_SUBSCRIPTION")

if(tenant == "" || app == "" || password == "" || subscription == "")
    skip("Authentication tests skipped: ARM credentials not set")


sub <- AzureRMR::az_rm$new(tenant=tenant, app=app, password=password)$get_subscription(subscription)
rgname <- paste(sample(letters, 20, replace=TRUE), collapse="")
rg <- sub$create_resource_group(rgname, location="australiaeast")

test_that("ADLSgen2 client interface works",
{

    storname <- paste(sample(letters, 20, replace=TRUE), collapse="")
    stor <- rg$create_storage_account(storname, hierarchical_namespace_enabled=TRUE)

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
    con <- textConnection(json)
    upload_adls_file(fs, con, "iris.json")

    con_dl1 <- file.path(tempdir(), "iris.json")
    suppressWarnings(file.remove(con_dl1))
    download_adls_file(fs, "iris.json", con_dl1)
    expect_identical(readBin("../resources/iris.json", "raw", n=1e5), readBin(con_dl1, "raw", n=1e5))

    rds <- serialize(iris, NULL)
    con <- rawConnection(rds)
    upload_adls_file(fs, con, "iris.rds")

    con_dl2 <- file.path(tempdir(), "iris.rds")
    suppressWarnings(file.remove(con_dl2))
    download_adls_file(fs, "iris.rds", con_dl2)
    expect_identical(readBin("../resources/iris.rds", "raw", n=1e5), readBin(con_dl2, "raw", n=1e5))

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

rg$delete(confirm=FALSE)
