context("File client interface")

tenant <- Sys.getenv("AZ_TEST_TENANT_ID")
app <- Sys.getenv("AZ_TEST_APP_ID")
password <- Sys.getenv("AZ_TEST_PASSWORD")
subscription <- Sys.getenv("AZ_TEST_SUBSCRIPTION")

if(tenant == "" || app == "" || password == "" || subscription == "")
    skip("Authentication tests skipped: ARM credentials not set")


sub <- az_rm$new(tenant=tenant, app=app, password=password)$get_subscription(subscription)
rgname <- paste(sample(letters, 20, replace=TRUE), collapse="")
rg <- sub$create_resource_group(rgname, location="australiaeast")

test_that("File client interface works",
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

    # ways of deleting a share
    delete_file_share(share, confirm=FALSE)
    delete_file_share(fl, "newshare2", confirm=FALSE)
    delete_file_share(paste0(fl$url, "newshare3"), key=fl$key, confirm=FALSE)
    Sys.sleep(5)
    expect_true(is_empty(list_file_shares(fl)))
})

rg$delete(confirm=FALSE)
