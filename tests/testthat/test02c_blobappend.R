context("Append blobs")

tenant <- Sys.getenv("AZ_TEST_TENANT_ID")
app <- Sys.getenv("AZ_TEST_APP_ID")
password <- Sys.getenv("AZ_TEST_PASSWORD")
subscription <- Sys.getenv("AZ_TEST_SUBSCRIPTION")

if(tenant == "" || app == "" || password == "" || subscription == "")
    skip("Authentication tests skipped: ARM credentials not set")

rgname <- Sys.getenv("AZ_TEST_STORAGE_RG")
storname <- Sys.getenv("AZ_TEST_STORAGE_NOHNS")

if(rgname == "" || storname == "")
    skip("Blob/ADLS interop tests skipped: resource names not set")

sub <- AzureRMR::az_rm$new(tenant=tenant, app=app, password=password)$get_subscription(subscription)
stor <- sub$get_resource_group(rgname)$get_storage_account(storname)

bl <- stor$get_blob_endpoint()

opts <- options(azure_storage_progress_bar=FALSE)


test_that("Append blob works",
{
    cont_name <- paste0(sample(letters, 10, TRUE), collapse="")
    cont <- create_blob_container(bl, cont_name)

    expect_silent(upload_blob(cont, "../resources/iris.csv", "iris.csv", type="AppendBlob", append=FALSE))
    expect_true(blob_exists(cont, "iris.csv"))

    expect_silent(upload_blob(cont, "../resources/iris.csv", "iris.csv", type="AppendBlob", append=TRUE))

    # check that mixed blob types don't break things
    expect_silent(upload_blob(cont, "../resources/iris.csv", "iris0.csv"))

    expect_is(blobs <- list_blobs(cont, info="all"), "data.frame")
    expect_identical(nrow(blobs), 2L)

    appblob <- blobs[which(blobs$blobtype == "AppendBlob"), ]
    blkblob <- blobs[which(blobs$blobtype == "BlockBlob"), ]
    fsize <- file.size("../resources/iris.csv")
    expect_true(nrow(appblob) == 1 && appblob$name == "iris.csv" && appblob$size == 2*fsize)
    expect_true(nrow(blkblob) == 1 && blkblob$name == "iris0.csv")

    # cannot append to non-append blob
    expect_error(upload_blob(cont, "../resources/iris.csv", "iris0.csv", type="AppendBlob", append=TRUE))
    expect_error(upload_blob(cont, "../resources/iris.csv", "iris0.csv", type="AppendBlob", append=FALSE))

    dlfile <- tempfile()
    download_blob(cont, "iris.csv", dlfile)
    expect_true(file.size(dlfile) == 2*fsize)

    dl <- readLines(dlfile)
    src <- readLines("../resources/iris.csv")
    n <- length(src)
    expect_true(length(dl) == 2*n && identical(src, dl[1:n]) && identical(src, dl[(n+1):(2*n)]))
})


teardown(
{
    options(opts)
    conts <- list_blob_containers(bl)
    lapply(conts, delete_blob_container, confirm=FALSE)
})
