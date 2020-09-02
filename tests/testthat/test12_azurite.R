context("Azurite storage emulator")

# currently hardcoded to use account name=account1, shared key=key1
res <- try(httr::GET("http://127.0.0.1:10000/account1"), silent=TRUE)
if(inherits(res, "try-error"))
    skip("Storage emulator tests skipped: Azurite not running")

opts <- options(azure_storage_progress_bar=FALSE)

test_that("Blob storage methods work",
{
    expect_warning(endp <- blob_endpoint("http://127.0.0.1:10000/account1", key="key1"))
    expect_is(endp, "blob_endpoint")

    expect_is(list_blob_containers(endp), "list")
    cont <- create_blob_container(endp, "container1")
    expect_is(cont, "blob_container")

    expect_silent(upload_blob(cont, "../resources/iris.csv"))
    expect_is(list_blobs(cont), "data.frame")
    expect_silent(download_blob(cont, "iris.csv", tempfile()))

    expect_silent(delete_blob_container(cont, confirm=FALSE))
    expect_true(is_empty(list_blob_containers(endp)))
})


teardown({
    endp <- suppressWarnings(blob_endpoint("http://127.0.0.1:10000/account1", key="key1"))
    conts <- list_blob_containers(endp)
    lapply(conts, delete_blob_container, confirm=FALSE)
    options(opts)
})
