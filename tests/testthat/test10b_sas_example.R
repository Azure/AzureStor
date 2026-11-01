test_that("account SAS examples use a valid base64-encoded key", {
    key <- openssl::base64_encode(charToRaw("example-key"))
    start <- Sys.Date()
    expiry <- start + 7

    direct_sas <- get_account_sas("mystorage", key, start=start, expiry=expiry)
    endp <- storage_endpoint("https://mystorage.blob.core.windows.net", key=key)
    endpoint_sas <- get_account_sas(endp, start=start, expiry=expiry)

    expect_match(direct_sas, "sig=")
    expect_match(endpoint_sas, "sig=")
})
