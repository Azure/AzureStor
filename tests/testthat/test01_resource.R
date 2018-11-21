context("ARM interface")

tenant <- Sys.getenv("AZ_TEST_TENANT_ID")
app <- Sys.getenv("AZ_TEST_APP_ID")
password <- Sys.getenv("AZ_TEST_PASSWORD")
subscription <- Sys.getenv("AZ_TEST_SUBSCRIPTION")

if(tenant == "" || app == "" || password == "" || subscription == "")
    skip("Authentication tests skipped: ARM credentials not set")


sub <- az_rm$new(tenant=tenant, app=app, password=password)$get_subscription(subscription)


test_that("ARM interface works",
{
    rgname <- paste(sample(letters, 20, replace=TRUE), collapse="")
    expect_false(sub$resource_group_exists(rgname))
    rg <- sub$create_resource_group(rgname, location="australiaeast")
    expect_true(sub$resource_group_exists(rgname))

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

    expect_is(stor, "az_storage")
    expect_true(stor$kind == "StorageV2")
    expect_true(stor$properties$supportsHttpsTrafficOnly)

    keys <- stor$list_keys()
    expect_true(is.character(keys))

    keys_new <- stor$regen_key(1)
    expect_false(keys_new[1] == keys[1])

    blobstorname <- paste(sample(letters, 20, replace=TRUE), collapse="")
    blobstor <- rg$create_storage_account(blobstorname, kind="BlobStorage")
    Sys.sleep(5)
    expect_is(blobstor, "az_storage")
    expect_true(blobstor$kind == "BlobStorage")
    expect_true(blobstor$properties$accessTier == "Hot")

    stor$delete(confirm=FALSE)
    blobstor$delete(confirm=FALSE)
    Sys.sleep(5)
    expect_false(rg$resource_exists(type="Microsoft.Storage/storageAccounts", name=storname))
    expect_false(rg$resource_exists(type="Microsoft.Storage/storageAccounts", name=blobstorname))

    rg$delete(confirm=FALSE)
})
