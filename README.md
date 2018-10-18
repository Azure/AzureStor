# AzureStor

This package provides both an admin- and user-side interface to [Azure Storage Services](https://docs.microsoft.com/en-us/rest/api/storageservices/). The admin interface uses R6 classes and builds on the tools provided by [AzureRMR](https://github.com/hong-revo/AzureRMR). The user-side interface provides easy access to Azure storage accounts via S3 classes and methods.

Sample admin workflow:

```r
library(AzureRMR)
library(AzureStor)

# authenticate with Resource Manager
az <- az_rm$new(tenant="xxx-xxx-xxx", app="yyy-yyy-yyy", secret="{secret goes here}")

sub1 <- az$get_subscription("5710aa44-281f-49fe-bfa6-69e66bb55b11")
rg <- sub1$get_resource_group("rdev1")


# get an existing storage account
rdevstor1 <- rg$get_storage("rdevstor1")
rdevstor1
#<Azure resource Microsoft.Storage/storageAccounts/rdevstor1>
#  Account type: Storage 
#  SKU: name=Standard_LRS, tier=Standard 
#  Endpoints:
#    blob: https://rdevstor1.blob.core.windows.net/
#    queue: https://rdevstor1.queue.core.windows.net/
#    table: https://rdevstor1.table.core.windows.net/
#    file: https://rdevstor1.file.core.windows.net/ 
#---
#  id: /subscriptions/5710aa44-281f-49fe-bfa6-69e66bb55b11/resourceGroups/rdev1/providers/Microsoft.Sto ...
#  identity: NULL
#  is_synced: TRUE
#  location: australiasoutheast
#  managed_by: NULL
#  plan: NULL
#  properties: list(networkAcls, trustedDirectories, supportsHttpsTrafficOnly, encryption,
#    provisioningState, creationTime, primaryEndpoints, primaryLocation, statusOfPrimary)
#  tags: list()
#---
#  Methods:
#    check, delete, do_operation, get_account_sas, get_blob_endpoint, get_file_endpoint, list_keys,
#    set_api_version, sync_fields, update

# retrieve admin keys
rdevstor1$list_keys()

# create a shared access signature (SAS)
rdevstor1$get_account_sas()

# obtain an endpoint object for accessing storage
rdevstor1$get_blob_endpoint()
#Azure blob storage endpoint
#URL: https://rdevstor1.blob.core.windows.net/
#Access key: <hidden>
#Account shared access signature: <none supplied>
#Storage API version: 2017-07-29

# create a new storage account
blobstor2 <- rg$create_storage_account("blobstor2", location="australiaeast", kind="BlobStorage")

# delete it (will ask for confirmation)
blobstor2$delete()
```


The user-side interface in AzureStor is implemented using S3 classes. This is for consistency with other data access packages in R, which mostly use S3. It also emphasises the distinction between Resource Manager (which is for interacting with the storage account itself) and the user client (which is for accessing files and data stored in the account).

Accessing blob storage:

```r
# get the endpoint from a storage ARM object
bl <- rdevstor1$get_blob_endpoint()

# for users without ARM credentials, use the storage_endpoint() function and provide a key
bl <- storage_endpoint("https://rdevstor1.blob.core.windows.net", key="/Uq3rxh0lbYErt...")

# can also provide a shared access signature
# providing neither a key nor SAS allows only public access
bl <- storage_endpoint("https://rdevstor1.blob.core.windows.net", sas="sv=2015-04-05&ss=...")


list_blob_containers(bl)
#$container2
#Azure blob container
#Endpoint URL: https://rdevstor1.blob.core.windows.net/
#Access key: <hidden>
#Account shared access signature: <none supplied>
#Storage API version: 2017-07-29
#
#$privcontainer
#Azure blob container
#Endpoint URL: https://rdevstor1.blob.core.windows.net/
#Access key: <hidden>
#Account shared access signature: <none supplied>
#Storage API version: 2017-07-29

library(magrittr)
priv <- bl %>% blob_container("privcontainer")

priv %>% upload_blob("../downloads/test.file.gz", "test.gz")

priv %>% list_blobs()
#[1] "test.gz"

priv %>% download_blob("test.gz", "../downloads/test.file2.gz")

# you can also do an authenticated download from a full URL
download_from_url("https://rdevstor1.blob.core.windows.net/privcontainer/test.gz",
                  "../downloads/test.file3.gz",
                  key="/Uq3rxh0lbYErt...")
```

Accessing file storage works much the same way:

```r
# get the file endpoint, either from ARM or standalone
fs <- rdevstor1$get_file_endpoint()
fs <- storage_endpoint("https://rdevstor1.file.core.windows.net", key="/Uq3rxh0lbYErt...")
fs <- storage_endpoint("https://rdevstor1.file.core.windows.net", sas="sv=2015-04-05&ss=...")


fs %>% list_file_shares()
#$share1
#Azure file share
#Endpoint URL: https://rdevstor1.file.core.windows.net/
#Access key: <hidden>
#Account shared access signature: <none supplied>
#Storage API version: 2017-07-29
#
#$share2
#Azure file share
#Endpoint URL: https://rdevstor1.file.core.windows.net/
#Access key: <hidden>
#Account shared access signature: <none supplied>
#Storage API version: 2017-07-29

sh1 <- fs %>% file_share("share1")

sh1 %>% list_azure_files("/")
#             name      type   size
#1           mydir Directory     NA
#2 irisrf_dput.txt      File 731930
#3       storage.R      File   3189

sh1 %>% download_azure_file("irisrf_dput.txt", "misc/file.txt")
sh1 %>% upload_azure_file("misc/file.txt", "foobar/upload.txt")
sh1 %>% delete_azure_file("/foobar/upload.txt")

# authenticated file transfer to/from a URL also works with file shares
download_from_url("https://rdevstor1.file.core.windows.net/share1/irisrf_dput.txt",
                  "misc/file2.txt",
                  key="/Uq3rxh0lbYErt...")
```

