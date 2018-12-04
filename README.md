# AzureStor

This package provides both an admin- and user-side interface to [Azure Storage Services](https://docs.microsoft.com/en-us/rest/api/storageservices/). The admin interface uses R6 classes and builds on the tools provided by [AzureRMR](https://github.com/hong-revo/AzureRMR). The user-side interface provides easy access to Azure storage accounts via S3 classes and methods.

Sample admin workflow:

```r
library(AzureRMR)
library(AzureStor)

# authenticate with Resource Manager
az <- az_rm$new(tenant="myaadtenant.onmicrosoft.com", app="app_id", password="password")

sub1 <- az$get_subscription("subscription_id")
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
# ...

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

AzureStor includes client methods for blob storage, file storage, and Azure Data Lake Storage Gen2 (experimental).

Accessing blob storage:

```r
# get the endpoint from a storage ARM object
bl <- rdevstor1$get_blob_endpoint()

# for users without ARM credentials, use the storage_endpoint() function and provide a key
bl <- storage_endpoint("https://rdevstor1.blob.core.windows.net", key="access_key")

# can also provide a shared access signature
# providing neither a key nor SAS allows only public access
bl <- storage_endpoint("https://rdevstor1.blob.core.windows.net", sas="my_sas")

# list of blob containers in this account
list_blob_containers(bl)

# using pipes
library(magrittr)

# create a new blob container and transfer a file
cont <- bl %>% create_blob_container("newcontainer")
cont %>% upload_blob("../downloads/test.file.gz", "test.gz")
cont %>% list_blobs()
cont %>% download_blob("test.gz", "../downloads/test.file2.gz")

# you can also do an authenticated download from a full URL
download_from_url("https://rdevstor1.blob.core.windows.net/privcontainer/test.gz",
                  "../downloads/test.file3.gz",
                  key="access_key")
```

Accessing ADLSgen2 and file storage works much the same way, but with the addition of being able to manipulate directories:

```r
# get the ADLSgen2 endpoint, either from the resource object or standalone
ad <- rdevstor1$get_adls_endpoint()
ad <- storage_endpoint("https://rdevstor1.dfs.core.windows.net", key="access_key")
ad <- storage_endpoint("https://rdevstor1.dfs.core.windows.net", sas="my_sas")

# ADLS filesystems are analogous to blob containers
ad %>% list_adls_filesystems()

# create a new filesystem and transfer some files
fs1 <- ad %>% create_file_filesystem("filesystem1")

fs1 %>% list_adls_files("/")

fs1 %>% create_adls_directory("foobar")
fs1 %>% upload_adls_file("file.txt", "foobar/upload.txt")
fs1 %>% download_adls_file("foobar/upload.txt", "file_dl.txt")
```

---
[![cloudyr project logo](https://i.imgur.com/JHS98Y7.png)](https://github.com/cloudyr)
