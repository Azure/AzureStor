# AzureStor

[![CRAN](https://www.r-pkg.org/badges/version/AzureStor)](https://cran.r-project.org/package=AzureStor)
![Downloads](https://cranlogs.r-pkg.org/badges/AzureStor)
[![Travis Build Status](https://travis-ci.org/cloudyr/AzureStor.png?branch=master)](https://travis-ci.org/cloudyr/AzureStor)

This package implements both an admin- and client-side interface to [Azure Storage Services](https://docs.microsoft.com/en-us/rest/api/storageservices/). The admin interface uses R6 classes and extends the framework provided by [AzureRMR](https://github.com/hong-revo/AzureRMR). The client interface provides several S3 methods for efficiently managing storage and performing file transfers.

## Storage endpoints

The interface for accessing storage is similar across blobs, files and ADLSGen2. You call the `storage_endpoint` function and provide the endpoint URI, along with your authentication credentials. AzureStor will figure out the type of storage from the URI.

AzureStor supports all the different ways you can authenticate with a storage endpoint:
- Blob storage supports authenticating with an access key, shared access signature (SAS), or an Azure Active Directory (AAD) OAuth token;
- File storage supports access key and SAS;
- ADLSgen2 supports access key and AAD token.

In the case of an AAD token, you can also provide an object obtained via `AzureAuth::get_azure_token()`. If you do this, AzureStor can automatically refresh the token for you when it expires.

```r
# various endpoints for an account: blob, file, ADLS2
bl_endp_key <- storage_endpoint("https://mystorage.blob.core.windows.net", key="access_key")
fl_endp_sas <- storage_endpoint("https://mystorage.file.core.windows.net", sas="my_sas")
ad_endp_tok <- storage_endpoint("https://mystorage.dfs.core.windows.net", token="my_token")

# alternative (recommended) way of supplying an AAD token
token <- AzureRMR::get_azure_token("https://mystorage.dfs.core.windows.net",
                                   tenant="myaadtenant", app="app_id", password="mypassword"))
ad_endp_tok2 <- storage_endpoint("https://mystorage.dfs.core.windows.net", token=token)
```

## Listing, creating and deleting containers

AzureStor provides a rich framework for managing storage. The following generics allow you to manage storage containers:

- `storage_container`: get a storage container (blob container, file share or ADLS filesystem)
- `create_storage_container`
- `delete_storage_container`
- `list_storage_containers`

```r
# example of working with containers (blob storage)
list_storage_containers(bl_endp_key)
cont <- storage_container(bl_endp, "mycontainer")
newcont <- create_storage_container(bl_endp, "newcontainer")
delete_storage_container(newcont)
```

## Files and blobs

These functions for working with objects within a storage container:

- `list_storage_files`: list files/blobs in a directory (for ADLSgen2 and file storage) or blob container
- `create_storage_dir`: for ADLSgen2 and file storage, create a directory
- `delete_storage_dir`: for ADLSgen2 and file storage, delete a directory
- `delete_storage_file`: delete a file or blob
- `storage_upload`/`storage_download`: transfer a file to or from a storage container
- `storage_multiupload`/`storage_multidownload`: transfer multiple files in parallel to or from a storage container


```r
# example of working with files and directories (ADLSgen2)
cont <- storage_container(ad_end_tok, "myfilesystem")
list_storage_files(cont)
create_storage_dir(cont, "newdir")
storage_download(cont, "/readme.txt", "~/readme.txt")
storage_multiupload(cont, "N:/data/*.*", "newdir")  # uploading everything in a directory, in parallel
```

## Uploading and downloading

AzureStor includes a number of extra features to make transferring files efficient.

### Parallel file transfers

 First, as noted above, you can transfer multiple files in parallel using the `multiupload_*`/`multidownload_*` functions. These use a pool of background R processes to do the transfers in parallel, which usually results in major speedups when transferring multiple small files. The pool is created the first time a parallel file transfer is performed, and persists for the duration of the R session; this means you don't have to wait for the pool to be (re-)created each time.

```r
# uploading/downloading multiple files at once: use a wildcard to specify files to transfer
multiupload_adls_file(filesystem, src="N:/logfiles/*.zip", dest="/")
multidownload_adls_file(filesystem, src="/monthly/jan*.*", dest="~/data/january")
```

### Transfer to and from connections

Second, you can upload a (single) in-memory R object via a _connection_, and similarly, you can download a file to a connection, or return it as a raw vector. This lets you transfer an object without having to create a temporary file as an intermediate step.

```r
# uploading serialized R objects via connections
json <- jsonlite::toJSON(iris, pretty=TRUE, auto_unbox=TRUE)
con <- textConnection(json)
upload_blob(cont, src=con, dest="iris.json")

rds <- serialize(iris, NULL)
con <- rawConnection(rds)
upload_blob(cont, src=con, dest="iris.rds")

# downloading files into memory: as a raw vector with dest=NULL, and via a connection
rawvec <- download_blob(cont, src="iris.json", dest=NULL)
rawToChar(rawvec)

con <- rawConnection(raw(0), "r+")
download_blob(cont, src="iris.rds", dest=con)
unserialize(con)
```

### Interface to AzCopy

Third, AzureStor includes an interface to [AzCopy](https://docs.microsoft.com/en-us/azure/storage/common/storage-use-azcopy-v10), Microsoft's high-performance commandline utility for copying files to and from storage. To take advantage of this, simply include the argument `use_azcopy=TRUE` on any upload or download function. AzureStor will then call AzCopy to perform the file transfer, rather than using its own internal code. In addition, a `call_azcopy` function is provided to let you use AzCopy for any task.

```r
# use azcopy to download
myfs <- storage_container(ad_endp, "myfilesystem")
storage_download(adlsfs, "/incoming/bigfile.tar.gz", "/data")

# use azcopy to sync a local and remote dir
call_azcopy('sync c:/local/path "https://mystorage.blob.core.windows.net/mycontainer" --recursive=true')
```

For more information, see the [AzCopy repo on GitHub](https://github.com/Azure/azure-storage-azcopy).

**Note that AzureStor uses AzCopy version 10. It is incompatible with versions 8.1 and earlier.**


## Admin interface

Finally, AzureStor's admin-side interface allows you to easily create and delete resource accounts, as well as obtain access keys and generate a SAS. Here is a sample workflow:

```r
library(AzureRMR)
library(AzureStor)

# authenticate with Resource Manager
az <- az_rm$new(tenant="myaadtenant.onmicrosoft.com", app="app_id", password="password")

sub1 <- az$get_subscription("subscription_id")
rg <- sub1$get_resource_group("resgroup")


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
rdevstor1$get_account_sas(permissions="rw")

# obtain an endpoint object for accessing storage (will have the access key included by default)
rdevstor1$get_blob_endpoint()
#Azure blob storage endpoint
#URL: https://rdevstor1.blob.core.windows.net/
#Access key: <hidden>
#Azure Active Directory token: <none supplied>
#Account shared access signature: <none supplied>
#Storage API version: 2018-03-28

# create a new storage account
blobstor2 <- rg$create_storage_account("blobstor2", location="australiaeast", kind="BlobStorage")

# delete it (will ask for confirmation)
blobstor2$delete()
```

---
[![cloudyr project logo](https://i.imgur.com/JHS98Y7.png)](https://github.com/cloudyr)
