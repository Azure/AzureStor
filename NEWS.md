# AzureStor 2.0.1.9000

- By default, HTTP(S) requests to the storage endpoint will now be retried on encountering a network error. To change the number of retries, call `options(azure_storage_retries=N)` where N >= 0. Setting this option to zero disables retrying.
- Downloading now proceeds in blocks, much like uploading. The default block size is set to 16MB for blob and ADLSgen2, and 4MB for file storage. While this reduces the throughput slightly (basically there is at least one extra REST call involved), it allows retrying a failed transfer on a per-block basis rather than having to redownload the entire file.

# AzureStor 2.0.1

- Display a progress bar when downloading single files. To turn this on or off, call `options(azure_dl_progress_bar=TRUE|FALSE)`.
- Fix `upload_to_url`/`download_from_url` bugs introduced in last update.

# AzureStor 2.0.0

## Significant user-visible changes

* Support authentication via Azure Active Directory tokens for blob and ADLSgen2 storage.
* Support uploading and downloading to in-memory R objects, without having to create a temporary file. Uploading can be done with `src` a `rawConnection` or `textConnection` object. For downloading, if `dest` is `NULL`, the downloaded data is returned as a raw vector, or if `dest` is a `rawConnection`, in the connection object.  See the examples in the documentation.
* Implement parallel file transfers using a background pool of R processes. This can significantly speed up transfers when working with multiple small files.
* Experimental support for using the Microsoft AzCopy commandline utility to perform file transfers. Set the argument `use_azcopy=TRUE` in any upload or download function to call AzCopy rather than relying on internal R code. The `call_azcopy` function also allows you to run AzCopy with arbitrary arguments. Requires [AzCopy version 10](https://github.com/Azure/azure-storage-azcopy).
* New generics for storage operations:
  - `storage_container`, `create_storage_container`, `delete_storage_container`, `list_storage_containers` for managing containers (blob containers, file shares, ADLSgen2 filesystems)
  - `storage_upload`, `storage_download`, `storage_multiupload`, `storage_multidownload` for file transfers
  - `list_storage_files`, `create_storage_dir`, `delete_storage_dir`, `delete_storage_file` for managing objects within a container

## Other changes

* Add ADLS upload/download support to `upload_to_url` and `download_from_url`.
* Set default blocksize for `upload_azure_file` to 4MB, the maximum permitted by the API (#5).
* Allow resource group and subscription accessor methods to work when AzureStor is not on the search path.

# AzureStor 1.0.0

* Submitted to CRAN

# AzureStor 0.9.0

* Moved to cloudyr organisation
