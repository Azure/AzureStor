# AzureStor 2.1.1.9000

- The multiple-file transfer functions now accept vectors of pathnames as their source and destination arguments. See the online help for more details on how to specify these.
- The file transfer methods (up and down) can now create subdirectories that are specified in their destination argument, if necessary. For ADLS and blob uploading this happens automatically; for Azure file uploading it requires a separate API call which can be slow, so is optional.
- Add `recursive` argument to Azure file storage methods for recursing through subdirectories. Note that this can be slow, so try to use a non-recursive solution where possible.
- Creating a service-specific endpoint (`file_endpoint`, `blob_endpoint`, `adls_endpoint`) with an invalid URL will now warn, instead of throwing an error. This enables using tools like Azurite, which use a local address as the endpoint. Calling `storage_endpoint` with an invalid URL will still throw an error, as the function has no way of telling which storage service is required.
- Remove the warning about ADLSgen2 not supporting shared access signatures (SAS).
- Background process pool functionality has been moved to AzureRMR. This removes code duplication, and also makes it available for other packages that may benefit.
- Only display the file transfer progress bar in an interactive session.
- The default directory for `list_azure_files` is now the root, mirroring the behaviour for blobs and ADLSgen2.
- Make output format for `list_adls_files`, `list_blobs` and `list_azure_files` more consistent. The first 2 columns for a data frame output are now always `name` and `size`; the size of a directory is zero. The 3rd column for non-blobs is `isdir` which is TRUE/FALSE depending on whether the object is a directory or file. Any additional columns remain storage type-specific.

# AzureStor 2.1.1

- Correctly handle ADLSgen2 and file storage listings with more than 5000 files.
- Fix a bug in confirmation prompts. On R >= 3.5, prompts will now use `utils::askYesNo`; as a side-effect, Windows users who are using RGUI.exe will see a popup dialog box instead of a message in the terminal.

# AzureStor 2.1.0

- Update client API versions to "2018-11-09".
- Fix bug with blob uploading using azcopy.
- Add `copy_url_to_blob` function, for directly copying a HTTP\[S\] URL to blob storage. The corresponding generic is `copy_url_to_storage`, with a method for blob containers (only).

# AzureStor 2.0.2

- By default, HTTP(S) requests to the storage endpoint will now be retried on encountering a network error. To change the number of retries, call `options(azure_storage_retries=N)` where N >= 0. Setting this option to zero disables retrying.
- Downloading now proceeds in blocks, much like uploading. The default block size is set to 16MB for blob and ADLSgen2, and 4MB for file storage. While this reduces the throughput slightly (basically there is at least one extra REST call involved), it allows retrying a failed transfer on a per-block basis rather than having to redownload the entire file.
- Also display the progress bar for uploads. The command to enable/disable the progress bar is now `options(azure_storage_progress_bar=TRUE|FALSE)`.

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
