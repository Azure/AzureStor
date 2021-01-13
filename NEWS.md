# AzureStor 3.4.0

- Add helper functions to transfer data in commonly-used formats. These work via connections and so do not create temporary files on disk.
  - `storage_save_rds`/`storage_load_rds`
  - `storage_save_rdata`/`storage_load_rdata`
  - `storage_write_delim`/`storage_read_delim` (for tab-delimited files)
  - `storage_write_csv`/`storage_read_csv`
  - `storage_write_csv2`/`storage_read_csv2`

# AzureStor 3.3.0

- ADLS, file and block blob uploads gain the option to compute and store the MD5 hash of the uploaded file, via the `put_md5` argument to `upload_adls_file`, `upload_azure_file` and `upload_blob`.
- Similarly, downloads gain the option to verify the integrity of the downloaded file using the MD5 hash, via the `check_md5` argument to `download_adls_file`, `download_azure_file` and `download_blob`. This requires that the file's `Content-MD5` property is set.
- Add support for uploading to [append blobs](https://docs.microsoft.com/en-us/rest/api/storageservices/understanding-block-blobs--append-blobs--and-page-blobs), which are a type of blob optimized for append operations. They are useful for data that is constantly growing, but should not be modified once written, such as server logs. See `?upload_blob` for more details.
- Add support for the [Azurite](https://docs.microsoft.com/en-us/azure/storage/common/storage-use-azurite) and [Azure SDK](https://docs.microsoft.com/en-us/azure/storage/common/storage-use-emulator) storage emulators. To connect to the endpoint, use the service-specific functions `blob_endpoint` and `queue_endpoint` (the latter from the AzureQstor package), passing the full URL including the account name: `blob_endpoint("http://127.0.0.1:10000/myaccount", key="mykey")`. The warning about an unrecognised endpoint can be ignored. See the linked pages for full details on how to authenticate to the emulator.<br/>
  Note that the Azure SDK emulator is no longer being actively developed; it's recommended to use Azurite.
- Add a 10-second fuzz factor to the default starting datetime for a generated SAS, to allow for differences in clocks.
- More fixes to the directory handling of `list_blobs()`.
- All uploads now include the `Content-MD5` header in the HTTP requests, as an error-checking mechanism.
- Change maintainer email address.

# AzureStor 3.2.3

- Fix file transfers with filenames containing non-ASCII characters on Windows.
- Require httr >= 1.4.0 for proper support of HEAD requests (reported by @scottporter).

# AzureStor 3.2.2

- Extended support for directories in blob storage. Note that since blob storage doesn't have true directories, there are some warts to be aware of; see `?blob` for more details.
  - Implement non-recursive directory listings for `list_blobs`, thanks to @cantpitch.
  - Fixes to the directory detection logic of `list_blobs`.
  - Implement `create_blob_dir` and `delete_blob_dir`.
- Remove broken implementation for recursive deleting of subdirectory contents for file storage.

# AzureStor 3.2.1

- Internal changes to handle differences in how table storage returns error messages.
- Update tests for R 4.0.

# AzureStor 3.2.0

- Update client API versions to "2019-07-07".
- Basic support for the new file storage permissions API. When uploading files, they will be created with default filesystem parameters: "inherit" permissions, "now" creation/modified datetimes, and unset attributes. This has no impact on blob and ADLSgen2.
- Remove a redundant API call to set the Content-Type after a blob or file storage upload.
- `list_storage_containers` and related methods will now check for a continuation marker to avoid returning prematurely (thanks to @StatKalli for reporting and providing a fix).
- Extended support for generating an account SAS (which should not be confused with _using_ one):
  - Add `get_account_sas` S3 generic, with methods for `az_storage` resource objects and client endpoints. The `az_storage$get_account_sas` R6 method now simply calls the S3 method.
  - Generating the SAS now uses internal R code, rather than making an API call. The resulting SAS should also work with azcopy.
- Add support for generating a user delegation SAS. Note that _using_ a user delegation SAS has always worked.
  - New S3 generics `get_user_delegation_key` and `get_user_delegation_sas`, with methods for `az_storage` objects and blob endpoints. Similar R6 methods added for `az_storage` objects.
  - New `revoke_user_delegation_keys` generic and methods to invalidate all user delegation keys for a storage account (and all SAS's generated with those keys).
  - See `?sas` for more information.
- The `silent` argument for `call_azcopy` now uses the `azure_storage_azcopy_silent` system option for its default value, falling back to FALSE if this is unset.
- Bug fixes for internal functions.

# AzureStor 3.1.1

- Expose `sign_request` as a generic, dispatching on the endpoint class. This is to allow for the fact that table storage uses a different signing scheme to the other storage services.
- Fix a bug in `list_adls_files` that could result in an error with large numbers of files.
- Add a `timeout` argument to `call_storage_endpoint` that sets the number of seconds to wait for an API call.
- Fix authentication logic for retrying failed requests.

# AzureStor 3.1.0

## Significant user-visible changes

- Enhanced support for AzCopy:
  - Calling AzCopy from the various upload/download methods can now use existing authentication credentials without needing to login separately. Note that AzCopy only supports SAS and OAuth authentication, not access key.
  - `call_azcopy` now uses the processx package under the hood, which is a powerful and flexible framework for running external programs from R. The interface is slightly changed: rather than taking the entire commandline as a single string, `call_azcopy` now expects each AzCopy commandline option to be an individual argument. See `?call_azcopy` for examples of the new interface.
  - Recursive file transfers with AzCopy is now supported.

## Other changes

- New `storage_file_exists` generic to check for file existence, which dispatches to `blob_exists`, `azure_file_exists` and `adls_file_exists` for the individual storage types.
- Move AAD token validity check inside the retry loop in `call_storage_endpoint`; this fixes a bug where the token could expire during a long transfer.
- Default destination arguments now work for file transfer generics as well.

# AzureStor 3.0.1

- Uploading with ADLS now sets the Content-Type property correctly.
- Fixes to support blob/ADLS interoperability, which has just gone GA.
- In `list_blobs` and `list_adls_files`, check that a field exists before trying to modify it (works around problem of possibly inconsistent response from the endpoint).
- Allow passing a SAS with a leading `?` (as generated by the Azure Portal and Storage Explorer) to the client functions.
- Fix some bugs in `multidownload_blob`.
- The `az_storage$get_*_endpoint()` methods now support passing an AAD token for authentication.

# AzureStor 3.0.0

## Significant user-visible changes

- Substantial enhancements to multiple-file transfers (up and down):
  - These can now accept a vector of pathnames as the source and destination arguments.
  - Alternatively, for a wildcard source, add the ability to recurse through subdirectories. Any directory structure in the source will be reproduced at the destination.
- Related to the above: the file transfer methods can now create subdirectories that are specified in their destination argument. For ADLS and blob uploading this happens automatically; for Azure file uploading it requires a separate API call which can be slow, so is optional.
- The default destination directory when transferring files (not connections) is now the (remote) root for uploading, and the (local) current directory for downloading.
- Significant changes to file storage methods for greater consistency with the other storage types:
  - The default directory for `list_azure_files` is now the root, mirroring the behaviour for blobs and ADLSgen2.
  - The output of `list_azure_files` now includes the full path as part of the file/directory name.
  - Add `recursive` argument to `list_azure_files`, `create_azure_dir` and `delete_azure_dir` for recursing through subdirectories. Like with file transfers, for Azure file storage this can be slow, so try to use a non-recursive solution where possible.
- Make output format for `list_adls_files`, `list_blobs` and `list_azure_files` more consistent. The first 2 columns for a data frame output are now always `name` and `size`; the size of a directory is NA. The 3rd column for non-blobs is `isdir` which is TRUE/FALSE depending on whether the object is a directory or file. Any additional columns remain storage type-specific.
- New `get_storage_metadata` and `set_storage_metadata` methods for managing user-specified properties (metadata) for objects.
- Revamped methods for getting standard properties, which are now all methods for `get_storage_properties` rather than having specific functions for blobs, files and directories.
- Creating a service-specific endpoint (`file_endpoint`, `blob_endpoint`, `adls_endpoint`) with an invalid URL will now warn, instead of throwing an error. This enables using tools like Azurite, which use a local address as the endpoint. Calling `storage_endpoint` with an invalid URL will still throw an error, as the function has no way of telling which storage service is required.

## Other changes

- Remove the warning about ADLSgen2 not supporting shared access signatures (SAS).
- Background process pool functionality has been moved to AzureRMR. This removes code duplication, and also makes it available for other packages that may benefit.
- Only display the file transfer progress bar in an interactive session.
- Export `do_container_op` and `call_storage_endpoint` to allow direct calls to the storage account endpoint.
- Fix S3 dispatching bugs in `copy_url_to_storage` and `multicopy_url_to_storage`.
- A `NULL` download destination will now actually return a raw vector as opposed to a connection, matching what the documentation says.

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
