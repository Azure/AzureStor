# AzureStor 1.0.0.9000

## Significant user-visible changes

* Support authentication via Azure Active Directory tokens for blob and ADLSgen2 storage.
* Support uploading and downloading to in-memory R objects, without having to create a temporary file. Uploading can be done with `src` a `rawConnection` or `textConnection` object. For downloading, if `dest` is `NULL`, the downloaded data is returned as a raw vector, or if `dest` is a `rawConnection`, in the connection object.  See the examples in the documentation.
* Implement parallel file transfers using a background pool of R processes. This can significantly speed up transfers when working with multiple small files.
* Rename `upload_to_url`/`download_from_url` to `upload_to_azure` and `download_from_azure` respectively, to emphasise that these functions are for interacting with Azure storage, not websites in general. The old functions will still work, but print a warning.
* Experimental support for using the Microsoft AzCopy commandline utility to perform file transfers. Set the argument `use_azcopy=TRUE` in any upload or download function to call AzCopy rather than relying on internal R code. The `call_azcopy` function also allows you to run AzCopy with arbitrary arguments. Requires [AzCopy version 10](https://github.com/Azure/azure-storage-azcopy).

## Other changes

* Add ADLS upload/download support to `upload_to_azure` and `download_from_azure`.
* Set default blocksize for `upload_azure_file` to 4MB, the maximum permitted by the API (#5).
* Allow resource group and subscription accessor methods to work when AzureStor is not on the search path.

# AzureStor 1.0.0

* Submitted to CRAN

# AzureStor 0.9.0

* Moved to cloudyr organisation
