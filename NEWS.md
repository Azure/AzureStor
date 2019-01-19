# AzureStor 1.0.0.9000

* Add role-based access control via Azure Active Directory tokens for blob and ADLSgen2 storage.
* Add ADLS upload/download support to `upload_to_url` and `download_from_url`.
* Support uploading and downloading to in-memory R objects, without having to create a temporary file. Uploading can be done with `src` a `rawConnection` or `textConnection` object. For downloading, if `dest` is `NULL`, the downloaded data is returned as a raw vector, or if `dest` is a `rawConnection`, in the connection object.  See the examples in the documentation.
* Implement parallel file transfers using a background pool of R processes. This can significantly speed up transfers when working with multiple small files.
* Set default blocksize for `upload_azure_file` to 4MB, the maximum permitted by the API (#5).

# AzureStor 1.0.0

* Submitted to CRAN

# AzureStor 0.9.0

* Moved to cloudyr organisation
