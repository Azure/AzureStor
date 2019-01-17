# AzureStor 1.0.0.9000

* Add role-based access control via Azure Active Directory tokens for blob and ADLSgen2 storage.
* Add ADLS upload/download support to `upload_to_url` and `download_from_url`.
* Support uploading from a `textConnection` or `rawConnection` object, which avoids having to create a temporary file when uploading R objects in memory. See the examples for `upload_blob`, `upload_azure_file` and `upload_adls_file`.
* Set default blocksize for `upload_azure_file` to 4MB, the maximum permitted by the API (#5).

# AzureStor 1.0.0

* Submitted to CRAN

# AzureStor 0.9.0

* Moved to cloudyr organisation
