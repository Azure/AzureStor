#' Storage account resource class
#'
#' Class representing a storage account, exposing methods for working with it.
#'
#' @docType class
#' @section Methods:
#' The following methods are available, in addition to those provided by the [AzureRMR::az_resource] class:
#' - `new(...)`: Initialize a new storage object. See 'Initialization'.
#' - `list_keys()`: Return the access keys for this account.
#' - `get_account_sas(...)`: Return an account shared access signature (SAS). See 'Creating a shared access signature' below.
#' - `get_user_delegation_key(...)`: Returns a key that can be used to construct a user delegation SAS.
#' - `get_user_delegation_sas(...)`: Return a user delegation SAS.
#' - `revoke_user_delegation_keys()`: Revokes all user delegation keys for the account. This also renders all SAS's obtained via such keys invalid.
#' - `get_blob_endpoint(key, sas)`: Return the account's blob storage endpoint, along with an access key and/or a SAS. See 'Endpoints' for more details
#' - `get_file_endpoint(key, sas)`: Return the account's file storage endpoint.
#' - `regen_key(key)`: Regenerates (creates a new value for) an access key. The argument `key` can be 1 or 2.
#'
#' @section Initialization:
#' Initializing a new object of this class can either retrieve an existing storage account, or create a account on the host. Generally, the best way to initialize an object is via the `get_storage_account`, `create_storage_account` or `list_storage_accounts` methods of the [az_resource_group] class, which handle the details automatically.
#'
#' @section Creating a shared access signature:
#' Note that you don't need to worry about this section if you have been _given_ a SAS, and only want to use it to access storage.
#'
#' AzureStor supports generating three kinds of SAS: account, service and user delegation. An account SAS can be used with any type of storage. A service SAS can be used with blob and file storage, whle a user delegation SAS can be used with blob and ADLS2 storage.
#'
#' To create an account SAS, call the `get_account_sas()` method. This has the following signature:
#'
#' ```
#' get_account_sas(key=self$list_keys()[1], start=NULL, expiry=NULL, services="bqtf", permissions="rl",
#'                 resource_types="sco", ip=NULL, protocol=NULL)
#' ```
#'
#' To create a service SAS, call the `get_service_sas()` method, which has the following signature:
#'
#' ```
#' get_service_sas(key=self$list_keys()[1], resource, service, start=NULL, expiry=NULL, permissions="r",
#'                 resource_type=NULL, ip=NULL, protocol=NULL, policy=NULL, snapshot_time=NULL)
#' ```
#'
#' To create a user delegation SAS, you must first create a user delegation _key_. This takes the place of the account's access key in generating the SAS. The `get_user_delegation_key()` method has the following signature:
#'
#' ```
#' get_user_delegation_key(token=self$token, key_start=NULL, key_expiry=NULL)
#' ```
#'
#' Once you have a user delegation key, you can use it to obtain a user delegation sas. The `get_user_delegation_sas()` method has the following signature:
#'
#' ```
#' get_user_delegation_sas(key, resource, start=NULL, expiry=NULL, permissions="rl",
#'                         resource_type="c", ip=NULL, protocol=NULL, snapshot_time=NULL)
#' ```
#'
#' (Note that the `key` argument for this method is the user delegation key, _not_ the account key.)
#'
#' To invalidate all user delegation keys, as well as the SAS's generated with them, call the `revoke_user_delegation_keys()` method. This has the following signature:
#'
#' ```
#' revoke_user_delegation_keys()
#' ```
#'
#' See the [Shared access signatures][sas] page for more information about this topic.
#'
#' @section Endpoints:
#' The client-side interaction with a storage account is via an _endpoint_. A storage account can have several endpoints, one for each type of storage supported: blob, file, queue and table.
#'
#' The client-side interface in AzureStor is implemented using S3 classes. This is for consistency with other data access packages in R, which mostly use S3. It also emphasises the distinction between Resource Manager (which is for interacting with the storage account itself) and the client (which is for accessing files and data stored in the account).
#'
#' To create a storage endpoint independently of Resource Manager (for example if you are a user without admin or owner access to the account), use the [blob_endpoint] or [file_endpoint] functions.
#'
#' If a storage endpoint is created without an access key and SAS, only public (anonymous) access is possible.
#'
#' @seealso
#' [blob_endpoint], [file_endpoint],
#' [create_storage_account], [get_storage_account], [delete_storage_account], [Date], [POSIXt]
#'
#' [Azure Storage Provider API reference](https://docs.microsoft.com/en-us/rest/api/storagerp/),
#' [Azure Storage Services API reference](https://docs.microsoft.com/en-us/rest/api/storageservices/)
#'
#' [Create an account SAS](https://docs.microsoft.com/en-us/rest/api/storageservices/create-account-sas),
#' [Create a user delegation SAS](https://docs.microsoft.com/en-us/rest/api/storageservices/create-user-delegation-sas),
#' [Create a service SAS](https://docs.microsoft.com/en-us/rest/api/storageservices/create-service-sas)
#'
#' @examples
#' \dontrun{
#'
#' # recommended way of retrieving a resource: via a resource group object
#' stor <- resgroup$get_storage_account("mystorage")
#'
#' # list account access keys
#' stor$list_keys()
#'
#' # regenerate a key
#' stor$regen_key(1)
#'
#' # storage endpoints
#' stor$get_blob_endpoint()
#' stor$get_file_endpoint()
#'
#' }
#' @export
az_storage <- R6::R6Class("az_storage", inherit=AzureRMR::az_resource,

public=list(

    list_keys=function()
    {
        keys <- named_list(private$res_op("listKeys", http_verb="POST")$keys, "keyName")
        sapply(keys, `[[`, "value")
    },

    get_account_sas=function(key=self$list_keys()[1], start=NULL, expiry=NULL, services="bqtf", permissions="rl",
                             resource_types="sco", ip=NULL, protocol=NULL)
    {
        get_account_sas(self, key=key, start=start, expiry=expiry, services=services, permissions=permissions,
                        resource_types=resource_types, ip=ip, protocol=protocol)
    },

    get_user_delegation_key=function(token=self$token, key_start=NULL, key_expiry=NULL)
    {
        get_user_delegation_key(self, token=token, key_start=key_start, key_expiry=key_expiry)
    },

    revoke_user_delegation_keys=function()
    {
        revoke_user_delegation_keys(self)
    },

    get_user_delegation_sas=function(key, resource, start=NULL, expiry=NULL, permissions="rl",
                                     resource_type="c", ip=NULL, protocol=NULL, snapshot_time=NULL)
    {
        get_user_delegation_sas(self, key=key, resource=resource, start=start, expiry=expiry, permissions=permissions,
                                resource_type=resource_type, ip=ip, protocol=protocol, snapshot_time=snapshot_time)
    },

    get_service_sas=function(key=self$list_keys()[1], resource, service, start=NULL, expiry=NULL, permissions="r",
                             resource_type=NULL, ip=NULL, protocol=NULL, policy=NULL, snapshot_time=NULL, directory_depth=NULL)
    {
        get_service_sas(self, key=key, resource=resource, service=service, start=start, expiry=expiry,
                        permissions=permissions, resource_type=resource_type, ip=ip, protocol=protocol, policy=policy,
                        snapshot_time=snapshot_time, directory_depth=directory_depth)
    },

    get_blob_endpoint=function(key=self$list_keys()[1], sas=NULL, token=NULL)
    {
        blob_endpoint(self$properties$primaryEndpoints$blob, key=key, sas=sas, token=token)
    },

    get_file_endpoint=function(key=self$list_keys()[1], sas=NULL, token=NULL)
    {
        file_endpoint(self$properties$primaryEndpoints$file, key=key, sas=sas, token=token)
    },

    get_adls_endpoint=function(key=self$list_keys()[1], sas=NULL, token=NULL)
    {
        adls_endpoint(self$properties$primaryEndpoints$dfs, key=key, sas=sas, token=token)
    },

    regen_key=function(key=1)
    {
        body <- list(keyName=paste0("key", key))
        keys <- self$do_operation("regenerateKey", body=body, encode="json", http_verb="POST")
        keys <- named_list(keys$keys, "keyName")
        sapply(keys, `[[`, "value")
    },

    print=function(...)
    {
        cat("<Azure resource ", self$type, "/", self$name, ">\n", sep="")

        endp <- self$properties$primaryEndpoints
        endp <- paste0("    ", names(endp), ": ", endp, collapse="\n")
        sku <- unlist(self$sku)

        cat("  Account type:", self$kind, "\n")
        cat("  SKU:", paste0(names(sku), "=", sku, collapse=", "), "\n")
        cat("  Endpoints:\n")
        cat(endp, "\n")
        cat("---\n")

        cat(AzureRMR::format_public_fields(self, exclude=c("subscription", "resource_group",
                                           "type", "name", "kind", "sku")))
        cat(AzureRMR::format_public_methods(self))
        invisible(NULL)
    }
))
