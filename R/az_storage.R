#' Storage account resource class
#'
#' Class representing a storage account, exposing methods for working with it.
#'
#' @docType class
#' @section Methods:
#' The following methods are available, in addition to those provided by the [AzureRMR::az_resource] class:
#' - `new(...)`: Initialize a new storage object. See 'Initialization'.
#' - `list_keys()`: Return the access keys for this account.
#' - `get_account_sas(...)`: Return an account shared access signature (SAS). See 'Shared access signatures' for more details.
#' - `get_blob_endpoint(key, sas)`: Return the account's blob storage endpoint, along with an access key and/or a SAS. See 'Endpoints' for more details
#' - `get_file_endpoint(key, sas)`: Return the account's file storage endpoint.
#' - `regen_key(key)`: Regenerates (creates a new value for) an access key. The argument `key` can be 1 or 2.
#'
#' @section Initialization:
#' Initializing a new object of this class can either retrieve an existing storage account, or create a account on the host. Generally, the best way to initialize an object is via the `get_storage_account`, `create_storage_account` or `list_storage_accounts` methods of the [az_resource_group] class, which handle the details automatically.
#'
#' @section Shared access signatures:
#' The simplest way for a user to access files and data in a storage account is to give them the account's access key. This gives them full control of the account, and so may be a security risk. An alternative is to provide the user with a _shared access signature_ (SAS), which limits access to specific resources and only for a set length of time.
#'
#' To create an account SAS, call the `get_account_sas()` method with the following arguments:
#' - `start`: The starting access date/time, as a `Date` or `POSIXct` value. Defaults to the current time.
#' - `expiry`: The ending access date/time, as a `Date` or `POSIXct` value.  Defaults to 8 hours after the start time.
#' - `services`: Which services to allow access to. A string containing a combination of the letters `b`, `f`, `q`, `t` for blob, file, queue and table access. Defaults to `bfqt`.
#' - `permissions`: Which permissions to grant. A string containing a combination of the letters `r` (read), `w` (write), `d` (delete), `l` (list), `a` (add), `c` (create), `u` (update) , `p` (process). Defaults to `r`.
#' - `resource_types`: Which levels of the resource type hierarchy to allow access to. A string containing a combination of the letters `s` (service), `c` (container), `o` (object). Defaults to `sco`.
#' - ip: An IP address or range to grant access to.
#' - `protocol`: Which protocol to allow, either `"http"`, `"http,https"` or `"https"`. Defaults to NULL, which is the same as `"http,https"`.
#' - `key`: the access key used to sign (authorise) the SAS.
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
#' [create_storage_account], [get_storage_account], [delete_storage_account], [Date], [POSIXt],
#' [Azure Storage Provider API reference](https://docs.microsoft.com/en-us/rest/api/storagerp/),
#' [Azure Storage Services API reference](https://docs.microsoft.com/en-us/rest/api/storageservices/)
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
#' # generate a shared access signature for blob storage, expiring in 7 days time
#' today <- Sys.time()
#' stor$get_account_sas(expiry=today + 7*24*60*60, services="b", permissions="rw")
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

    get_account_sas=function(start=NULL, expiry=NULL, services="bqtf", permissions="r",
                             resource_types="sco", ip=NULL, protocol=NULL, key=NULL)
    {
        dates <- private$set_sas_dates(start, expiry)
        parms <- list(keyToSign=key,
                      signedExpiry=dates$expiry, signedIp=ip, signedPermission=permissions, signedProtocol=protocol,
                      signedResourceTypes=resource_types, signedServices=services, signedStart=dates$start)

        self$do_operation("listAccountSas", body=parms, encode="json", http_verb="POST")$accountSasToken
    },

    # hide for now
    #get_service_sas=function(start=NULL, expiry=NULL, path=NULL, service=NULL, permissions="r",
                             #ip=NULL, protocol=NULL, key=NULL)
    #{
        #dates <- private$set_sas_dates(start, expiry)
        #parms <- list(keyToSign=key, canonicalizedResource=path,
                      #signedExpiry=dates$expiry, signedIp=ip, signedPermission=permissions, signedProtocol=protocol,
                      #signedResource=service, signedStart=dates$start)

        #self$do_operation("listServiceSas", body=parms, encode="json", http_verb="POST")$serviceSasToken
    #},

    get_blob_endpoint=function(key=self$list_keys()[1], sas=NULL)
    {
        if(!is_empty(self$properties$isHnsEnabled) && self$properties$isHnsEnabled)
            warning("Blob endpoint not available because hierarchical namespace is enabled for this account",
                    call.=FALSE)
        blob_endpoint(self$properties$primaryEndpoints$blob, key=key, sas=sas)
    },

    get_file_endpoint=function(key=self$list_keys()[1], sas=NULL)
    {
        file_endpoint(self$properties$primaryEndpoints$file, key=key, sas=sas)
    },

    get_adls_endpoint=function(key=self$list_keys()[1], sas=NULL)
    {
        adls_endpoint(self$properties$primaryEndpoints$dfs, key=key, sas=sas)
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
),

private=list(

    set_sas_dates=function(start, expiry)
    {
        if(is.null(start))
            start <- Sys.time()
        else if(!inherits(start, "POSIXt"))
            start <- as.POSIXct(start, origin="1970-01-01")

        if(is.null(expiry)) # by default, 8 hours after start
            expiry <- start + 8 * 60 * 60
        else if(!inherits(expiry, "POSIXt"))
            expiry <- as.POSIXct(expiry, origin="1970-01-01")

        if(inherits(start, c("POSIXt", "Date")))
            start <- strftime(start, "%Y-%m-%dT%H:%M:%SZ", tz="UTC")
        if(inherits(expiry, c("POSIXt", "Date")))
            expiry <- strftime(expiry, "%Y-%m-%dT%H:%M:%SZ", tz="UTC")

        list(start=start, expiry=expiry)
    }
))
