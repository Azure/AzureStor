#' Create a storage endpoint object
#'
#' Create a storage endpoint object, for interacting with blob, file, table, queue or ADLSgen2 storage.
#'
#' @param endpoint The URL (hostname) for the endpoint. This must be of the form `http[s]://{account-name}.{type}.{core-host-name}`, where `type` is one of `"dfs"` (corresponding to ADLSgen2), `"blob"`, `"file"`, `"queue"` or `"table"`. On the public Azure cloud, endpoints will be of the form `https://{account-name}.{type}.core.windows.net`.
#' @param key The access key for the storage account.
#' @param token An Azure Active Directory (AAD) authentication token. This can be either a string, or an object of class AzureToken created by [AzureRMR::get_azure_token]. The latter is the recommended way of doing it, as it allows for automatic refreshing of expired tokens.
#' @param sas A shared access signature (SAS) for the account.
#' @param api_version The storage API version to use when interacting with the host. Defaults to `"2019-07-07"`.
#' @param service For `storage_endpoint`, the service endpoint type: either "blob", "file", "adls", "queue" or "table". If this is missing, it is inferred from the endpoint hostname.
#' @param x For the print method, a storage endpoint object.
#' @param ... For the print method, further arguments passed to lower-level functions.
#'
#' @details
#' This is the starting point for the client-side storage interface in AzureRMR. `storage_endpoint` is a generic function to create an endpoint for any type of Azure storage while `adls_endpoint`, `blob_endpoint` and `file_endpoint` create endpoints for those types.
#'
#' If multiple authentication objects are supplied, they are used in this order of priority: first an access key, then an AAD token, then a SAS. If no authentication objects are supplied, only public (anonymous) access to the endpoint is possible.
#'
#' @section Storage emulators:
#' AzureStor supports connecting to the [Azure SDK](https://learn.microsoft.com/en-us/azure/storage/common/storage-use-emulator) and [Azurite](https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite) emulators for blob and queue storage. To connect, pass the full URL of the endpoint, including the account name, to the `blob_endpoint` and `queue_endpoint` methods (the latter from the AzureQstor package). The warning about an unrecognised endpoint can be ignored. See the linked pages, and the examples below, for details on how to authenticate with the emulator.
#'
#' Note that the Azure SDK emulator is no longer being actively developed; it's recommended to use Azurite for development work.
#'
#' @return
#' `storage_endpoint` returns an object of S3 class `"adls_endpoint"`, `"blob_endpoint"`, `"file_endpoint"`, `"queue_endpoint"` or `"table_endpoint"` depending on the type of endpoint. All of these also inherit from class `"storage_endpoint"`. `adls_endpoint`, `blob_endpoint` and `file_endpoint` return an object of the respective class.
#'
#' Note that while endpoint classes exist for all storage types, currently AzureStor only includes methods for interacting with ADLSgen2, blob and file storage.
#'
#' @seealso
#' [create_storage_account], [adls_filesystem], [create_adls_filesystem], [file_share], [create_file_share], [blob_container], [create_blob_container]
#'
#' @examples
#' \dontrun{
#'
#' # obtaining an endpoint from the storage account resource object
#' stor <- AzureRMR::get_azure_login()$
#'     get_subscription("sub_id")$
#'     get_resource_group("rgname")$
#'     get_storage_account("mystorage")
#' stor$get_blob_endpoint()
#'
#' # creating an endpoint standalone
#' blob_endpoint("https://mystorage.blob.core.windows.net/", key="access_key")
#'
#' # using an OAuth token for authentication -- note resource is 'storage.azure.com'
#' token <- AzureAuth::get_azure_token("https://storage.azure.com",
#'                                     "myaadtenant", "app_id", "password")
#' adls_endpoint("https://myadlsstorage.dfs.core.windows.net/", token=token)
#'
#'
#' ## Azurite storage emulator:
#'
#' # connecting to Azurite with the default account and key (these also work for the Azure SDK)
#' azurite_account <- "devstoreaccount1"
#' azurite_key <-
#'    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
#' blob_endpoint(paste0("http://127.0.0.1:10000/", azurite_account), key=azurite_key)
#'
#' # to use a custom account name and key, set the AZURITE_ACCOUNTS env var before starting Azurite
#' Sys.setenv(AZURITE_ACCOUNTS="account1:key1")
#' blob_endpoint("http://127.0.0.1:10000/account1", key="key1")
#'
#' }
#' @aliases endpoint blob_endpoint file_endpoint queue_endpoint table_endpoint
#' @export
storage_endpoint <- function(endpoint, key=NULL, token=NULL, sas=NULL, api_version, service)
{
    if(missing(service))
    {
        service <- sapply(c("blob", "file", "queue", "table", "adls"),
                       function(x) is_endpoint_url(endpoint, x))
        if(!any(service))
            stop("Unknown endpoint service", call.=FALSE)
        service <- names(service)[service]
    }

    if(missing(api_version))
        api_version <- getOption("azure_storage_api_version")

    obj <- list(url=endpoint, key=key, token=token, sas=sas, api_version=api_version)
    class(obj) <- c(paste0(service, "_endpoint"), "storage_endpoint")
    obj
}

#' @rdname storage_endpoint
#' @export
blob_endpoint <- function(endpoint, key=NULL, token=NULL, sas=NULL,
                          api_version=getOption("azure_storage_api_version"))
{
    obj <- list(url=endpoint, key=key, token=token, sas=sas, api_version=api_version)
    class(obj) <- c("blob_endpoint", "storage_endpoint")
    obj
}

#' @rdname storage_endpoint
#' @export
file_endpoint <- function(endpoint, key=NULL, token=NULL, sas=NULL,
                          api_version=getOption("azure_storage_api_version"))
{
    obj <- list(url=endpoint, key=key, token=token, sas=sas, api_version=api_version)
    class(obj) <- c("file_endpoint", "storage_endpoint")
    obj
}

#' @rdname storage_endpoint
#' @export
adls_endpoint <- function(endpoint, key=NULL, token=NULL, sas=NULL,
                          api_version=getOption("azure_storage_api_version"))
{
    obj <- list(url=endpoint, key=key, token=token, sas=sas, api_version=api_version)
    class(obj) <- c("adls_endpoint", "storage_endpoint")
    obj
}


#' @rdname storage_endpoint
#' @export
print.storage_endpoint <- function(x, ...)
{
    type <- sub("_endpoint$", "", class(x)[1])
    cat(sprintf("Azure %s storage endpoint\n", type))
    cat(sprintf("URL: %s\n", x$url))

    if(!is_empty(x$key))
        cat("Access key: <hidden>\n")
    else cat("Access key: <none supplied>\n")

    if(!is_empty(x$token))
    {
        cat("Azure Active Directory token:\n")
        print(x$token)
    }
    else cat("Azure Active Directory token: <none supplied>\n")

    if(!is_empty(x$sas))
        cat("Account shared access signature: <hidden>\n")
    else cat("Account shared access signature: <none supplied>\n")

    cat(sprintf("Storage API version: %s\n", x$api_version))
    invisible(x)
}


#' @rdname storage_endpoint
#' @export
print.adls_endpoint <- function(x, ...)
{
    cat("Azure Data Lake Storage Gen2 endpoint\n")
    cat(sprintf("URL: %s\n", x$url))

    if(!is_empty(x$key))
        cat("Access key: <hidden>\n")
    else cat("Access key: <none supplied>\n")

    if(!is_empty(x$token))
    {
        cat("Azure Active Directory token:\n")
        print(x$token)
    }
    else cat("Azure Active Directory token: <none supplied>\n")

    if(!is_empty(x$sas))
        cat("Account shared access signature: <hidden>\n")
    else cat("Account shared access signature: <none supplied>\n")

    cat(sprintf("Storage API version: %s\n", x$api_version))
    invisible(x)
}

