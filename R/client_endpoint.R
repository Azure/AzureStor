#' Create a storage endpoint object
#'
#' Create a storage endpoint object, for interacting with blob, file, table, queue or ADLSgen2 storage. Currently (as of December 2018) ADLSgen2 is in general-access public preview.
#'
#' @param endpoint The URL (hostname) for the endpoint. This must be of the form `http[s]://{account-name}.{type}.{core-host-name}`, where `type` is one of `"dfs"` (corresponding to ADLSgen2), `"blob"`, `"file"`, `"queue"` or `"table"`. On the public Azure cloud, endpoints will be of the form `https://{account-name}.{type}.core.windows.net`.
#' @param key The access key for the storage account.
#' @param token An Azure Active Directory (AAD) authentication token. This can be either a string, or an object of class [AzureRMR::AzureToken] created by [AzureRMR::get_azure_token]. The latter is the recommended way of doing it, as it allows for automatic refreshing of expired tokens.
#' @param sas A shared access signature (SAS) for the account.
#' @param api_version The storage API version to use when interacting with the host. Defaults to `"2018-06-17"` for the ADLSgen2 endpoint, and `"2018-03-28"` for the others.
#' @param x For the print method, a storage endpoint object.
#' @param ... For the print method, further arguments passed to lower-level functions.
#'
#' @details
#' This is the starting point for the client-side storage interface in AzureRMR. `storage_endpoint` is a generic function to create an endpoint for any type of Azure storage while `adls_endpoint`, `blob_endpoint` and `file_endpoint` create endpoints for those types.
#'
#' If multiple authentication objects are supplied, they are used in this order of priority: first an access key, then an AAD token, then a SAS. If no authentication objects are supplied, only public (anonymous) access to the endpoint is possible. Note that authentication with a SAS is not currently supported by ADLSgen2, and authentication with an AAD token is not supported by file storage.
#'
#' @return
#' `storage_endpoint` returns an object of S3 class `"adls_endpoint"`, `"blob_endpoint"`, `"file_endpoint"`, `"queue_endpoint"` or `"table_endpoint"` depending on the type of endpoint. All of these also inherit from class `"storage_endpoint"`. `adls_endpoint`, `blob_endpoint` and `file_endpoint` return an object of the respective class.
#'
#' Currently AzureStor only includes methods for interacting with ADLSgen2 (experimental), blob and file storage.
#'
#' Currently (as of December 2018), if the storage account has hierarchical namespaces enabled, the blob API for the account is disabled. The blob endpoint is still accessible, but blob operations on the endpoint will fail. Full interoperability between blobs and ADLS is planned for 2019.
#'
#' @seealso
#' [create_storage_account], [adls_filesystem], [create_adls_filesystem], [file_share], [create_file_share], [blob_container], [create_blob_container]
#'
#' @examples
#' \dontrun{
#'
#' # obtaining an endpoint from the storage account resource object
#' endp <- stor$get_blob_endpoint()
#'
#' # creating an endpoint standalone
#' endp <- blob_endpoint("https://mystorage.blob.core.windows.net/", key="access_key")
#'
#' }
#' @aliases endpoint blob_endpoint file_endpoint queue_endpoint table_endpoint
#' @export
storage_endpoint <- function(endpoint, key=NULL, token=NULL, sas=NULL, api_version)
{
    type <- sapply(c("blob", "file", "queue", "table", "adls"),
                   function(x) is_endpoint_url(endpoint, x))
    if(!any(type))
        stop("Unknown endpoint type", call.=FALSE)
    type <- names(type)[type]

    # handle api version wart
    if(missing(api_version))
    {
        api_version <- if(type == "adls")
            getOption("azure_adls_api_version")
        else getOption("azure_storage_api_version")
    }

    if(type == "adls" && !is_empty(sas))
        warning("ADLSgen2 does not support authentication with a shared access signature")

    if(type == "file" && !is_empty(token))
        warning("File storage does not support authentication with an AAD token")

    obj <- list(url=endpoint, key=key, token=token, sas=sas, api_version=api_version)
    class(obj) <- c(paste0(type, "_endpoint"), "storage_endpoint")
    obj
}

#' @rdname storage_endpoint
#' @export
blob_endpoint <- function(endpoint, key=NULL, token=NULL, sas=NULL,
                          api_version=getOption("azure_storage_api_version"))
{
    if(!is_endpoint_url(endpoint, "blob"))
        stop("Not a blob endpoint", call.=FALSE)

    obj <- list(url=endpoint, key=key, token=token, sas=sas, api_version=api_version)
    class(obj) <- c("blob_endpoint", "storage_endpoint")
    obj
}

#' @rdname storage_endpoint
#' @export
file_endpoint <- function(endpoint, key=NULL, token=NULL, sas=NULL,
                          api_version=getOption("azure_storage_api_version"))
{
    if(!is_endpoint_url(endpoint, "file"))
        stop("Not a file endpoint", call.=FALSE)

    if(!is_empty(token))
        warning("File storage does not support authentication with an AAD token")

    obj <- list(url=endpoint, key=key, token=token, sas=sas, api_version=api_version)
    class(obj) <- c("file_endpoint", "storage_endpoint")
    obj
}

#' @rdname storage_endpoint
#' @export
adls_endpoint <- function(endpoint, key=NULL, token=NULL, sas=NULL,
                          api_version=getOption("azure_adls_api_version"))
{
    if(!is_endpoint_url(endpoint, "adls"))
        stop("Not an ADLS Gen2 endpoint", call.=FALSE)

    if(!is_empty(sas))
        warning("ADLSgen2 does not support authentication with a shared access signature")

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

