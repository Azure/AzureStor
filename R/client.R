#' Create a storage endpoint object
#'
#' @param endpoint The URL (hostname) for the endpoint. This must be of the form `http[s]://{account-name}.{type}.{core-host-name}`, where `type` is one of `"blob"`, `"file"`, `"queue"` or `"table"`. On the public Azure cloud, endpoints will be of the form `https://{account-name}.{type}.core.windows.net`.
#' @param key The access key for the storage account.
#' @param sas A shared access signature (SAS) for the account. If `key` is also provided, the SAS is not used. If neither `key` nor `sas` are provided, only public (anonymous) access to the endpoint is possible.
#' @param api_version The storage API version to use when interacting with the host. Currently defaults to `"2017-07-29"`.
#' @param x For the print method, a storage endpoint object.
#' @param ... For the print method, further arguments passed to lower-level functions.
#'
#' @details
#' This is the starting point for the client-side storage interface in AzureRMR. `storage_endpoint` is a generic function to create an endpoint for any type of Azure storage while `blob_endpoint` and `file_endpoint` create endpoints for those types.
#'
#' @return
#' `storage_endpoint` returns an object of S3 class `"blob_endpoint"`, `"file_endpoint"`, `"queue_endpoint"` or `"table_endpoint"` depending on the type of endpoint. All of these also inherit from class `"storage_endpoint"`. `blob_endpoint` and `file_endpoint` return an object of the respective type.
#'
#' @seealso
#' [az_storage], [file_share], [create_file_share], [blob_container], [create_blob_container]
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
storage_endpoint <- function(endpoint, key=NULL, sas=NULL, api_version=getOption("azure_storage_api_version"))
{
    type <- sapply(c("blob", "file", "queue", "table"),
                   function(x) is_endpoint_url(endpoint, x))
    if(!any(type))
        stop("Unknown endpoint type", call.=FALSE)
    type <- names(type)[type]

    obj <- list(url=endpoint, key=key, sas=sas, api_version=api_version)
    class(obj) <- c(paste0(type, "_endpoint"), "storage_endpoint")
    obj
}

#' @rdname storage_endpoint
#' @export
blob_endpoint <- function(endpoint, key=NULL, sas=NULL, api_version=getOption("azure_storage_api_version"))
{
    if(!is_endpoint_url(endpoint, "blob"))
        stop("Not a blob endpoint", call.=FALSE)

    obj <- list(url=endpoint, key=key, sas=sas, api_version=api_version)
    class(obj) <- c("blob_endpoint", "storage_endpoint")
    obj
}

#' @rdname storage_endpoint
#' @export
file_endpoint <- function(endpoint, key=NULL, sas=NULL, api_version=getOption("azure_storage_api_version"))
{
    if(!is_endpoint_url(endpoint, "file"))
        stop("Not a file endpoint", call.=FALSE)

    obj <- list(url=endpoint, key=key, sas=sas, api_version=api_version)
    class(obj) <- c("file_endpoint", "storage_endpoint")
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
    if(!is_empty(x$sas))
        cat("Account shared access signature: <hidden>\n")
    else cat("Account shared access signature: <none supplied>\n")
    cat(sprintf("Storage API version: %s\n", x$api_version))
    invisible(x)
}


#' Generic upload and download
#'
#' @param src,dest The source and destination files/URLs. Paths are allowed.
#' @param key,sas Authentication arguments: an access key or a shared access signature (SAS). If a key is is provided, the SAS is not used. If neither an access key nor a SAS are provided, only public (anonymous) access to the share is possible.
#' @param ... Further arguments to pass to lower-level functions.
#' @param overwrite For downloading, whether to overwrite any destination files that exist.
#'
#' @details
#' These functions allow you to transfer files to and from a storage account, given the URL of the destination (for uploading) or source (for downloading). They dispatch to [upload_azure_file]/[download_azure_file] for a file storage URL and [upload_blob]/[download_blob] for a blob storage URL respectively.
#'
#' You can provide a SAS either as part of the URL itself, or in the `sas` argument.
#'
#' @seealso
#' [download_azure_file], [download_blob], [az_storage]
#'
#' @examples
#' \dontrun{
#'
#' # authenticated download with an access key
#' download_from_url("https://mystorage.blob.core.windows.net/mycontainer/bigfile.zip",
#'                   "~/bigfile.zip",
#'                   key="access_key")
#'
#' }
#' @rdname file_transfer
#' @export
download_from_url <- function(src, dest, key=NULL, sas=NULL, ..., overwrite=FALSE)
{
    az_path <- parse_storage_url(src)
    if(is.null(sas))
        sas <- find_sas(src)
    endpoint <- storage_endpoint(az_path[1], key=key, sas=sas, ...)

    if(inherits(endpoint, "blob_endpoint"))
    {
        cont <- blob_container(endpoint, az_path[2])
        download_blob(cont, az_path[3], dest, overwrite=overwrite)
    }
    else if(inherits(endpoint, "file_endpoint"))
    {
        share <- file_share(endpoint, az_path[2])
        download_azure_file(share, az_path[3], dest, overwrite=overwrite)
    }
    else stop("Unknown storage endpoint", call.=FALSE)
}


#' @rdname file_transfer
#' @export
upload_to_url <- function(src, dest, key=NULL, sas=NULL, ...)
{
    az_path <- parse_storage_url(dest)
    if(is.null(sas))
        sas <- find_sas(dest)
    endpoint <- storage_endpoint(az_path[1], key=key, sas=sas, ...)

    if(inherits(endpoint, "blob_endpoint"))
    {
        cont <- blob_container(endpoint, az_path[2])
        upload_blob(cont, src, az_path[3])
    }
    else if(inherits(endpoint, "file_endpoint"))
    {
        share <- file_share(endpoint, az_path[2])
        upload_azure_file(share, src, az_path[3])
    }
    else stop("Unknown storage endpoint", call.=FALSE)
}


find_sas <- function(url)
{
    querymark <- regexpr("\\?.+$", url)
    if(querymark == -1)
        NULL
    else substr(url, querymark + 1, nchar(url))
}

