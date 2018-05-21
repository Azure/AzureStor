#' Create a storage endpoint object
#'
#' @param endpoint The URL (hostname) for the endpoint. This must be of the form `http[s]://{account-name}.{type}.{core-host-name}`, where `type` is one of `"blob"`, `"file"`, `"queue"` or `"table"`. On the public Azure cloud, endpoints will be of the form `https://{account-name}.{type}.core.windows.net`.
#' @param key The access key for the storage account.
#' @param sas A shared access signature (SAS) for the account. If `key` is also provided, the SAS is not used. If neither `key` nor `sas` are provided, only public (anonymous) access to the endpoint is possible.
#' @param api_version The storage API version to use when interacting with the host. Currently defaults to `"2017-07-29"`.
#'
#' @details
#' This is the starting point for the client-side storage interface in AzureRMR.
#'
#' @return
#' An object of S3 class `"blob_endpoint"`, `"file_endpoint"`, `"queue_endpoint"` or `"table_endpoint"` depending on the type of endpoint. All of these also inherit from class `"storage_endpoint"`.
#'
#' @seealso
#' [az_storage], [file_share], [create_file_share], [blob_container], [create_blob_container]
#'
#' @aliases endpoint blob_endpoint file_endpoint queue_endpoint table_endpoint
#' @export
storage_endpoint <- function(endpoint, key=NULL, sas=NULL, api_version=getOption("azure_storage_api_version"))
{
    if(is_empty(endpoint))
        stop("Invalid endpoint type", call.=FALSE)

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
print.storage_endpoint <- function(object)
{
    type <- sub("_endpoint$", "", class(object)[1])
    cat(sprintf("Azure %s storage endpoint\n", type))
    cat(sprintf("URL: %s\n", object$url))
    if(!is_empty(object$key))
        cat("Access key: <hidden>\n")
    else cat("Access key: <none supplied>\n")
    if(!is_empty(object$sas))
        cat("Account shared access signature: <hidden>\n")
    else cat("Account shared access signature: <none supplied>\n")
    cat(sprintf("Storage API version: %s", object$api_version))
    invisible(object)
}


#' Generic upload and download
#'
#' @param src,dest The source and destination files/URLs. Paths are allowed.
#' @param ... Further arguments to pass to lower-level functions. In particular, use `key` and/or `sas` to supply an access key or SAS. Without a key or SAS, only public (anonymous) access is possible.
#' @param overwrite For downloading, whether to overwrite any destination files that exist.
#'
#' @details
#' These functions allow you to transfer files to and from a storage account, given the URL of the destination (for uploading) or source (for downloading). They dispatch to [upload_azure_file]/[download_azure_file] for a file storage URL and [upload_blob]/[download_blob] for a blob storage URL respectively.
#'
#' @seealso
#' [download_azure_file], [download_blob]. [az_storage]
#'
#' @rdname file_transfer
#' @export
download_from_url <- function(src, dest, ..., overwrite=FALSE)
{
    az_path <- parse_storage_url(src)
    endpoint <- storage_endpoint(az_path[1], ...)

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
upload_to_url <- function(src, dest, ...)
{
    az_path <- parse_storage_url(dest)
    endpoint <- storage_endpoint(az_path[1], ...)

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


