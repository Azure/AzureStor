#' @export
storage_endpoint <- function(endpoint,
                             key=NULL, sas=NULL, api_version=getOption("azure_storage_api_version"),
                             type=c("blob", "file", "queue", "table"))
{
    if(missing(type)) # determine type of endpoint from url
    {
        type <- sapply(type, function(x) is_endpoint_url(endpoint, x))
        if(!any(type))
            stop("Unknown endpoint type", call.=FALSE)
        type <- names(type)[type]
    }
    else
    {
        type <- match.arg(type)
        if(!is_endpoint_url(endpoint, type))
            stop("Unknown endpoint type", call.=FALSE)
    }
    obj <- list(url=endpoint, key=key, sas=sas, api_version=api_version)
    class(obj) <- c(paste0(type, "_endpoint"), "storage_endpoint")
    obj
}


#' @export
download_from_url <- function(src, dest, ..., overwrite=FALSE)
{
    az_path <- parse_storage_url(src)
    endpoint <- storage_endpoint(az_path[1], ...)

    if(inherits(endpoint, "blob_endpoint"))
    {
        cont <- blob_container(endpoint, az_path[2])
        download_azure_blob(cont, az_path[3], dest, overwrite=overwrite)
    }
    else if(inherits(endpoint, "file_endpoint"))
    {
        share <- file_share(endpoint, az_path[2])
        download_azure_file(share, az_path[3], dest, overwrite=overwrite)
    }
    else stop("Unknown storage endpoint", call.=FALSE)
}


#' @export
upload_to_url <- function(src, dest, ...)
{
    az_path <- parse_storage_url(dest)
    endpoint <- storage_endpoint(az_path[1], ...)

    if(inherits(endpoint, "blob_endpoint"))
    {
        cont <- blob_container(endpoint, az_path[2])
        upload_azure_blob(cont, src, az_path[3])
    }
    else if(inherits(endpoint, "file_endpoint"))
    {
        share <- file_share(endpoint, az_path[2])
        upload_azure_file(share, src, az_path[3])
    }
    else stop("Unknown storage endpoint", call.=FALSE)
}


