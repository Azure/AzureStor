#' @export
az_blob_endpoint <- function(endpoint, key=NULL, sas=NULL, api_version=getOption("azure_storage_api_version"))
{
    if(!grepl(".blob.", endpoint, fixed=TRUE))
        stop("Not a blob storage endpoint", call.=FALSE)
    obj <- list(url=endpoint, key=key, sas=sas, api_version=api_version)
    class(obj) <- "blob_endpoint"
    obj
}


#' @export
az_list_blob_containers <- function(endpoint)
{
    stopifnot(inherits(endpoint, "blob_endpoint"))
    lst <- do_storage_call(endpoint$url, "/", options=list(comp="list"),
                           key=endpoint$key, sas=endpoint$sas, api_version=endpoint$api_version)

    lst <- lapply(lst$Containers, function(cont) az_blob_container(endpoint, cont$Name[[1]]))
    named_list(lst)
}


#' @export
az_blob_container <- function(endpoint, name, key=NULL, sas=NULL, api_version=getOption("azure_storage_api_version"))
{
    if(missing(name) && is_url(endpoint))
    {
        stor_path <- parse_storage_url(endpoint)
        name <- stor_path[2]
        endpoint <- az_blob_endpoint(stor_path[1], key, sas, api_version)
    }

    obj <- list(name=name, endpoint=endpoint)
    class(obj) <- "blob_container"
    obj
}


#' @export
az_create_blob_container <- function(endpoint, name, key=NULL, sas=NULL,
                                     api_version=getOption("azure_storage_api_version"),
                                     public_access=c("none", "blob", "container"))
{
    if(missing(name) && is_url(endpoint))
    {
        stor_path <- parse_storage_url(endpoint)
        name <- stor_path[2]
        endpoint <- az_blob_endpoint(stor_path[1], key, sas, api_version)
    }

    public_access <- match.arg(public_access)
    headers <- if(public_access != "none")
        list("x-ms-blob-public-access"=public_access)
    else list()

    obj <- az_blob_container(endpoint, name)
    do_container_op(obj, options=list(restype="container"), headers=headers, http_verb="PUT")
    obj
}


#' @export
az_delete_blob_container <- function(container, confirm=TRUE)
{
    if(confirm && interactive())
    {
        endp <- container$endpoint
        path <- paste0(endp$url, endp$name, "/")
        yn <- readline(paste0("Are you sure you really want to delete the container '", path, "'? (y/N) "))
        if(tolower(substr(yn, 1, 1)) != "y")
            return(invisible(NULL))
    }

    do_container_op(container, options=list(restype="container"), http_verb="DELETE")
}


#' @export
az_list_blobs <- function(container)
{
    lst <- do_container_op(container, options=list(comp="list", restype="container"))
    if(is_empty(lst$Blobs))
        list()
    else unname(sapply(lst$Blobs, function(b) b$Name[[1]]))
}


#' @export
az_upload_blob <- function(container, src, dest, type="BlockBlob")
{
    body <- readBin(src, "raw", file.info(src)$size)
    hash <- openssl::base64_encode(openssl::md5(body))

    headers <- list("content-length"=length(body),
                    "content-md5"=hash,
                    "content-type"="application/octet-stream",
                    "x-ms-blob-type"=type)

    do_container_op(container, dest, headers=headers, body=body,
                 http_verb="PUT")
}


#' @export
az_download_blob <- function(container, src, dest, overwrite=FALSE)
{
    do_container_op(container, src, config=httr::write_disk(dest, overwrite))
}


#' @export
az_delete_blob <- function(container, blob, confirm=TRUE)
{
    if(confirm && interactive())
    {
        endp <- container$endpoint
        path <- paste0(endp$url, endp$name, blob, "/")
        yn <- readline(paste0("Are you sure you really want to delete '", path, "'? (y/N) "))
        if(tolower(substr(yn, 1, 1)) != "y")
            return(invisible(NULL))
    }

    do_container_op(container, blob, http_verb="DELETE")
}

