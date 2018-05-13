#' @export
blob_connection <- function(endpoint, key=NULL, sas=NULL, api_version=getOption("azure_storage_api_version"))
{
    obj <- list(endpoint=endpoint, key=key, sas=sas, api_version=api_version)
    class(obj) <- "blob_connection"
    obj
}


#' @export
list_blob_containers <- function(blob_con)
{
    stopifnot(inherits(blob_con, "blob_connection"))
    lst <- do_storage_call(blob_con$endpoint, "/", options=list(comp="list"),
                           key=blob_con$key, sas=blob_con$sas, api_version=blob_con$api_version)

    lst <- lapply(lst$Containers, function(cont) blob_container(blob_con, cont$Name[[1]]))
    named_list(lst)
}


#' @export
blob_container <- function(blob_con, name, key=NULL, sas=NULL, api_version=getOption("azure_storage_api_version"))
{
    if(missing(name) && is_url(blob_con))
    {
        stor_path <- parse_storage_url(blob_con)
        name <- stor_path[2]
        blob_con <- blob_connection(stor_path[1], key, sas, api_version)
    }

    obj <- list(name=name, con=blob_con)
    class(obj) <- "blob_container"
    obj
}


#' @export
create_blob_container <- function(blob_con, name, key=NULL, sas=NULL, api_version=getOption("azure_storage_api_version"),
                                  public_access=c("none", "blob", "container"))
{
    if(missing(name) && is_url(blob_con))
    {
        stor_path <- parse_storage_url(blob_con)
        name <- stor_path[2]
        blob_con <- blob_connection(stor_path[1], key, sas, api_version)
    }

    public_access <- match.arg(public_access)
    headers <- if(public_access != "none")
        list("x-ms-blob-public-access"=public_access)
    else list()

    obj <- blob_container(blob_con, name)
    container_op(obj, options=list(restype="container"), headers=headers, http_verb="PUT")
    obj
}


#' @export
delete_blob_container <- function(container, confirm=TRUE)
{
    if(confirm && interactive())
    {
        con <- container$con
        path <- paste0(con$endpoint, con$name, "/")
        yn <- readline(paste0("Are you sure you really want to delete blob container '", path, "'? (y/N) "))
        if(tolower(substr(yn, 1, 1)) != "y")
            return(invisible(NULL))
    }

    container_op(container, options=list(restype="container"), http_verb="DELETE")
    invisible(NULL)
}


#' @export
list_blobs <- function(container)
{
    lst <- container_op(container, options=list(comp="list", restype="container"))
    if(is_empty(lst$Blobs))
        list()
    else unname(sapply(lst$Blobs, function(b) b$Name[[1]]))
}


#' @export
upload_blob <- function(container, src, dest, type="BlockBlob")
{
    body <- readBin(src, "raw", file.info(src)$size)
    hash <- openssl::base64_encode(openssl::md5(body))

    headers <- list("content-length"=length(body),
                    "content-md5"=hash,
                    "content-type"="application/octet-stream",
                    "x-ms-blob-type"=type)

    container_op(container, dest, headers=headers, body=body,
                 http_verb="PUT")
}


#' @export
download_blob <- function(container, src, dest, overwrite=FALSE)
{
    container_op(container, src, config=httr::write_disk(dest, overwrite))
}


#' @export
delete_blob <- function(container, blob, confirm=TRUE)
{
    if(confirm && interactive())
    {
        con <- container$con
        path <- paste0(con$endpoint, con$name, blob, "/")
        yn <- readline(paste0("Are you sure you really want to delete blob '", path, "'? (y/N) "))
        if(tolower(substr(yn, 1, 1)) != "y")
            return(invisible(NULL))
    }

    container_op(container, blob, http_verb="DELETE")
}


container_op=function(container, blob="", options=list(), headers=list(), http_verb="GET", ...)
{
    con <- container$con
    path <- paste0(container$name, "/", blob)
    do_storage_call(con$endpoint, path, options=options, headers=headers,
                    key=con$key, sas=con$sas, api_version=con$api_version,
                    http_verb=http_verb, ...)
}


parse_storage_url <- function(url)
{
    url <- httr::parse_url(url)
    endpoint <- get_hostroot(url)
    store <- sub("/.*$", "", url$path)
    path <- sub("^[^/]+/", "", url$path)
    c(endpoint, store, path)
}
