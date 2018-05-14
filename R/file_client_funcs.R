#' @export
list_file_shares <- function(endpoint)
{
    stopifnot(inherits(endpoint, "file_endpoint"))
    lst <- do_storage_call(endpoint$url, "/", options=list(comp="list"),
                           key=endpoint$key, sas=endpoint$sas, api_version=endpoint$api_version)

    lst <- lapply(lst$Shares, function(cont) file_share(endpoint, cont$Name[[1]]))
    named_list(lst)
}


#' @export
file_share <- function(endpoint, name, key=NULL, sas=NULL, api_version=getOption("azure_storage_api_version"))
{
    if(missing(name) && is_url(endpoint))
    {
        stor_path <- parse_storage_url(endpoint)
        endpoint <- storage_endpoint(stor_path[1], key, sas, api_version)
        name <- stor_path[2]
    }

    obj <- list(name=name, endpoint=endpoint)
    class(obj) <- "file_share"
    obj
}


#' @export
create_file_share <- function(endpoint, name, key=NULL, sas=NULL,
                                 api_version=getOption("azure_storage_api_version"),
                                 ...)
{
    if(missing(name) && is_url(endpoint))
    {
        stor_path <- parse_storage_url(endpoint)
        name <- stor_path[2]
        endpoint <- storage_endpoint(stor_path[1], key, sas, api_version)
    }

    obj <- file_share(endpoint, name)
    do_container_op(obj, options=list(restype="share"), headers=list(...), http_verb="PUT")
    obj
}


#' @export
delete_file_share <- function(share, confirm=TRUE)
{
    if(confirm && interactive())
    {
        endp <- share$endpoint
        path <- paste0(endp$url, endp$name, "/")
        yn <- readline(paste0("Are you sure you really want to delete the share '", path, "'? (y/N) "))
        if(tolower(substr(yn, 1, 1)) != "y")
            return(invisible(NULL))
    }
    do_container_op(share, options=list(restype="share"), http_verb="DELETE")
}


#' @export
list_azure_files <- function(share, dir)
{
    lst <- do_container_op(share, dir, options=list(comp="list", restype="directory"))
    unname(vapply(lst$Entries, function(b) b$Name[[1]], FUN.VALUE=character(1)))
}


#' @export
upload_azure_file <- function(share, src, dest)
{
    # TODO: upload in chunks 
    body <- readBin(src, "raw", file.info(src)$size)

    # first, create the file
    headers <- list("x-ms-type"="file",
                    "x-ms-content-length"=length(body))
    do_container_op(share, dest, headers=headers, http_verb="PUT")

    # then write the bytes into it
    hash <- openssl::base64_encode(openssl::md5(body))
    options <- list(comp="range")
    headers <- list("content-length"=length(body),
                    "range"=paste0("bytes=0-", length(body) - 1),
                    "content-md5"=hash,
                    "content-type"="application/octet-stream",
                    "x-ms-write"="Update")

    do_container_op(share, dest, options=options, headers=headers, body=body, http_verb="PUT")
}


#' @export
download_azure_file <- function(share, src, dest, overwrite=FALSE)
{
    do_container_op(share, src, config=httr::write_disk(dest, overwrite))
}


#' @export
delete_azure_file <- function(share, file, confirm=TRUE)
{
    if(confirm && interactive())
    {
        endp <- share$endpoint
        path <- paste0(endp$url, endp$name, file, "/")
        yn <- readline(paste0("Are you sure you really want to delete '", path, "'? (y/N) "))
        if(tolower(substr(yn, 1, 1)) != "y")
            return(invisible(NULL))
    }
    do_container_op(share, file, http_verb="DELETE")
}


#' @export
create_azure_dir <- function(share, dir)
{
    do_container_op(share, dir, options=list(restype="directory"), http_verb="PUT")
}


#' @export
delete_azure_dir <- function(share, dir, confirm=TRUE)
{
    if(confirm && interactive())
    {
        endp <- share$endpoint
        path <- paste0(endp$url, endp$name, dir, "/")
        yn <- readline(paste0("Are you sure you really want to delete directory '", path, "'? (y/N) "))
        if(tolower(substr(yn, 1, 1)) != "y")
            return(invisible(NULL))
    }
    do_container_op(share, file, options=list(restype="directory"), http_verb="DELETE")
}

