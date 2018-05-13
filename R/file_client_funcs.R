#' @export
az_file_endpoint <- function(endpoint, key=NULL, sas=NULL, api_version=getOption("azure_storage_api_version"))
{
    if(!grepl(".file.", endpoint, fixed=TRUE))
        stop("Not a file storage endpoint", call.=FALSE)
    obj <- list(endpoint=endpoint, key=key, sas=sas, api_version=api_version)
    class(obj) <- "file_endpoint"
    obj
}


#' @export
az_list_file_shares <- function(fs_con)
{
    stopifnot(inherits(fs_con, "file_endpoint"))
    lst <- do_storage_call(fs_con$endpoint, "/", options=list(comp="list"),
                           key=fs_con$key, sas=fs_con$sas, api_version=fs_con$api_version)

    lst <- lapply(lst$Shares, function(cont) az_file_share(fs_con, cont$Name[[1]]))
    named_list(lst)
}


#' @export
az_file_share <- function(fs_con, name, key=NULL, sas=NULL, api_version=getOption("azure_storage_api_version"))
{
    if(missing(name) && is_url(fs_con))
    {
        stor_path <- parse_storage_url(fs_con)
        name <- stor_path[2]
        fs_con <- az_file_endpoint(stor_path[1], key, sas, api_version)
    }

    obj <- list(name=name, con=fs_con)
    class(obj) <- "file_share"
    obj
}


#' @export
az_create_file_share <- function(fs_con, name, key=NULL, sas=NULL, api_version=getOption("azure_storage_api_version"))
{
    if(missing(name) && is_url(fs_con))
    {
        stor_path <- parse_storage_url(fs_con)
        name <- stor_path[2]
        fs_con <- az_file_endpoint(stor_path[1], key, sas, api_version)
    }

    obj <- az_file_share(fs_con, name)
    container_op(obj, options=list(restype="share"), http_verb="PUT")
    obj
}


#' @export
az_delete_file_share <- function(share, confirm=TRUE)
{
    if(confirm && interactive())
    {
        con <- container$con
        path <- paste0(con$endpoint, con$name, "/")
        yn <- readline(paste0("Are you sure you really want to delete the share '", path, "'? (y/N) "))
        if(tolower(substr(yn, 1, 1)) != "y")
            return(invisible(NULL))
    }

    container_op(share, options=list(restype="share"), http_verb="DELETE")
}


#' @export
az_list_files <- function(share, dir)
{
    lst <- container_op(share, dir, options=list(comp="list", restype="directory"))
    if(is_empty(lst$Entries))
        list()
    else unname(sapply(lst$Entries, function(b) b$Name[[1]]))
}


#' @export
az_upload_file <- function(share, src, dest)
{
    body <- readBin(src, "raw", file.info(src)$size)

    # first, create the file
    headers <- list("x-ms-type"="file",
                    "x-ms-content-length"=length(body))
    container_op(share, dest, headers=headers, http_verb="PUT")

    # then write the bytes into it
    hash <- openssl::base64_encode(openssl::md5(body))
    options <- list(comp="range")
    headers <- list("content-length"=length(body),
                    "range"=paste0("bytes=0-", length(body) - 1),
                    "content-md5"=hash,
                    "content-type"="application/octet-stream",
                    "x-ms-write"="Update")

    container_op(share, dest, options=options, headers=headers, body=body, http_verb="PUT")
}


#' @export
az_download_file <- function(share, src, dest, overwrite=FALSE)
{
    container_op(share, src, config=httr::write_disk(dest, overwrite))
}


#' @export
az_delete_file <- function(share, file, confirm=TRUE)
{
    if(confirm && interactive())
    {
        con <- share$con
        path <- paste0(con$endpoint, con$name, file, "/")
        yn <- readline(paste0("Are you sure you really want to delete '", path, "'? (y/N) "))
        if(tolower(substr(yn, 1, 1)) != "y")
            return(invisible(NULL))
    }

    container_op(share, file, http_verb="DELETE")
}

