#' Operations on a file endpoint
#'
#' Get, list, create, or delete file shares.
#'
#' @param endpoint Either a file endpoint object as created by [storage_endpoint], or a character string giving the URL of the endpoint.
#' @param key,sas If an endpoint object is not supplied, authentication details. If a key is provided, the SAS is not used. If neither an access key nor a SAS are provided, only public (anonymous) access to the share is possible.
#' @param api_version If an endpoint object is not supplied, the storage API version to use when interacting with the host. Currently defaults to `"2017-07-29"`.
#' @param name The name of the file share to get, create, or delete.
#' @param confirm For deleting a share, whether to ask for confirmation.
#' @param ... Further arguments passed to lower-level functions.
#'
#' @details
#' You can call these functions in a couple of ways: by passing the full URL of the share, or by passing the endpoint object and the name of the share as a string.
#'
#' @return
#' For `file_share` and `create_file_share`, an S3 object representing an existing or created share respectively.
#'
#' For `list_file_shares`, a list of such objects.
#'
#' @seealso [storage_endpoint], [az_storage]
#'
#' @rdname file_endpoint
#' @export
file_share <- function(endpoint, ...)
{
    UseMethod("file_share")
}

#' @rdname file_endpoint
#' @export
file_share.character <- function(endpoint, key=NULL, sas=NULL, api_version=getOption("azure_storage_api_version"))
{
    do.call(file_share, generate_endpoint_container(endpoint, key, sas, api_version))
}

#' @rdname file_endpoint
#' @export
file_share.file_endpoint <- function(endpoint, name)
{
    obj <- list(name=name, endpoint=endpoint)
    class(obj) <- "file_share"
    obj
}

#' @rdname file_endpoint
#' @export
print.file_share <- function(object, ...)
{
    cat("Azure file share '", object$name, "'\n", sep="")
    cat(sprintf("URL: %s\n", paste0(object$endpoint$url, object$name)))
    if(!is_empty(object$endpoint$key))
        cat("Access key: <hidden>\n")
    else cat("Access key: <none supplied>\n")
    if(!is_empty(object$endpoint$sas))
        cat("Account shared access signature: <hidden>\n")
    else cat("Account shared access signature: <none supplied>\n")
    cat(sprintf("Storage API version: %s\n", object$endpoint$api_version))
    invisible(object)
}



#' @rdname file_endpoint
#' @export
list_file_shares <- function(endpoint, ...)
{
    UseMethod("list_file_shares")
}

#' @rdname file_endpoint
#' @export
list_file_shares.character <- function(endpoint, key=NULL, sas=NULL,
                                       api_version=getOption("azure_storage_api_version"))
{
    do.call(list_file_shares, generate_endpoint_container(endpoint, key, sas, api_version))
}

#' @rdname file_endpoint
#' @export
list_file_shares.file_endpoint <- function(endpoint, ...)
{
    lst <- do_storage_call(endpoint$url, "/", options=list(comp="list"),
                           key=endpoint$key, sas=endpoint$sas, api_version=endpoint$api_version)

    lst <- lapply(lst$Shares, function(cont) file_share(endpoint, cont$Name[[1]]))
    named_list(lst)
}



#' @rdname file_endpoint
#' @export
create_file_share <- function(endpoint, ...)
{
    UseMethod("create_file_share")
}

#' @rdname file_endpoint
#' @export
create_file_share.character <- function(endpoint, key=NULL, sas=NULL,
                                        api_version=getOption("azure_storage_api_version"),
                                        ...)
{
    endp <- generate_endpoint_container(endpoint, key, sas, api_version)
    create_file_share(endp$endpoint, endp$name, ...)
}

#' @rdname file_endpoint
#' @export
create_file_share.file_endpoint <- function(endpoint, name, ...)
{
    obj <- file_share(endpoint, name)
    do_container_op(obj, options=list(restype="share"), headers=list(...), http_verb="PUT")
    obj
}



#' @rdname file_endpoint
#' @export
delete_file_share <- function(endpoint, ...)
{
    UseMethod("delete_file_share")
}

#' @rdname file_endpoint
#' @export
delete_file_share.character <- function(endpoint, key=NULL, sas=NULL,
                                        api_version=getOption("azure_storage_api_version"),
                                        ...)
{
    endp <- generate_endpoint_container(endpoint, key, sas, api_version)
    delete_file_share(endp$endpoint, endp$name, ...)
}

#' @rdname file_endpoint
#' @export
delete_file_share.file_endpoint <- function(endpoint, name, confirm=TRUE)
{
    if(confirm && interactive())
    {
        path <- paste0(endpoint$url, name)
        yn <- readline(paste0("Are you sure you really want to delete the share '", path, "'? (y/N) "))
        if(tolower(substr(yn, 1, 1)) != "y")
            return(invisible(NULL))
    }

    obj <- file_share(endpoint, name)
    do_container_op(obj, options=list(restype="share"), http_verb="DELETE")
}


#' Operations on a file share
#'
#' Upload, download, or delete a file; list files in a directory; create or delete directories.
#'
#' @param share A file share object.
#' @param dir,file A string naming a directory or file respectively.
#' @param all_info Whether to return names only, or all information in a directory listing.
#' @param src,dest The source and destination filenames for uploading and downloading. Paths are allowed.
#' @param confirm Whether to ask for confirmation on deleting a file or directory.
#'
#' @return
#' For `list_azure_files`, if `all_info=FALSE`, a vector of file/directory names. If `all_info=TRUE`, a data frame giving the file size and whether each object is a file or directory.
#'
#' @seealso
#' [file_share], [az_storage]
#'
#' @rdname file_share
#' @export
list_azure_files <- function(share, dir, all_info=TRUE)
{
    lst <- do_container_op(share, dir, options=list(comp="list", restype="directory"))

    name <- vapply(lst$Entries, function(ent) ent$Name[[1]], FUN.VALUE=character(1))
    if(!all_info)
        return(name)
 
    type <- if(is_empty(name)) character(0) else names(name)
    size <- vapply(lst$Entries,
                   function(ent) if(is_empty(ent$Properties)) NA_character_ else ent$Properties$`Content-Length`[[1]],
                   FUN.VALUE=character(1))

    data.frame(name=name, type=type, size=as.numeric(size), stringsAsFactors=FALSE)
}

#' @rdname file_share
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

#' @rdname file_share
#' @export
download_azure_file <- function(share, src, dest, overwrite=FALSE)
{
    do_container_op(share, src, config=httr::write_disk(dest, overwrite))
}

#' @rdname file_share
#' @export
delete_azure_file <- function(share, file, confirm=TRUE)
{
    if(confirm && interactive())
    {
        endp <- share$endpoint
        path <- paste0(endp$url, share$name, "/", file)
        yn <- readline(paste0("Are you sure you really want to delete '", path, "'? (y/N) "))
        if(tolower(substr(yn, 1, 1)) != "y")
            return(invisible(NULL))
    }
    do_container_op(share, file, http_verb="DELETE")
}

#' @rdname file_share
#' @export
create_azure_dir <- function(share, dir)
{
    do_container_op(share, dir, options=list(restype="directory"), http_verb="PUT")
}

#' @rdname file_share
#' @export
delete_azure_dir <- function(share, dir, confirm=TRUE)
{
    if(confirm && interactive())
    {
        endp <- share$endpoint
        path <- paste0(endp$url, share$name, "/", dir)
        yn <- readline(paste0("Are you sure you really want to delete directory '", path, "'? (y/N) "))
        if(tolower(substr(yn, 1, 1)) != "y")
            return(invisible(NULL))
    }
    do_container_op(share, file, options=list(restype="directory"), http_verb="DELETE")
}

