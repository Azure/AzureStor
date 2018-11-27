#' Operations on a file endpoint
#'
#' Get, list, create, or delete file shares.
#'
#' @param endpoint Either a file endpoint object as created by [storage_endpoint], or a character string giving the URL of the endpoint.
#' @param key,sas If an endpoint object is not supplied, authentication details. If a key is provided, the SAS is not used. If neither an access key nor a SAS are provided, only public (anonymous) access to the share is possible.
#' @param api_version If an endpoint object is not supplied, the storage API version to use when interacting with the host. Currently defaults to `"2018-03-28"`.
#' @param name The name of the file share to get, create, or delete.
#' @param confirm For deleting a share, whether to ask for confirmation.
#' @param x For the print method, a file share object.
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
#' @examples
#' \dontrun{
#'
#' endp <- file_endpoint("https://mystorage.file.core.windows.net/", key="access_key")
#'
#' # list file shares
#' list_file_shares(endp)
#'
#' # get, create, and delete a file share
#' file_share(endp, "myshare")
#' create_file_share(endp, "newshare")
#' delete_file_share(endp, "newshare")
#'
#' # alternative way to do the same
#' file_share("https://mystorage.file.file.windows.net/myshare", key="access_key")
#' create_file_share("https://mystorage.file.core.windows.net/newshare", key="access_key")
#' delete_file_share("https://mystorage.file.core.windows.net/newshare", key="access_key")
#'
#' }
#' @rdname file_share
#' @export
file_share <- function(endpoint, ...)
{
    UseMethod("file_share")
}

#' @rdname file_share
#' @export
file_share.character <- function(endpoint, key=NULL, sas=NULL,
                                 api_version=getOption("azure_storage_api_version"),
                                 ...)
{
    do.call(file_share, generate_endpoint_container(endpoint, key, sas, api_version))
}

#' @rdname file_share
#' @export
file_share.file_endpoint <- function(endpoint, name, ...)
{
    obj <- list(name=name, endpoint=endpoint)
    class(obj) <- "file_share"
    obj
}

#' @rdname file_share
#' @export
print.file_share <- function(x, ...)
{
    cat("Azure file share '", x$name, "'\n", sep="")
    cat(sprintf("URL: %s\n", paste0(x$endpoint$url, x$name)))
    if(!is_empty(x$endpoint$key))
        cat("Access key: <hidden>\n")
    else cat("Access key: <none supplied>\n")
    if(!is_empty(x$endpoint$sas))
        cat("Account shared access signature: <hidden>\n")
    else cat("Account shared access signature: <none supplied>\n")
    cat(sprintf("Storage API version: %s\n", x$endpoint$api_version))
    invisible(x)
}



#' @rdname file_share
#' @export
list_file_shares <- function(endpoint, ...)
{
    UseMethod("list_file_shares")
}

#' @rdname file_share
#' @export
list_file_shares.character <- function(endpoint, key=NULL, sas=NULL,
                                       api_version=getOption("azure_storage_api_version"),
                                       ...)
{
    do.call(list_file_shares, generate_endpoint_container(endpoint, key, sas, api_version))
}

#' @rdname file_share
#' @export
list_file_shares.file_endpoint <- function(endpoint, ...)
{
    lst <- do_storage_call(endpoint$url, "/", options=list(comp="list"),
                           key=endpoint$key, sas=endpoint$sas, api_version=endpoint$api_version)

    lst <- lapply(lst$Shares, function(cont) file_share(endpoint, cont$Name[[1]]))
    named_list(lst)
}



#' @rdname file_share
#' @export
create_file_share <- function(endpoint, ...)
{
    UseMethod("create_file_share")
}

#' @rdname file_share
#' @export
create_file_share.character <- function(endpoint, key=NULL, sas=NULL,
                                        api_version=getOption("azure_storage_api_version"),
                                        ...)
{
    endp <- generate_endpoint_container(endpoint, key, sas, api_version)
    create_file_share(endp$endpoint, endp$name, ...)
}

#' @rdname file_share
#' @export
create_file_share.file_share <- function(endpoint, ...)
{
    create_file_share(endpoint$endpoint, endpoint$name)
}

#' @rdname file_share
#' @export
create_file_share.file_endpoint <- function(endpoint, name, ...)
{
    obj <- file_share(endpoint, name)
    do_container_op(obj, options=list(restype="share"), headers=list(...), http_verb="PUT")
    obj
}



#' @rdname file_share
#' @export
delete_file_share <- function(endpoint, ...)
{
    UseMethod("delete_file_share")
}

#' @rdname file_share
#' @export
delete_file_share.character <- function(endpoint, key=NULL, sas=NULL,
                                        api_version=getOption("azure_storage_api_version"),
                                        ...)
{
    endp <- generate_endpoint_container(endpoint, key, sas, api_version)
    delete_file_share(endp$endpoint, endp$name, ...)
}

#' @rdname file_share
#' @export
delete_file_share.file_share <- function(endpoint, ...)
{
    delete_file_share(endpoint$endpoint, endpoint$name, ...)
}

#' @rdname file_share
#' @export
delete_file_share.file_endpoint <- function(endpoint, name, confirm=TRUE, ...)
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
#' @param info Whether to return names only, or all information in a directory listing.
#' @param src,dest The source and destination filenames for uploading and downloading. Paths are allowed.
#' @param confirm Whether to ask for confirmation on deleting a file or directory.
#' @param blocksize The number of bytes to upload per HTTP(S) request.
#' @param overwrite When downloading, whether to overwrite an existing destination file.
#' @param prefix For `list_azure_files`, filters the result to return only files and directories whose name begins with this prefix.
#'
#' @return
#' For `list_azure_files`, if `info="name"`, a vector of file/directory names. If `info="all"`, a data frame giving the file size and whether each object is a file or directory.
#'
#' @seealso
#' [file_share], [az_storage]
#'
#' @examples
#' \dontrun{
#'
#' share <- file_share("https://mystorage.file.core.windows.net/myshare", key="access_key")
#'
#' list_azure_files(share, "/")
#'
#' create_azure_dir(share, "/newdir")
#'
#' upload_azure_file(share, "~/bigfile.zip", dest="/newdir/bigfile.zip")
#' download_azure_file(share, "/newdir/bigfile.zip", dest="~/bigfile_downloaded.zip")
#'
#' delete_azure_file(share, "/newdir/bigfile.zip")
#' delete_azure_dir(share, "/newdir")
#'
#' }
#' @rdname file
#' @export
list_azure_files <- function(share, dir, info=c("all", "name"),
                             prefix=NULL)
{
    info <- match.arg(info)

    opts <- list(comp="list", restype="directory")
    if(!is_empty(prefix))
        opts <- c(opts, prefix=as.character(prefix))

    lst <- do_container_op(share, dir, options=opts)

    name <- vapply(lst$Entries, function(ent) ent$Name[[1]], FUN.VALUE=character(1))
    if(info == "name")
        return(name)
 
    type <- if(is_empty(name)) character(0) else names(name)
    size <- vapply(lst$Entries,
                   function(ent) if(is_empty(ent$Properties)) NA_character_ else ent$Properties$`Content-Length`[[1]],
                   FUN.VALUE=character(1))

    data.frame(name=name, type=type, size=as.numeric(size), stringsAsFactors=FALSE)
}

#' @rdname file
#' @export
upload_azure_file <- function(share, src, dest, blocksize=2^24)
{
    if(inherits(src, "textConnection"))
    {
        src <- charToRaw(paste0(readLines(src), collapse="\n"))
        nbytes <- length(src)
        con <- rawConnection(src)
    }
    else
    {
        con <- file(src, open="rb")
        nbytes <- file.info(src)$size
    }
    on.exit(close(con))

    # first, create the file
    # ensure content-length is never exponential notation
    headers <- list("x-ms-type"="file",
                    "x-ms-content-length"=sprintf("%.0f", nbytes))
    do_container_op(share, dest, headers=headers, http_verb="PUT")

    # then write the bytes into it, one block at a time
    options <- list(comp="range")
    headers <- list("x-ms-write"="Update")

    # upload each block
    blocklist <- list()
    range_begin <- 0
    while(range_begin < nbytes)
    {
        body <- readBin(con, "raw", blocksize)
        thisblock <- length(body)
        if(thisblock == 0)  # sanity check
            break

        # ensure content-length is never exponential notation
        headers[["content-length"]] <- sprintf("%.0f", thisblock)
        headers[["range"]] <- sprintf("bytes=%s-%s", range_begin, range_begin + thisblock - 1)

        do_container_op(share, dest, headers=headers, body=body, options=options, http_verb="PUT")

        range_begin <- range_begin + thisblock
    }

    # set content type
    do_container_op(share, dest, headers=list("x-ms-content-type"=mime::guess_type(src)),
                    options=list(comp="properties"),
                    http_verb="PUT")
    invisible(NULL)
}

#' @rdname file
#' @export
download_azure_file <- function(share, src, dest, overwrite=FALSE)
{
    do_container_op(share, src, config=httr::write_disk(dest, overwrite))
}

#' @rdname file
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

#' @rdname file
#' @export
create_azure_dir <- function(share, dir)
{
    do_container_op(share, dir, options=list(restype="directory"), http_verb="PUT")
}

#' @rdname file
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
    do_container_op(share, dir, options=list(restype="directory"), http_verb="DELETE")
}

