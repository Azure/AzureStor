#' Operations on an Azure Data Lake Storage Gen2 endpoint
#'
#' Get, list, create, or delete ADLSgen2 filesystems. Currently (as of December 2018) ADLSgen2 is in general-access public preview.
#'
#' @param endpoint Either an ADLSgen2 endpoint object as created by [storage_endpoint] or [adls_endpoint], or a character string giving the URL of the endpoint.
#' @param key,sas If an endpoint object is not supplied, authentication details. Currently the `sas` argument is unused.
#' @param api_version If an endpoint object is not supplied, the storage API version to use when interacting with the host. Currently defaults to `"2018-06-17"`.
#' @param name The name of the filesystem to get, create, or delete.
#' @param confirm For deleting a filesystem, whether to ask for confirmation.
#' @param x For the print method, a file share object.
#' @param ... Further arguments passed to lower-level functions.
#'
#' @details
#' You can call these functions in a couple of ways: by passing the full URL of the share, or by passing the endpoint object and the name of the share as a string.
#'
#' If hierarchical namespaces are enabled, there is no interoperability of the blob and ADLSgen2 storage systems. Blob containers will show up in listings of ADLS filesystems, and vice-versa, but the _contents_ of the storage are independent: files that are uploaded as blobs cannot be accessed via ADLS methods, and similarly, files and directories created via ADLS will be invisible to blob methods. Full interoperability between blobs and ADLS is planned for 2019.
#'
#' @return
#' For `adls_filesystem` and `create_adls_filesystem`, an S3 object representing an existing or created filesystem respectively.
#'
#' For `list_adls_filesystems`, a list of such objects.
#'
#' @seealso [storage_endpoint], [az_storage]
#'
#' @examples
#' \dontrun{
#'
#' endp <- adls_endpoint("https://mystorage.dfs.core.windows.net/", key="access_key")
#'
#' # list ADLSgen2 filesystems
#' list_adls_filesystems(endp)
#'
#' # get, create, and delete a filesystem
#' adls_filesystem(endp, "myfs")
#' create_adls_filesystem(endp, "newfs")
#' delete_adls_filesystem(endp, "newfs")
#'
#' # alternative way to do the same
#' adls_filesystem("https://mystorage.dfs.core.windows.net/myfs", key="access_key")
#' create_adls_filesystem("https://mystorage.dfs.core.windows.net/newfs", key="access_key")
#' delete_adls_filesystem("https://mystorage.dfs.core.windows.net/newfs", key="access_key")
#'
#' }
#' @rdname adls_filesystem
#' @export
adls_filesystem <- function(endpoint, ...)
{
    UseMethod("adls_filesystem")
}

#' @rdname adls_filesystem
#' @export
adls_filesystem.character <- function(endpoint, key=NULL, sas=NULL,
                                      api_version=getOption("azure_storage_api_version"),
                                      ...)
{
    do.call(adls_filesystem, generate_endpoint_container(endpoint, key, sas, api_version))
}

#' @rdname adls_filesystem
#' @export
adls_filesystem.adls_endpoint <- function(endpoint, name, ...)
{
    obj <- list(name=name, endpoint=endpoint)
    class(obj) <- "adls_filesystem"
    obj
}

#' @rdname adls_filesystem
#' @export
print.adls_filesystem <- function(x, ...)
{
    cat("Azure Data Lake Storage Gen2 filesystem '", x$name, "'\n", sep="")
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



#' @rdname adls_filesystem
#' @export
list_adls_filesystems <- function(endpoint, ...)
{
    UseMethod("list_adls_filesystems")
}

#' @rdname adls_filesystem
#' @export
list_adls_filesystems.character <- function(endpoint, key=NULL, sas=NULL,
                                            api_version=getOption("azure_adls_api_version"),
                                            ...)
{
    do.call(list_adls_filesystems, generate_endpoint_container(endpoint, key, sas, api_version))
}

#' @rdname adls_filesystem
#' @export
list_adls_filesystems.adls_endpoint <- function(endpoint, ...)
{
    lst <- do_storage_call(endpoint$url, "/", options=list(resource="account"),
                           key=endpoint$key, sas=endpoint$sas, api_version=endpoint$api_version)

    sapply(lst$filesystems$name, function(fs) adls_filesystem(endpoint, fs), simplify=FALSE)
}



#' @rdname adls_filesystem
#' @export
create_adls_filesystem <- function(endpoint, ...)
{
    UseMethod("create_adls_filesystem")
}

#' @rdname adls_filesystem
#' @export
create_adls_filesystem.character <- function(endpoint, key=NULL, sas=NULL,
                                             api_version=getOption("azure_adls_api_version"),
                                             ...)
{
    endp <- generate_endpoint_container(endpoint, key, sas, api_version)
    create_adls_filesystem(endp$endpoint, endp$name, ...)
}

#' @rdname adls_filesystem
#' @export
create_adls_filesystem.adls_filesystem <- function(endpoint, ...)
{
    create_adls_filesystem(endpoint$endpoint, endpoint$name)
}

#' @rdname adls_filesystem
#' @export
create_adls_filesystem.adls_endpoint <- function(endpoint, name, ...)
{
    obj <- adls_filesystem(endpoint, name)
    do_container_op(obj, options=list(resource="filesystem"), http_verb="PUT")
    obj
}



#' @rdname adls_filesystem
#' @export
delete_adls_filesystem <- function(endpoint, ...)
{
    UseMethod("delete_adls_filesystem")
}

#' @rdname adls_filesystem
#' @export
delete_adls_filesystem.character <- function(endpoint, key=NULL, sas=NULL,
                                             api_version=getOption("azure_adls_api_version"),
                                             ...)
{
    endp <- generate_endpoint_container(endpoint, key, sas, api_version)
    delete_adls_filesystem(endp$endpoint, endp$name, ...)
}

#' @rdname adls_filesystem
#' @export
delete_adls_filesystem.adls_filesystem <- function(endpoint, ...)
{
    delete_adls_filesystem(endpoint$endpoint, endpoint$name, ...)
}

#' @rdname adls_filesystem
#' @export
delete_adls_filesystem.adls_endpoint <- function(endpoint, name, confirm=TRUE, ...)
{
    if(confirm && interactive())
    {
        path <- paste0(endpoint$url, name)
        yn <- readline(paste0("Are you sure you really want to delete the filesystem '", path, "'? (y/N) "))
        if(tolower(substr(yn, 1, 1)) != "y")
            return(invisible(NULL))
    }

    obj <- adls_filesystem(endpoint, name)
    do_container_op(obj, options=list(resource="filesystem"), http_verb="DELETE")
}


#' Operations on an Azure Data Lake Storage Gen2 filesystem
#'
#' Upload, download, or delete a file; list files in a directory; create or delete directories.
#'
#' @param filesystem An ADLSgen2 filesystem object.
#' @param dir,file A string naming a directory or file respectively.
#' @param info Whether to return names only, or all information in a directory listing.
#' @param src,dest The source and destination filenames for uploading and downloading. Paths are allowed.
#' @param confirm Whether to ask for confirmation on deleting a file or directory.
#' @param blocksize The number of bytes to upload per HTTP(S) request.
#' @param lease The lease for a file, if present.
#' @param overwrite When downloading, whether to overwrite an existing destination file.
#' @param recursive For `list_adls_files`, and `delete_adls_dir`, whether the operation should recurse through subdirectories. For `delete_adls_dir`, this must be TRUE to delete a non-empty directory.
#'
#' @return
#' For `list_adls_files`, if `info="name"`, a vector of file/directory names. If `info="all"`, a data frame giving the file size and whether each object is a file or directory.
#'
#' @seealso
#' [adls_filesystem], [az_storage]
#'
#' @examples
#' \dontrun{
#'
#' fs <- adls_filesystem("https://mystorage.dfs.core.windows.net/myfilesystem", key="access_key")
#'
#' list_adls_files(fs, "/")
#'
#' create_adls_dir(fs, "/newdir")
#'
#' upload_adls_file(fs, "~/bigfile.zip", dest="/newdir/bigfile.zip")
#' download_adls_file(fs, "/newdir/bigfile.zip", dest="~/bigfile_downloaded.zip")
#'
#' delete_adls_file(fs, "/newdir/bigfile.zip")
#' delete_adls_dir(fs, "/newdir")
#'
#' }
#' @rdname adls
#' @export
list_adls_files <- function(filesystem, dir="/", info=c("all", "name"),
                            recursive=FALSE)
{
    info <- match.arg(info)

    opts <- list(recursive=tolower(as.character(recursive)), resource="filesystem")
    opts <- c(opts, directory=as.character(dir))

    lst <- do_container_op(filesystem, "", options=opts)
    if(info == "all")
    {
        out <- lst$paths

        # cater for null output
        if(is_empty(out))
            return(data.frame(
                name=character(0),
                contentLength=numeric(0),
                isDirectory=logical(0),
                lastModified=numeric(0)))

        # normalise output
        if(is.null(out$isDirectory))
            out$isDirectory <- FALSE
        else out$isDirectory <- !is.na(out$isDirectory)
        if(is.null(out$contentLength))
            out$contentLength <- 0
        else out$contentLength[is.na(out$contentLength)] <- 0
        if(is.null(out$etag))
            out$etag <- ""
        else out$etag[is.na(out$etag)] <- ""
        if(is.null(out$permissions))
            out$permissions <- ""
        else out$permissions[is.na(out$permissions)] <- ""
        out <- out[c("name", "contentLength", "isDirectory", "lastModified", "permissions", "etag")]

        if(all(out$permissions == ""))
            out$permissions <- NULL
        if(all(out$etag == ""))
            out$etag <- NULL
        out
    }
    else as.character(lst$paths$name)
}


#' @rdname adls
#' @export
upload_adls_file <- function(filesystem, src, dest, blocksize=2^24, lease=NULL)
{
    con <- if(inherits(src, "textConnection"))
        rawConnection(charToRaw(paste0(readLines(src), collapse="\n")))
    else file(src, open="rb")
    on.exit(close(con))

    # create the file
    content_type <- mime::guess_type(src)
    headers <- list(`x-ms-content-type`=content_type)
    #if(!is.null(lease))
        #headers[["x-ms-lease-id"]] <- as.character(lease)
    do_container_op(filesystem, dest, options=list(resource="file"), headers=headers, http_verb="PUT")

    # transfer the contents
    blocklist <- list()
    pos <- 0
    while(1)
    {
        body <- readBin(con, "raw", blocksize)
        thisblock <- length(body)
        if(thisblock == 0)
            break

        headers <- list(
            `content-type`="application/octet-stream",
            `content-length`=sprintf("%.0f", thisblock)
        )
        opts <- list(action="append", position=sprintf("%.0f", pos))

        do_container_op(filesystem, dest, options=opts, headers=headers, body=body, http_verb="PATCH")
        pos <- pos + thisblock
    }

    # flush contents
    do_container_op(filesystem, dest,
        options=list(action="flush", position=sprintf("%.0f", pos)),
        http_verb="PATCH")
}


#' @rdname adls
#' @export
download_adls_file <- function(filesystem, src, dest, overwrite=FALSE)
{
    do_container_op(filesystem, src, config=httr::write_disk(dest, overwrite))
}



#' @rdname adls
#' @export
delete_adls_file <- function(filesystem, file, confirm=TRUE)
{
    if(confirm && interactive())
    {
        endp <- filesystem$endpoint
        path <- paste0(endp$url, filesystem$name, "/", file)
        yn <- readline(paste0("Are you sure you really want to delete '", path, "'? (y/N) "))
        if(tolower(substr(yn, 1, 1)) != "y")
            return(invisible(NULL))
    }

    do_container_op(filesystem, file, http_verb="DELETE")
}



#' @rdname adls
#' @export
create_adls_dir <- function(filesystem, dir)
{
    do_container_op(filesystem, dir, options=list(resource="directory"), http_verb="PUT")
}


#' @rdname adls
#' @export
delete_adls_dir <- function(filesystem, dir, recursive=FALSE, confirm=TRUE)
{
    if(confirm && interactive())
    {
        endp <- filesystem$endpoint
        path <- paste0(endp$url, filesystem$name, "/", dir)
        yn <- readline(paste0("Are you sure you really want to delete directory '", path, "'? (y/N) "))
        if(tolower(substr(yn, 1, 1)) != "y")
            return(invisible(NULL))
    }

    opts <- list(recursive=tolower(as.character(recursive)))
    do_container_op(filesystem, dir, options=opts, http_verb="DELETE")
}

