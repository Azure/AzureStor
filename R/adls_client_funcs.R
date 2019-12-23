#' Operations on an Azure Data Lake Storage Gen2 endpoint
#'
#' Get, list, create, or delete ADLSgen2 filesystems.
#'
#' @param endpoint Either an ADLSgen2 endpoint object as created by [storage_endpoint] or [adls_endpoint], or a character string giving the URL of the endpoint.
#' @param key,token,sas If an endpoint object is not supplied, authentication credentials: either an access key, an Azure Active Directory (AAD) token, or a SAS, in that order of priority. Currently the `sas` argument is unused.
#' @param api_version If an endpoint object is not supplied, the storage API version to use when interacting with the host. Currently defaults to `"2018-11-09"`.
#' @param name The name of the filesystem to get, create, or delete.
#' @param confirm For deleting a filesystem, whether to ask for confirmation.
#' @param x For the print method, a filesystem object.
#' @param ... Further arguments passed to lower-level functions.
#'
#' @details
#' You can call these functions in a couple of ways: by passing the full URL of the filesystem, or by passing the endpoint object and the name of the filesystem as a string.
#'
#' If authenticating via AAD, you can supply the token either as a string, or as an object of class AzureToken, created via [AzureRMR::get_azure_token]. The latter is the recommended way of doing it, as it allows for automatic refreshing of expired tokens.
#'
#' @return
#' For `adls_filesystem` and `create_adls_filesystem`, an S3 object representing an existing or created filesystem respectively.
#'
#' For `list_adls_filesystems`, a list of such objects.
#'
#' @seealso
#' [storage_endpoint], [az_storage], [storage_container]
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
adls_filesystem.character <- function(endpoint, key=NULL, token=NULL, sas=NULL,
                                      api_version=getOption("azure_storage_api_version"),
                                      ...)
{
    do.call(adls_filesystem, generate_endpoint_container(endpoint, key, token, sas, api_version))
}

#' @rdname adls_filesystem
#' @export
adls_filesystem.adls_endpoint <- function(endpoint, name, ...)
{
    obj <- list(name=name, endpoint=endpoint)
    class(obj) <- c("adls_filesystem", "storage_container")
    obj
}

#' @rdname adls_filesystem
#' @export
print.adls_filesystem <- function(x, ...)
{
    cat("Azure Data Lake Storage Gen2 filesystem '", x$name, "'\n", sep="")
    url <- httr::parse_url(x$endpoint$url)
    url$path <- x$name
    cat(sprintf("URL: %s\n", httr::build_url(url)))

    if(!is_empty(x$endpoint$key))
        cat("Access key: <hidden>\n")
    else cat("Access key: <none supplied>\n")

    if(!is_empty(x$endpoint$token))
    {
        cat("Azure Active Directory token:\n")
        print(x$endpoint$token)
    }
    else cat("Azure Active Directory token: <none supplied>\n")

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
list_adls_filesystems.character <- function(endpoint, key=NULL, token=NULL, sas=NULL,
                                            api_version=getOption("azure_adls_api_version"),
                                            ...)
{
    do.call(list_adls_filesystems, generate_endpoint_container(endpoint, key, token, sas, api_version))
}

#' @rdname adls_filesystem
#' @export
list_adls_filesystems.adls_endpoint <- function(endpoint, ...)
{
    lst <- call_storage_endpoint(endpoint, "/", options=list(resource="account"))

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
create_adls_filesystem.character <- function(endpoint, key=NULL, token=NULL, sas=NULL,
                                             api_version=getOption("azure_adls_api_version"),
                                             ...)
{
    endp <- generate_endpoint_container(endpoint, key, token, sas, api_version)
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
delete_adls_filesystem.character <- function(endpoint, key=NULL, token=NULL, sas=NULL,
                                             api_version=getOption("azure_adls_api_version"),
                                             ...)
{
    endp <- generate_endpoint_container(endpoint, key, token, sas, api_version)
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
    if(!delete_confirmed(confirm, paste0(endpoint$url, name), "filesystem"))
        return(invisible(NULL))

    obj <- adls_filesystem(endpoint, name)
    invisible(do_container_op(obj, options=list(resource="filesystem"), http_verb="DELETE"))
}


#' Operations on an Azure Data Lake Storage Gen2 filesystem
#'
#' Upload, download, or delete a file; list files in a directory; create or delete directories; check file existence.
#'
#' @param filesystem An ADLSgen2 filesystem object.
#' @param dir,file A string naming a directory or file respectively.
#' @param info Whether to return names only, or all information in a directory listing.
#' @param src,dest The source and destination paths/files for uploading and downloading. See 'Details' below.
#' @param confirm Whether to ask for confirmation on deleting a file or directory.
#' @param blocksize The number of bytes to upload/download per HTTP(S) request.
#' @param recursive For the multiupload/download functions, whether to recursively transfer files in subdirectories. For `list_adls_files`, and `delete_adls_dir`, whether the operation should recurse through subdirectories. For `delete_adls_dir`, this must be TRUE to delete a non-empty directory.
#' @param lease The lease for a file, if present.
#' @param overwrite When downloading, whether to overwrite an existing destination file.
#' @param use_azcopy Whether to use the AzCopy utility from Microsoft to do the transfer, rather than doing it in R.
#' @param max_concurrent_transfers For `multiupload_adls_file` and `multidownload_adls_file`, the maximum number of concurrent file transfers. Each concurrent file transfer requires a separate R process, so limit this if you are low on memory.
#'
#' @details
#' `upload_adls_file` and `download_adls_file` are the workhorse file transfer functions for ADLSgen2 storage. They each take as inputs a _single_ filename as the source for uploading/downloading, and a single filename as the destination. Alternatively, for uploading, `src` can be a [textConnection] or [rawConnection] object; and for downloading, `dest` can be NULL or a `rawConnection` object. If `dest` is NULL, the downloaded data is returned as a raw vector, and if a raw connection, it will be placed into the connection. See the examples below.
#'
#' `multiupload_adls_file` and `multidownload_adls_file` are functions for uploading and downloading _multiple_ files at once. They parallelise file transfers by using the background process pool provided by AzureRMR, which can lead to significant efficiency gains when transferring many small files. There are two ways to specify the source and destination for these functions:
#' - Both `src` and `dest` can be vectors naming the individual source and destination pathnames.
#' - The `src` argument can be a wildcard pattern expanding to one or more files, with `dest` naming a destination directory. In this case, if `recursive` is true, the file transfer will replicate the source directory structure at the destination.
#'
#' `upload_adls_file` and `download_adls_file` can display a progress bar to track the file transfer. You can control whether to display this with `options(azure_storage_progress_bar=TRUE|FALSE)`; the default is TRUE.
#'
#' @return
#' For `list_adls_files`, if `info="name"`, a vector of file/directory names. If `info="all"`, a data frame giving the file size and whether each object is a file or directory.
#'
#' For `download_adls_file`, if `dest=NULL`, the contents of the downloaded file as a raw vector.
#'
#' For `adls_file_exists`, either TRUE or FALSE.
#'
#' @seealso
#' [adls_filesystem], [az_storage], [storage_download], [call_azcopy]
#'
#' @examples
#' \dontrun{
#'
#' fs <- adls_filesystem("https://mystorage.dfs.core.windows.net/myfilesystem", key="access_key")
#'
#' list_adls_files(fs, "/")
#' list_adls_files(fs, "/", recursive=TRUE)
#'
#' create_adls_dir(fs, "/newdir")
#'
#' upload_adls_file(fs, "~/bigfile.zip", dest="/newdir/bigfile.zip")
#' download_adls_file(fs, "/newdir/bigfile.zip", dest="~/bigfile_downloaded.zip")
#'
#' delete_adls_file(fs, "/newdir/bigfile.zip")
#' delete_adls_dir(fs, "/newdir")
#'
#' # uploading/downloading multiple files at once
#' multiupload_adls_file(fs, "/data/logfiles/*.zip")
#' multidownload_adls_file(fs, "/monthly/jan*.*", "/data/january")
#'
#' # you can also pass a vector of file/pathnames as the source and destination
#' src <- c("file1.csv", "file2.csv", "file3.csv")
#' dest <- paste0("uploaded_", src)
#' multiupload_adls_file(share, src, dest)
#'
#' # uploading serialized R objects via connections
#' json <- jsonlite::toJSON(iris, pretty=TRUE, auto_unbox=TRUE)
#' con <- textConnection(json)
#' upload_adls_file(fs, con, "iris.json")
#'
#' rds <- serialize(iris, NULL)
#' con <- rawConnection(rds)
#' upload_adls_file(fs, con, "iris.rds")
#'
#' # downloading files into memory: as a raw vector, and via a connection
#' rawvec <- download_adls_file(fs, "iris.json", NULL)
#' rawToChar(rawvec)
#'
#' con <- rawConnection(raw(0), "r+")
#' download_adls_file(fs, "iris.rds", con)
#' unserialize(con)
#'
#' }
#' @rdname adls
#' @export
list_adls_files <- function(filesystem, dir="/", info=c("all", "name"),
                            recursive=FALSE)
{
    info <- match.arg(info)
    opts <- list(
        recursive=tolower(as.character(recursive)),
        resource="filesystem",
        directory=as.character(dir)
    )

    out <- NULL
    repeat
    {
        res <- do_container_op(filesystem, "", options=opts, http_status_handler="pass")
        httr::stop_for_status(res, storage_error_message(res))
        out <- rbind(out, httr::content(res, simplifyVector=TRUE)$paths)
        headers <- httr::headers(res)
        if(is_empty(headers$`x-ms-continuation`))
            break
        else opts$continuation <- headers$`x-ms-continuation`
    }

    if(info == "all")
    {
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
            out$contentLength <- NA_real_
        else out$contentLength[is.na(out$contentLength)] <- NA_real_
        if(is.null(out$etag))
            out$etag <- ""
        else out$etag[is.na(out$etag)] <- ""
        if(is.null(out$permissions))
            out$permissions <- ""
        else out$permissions[is.na(out$permissions)] <- ""

        out <- out[c("name", "contentLength", "isDirectory", "lastModified", "permissions", "etag")]
        if(!is.null(out$contentLength))
            out$contentLength <- as.numeric(out$contentLength)
        if(!is.null(out$lastModified))
            out$lastModified <- as_datetime(out$lastModified)
        names(out)[c(2, 3)] <- c("size", "isdir")

        if(all(out$permissions == ""))
            out$permissions <- NULL
        if(all(out$etag == ""))
            out$etag <- NULL

        # needed when dir was created in a non-HNS enabled account
        out$size[out$isdir] <- NA

        out
    }
    else as.character(out$name)
}


#' @rdname adls
#' @export
multiupload_adls_file <- function(filesystem, src, dest, recursive=FALSE, blocksize=2^22, lease=NULL,
                                   use_azcopy=FALSE,
                                   max_concurrent_transfers=10)
{
    if(use_azcopy)
        return(azcopy_upload(filesystem, src, dest, blocksize=blocksize, lease=lease, recursive=recursive))

    multiupload_internal(filesystem, src, dest, recursive=recursive, blocksize=blocksize, lease=lease,
                         max_concurrent_transfers=max_concurrent_transfers)
}


#' @rdname adls
#' @export
upload_adls_file <- function(filesystem, src, dest=basename(src), blocksize=2^24, lease=NULL, use_azcopy=FALSE)
{
    if(use_azcopy)
        azcopy_upload(filesystem, src, dest, blocksize=blocksize, lease=lease)
    else upload_adls_file_internal(filesystem, src, dest, blocksize=blocksize, lease=lease)
}


#' @rdname adls
#' @export
multidownload_adls_file <- function(filesystem, src, dest, recursive=FALSE, blocksize=2^24, overwrite=FALSE,
                                    use_azcopy=FALSE,
                                    max_concurrent_transfers=10)
{
    if(use_azcopy)
        return(azcopy_download(filesystem, src, dest, overwrite=overwrite, recursive=recursive))

    multidownload_internal(filesystem, src, dest, recursive=recursive, blocksize=blocksize, overwrite=overwrite,
                           max_concurrent_transfers=max_concurrent_transfers)
}


#' @rdname adls
#' @export
download_adls_file <- function(filesystem, src, dest=basename(src), blocksize=2^24, overwrite=FALSE, use_azcopy=FALSE)
{
    if(use_azcopy)
        azcopy_download(filesystem, src, dest, overwrite=overwrite)
    else download_adls_file_internal(filesystem, src, dest, blocksize=blocksize, overwrite=overwrite)
}


#' @rdname adls
#' @export
delete_adls_file <- function(filesystem, file, confirm=TRUE)
{
    if(!delete_confirmed(confirm, paste0(filesystem$endpoint$url, filesystem$name, "/", file), "file"))
        return(invisible(NULL))

    invisible(do_container_op(filesystem, file, http_verb="DELETE"))
}


#' @rdname adls
#' @export
create_adls_dir <- function(filesystem, dir)
{
    invisible(do_container_op(filesystem, dir, options=list(resource="directory"), http_verb="PUT"))
}


#' @rdname adls
#' @export
delete_adls_dir <- function(filesystem, dir, recursive=FALSE, confirm=TRUE)
{
    if(!delete_confirmed(confirm, paste0(filesystem$endpoint$url, filesystem$name, "/", dir), "directory"))
        return(invisible(NULL))

    opts <- list(recursive=tolower(as.character(recursive)))
    invisible(do_container_op(filesystem, dir, options=opts, http_verb="DELETE"))
}

#' @rdname adls
#' @export
adls_file_exists <- function(filesystem, file)
{
    res <- do_container_op(filesystem, file, headers = list(), http_verb = "HEAD", http_status_handler = "pass")
    if (httr::status_code(res) == 404L)
        return(FALSE)

    httr::stop_for_status(res, storage_error_message(res))
    return(TRUE)
}
