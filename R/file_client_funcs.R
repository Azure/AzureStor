#' Operations on a file endpoint
#'
#' Get, list, create, or delete file shares.
#'
#' @param endpoint Either a file endpoint object as created by [storage_endpoint], or a character string giving the URL of the endpoint.
#' @param key,token,sas If an endpoint object is not supplied, authentication credentials: either an access key, an Azure Active Directory (AAD) token, or a SAS, in that order of priority. 
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
#' @seealso
#' [storage_endpoint], [az_storage], [storage_container]
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
file_share.character <- function(endpoint, key=NULL, token=NULL, sas=NULL,
                                 api_version=getOption("azure_storage_api_version"),
                                 ...)
{
    do.call(file_share, generate_endpoint_container(endpoint, key, token, sas, api_version))
}

#' @rdname file_share
#' @export
file_share.file_endpoint <- function(endpoint, name, ...)
{
    obj <- list(name=name, endpoint=endpoint)
    class(obj) <- c("file_share", "storage_container")
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
list_file_shares.character <- function(endpoint, key=NULL, token=NULL, sas=NULL,
                                       api_version=getOption("azure_storage_api_version"),
                                       ...)
{
    do.call(list_file_shares, generate_endpoint_container(endpoint, key, token, sas, api_version))
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
create_file_share.character <- function(endpoint, key=NULL, token=NULL, sas=NULL,
                                        api_version=getOption("azure_storage_api_version"),
                                        ...)
{
    endp <- generate_endpoint_container(endpoint, key, token, sas, api_version)
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
delete_file_share.character <- function(endpoint, key=NULL, token=NULL, sas=NULL,
                                        api_version=getOption("azure_storage_api_version"),
                                        ...)
{
    endp <- generate_endpoint_container(endpoint, key, token, sas, api_version)
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
#' @param src,dest The source and destination files for uploading and downloading. For uploading, `src` can also be a [textConnection] or [rawConnection] object to allow transferring in-memory R objects without creating a temporary file.
#' @param confirm Whether to ask for confirmation on deleting a file or directory.
#' @param blocksize The number of bytes to upload/download per HTTP(S) request.
#' @param overwrite When downloading, whether to overwrite an existing destination file.
#' @param retries The number of times the file transfer functions will retry when they encounter an error. Set this to 0 to disable retries. This is applied per block for uploading, and to the entire file for downloading.
#' @param use_azcopy Whether to use the AzCopy utility from Microsoft to do the transfer, rather than doing it in R.
#' @param max_concurrent_transfers For `multiupload_azure_file` and `multidownload_azure_file`, the maximum number of concurrent file transfers. Each concurrent file transfer requires a separate R process, so limit this if you are low on memory.
#' @param prefix For `list_azure_files`, filters the result to return only files and directories whose name begins with this prefix.
#'
#' @details
#' `upload_azure_file` and `download_azure_file` are the workhorse file transfer functions for file storage. They each take as inputs a _single_ filename or connection as the source for uploading/downloading, and a single filename as the destination.
#'
#' `multiupload_azure_file` and `multidownload_azure_file` are functions for uploading and downloading _multiple_ files at once. They parallelise file transfers by deploying a pool of R processes in the background, which can lead to significantly greater efficiency when transferring many small files. They take as input a _wildcard_ pattern as the source, which expands to one or more files. The `dest` argument should be a directory.
#'
#' The file transfer functions also support working with connections to allow transferring R objects without creating temporary files. For uploading, `src` can be a [textConnection] or [rawConnection] object. For downloading, `dest` can be NULL or a `rawConnection` object. In the former case, the downloaded data is returned as a raw vector, and for the latter, it will be placed into the connection. See the examples below.
#'
#' By default, `download_azure_file` will display a progress bar as it is downloading. To turn this off, use `options(azure_dl_progress_bar=FALSE)`. To turn the progress bar back on, use `options(azure_dl_progress_bar=TRUE)`.
#'
#' @return
#' For `list_azure_files`, if `info="name"`, a vector of file/directory names. If `info="all"`, a data frame giving the file size and whether each object is a file or directory.
#'
#' For `download_azure_file`, if `dest=NULL`, the contents of the downloaded file as a raw vector.
#'
#' @seealso
#' [file_share], [az_storage], [storage_download], [call_azcopy]
#'
#' [AzCopy version 10 on GitHub](https://github.com/Azure/azure-storage-azcopy)
#'
#' @examples
#' \dontrun{
#'
#' share <- file_share("https://mystorage.file.core.windows.net/myshare", key="access_key")
#'
#' list_azure_files(share, "/")
#' list_azure_files(share, "/", recursive=TRUE)
#'
#' create_azure_dir(share, "/newdir")
#'
#' upload_azure_file(share, "~/bigfile.zip", dest="/newdir/bigfile.zip")
#' download_azure_file(share, "/newdir/bigfile.zip", dest="~/bigfile_downloaded.zip")
#'
#' delete_azure_file(share, "/newdir/bigfile.zip")
#' delete_azure_dir(share, "/newdir")
#'
#' # uploading/downloading multiple files at once
#' multiupload_azure_file(share, "/data/logfiles/*.zip")
#' multidownload_azure_file(share, "/monthly/jan*.*", "/data/january")
#'
#' # uploading serialized R objects via connections
#' json <- jsonlite::toJSON(iris, pretty=TRUE, auto_unbox=TRUE)
#' con <- textConnection(json)
#' upload_azure_file(share, con, "iris.json")
#'
#' rds <- serialize(iris, NULL)
#' con <- rawConnection(rds)
#' upload_azure_file(share, con, "iris.rds")
#'
#' # downloading files into memory: as a raw vector, and via a connection
#' rawvec <- download_azure_file(share, "iris.json", NULL)
#' rawToChar(rawvec)
#'
#' con <- rawConnection(raw(0), "r+")
#' download_azure_file(share, "iris.rds", con)
#' unserialize(con)
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
                   function(ent) if(is_empty(ent$Properties)) NA_character_
                                 else ent$Properties$`Content-Length`[[1]],
                   FUN.VALUE=character(1))

    data.frame(name=name, type=type, size=as.numeric(size), stringsAsFactors=FALSE)
}

#' @rdname file
#' @export
upload_azure_file <- function(share, src, dest, blocksize=2^22, retries=5, use_azcopy=FALSE)
{
    if(use_azcopy)
        azcopy_upload(share, src, dest, blocksize=blocksize)
    else upload_azure_file_internal(share, src, dest, blocksize=blocksize, retries=retries)
}

#' @rdname file
#' @export
multiupload_azure_file <- function(share, src, dest, blocksize=2^22, retries=5,
                                   use_azcopy=FALSE,
                                   max_concurrent_transfers=10)
{
    if(use_azcopy)
        azcopy_upload(share, src, dest, blocksize=blocksize)
    else multiupload_azure_file_internal(share, src, dest, blocksize=blocksize, retries=retries,
                                         max_concurrent_transfers=max_concurrent_transfers)
}

#' @rdname file
#' @export
download_azure_file <- function(share, src, dest, blocksize=2^22, overwrite=FALSE, retries=5, use_azcopy=FALSE)
{
    if(use_azcopy)
        azcopy_download(share, src, dest, overwrite=overwrite)
    else download_azure_file_internal(share, src, dest, blocksize=blocksize, overwrite=overwrite, retries=retries)
}

#' @rdname file
#' @export
multidownload_azure_file <- function(share, src, dest, blocksize=2^22, overwrite=FALSE, retries=5,
                                     use_azcopy=FALSE,
                                     max_concurrent_transfers=10)
{
    if(use_azcopy)
        azcopy_download(share, src, dest, overwrite=overwrite)
    else multidownload_azure_file_internal(share, src, dest, blocksize=blocksize, overwrite=overwrite, retries=retries,
                                           max_concurrent_transfers=max_concurrent_transfers)
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

