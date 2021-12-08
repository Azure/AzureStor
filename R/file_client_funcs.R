#' Operations on a file endpoint
#'
#' Get, list, create, or delete file shares.
#'
#' @param endpoint Either a file endpoint object as created by [storage_endpoint], or a character string giving the URL of the endpoint.
#' @param key,token,sas If an endpoint object is not supplied, authentication credentials: either an access key, an Azure Active Directory (AAD) token, or a SAS, in that order of priority.
#' @param api_version If an endpoint object is not supplied, the storage API version to use when interacting with the host. Currently defaults to `"2019-07-07"`.
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
    res <- call_storage_endpoint(endpoint, "/", options=list(comp="list"))
    lst <- lapply(res$Shares, function(cont) file_share(endpoint, cont$Name[[1]]))

    while(length(res$NextMarker) > 0)
    {
        res <- call_storage_endpoint(endpoint, "/", options=list(comp="list", marker=res$NextMarker[[1]]))
        lst <- c(lst, lapply(res$Shares, function(cont) file_share(endpoint, cont$Name[[1]])))
    }
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
    if(!delete_confirmed(confirm, paste0(endpoint$url, name), "share"))
        return(invisible(NULL))

    obj <- file_share(endpoint, name)
    invisible(do_container_op(obj, options=list(restype="share"), http_verb="DELETE"))
}


#' Operations on a file share
#'
#' Upload, download, or delete a file; list files in a directory; create or delete directories; check file existence.
#'
#' @param share A file share object.
#' @param dir,file A string naming a directory or file respectively.
#' @param info Whether to return names only, or all information in a directory listing.
#' @param src,dest The source and destination files for uploading and downloading. See 'Details' below.
#' @param confirm Whether to ask for confirmation on deleting a file or directory.
#' @param recursive For the multiupload/download functions, whether to recursively transfer files in subdirectories. For `list_azure_dir`, whether to include the contents of any subdirectories in the listing. For `create_azure_dir`, whether to recursively create each component of a nested directory path. For `delete_azure_dir`, whether to delete a subdirectory's contents first. Note that in all cases this can be slow, so try to use a non-recursive solution if possible.
#' @param create_dir For the uploading functions, whether to create the destination directory if it doesn't exist. Again for the file storage API this can be slow, hence is optional.
#' @param blocksize The number of bytes to upload/download per HTTP(S) request.
#' @param overwrite When downloading, whether to overwrite an existing destination file.
#' @param use_azcopy Whether to use the AzCopy utility from Microsoft to do the transfer, rather than doing it in R.
#' @param max_concurrent_transfers For `multiupload_azure_file` and `multidownload_azure_file`, the maximum number of concurrent file transfers. Each concurrent file transfer requires a separate R process, so limit this if you are low on memory.
#' @param prefix For `list_azure_files`, filters the result to return only files and directories whose name begins with this prefix.
#' @param put_md5 For uploading, whether to compute the MD5 hash of the file(s). This will be stored as part of the file's properties.
#' @param check_md5 For downloading, whether to verify the MD5 hash of the downloaded file(s). This requires that the file's `Content-MD5` property is set. If this is TRUE and the `Content-MD5` property is missing, a warning is generated.
#'
#' @details
#' `upload_azure_file` and `download_azure_file` are the workhorse file transfer functions for file storage. They each take as inputs a _single_ filename as the source for uploading/downloading, and a single filename as the destination. Alternatively, for uploading, `src` can be a [textConnection] or [rawConnection] object; and for downloading, `dest` can be NULL or a `rawConnection` object. If `dest` is NULL, the downloaded data is returned as a raw vector, and if a raw connection, it will be placed into the connection. See the examples below.
#'
#' `multiupload_azure_file` and `multidownload_azure_file` are functions for uploading and downloading _multiple_ files at once. They parallelise file transfers by using the background process pool provided by AzureRMR, which can lead to significant efficiency gains when transferring many small files. There are two ways to specify the source and destination for these functions:
#' - Both `src` and `dest` can be vectors naming the individual source and destination pathnames.
#' - The `src` argument can be a wildcard pattern expanding to one or more files, with `dest` naming a destination directory. In this case, if `recursive` is true, the file transfer will replicate the source directory structure at the destination.
#'
#' `upload_azure_file` and `download_azure_file` can display a progress bar to track the file transfer. You can control whether to display this with `options(azure_storage_progress_bar=TRUE|FALSE)`; the default is TRUE.
#'
#' @section AzCopy:
#' `upload_azure_file` and `download_azure_file` have the ability to use the AzCopy commandline utility to transfer files, instead of native R code. This can be useful if you want to take advantage of AzCopy's logging and recovery features; it may also be faster in the case of transferring a very large number of small files. To enable this, set the `use_azcopy` argument to TRUE.
#'
#' Note that AzCopy only supports SAS and AAD (OAuth) token as authentication methods. AzCopy also expects a single filename or wildcard spec as its source/destination argument, not a vector of filenames or a connection.
#'
#' @return
#' For `list_azure_files`, if `info="name"`, a vector of file/directory names. If `info="all"`, a data frame giving the file size and whether each object is a file or directory.
#'
#' For `download_azure_file`, if `dest=NULL`, the contents of the downloaded file as a raw vector.
#'
#' For `azure_file_exists`, either TRUE or FALSE.
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
#' # you can also pass a vector of file/pathnames as the source and destination
#' src <- c("file1.csv", "file2.csv", "file3.csv")
#' dest <- paste0("uploaded_", src)
#' multiupload_azure_file(share, src, dest)
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
list_azure_files <- function(share, dir="/", info=c("all", "name"),
                             prefix=NULL, recursive=FALSE)
{
    info <- match.arg(info)
    opts <- list(comp="list", restype="directory")
    if(!is_empty(prefix))
        opts <- c(opts, prefix=as.character(prefix))

    out <- NULL
    repeat
    {
        lst <- do_container_op(share, dir, options=opts)
        out <- c(out, lst$Entries)
        if(is_empty(lst$NextMarker))
            break
        else opts$marker <- lst$NextMarker[[1]]
    }

    name <- vapply(out, function(ent) ent$Name[[1]], FUN.VALUE=character(1))
    isdir <- if(is_empty(name)) logical(0) else names(name) == "Directory"
    size <- vapply(out,
                   function(ent) if(is_empty(ent$Properties)) NA_character_
                                 else ent$Properties$`Content-Length`[[1]],
                   FUN.VALUE=character(1))

    df <- data.frame(name=name, size=as.numeric(size), isdir=isdir, stringsAsFactors=FALSE, row.names=NULL)
    df$name <- sub("^//", "", file.path(dir, df$name))

    if(recursive)
    {
        dirs <- df$name[df$isdir]

        nextlevel <- lapply(dirs, function(d) list_azure_files(share, d, info="all", prefix=prefix, recursive=TRUE))
        df <- do.call(vctrs::vec_rbind, c(list(df), nextlevel))
    }

    if(info == "name")
        df$name
    else df
}

#' @rdname file
#' @export
upload_azure_file <- function(share, src, dest=basename(src), create_dir=FALSE, blocksize=2^22, put_md5=FALSE,
                              use_azcopy=FALSE)
{
    if(use_azcopy)
        azcopy_upload(share, src, dest, blocksize=blocksize, put_md5=put_md5)
    else upload_azure_file_internal(share, src, dest, create_dir=create_dir, blocksize=blocksize, put_md5=put_md5)
}

#' @rdname file
#' @export
multiupload_azure_file <- function(share, src, dest, recursive=FALSE, create_dir=recursive, blocksize=2^22,
                                   put_md5=FALSE, use_azcopy=FALSE,
                                   max_concurrent_transfers=10)
{
    if(use_azcopy)
        return(azcopy_upload(share, src, dest, blocksize=blocksize, recursive=recursive, put_md5=put_md5))

    multiupload_internal(share, src, dest, recursive=recursive, create_dir=create_dir, blocksize=blocksize,
                         put_md5=put_md5, max_concurrent_transfers=max_concurrent_transfers)
}

#' @rdname file
#' @export
download_azure_file <- function(share, src, dest=basename(src), blocksize=2^22, overwrite=FALSE,
                                check_md5=FALSE, use_azcopy=FALSE)
{
    if(use_azcopy)
        azcopy_download(share, src, dest, overwrite=overwrite, check_md5=check_md5)
    else download_azure_file_internal(share, src, dest, blocksize=blocksize, overwrite=overwrite,
                                      check_md5=check_md5)
}

#' @rdname file
#' @export
multidownload_azure_file <- function(share, src, dest, recursive=FALSE, blocksize=2^22, overwrite=FALSE,
                                     check_md5=FALSE, use_azcopy=FALSE,
                                     max_concurrent_transfers=10)
{
    if(use_azcopy)
        return(azcopy_download(share, src, dest, overwrite=overwrite, recursive=recursive, check_md5=check_md5))

    multidownload_internal(share, src, dest, recursive=recursive, blocksize=blocksize, overwrite=overwrite,
                           check_md5=check_md5, max_concurrent_transfers=max_concurrent_transfers)
}

#' @rdname file
#' @export
delete_azure_file <- function(share, file, confirm=TRUE)
{
    if(!delete_confirmed(confirm, paste0(share$endpoint$url, share$name, "/", file), "file"))
        return(invisible(NULL))

    invisible(do_container_op(share, file, http_verb="DELETE"))
}

#' @rdname file
#' @export
create_azure_dir <- function(share, dir, recursive=FALSE)
{
    if(dir %in% c("/", "."))
        return(invisible(NULL))

    if(recursive)
        try(create_azure_dir(share, dirname(dir), recursive=TRUE), silent=TRUE)

    invisible(do_container_op(share, dir, options=list(restype="directory"),
        headers=dir_default_perms, http_verb="PUT"))
}

#' @rdname file
#' @export
delete_azure_dir <- function(share, dir, recursive=FALSE, confirm=TRUE)
{
    if(dir %in% c("/", ".") && !recursive)
        return(invisible(NULL))

    if(!delete_confirmed(confirm, paste0(share$endpoint$url, share$name, "/", dir), "directory"))
        return(invisible(NULL))

    if(recursive)
    {
        conts <- list_azure_files(share, dir, recursive=TRUE)
        for(i in rev(seq_len(nrow(conts))))
        {
            # delete all files and dirs
            # assumption is that files will be listed after their parent dir
            if(conts$isdir[i])
                delete_azure_dir(share, conts$name[i], recursive=FALSE, confirm=FALSE)
            else delete_azure_file(share, conts$name[i], confirm=FALSE)
        }
    }

    if(dir == "/")
        return(invisible(NULL))
    invisible(do_container_op(share, dir, options=list(restype="directory"), http_verb="DELETE"))
}

#' @rdname file
#' @export
azure_file_exists <- function(share, file)
{
    res <- do_container_op(share, file, headers = list(), http_verb = "HEAD", http_status_handler = "pass")
    if (httr::status_code(res) == 404L)
        return(FALSE)

    httr::stop_for_status(res, storage_error_message(res))
    return(TRUE)
}


azure_dir_exists <- function(share, dir)
{
    lst <- try(list_azure_files(share, dir, info="name"), silent=TRUE)
    !inherits(lst, "try-error")
}
