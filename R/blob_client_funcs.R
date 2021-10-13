#' Operations on a blob endpoint
#'
#' Get, list, create, or delete blob containers.
#'
#' @param endpoint Either a blob endpoint object as created by [storage_endpoint], or a character string giving the URL of the endpoint.
#' @param key,token,sas If an endpoint object is not supplied, authentication credentials: either an access key, an Azure Active Directory (AAD) token, or a SAS, in that order of priority. If no authentication credentials are provided, only public (anonymous) access to the share is possible.
#' @param api_version If an endpoint object is not supplied, the storage API version to use when interacting with the host. Currently defaults to `"2019-07-07"`.
#' @param name The name of the blob container to get, create, or delete.
#' @param confirm For deleting a container, whether to ask for confirmation.
#' @param lease For deleting a leased container, the lease ID.
#' @param public_access For creating a container, the level of public access to allow.
#' @param x For the print method, a blob container object.
#' @param ... Further arguments passed to lower-level functions.
#'
#' @details
#' You can call these functions in a couple of ways: by passing the full URL of the share, or by passing the endpoint object and the name of the container as a string.
#'
#' If authenticating via AAD, you can supply the token either as a string, or as an object of class AzureToken, created via [AzureRMR::get_azure_token]. The latter is the recommended way of doing it, as it allows for automatic refreshing of expired tokens.
#'
#' @return
#' For `blob_container` and `create_blob_container`, an S3 object representing an existing or created container respectively.
#'
#' For `list_blob_containers`, a list of such objects.
#'
#' @seealso
#' [storage_endpoint], [az_storage], [storage_container]
#'
#' @examples
#' \dontrun{
#'
#' endp <- blob_endpoint("https://mystorage.blob.core.windows.net/", key="access_key")
#'
#' # list containers
#' list_blob_containers(endp)
#'
#' # get, create, and delete a container
#' blob_container(endp, "mycontainer")
#' create_blob_container(endp, "newcontainer")
#' delete_blob_container(endp, "newcontainer")
#'
#' # alternative way to do the same
#' blob_container("https://mystorage.blob.core.windows.net/mycontainer", key="access_key")
#' create_blob_container("https://mystorage.blob.core.windows.net/newcontainer", key="access_key")
#' delete_blob_container("https://mystorage.blob.core.windows.net/newcontainer", key="access_key")
#'
#' # authenticating via AAD
#' token <- AzureRMR::get_azure_token(resource="https://storage.azure.com/",
#'     tenant="myaadtenant",
#'     app="myappid",
#'     password="mypassword")
#' blob_container("https://mystorage.blob.core.windows.net/mycontainer", token=token)
#'
#' }
#' @rdname blob_container
#' @export
blob_container <- function(endpoint, ...)
{
    UseMethod("blob_container")
}

#' @rdname blob_container
#' @export
blob_container.character <- function(endpoint, key=NULL, token=NULL, sas=NULL,
                                     api_version=getOption("azure_storage_api_version"),
                                     ...)
{
    do.call(blob_container, generate_endpoint_container(endpoint, key, token, sas, api_version))
}

#' @rdname blob_container
#' @export
blob_container.blob_endpoint <- function(endpoint, name, ...)
{
    obj <- list(name=name, endpoint=endpoint)
    class(obj) <- c("blob_container", "storage_container")
    obj
}

#' @rdname blob_container
#' @export
print.blob_container <- function(x, ...)
{
    cat("Azure blob container '", x$name, "'\n", sep="")
    url <- httr::parse_url(x$endpoint$url)
    url$path <- x$name
    cat(sprintf("URL: %s\n", httr::build_url(url)))

    if(!is_empty(x$endpoint$key))
        cat("Access key: <hidden>\n")
    else cat("Access key: <none supplied>\n")

    if(!is_empty(x$endpoint$token))
    {
        cat("Azure Active Directory access token:\n")
        print(x$endpoint$token)
    }
    else cat("Azure Active Directory access token: <none supplied>\n")

    if(!is_empty(x$endpoint$sas))
        cat("Account shared access signature: <hidden>\n")
    else cat("Account shared access signature: <none supplied>\n")

    cat(sprintf("Storage API version: %s\n", x$endpoint$api_version))
    invisible(x)
}


#' @rdname blob_container
#' @export
list_blob_containers <- function(endpoint, ...)
{
    UseMethod("list_blob_containers")
}

#' @rdname blob_container
#' @export
list_blob_containers.character <- function(endpoint, key=NULL, token=NULL, sas=NULL,
                                           api_version=getOption("azure_storage_api_version"),
                                           ...)
{
    do.call(list_blob_containers, generate_endpoint_container(endpoint, key, token, sas, api_version))
}

#' @rdname blob_container
#' @export
list_blob_containers.blob_endpoint <- function(endpoint, ...)
{
    res <- call_storage_endpoint(endpoint, "/", options=list(comp="list"))
    lst <- lapply(res$Containers, function(cont) blob_container(endpoint, cont$Name[[1]]))

    while(length(res$NextMarker) > 0)
    {
        res <- call_storage_endpoint(endpoint, "/", options=list(comp="list", marker=res$NextMarker[[1]]))
        lst <- c(lst, lapply(res$Containers, function(cont) blob_container(endpoint, cont$Name[[1]])))
    }
    named_list(lst)
}



#' @rdname blob_container
#' @export
create_blob_container <- function(endpoint, ...)
{
    UseMethod("create_blob_container")
}

#' @rdname blob_container
#' @export
create_blob_container.character <- function(endpoint, key=NULL, token=NULL, sas=NULL,
                                            api_version=getOption("azure_storage_api_version"),
                                            ...)
{
    endp <- generate_endpoint_container(endpoint, key, token, sas, api_version)
    create_blob_container(endp$endpoint, endp$name, ...)
}

#' @rdname blob_container
#' @export
create_blob_container.blob_container <- function(endpoint, ...)
{
    create_blob_container(endpoint$endpoint, endpoint$name)
}

#' @rdname blob_container
#' @export
create_blob_container.blob_endpoint <- function(endpoint, name, public_access=c("none", "blob", "container"), ...)
{
    public_access <- match.arg(public_access)
    headers <- if(public_access != "none")
        modifyList(list(...), list("x-ms-blob-public-access"=public_access))
    else list(...)

    obj <- blob_container(endpoint, name)
    do_container_op(obj, options=list(restype="container"), headers=headers, http_verb="PUT")
    obj
}



#' @rdname blob_container
#' @export
delete_blob_container <- function(endpoint, ...)
{
    UseMethod("delete_blob_container")
}

#' @rdname blob_container
#' @export
delete_blob_container.character <- function(endpoint, key=NULL, token=NULL, sas=NULL,
                                            api_version=getOption("azure_storage_api_version"),
                                            ...)
{
    endp <- generate_endpoint_container(endpoint, key, token, sas, api_version)
    delete_blob_container(endp$endpoint, endp$name, ...)
}

#' @rdname blob_container
#' @export
delete_blob_container.blob_container <- function(endpoint, ...)
{
    delete_blob_container(endpoint$endpoint, endpoint$name, ...)
}

#' @rdname blob_container
#' @export
delete_blob_container.blob_endpoint <- function(endpoint, name, confirm=TRUE, lease=NULL, ...)
{
    if(!delete_confirmed(confirm, paste0(endpoint$url, name), "container"))
        return(invisible(NULL))

    headers <- if(!is_empty(lease))
        list("x-ms-lease-id"=lease)
    else list()

    obj <- blob_container(endpoint, name)
    invisible(do_container_op(obj, options=list(restype="container"), headers=headers, http_verb="DELETE"))
}


#' Operations on a blob container or blob
#'
#' Upload, download, or delete a blob; list blobs in a container; create or delete directories; check blob availability.
#'
#' @param container A blob container object.
#' @param blob A string naming a blob.
#' @param dir For `list_blobs`, A string naming the directory. Note that blob storage does not support real directories; this argument simply filters the result to return only blobs whose names start with the given value.
#' @param src,dest The source and destination files for uploading and downloading. See 'Details' below.
#' @param info For `list_blobs`, level of detail about each blob to return: a vector of names only; the name, size, blob type, and whether this blob represents a directory; or all information.
#' @param confirm Whether to ask for confirmation on deleting a blob.
#' @param blocksize The number of bytes to upload/download per HTTP(S) request.
#' @param lease The lease for a blob, if present.
#' @param type When uploading, the type of blob to create. Currently only block and append blobs are supported.
#' @param append When uploading, whether to append the uploaded data to the destination blob. Only has an effect if `type="AppendBlob"`. If this is FALSE (the default) and the destination append blob exists, it is overwritten. If this is TRUE and the destination does not exist or is not an append blob, an error is thrown.
#' @param overwrite When downloading, whether to overwrite an existing destination file.
#' @param use_azcopy Whether to use the AzCopy utility from Microsoft to do the transfer, rather than doing it in R.
#' @param max_concurrent_transfers For `multiupload_blob` and `multidownload_blob`, the maximum number of concurrent file transfers. Each concurrent file transfer requires a separate R process, so limit this if you are low on memory.
#' @param prefix For `list_blobs`, an alternative way to specify the directory.
#' @param recursive For the multiupload/download functions, whether to recursively transfer files in subdirectories. For `list_blobs`, whether to include the contents of any subdirectories in the listing. For `delete_blob_dir`, whether to recursively delete subdirectory contents as well (not yet supported).
#' @param put_md5 For uploading, whether to compute the MD5 hash of the blob(s). This will be stored as part of the blob's properties. Only used for block blobs.
#' @param check_md5 For downloading, whether to verify the MD5 hash of the downloaded blob(s). This requires that the blob's `Content-MD5` property is set. If this is TRUE and the `Content-MD5` property is missing, a warning is generated.
#'
#' @details
#' `upload_blob` and `download_blob` are the workhorse file transfer functions for blobs. They each take as inputs a _single_ filename as the source for uploading/downloading, and a single filename as the destination. Alternatively, for uploading, `src` can be a [textConnection] or [rawConnection] object; and for downloading, `dest` can be NULL or a `rawConnection` object. If `dest` is NULL, the downloaded data is returned as a raw vector, and if a raw connection, it will be placed into the connection. See the examples below.
#'
#' `multiupload_blob` and `multidownload_blob` are functions for uploading and downloading _multiple_ files at once. They parallelise file transfers by using the background process pool provided by AzureRMR, which can lead to significant efficiency gains when transferring many small files. There are two ways to specify the source and destination for these functions:
#' - Both `src` and `dest` can be vectors naming the individual source and destination pathnames.
#' - The `src` argument can be a wildcard pattern expanding to one or more files, with `dest` naming a destination directory. In this case, if `recursive` is true, the file transfer will replicate the source directory structure at the destination.
#'
#' `upload_blob` and `download_blob` can display a progress bar to track the file transfer. You can control whether to display this with `options(azure_storage_progress_bar=TRUE|FALSE)`; the default is TRUE.
#'
#' `multiupload_blob` can upload files either as all block blobs or all append blobs, but not a mix of both.
#'
#' @section AzCopy:
#' `upload_blob` and `download_blob` have the ability to use the AzCopy commandline utility to transfer files, instead of native R code. This can be useful if you want to take advantage of AzCopy's logging and recovery features; it may also be faster in the case of transferring a very large number of small files. To enable this, set the `use_azcopy` argument to TRUE.
#'
#' The following points should be noted about AzCopy:
#' - It only supports SAS and AAD (OAuth) token as authentication methods. AzCopy also expects a single filename or wildcard spec as its source/destination argument, not a vector of filenames or a connection.
#' - Currently, it does _not_ support appending data to existing blobs.
#'
#' @section Directories:
#' Blob storage does not have true directories, instead using filenames containing a separator character (typically '/') to mimic a directory structure. This has some consequences:
#'
#' - The `isdir` column in the data frame output of `list_blobs` is a best guess as to whether an object represents a file or directory, and may not always be correct. Currently, `list_blobs` assumes that any object with a file size of zero is a directory.
#' - Zero-length files can cause problems for the blob storage service as a whole (not just AzureStor). Try to avoid uploading such files.
#' - `create_blob_dir` and `delete_blob_dir` function as expected only for accounts with hierarchical namespaces enabled. When this feature is disabled, directories do not exist as objects in their own right: to create a directory, simply upload a blob to that directory. To delete a directory, delete all the blobs within it; as far as the blob storage service is concerned, the directory then no longer exists.
#' - Similarly, the output of `list_blobs(recursive=TRUE)` can vary based on whether the storage account has hierarchical namespaces enabled.
#'
#' @return
#' For `list_blobs`, details on the blobs in the container. For `download_blob`, if `dest=NULL`, the contents of the downloaded blob as a raw vector. For `blob_exists` a flag whether the blob exists.
#'
#' @seealso
#' [blob_container], [az_storage], [storage_download], [call_azcopy]
#'
#' [AzCopy version 10 on GitHub](https://github.com/Azure/azure-storage-azcopy)
#' [Guide to the different blob types](https://docs.microsoft.com/en-us/rest/api/storageservices/understanding-block-blobs--append-blobs--and-page-blobs)
#'
#' @examples
#' \dontrun{
#'
#' cont <- blob_container("https://mystorage.blob.core.windows.net/mycontainer", key="access_key")
#'
#' list_blobs(cont)
#'
#' upload_blob(cont, "~/bigfile.zip", dest="bigfile.zip")
#' download_blob(cont, "bigfile.zip", dest="~/bigfile_downloaded.zip")
#'
#' delete_blob(cont, "bigfile.zip")
#'
#' # uploading/downloading multiple files at once
#' multiupload_blob(cont, "/data/logfiles/*.zip", "/uploaded_data")
#' multiupload_blob(cont, "myproj/*")  # no dest directory uploads to root
#' multidownload_blob(cont, "jan*.*", "/data/january")
#'
#' # append blob: concatenating multiple files into one
#' upload_blob(cont, "logfile1", "logfile", type="AppendBlob", append=FALSE)
#' upload_blob(cont, "logfile2", "logfile", type="AppendBlob", append=TRUE)
#' upload_blob(cont, "logfile3", "logfile", type="AppendBlob", append=TRUE)
#'
#' # you can also pass a vector of file/pathnames as the source and destination
#' src <- c("file1.csv", "file2.csv", "file3.csv")
#' dest <- paste0("uploaded_", src)
#' multiupload_blob(cont, src, dest)
#'
#' # uploading serialized R objects via connections
#' json <- jsonlite::toJSON(iris, pretty=TRUE, auto_unbox=TRUE)
#' con <- textConnection(json)
#' upload_blob(cont, con, "iris.json")
#'
#' rds <- serialize(iris, NULL)
#' con <- rawConnection(rds)
#' upload_blob(cont, con, "iris.rds")
#'
#' # downloading files into memory: as a raw vector, and via a connection
#' rawvec <- download_blob(cont, "iris.json", NULL)
#' rawToChar(rawvec)
#'
#' con <- rawConnection(raw(0), "r+")
#' download_blob(cont, "iris.rds", con)
#' unserialize(con)
#'
#' # copy from a public URL: Iris data from UCI machine learning repository
#' copy_url_to_blob(cont,
#'     "https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data",
#'     "iris.csv")
#'
#' }
#' @rdname blob
#' @export
list_blobs <- function(container, dir="/", info=c("partial", "name", "all"),
                       prefix=NULL, recursive=TRUE)
{
    info <- match.arg(info)

    opts <- list(comp="list", restype="container")

    # ensure last char is always '/', to get list of blobs in a subdir
    if(dir != "/")
    {
        if(!grepl("/$", dir))
            dir <- paste0(dir, "/")
        prefix <- dir
    }

    if(!is_empty(prefix))
        opts <- c(opts, prefix=as.character(prefix))

    if(!recursive)
        opts <- c(opts, delimiter="/")

    res <- do_container_op(container, options=opts)
    lst <- res$Blobs
    while(length(res$NextMarker) > 0)
    {
        opts$marker <- res$NextMarker[[1]]
        res <- do_container_op(container, options=opts)
        lst <- c(lst, res$Blobs)
    }

    if(info != "name")
    {
        prefixes <- lst[names(lst) == "BlobPrefix"]
        blobs <- lst[names(lst) == "Blob"]

        prefix_rows <- lapply(prefixes, function(prefix)
        {
            data.frame(Type="BlobPrefix",
                       Name=unlist(prefix$Name),
                       "Content-Length"=NA,
                       BlobType=NA,
                       stringsAsFactors=FALSE,
                       check.names=FALSE)
        })

        blob_rows <- lapply(blobs, function(blob)
        {
            # properties returned can vary for block/append/whatever blobs, and whether HNS is enabled
            normalize_blob_properties <- function(props)
            {
                all_props <- c(
                    "Creation-Time",
                    "Last-Modified",
                    "Etag",
                    "Content-Length",
                    "Content-Type",
                    "Content-Encoding",
                    "Content-Language",
                    "Content-CRC64",
                    "Content-MD5",
                    "Cache-Control",
                    "Content-Disposition",
                    "BlobType",
                    "AccessTier",
                    "AccessTierInferred",
                    "LeaseStatus",
                    "LeaseState",
                    "LeaseDuration",
                    "ServerEncrypted"
                )
                props[all_props[!all_props %in% names(props)]] <- NA
                props
            }

            props <- c(Type="Blob", Name=blob$Name, normalize_blob_properties(blob$Properties))
            data.frame(lapply(props, function(p) if(!is_empty(p)) unlist(p) else NA),
                              stringsAsFactors=FALSE, check.names=FALSE)
        })

        df_prefixes <- do.call(rbind, prefix_rows)
        df_blobs <- do.call(rbind, blob_rows)

        if(is.null(df_prefixes) & is.null(df_blobs))
            return(data.frame())
        else if(is.null(df_prefixes))
            df <- df_blobs
        else if(is.null(df_blobs))
            df <- df_prefixes
        else
        {
            missing_cols <- setdiff(colnames(df_blobs), intersect(colnames(df_prefixes), colnames(df_blobs)))
            df_prefixes[, missing_cols] <- NA
            df <- rbind(df_prefixes, df_blobs)
        }

        if(length(df) > 0)
        {
            row.names(df) <- NULL

            # reorder and rename first 2 columns for consistency with ADLS, file
            ndf <- names(df)
            namecol <- which(ndf == "Name")
            sizecol <- which(ndf == "Content-Length")
            typecol <- which(names(df) == "BlobType")
            names(df)[c(namecol, sizecol, typecol)] <- c("name", "size", "blobtype")

            df$size <- if(!is.null(df$size)) as.numeric(df$size) else NA
            df$size[df$size == 0] <- NA
            df$isdir <- is.na(df$size)

            dircol <- which(names(df) == "isdir")

            if(info == "all")
            {
                if(!is.null(df$`Last-Modified`))
                    df$`Last-Modified` <- as_datetime(df$`Last-Modified`)
                if(!is.null(df$`Creation-Time`))
                    df$`Creation-Time` <- as_datetime(df$`Creation-Time`)
                cbind(df[c(namecol, sizecol, dircol, typecol)], df[-c(namecol, sizecol, dircol, typecol)])
            }
            else df[c(namecol, sizecol, dircol, typecol)]
        }
        else data.frame()
    }
    else unname(vapply(lst, function(b) b$Name[[1]], FUN.VALUE=character(1)))
}

#' @rdname blob
#' @export
upload_blob <- function(container, src, dest=basename(src), type=c("BlockBlob", "AppendBlob"),
                        blocksize=if(type == "BlockBlob") 2^24 else 2^22,
                        lease=NULL, put_md5=FALSE, append=FALSE, use_azcopy=FALSE)
{
    type <- match.arg(type)
    if(use_azcopy)
        azcopy_upload(container, src, dest, type=type, blocksize=blocksize, lease=lease, put_md5=put_md5)
    else upload_blob_internal(container, src, dest, type=type, blocksize=blocksize, lease=lease,
                              put_md5=put_md5, append=append)
}

#' @rdname blob
#' @export
multiupload_blob <- function(container, src, dest, recursive=FALSE, type=c("BlockBlob", "AppendBlob"),
                             blocksize=if(type == "BlockBlob") 2^24 else 2^22,
                             lease=NULL, put_md5=FALSE, append=FALSE, use_azcopy=FALSE,
                             max_concurrent_transfers=10)
{
    type <- match.arg(type)
    if(use_azcopy)
        return(azcopy_upload(container, src, dest, type=type, blocksize=blocksize, lease=lease, put_md5=put_md5,
                             recursive=recursive))

    multiupload_internal(container, src, dest, recursive=recursive, type=type, blocksize=blocksize, lease=lease,
                         put_md5=put_md5, append=append, max_concurrent_transfers=max_concurrent_transfers)
}

#' @rdname blob
#' @export
download_blob <- function(container, src, dest=basename(src), blocksize=2^24, overwrite=FALSE, lease=NULL,
                          check_md5=FALSE, use_azcopy=FALSE)
{
    if(use_azcopy)
        azcopy_download(container, src, dest, overwrite=overwrite, lease=lease, check_md5=check_md5)
    else download_blob_internal(container, src, dest, blocksize=blocksize, overwrite=overwrite, lease=lease,
                                check_md5=check_md5)
}

#' @rdname blob
#' @export
multidownload_blob <- function(container, src, dest, recursive=FALSE, blocksize=2^24, overwrite=FALSE, lease=NULL,
                               check_md5=FALSE, use_azcopy=FALSE,
                               max_concurrent_transfers=10)
{
    if(use_azcopy)
        return(azcopy_download(container, src, dest, overwrite=overwrite, lease=lease, recursive=recursive,
                               check_md5=check_md5))

    multidownload_internal(container, src, dest, recursive=recursive, blocksize=blocksize, overwrite=overwrite,
                           lease=lease, check_md5=check_md5, max_concurrent_transfers=max_concurrent_transfers)
}

#' @rdname blob
#' @export
delete_blob <- function(container, blob, confirm=TRUE)
{
    if(!delete_confirmed(confirm, paste0(container$endpoint$url, container$name, "/", blob), "blob"))
        return(invisible(NULL))

    invisible(do_container_op(container, blob, http_verb="DELETE"))
}

#' @rdname blob
#' @export
create_blob_dir <- function(container, dir)
{
    # workaround: upload a zero-length file to the desired dir, then delete the file
    destfile <- file.path(dir, basename(tempfile()))

    opts <- options(azure_storage_progress_bar=FALSE)
    on.exit(options(opts))

    upload_blob(container, rawConnection(raw(0)), destfile)
    delete_blob(container, destfile, confirm=FALSE)
    invisible(NULL)
}

#' @rdname blob
#' @export
delete_blob_dir <- function(container, dir, recursive=FALSE, confirm=TRUE)
{
    if(dir %in% c("/", "."))
        return(invisible(NULL))

    if(!delete_confirmed(confirm, paste0(container$endpoint$url, container$name, "/", dir), "directory"))
        return(invisible(NULL))

    if(recursive)
    {
        conts <- list_blobs(container, dir, recursive=TRUE, info="name")
        for(n in conts)
            delete_blob(container, n, confirm=FALSE)
    }

    parent <- dirname(dir)
    if(parent == ".")
        parent <- "/"
    lst <- list_blobs(container, parent, recursive=FALSE)
    whichrow <- which(lst$name == paste0(dir, "/"))
    if(is_empty(whichrow) || !lst$isdir[whichrow])
        stop("Not a directory", call.=FALSE)

    delete_blob(container, dir, confirm=FALSE)
}

#' @rdname blob
#' @export
blob_exists <- function(container, blob)
{
    res <- do_container_op(container, blob, headers = list(), http_verb = "HEAD", http_status_handler = "pass")
    if(httr::status_code(res) == 404L)
        return(FALSE)

    httr::stop_for_status(res, storage_error_message(res))
    return(TRUE)
}

