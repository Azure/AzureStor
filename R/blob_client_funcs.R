#' Operations on a blob endpoint
#'
#' Get, list, create, or delete blob containers.
#'
#' @param endpoint Either a blob endpoint object as created by [storage_endpoint], or a character string giving the URL of the endpoint.
#' @param key,token,sas If an endpoint object is not supplied, authentication credentials: either an access key, an Azure Active Directory (AAD) token, or a SAS, in that order of priority. If no authentication credentials are provided, only public (anonymous) access to the share is possible.
#' @param api_version If an endpoint object is not supplied, the storage API version to use when interacting with the host. Currently defaults to `"2018-11-09"`.
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
    lst <- call_storage_endpoint(endpoint, "/", options=list(comp="list"))

    lst <- lapply(lst$Containers, function(cont) blob_container(endpoint, cont$Name[[1]]))
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
#' Upload, download, or delete a blob; list blobs in a container.
#'
#' @param container A blob container object.
#' @param blob A string naming a blob.
#' @param dir For `list_blobs`, A string naming the directory. Note that blob storage does not support real directories; this argument simply filters the result to return only blobs whose names start with the given value.
#' @param src,dest The source and destination files for uploading and downloading. See 'Details' below.
#' @param info For `list_blobs`, level of detail about each blob to return: a vector of names only; the name, size, and whether this blob represents a directory; or all information.
#' @param confirm Whether to ask for confirmation on deleting a blob.
#' @param blocksize The number of bytes to upload/download per HTTP(S) request.
#' @param lease The lease for a blob, if present.
#' @param type When uploading, the type of blob to create. Currently only block blobs are supported.
#' @param overwrite When downloading, whether to overwrite an existing destination file.
#' @param use_azcopy Whether to use the AzCopy utility from Microsoft to do the transfer, rather than doing it in R.
#' @param max_concurrent_transfers For `multiupload_blob` and `multidownload_blob`, the maximum number of concurrent file transfers. Each concurrent file transfer requires a separate R process, so limit this if you are low on memory.
#' @param prefix For `list_blobs`, an alternative way to specify the directory.
#' @param recursive This argument is for consistency with the methods for the other storage types. It is not used for blob storage.
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
#' @return
#' For `list_blobs`, details on the blobs in the container. For `download_blob`, if `dest=NULL`, the contents of the downloaded blob as a raw vector.
#'
#' @seealso
#' [blob_container], [az_storage], [storage_download], [call_azcopy]
#'
#' [AzCopy version 10 on GitHub](https://github.com/Azure/azure-storage-azcopy)
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
    if(dir != "/")
        prefix <- dir

    if(!is_empty(prefix))
        opts <- c(opts, prefix=as.character(prefix))

    res <- do_container_op(container, options=opts)
    lst <- res$Blobs
    while(length(res$NextMarker) > 0)
    {
        res <- do_container_op(container, options=list(comp="list", restype="container", marker=res$NextMarker[[1]]))
        lst <- c(lst, res$Blobs)
    }

    if(info != "name")
    {
        rows <- lapply(lst, function(blob)
        {
            props <- c(Name=blob$Name, blob$Properties)
            props <- data.frame(lapply(props, function(p) if(!is_empty(p)) unlist(p) else NA),
                                stringsAsFactors=FALSE, check.names=FALSE)

            # ADLS/blob interop: dir in hns-enabled acct does not have LeaseState field
            if(is.null(props$LeaseState))
                props$LeaseState <- NA
            props
        })

        df <- do.call(rbind, rows)
        if(length(df) > 0)
        {
            row.names(df) <- NULL

            # reorder and rename first 2 columns for consistency with ADLS, file
            ndf <- names(df)
            namecol <- which(ndf == "Name")
            sizecol <- which(ndf == "Content-Length")
            names(df)[c(namecol, sizecol)] <- c("name", "size")
            df$size <- as.numeric(df$size)

            # needed when dir was created using ADLS API
            # this works because content-type is always set for an actual file
            df$isdir <- is.na(df$LeaseState) | is.na(df$`Content-Type`)
            df$size[df$isdir] <- NA
            dircol <- which(names(df) == "isdir")

            if(info == "all")
            {
                if(!is.null(df$`Last-Modified`))
                    df$`Last-Modified` <- as_datetime(df$`Last-Modified`)
                if(!is.null(df$`Creation-Time`))
                    df$`Creation-Time` <- as_datetime(df$`Creation-Time`)
                cbind(df[c(namecol, sizecol, dircol)], df[-c(namecol, sizecol, dircol)])
            }
            else df[c(namecol, sizecol, dircol)]
        }
        else data.frame()
    }
    else unname(vapply(lst, function(b) b$Name[[1]], FUN.VALUE=character(1)))
}

#' @rdname blob
#' @export
upload_blob <- function(container, src, dest=basename(src), type="BlockBlob", blocksize=2^24, lease=NULL,
                        use_azcopy=FALSE)
{
    if(use_azcopy)
        azcopy_upload(container, src, dest, type=type, blocksize=blocksize, lease=lease)
    else upload_blob_internal(container, src, dest, type=type, blocksize=blocksize, lease=lease)
}

#' @rdname blob
#' @export
multiupload_blob <- function(container, src, dest, recursive=FALSE, type="BlockBlob", blocksize=2^24, lease=NULL,
                             use_azcopy=FALSE,
                             max_concurrent_transfers=10)
{
    if(use_azcopy)
        return(azcopy_upload(container, src, dest, type=type, blocksize=blocksize, lease=lease))

    multiupload_internal(container, src, dest, recursive=recursive, type=type, blocksize=blocksize, lease=lease,
                         max_concurrent_transfers=max_concurrent_transfers)
}

#' @rdname blob
#' @export
download_blob <- function(container, src, dest=basename(src), blocksize=2^24, overwrite=FALSE, lease=NULL,
                          use_azcopy=FALSE)
{
    if(use_azcopy)
        azcopy_download(container, src, dest, overwrite=overwrite, lease=lease)
    else download_blob_internal(container, src, dest, blocksize=blocksize, overwrite=overwrite, lease=lease)
}

#' @rdname blob
#' @export
multidownload_blob <- function(container, src, dest, recursive=FALSE, blocksize=2^24, overwrite=FALSE, lease=NULL,
                               use_azcopy=FALSE,
                               max_concurrent_transfers=10)
{
    if(use_azcopy)
        return(azcopy_download(container, src, dest, overwrite=overwrite, lease=lease))

    multidownload_internal(container, src, dest, recursive=recursive, blocksize=blocksize, overwrite=overwrite,
                           lease=lease, max_concurrent_transfers=max_concurrent_transfers)
}

#' @rdname blob
#' @export
delete_blob <- function(container, blob, confirm=TRUE)
{
    if(!delete_confirmed(confirm, paste0(container$endpoint$url, container$name, "/", blob), "blob"))
        return(invisible(NULL))

    invisible(do_container_op(container, blob, http_verb="DELETE"))
}


