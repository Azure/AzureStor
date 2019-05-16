#' Operations on a blob endpoint
#'
#' Get, list, create, or delete blob containers.
#'
#' @param endpoint Either a blob endpoint object as created by [storage_endpoint], or a character string giving the URL of the endpoint.
#' @param key,token,sas If an endpoint object is not supplied, authentication credentials: either an access key, an Azure Active Directory (AAD) token, or a SAS, in that order of priority. If no authentication credentials are provided, only public (anonymous) access to the share is possible.
#' @param api_version If an endpoint object is not supplied, the storage API version to use when interacting with the host. Currently defaults to `"2018-03-28"`.
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
#' Currently (as of February 2019), if hierarchical namespaces are enabled on a storage account, the blob API for the account is disabled. The blob endpoint is still accessible, but blob operations on the endpoint will fail. Full interoperability between blobs and ADLSgen2 is planned for later in 2019.
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
    cat(sprintf("URL: %s\n", paste0(x$endpoint$url, x$name)))

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
    lst <- do_storage_call(endpoint$url, "/", options=list(comp="list"),
                           key=endpoint$key, token=endpoint$token, sas=endpoint$sas,
                           api_version=endpoint$api_version)

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
    if(confirm && interactive())
    {
        path <- paste0(endpoint$url, name)
        yn <- readline(paste0("Are you sure you really want to delete the container '", path, "'? (y/N) "))
        if(tolower(substr(yn, 1, 1)) != "y")
            return(invisible(NULL))
    }
    headers <- if(!is_empty(lease))
        list("x-ms-lease-id"=lease)
    else list()

    obj <- blob_container(endpoint, name)
    do_container_op(obj, options=list(restype="container"), headers=headers, http_verb="DELETE")
}


#' Operations on a blob container or blob
#'
#' Upload, download, or delete a blob; list blobs in a container.
#'
#' @param container A blob container object.
#' @param blob A string naming a blob.
#' @param src,dest The source and destination files for uploading and downloading. See 'Details' below.For uploading, `src` can also be a [textConnection] or [rawConnection] object to allow transferring in-memory R objects without creating a temporary file. For downloading, 
#' @param info For `list_blobs`, level of detail about each blob to return: a vector of names only; the name, size and last-modified date (default); or all information.
#' @param confirm Whether to ask for confirmation on deleting a blob.
#' @param blocksize The number of bytes to upload/download per HTTP(S) request.
#' @param lease The lease for a blob, if present.
#' @param type When uploading, the type of blob to create. Currently only block blobs are supported.
#' @param overwrite When downloading, whether to overwrite an existing destination file.
#' @param retries The number of times the file transfer functions will retry when they encounter an error. Set this to 0 to disable retries. This is applied per block for uploading, and to the entire blob for downloading.
#' @param use_azcopy Whether to use the AzCopy utility from Microsoft to do the transfer, rather than doing it in R.
#' @param max_concurrent_transfers For `multiupload_blob` and `multidownload_blob`, the maximum number of concurrent file transfers. Each concurrent file transfer requires a separate R process, so limit this if you are low on memory.
#' @param prefix For `list_blobs`, filters the result to return only blobs whose name begins with this prefix.
#'
#' @details
#' `upload_blob` and `download_blob` are the workhorse file transfer functions for blobs. They each take as inputs a _single_ filename or connection as the source for uploading/downloading, and a single filename as the destination.
#'
#' `multiupload_blob` and `multidownload_blob` are functions for uploading and downloading _multiple_ blobs at once. They parallelise file transfers by deploying a pool of R processes in the background, which can lead to significantly greater efficiency when transferring many small files. They take as input a wildcard pattern as the source, which expands to one or more files. The `dest` argument should be a directory.
#'
#' The file transfer functions also support working with connections to allow transferring R objects without creating temporary files. For uploading, `src` can be a [textConnection] or [rawConnection] object. For downloading, `dest` can be NULL or a `rawConnection` object. In the former case, the downloaded data is returned as a raw vector, and for the latter, it will be placed into the connection. See the examples below.
#'
#' By default, `download_blob` will display a progress bar as it is downloading. To turn this off, use `options(azure_dl_progress_bar=FALSE)`. To turn the progress bar back on, use `options(azure_dl_progress_bar=TRUE)`.
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
#' }
#' @rdname blob
#' @export
list_blobs <- function(container, info=c("partial", "name", "all"),
                       prefix=NULL)
{
    info <- match.arg(info)

    opts <- list(comp="list", restype="container")
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
        })

        df <- do.call(rbind, rows)
        if(length(df) > 0)
        {
            df$`Last-Modified` <- as.POSIXct(df$`Last-Modified`, format="%a, %d %b %Y %H:%M:%S", tz="GMT")
            df$`Content-Length` <- as.numeric(df$`Content-Length`)
            row.names(df) <- NULL
            if(info == "partial")
                df[c("Name", "Last-Modified", "Content-Length")]
            else df
        }
        else list()
    }
    else unname(vapply(lst, function(b) b$Name[[1]], FUN.VALUE=character(1)))
}

#' @rdname blob
#' @export
upload_blob <- function(container, src, dest, type="BlockBlob", blocksize=2^24, lease=NULL, retries=5,
                        use_azcopy=FALSE)
{
    if(use_azcopy)
        azcopy_upload(container, src, dest, type=type, blocksize=blocksize, lease=lease)
    else upload_blob_internal(container, src, dest, type=type, blocksize=blocksize, lease=lease, retries=retries)
}

#' @rdname blob
#' @export
multiupload_blob <- function(container, src, dest, type="BlockBlob", blocksize=2^24, lease=NULL, retries=5,
                             use_azcopy=FALSE,
                             max_concurrent_transfers=10)
{
    if(use_azcopy)
        azcopy_upload(container, src, dest, type=type, blocksize=blocksize, lease=lease)
    else multiupload_blob_internal(container, src, dest, type=type, blocksize=blocksize, lease=lease, retries=retries,
                                   max_concurrent_transfers=max_concurrent_transfers)
}

#' @rdname blob
#' @export
download_blob <- function(container, src, dest, blocksize=2^24, overwrite=FALSE, lease=NULL, retries=5,
                          use_azcopy=FALSE)
{
    if(use_azcopy)
        azcopy_download(container, src, dest, overwrite=overwrite, lease=lease)
    else download_blob_internal(container, src, dest, blocksize=blocksize, overwrite=overwrite, lease=lease,
                                retries=retries)
}

#' @rdname blob
#' @export
multidownload_blob <- function(container, src, dest, blocksize=2^24, overwrite=FALSE, lease=NULL, retries=5,
                               use_azcopy=FALSE,
                               max_concurrent_transfers=10)
{
    if(use_azcopy)
        azcopy_download(container, src, dest, overwrite=overwrite, lease=lease)
    else multidownload_blob_internal(container, src, dest, blocksize=blocksize, overwrite=overwrite, lease=lease,
                                     retries=retries,
                                     max_concurrent_transfers=max_concurrent_transfers)
}

#' @rdname blob
#' @export
delete_blob <- function(container, blob, confirm=TRUE)
{
    if(confirm && interactive())
    {
        endp <- container$endpoint
        path <- paste0(endp$url, container$name, "/", blob)
        yn <- readline(paste0("Are you sure you really want to delete '", path, "'? (y/N) "))
        if(tolower(substr(yn, 1, 1)) != "y")
            return(invisible(NULL))
    }
    do_container_op(container, blob, http_verb="DELETE")
}


