#' Operations on a blob endpoint
#'
#' Get, list, create, or delete blob containers.
#'
#' @param endpoint Either a blob endpoint object as created by [storage_endpoint], or a character string giving the URL of the endpoint.
#' @param key,sas If an endpoint object is not supplied, authentication details. If a key is provided, the SAS is not used. If neither an access key nor a SAS are provided, only public (anonymous) access to the share is possible.
#' @param api_version If an endpoint object is not supplied, the storage API version to use when interacting with the host. Currently defaults to `"2017-07-29"`.
#' @param name The name of the blob container to get, create, or delete.
#' @param confirm For deleting a container, whether to ask for confirmation.
#' @param lease For deleting a leased container, the lease ID.
#' @param public_access For creating a container, the level of public access to allow.
#' @param ... Further arguments passed to lower-level functions.
#'
#' @details
#' You can call these functions in a couple of ways: by passing the full URL of the share, or by passing the endpoint object and the name of the container as a string.
#'
#' @return
#' For `blob_container` and `create_blob_container`, an S3 object representing an existing or created container respectively.
#'
#' For `list_blob_containers`, a list of such objects.
#'
#' @seealso [storage_endpoint], [az_storage]
#'
#' @rdname blob_endpoint
#' @export
blob_container <- function(endpoint, ...)
{
    UseMethod("blob_container")
}

#' @rdname blob_endpoint
#' @export
blob_container.character <- function(endpoint, key=NULL, sas=NULL, api_version=getOption("azure_storage_api_version"))
{
    do.call(blob_container, generate_endpoint_container(endpoint, key, sas, api_version))
}

#' @rdname blob_endpoint
#' @export
blob_container.blob_endpoint <- function(endpoint, name)
{
    obj <- list(name=name, endpoint=endpoint)
    class(obj) <- "blob_container"
    obj
}

#' @rdname blob_endpoint
#' @export
print.blob_container <- function(object, ...)
{
    cat("Azure blob container\n")
    cat(sprintf("Endpoint URL: %s\n", object$endpoint$url))
    if(!is_empty(object$endpoint$key))
        cat("Access key: <hidden>\n")
    else cat("Access key: <none supplied>\n")
    if(!is_empty(object$endpoint$sas))
        cat("Account shared access signature: <hidden>\n")
    else cat("Account shared access signature: <none supplied>\n")
    cat(sprintf("Storage API version: %s\n", object$endpoint$api_version))
    invisible(object)
}


#' @rdname blob_endpoint
#' @export
list_blob_containers <- function(endpoint, ...)
{
    UseMethod("list_blob_containers")
}

#' @rdname blob_endpoint
#' @export
list_blob_containers.character <- function(endpoint, key=NULL, sas=NULL,
                                           api_version=getOption("azure_storage_api_version"))
{
    do.call(list_blob_containers, generate_endpoint_container(endpoint, key, sas, api_version))
}

#' @rdname blob_endpoint
#' @export
list_blob_containers.blob_endpoint <- function(endpoint, ...)
{
    lst <- do_storage_call(endpoint$url, "/", options=list(comp="list"),
                           key=endpoint$key, sas=endpoint$sas, api_version=endpoint$api_version)

    lst <- lapply(lst$Containers, function(cont) blob_container(endpoint, cont$Name[[1]]))
    named_list(lst)
}



#' @rdname blob_endpoint
#' @export
create_blob_container <- function(endpoint, ...)
{
    UseMethod("create_blob_container")
}

#' @rdname blob_endpoint
#' @export
create_blob_container.character <- function(endpoint, key=NULL, sas=NULL,
                                            api_version=getOption("azure_storage_api_version"),
                                            ...)
{
    endp <- generate_endpoint_container(endpoint, key, sas, api_version)
    create_blob_container(endp$endpoint, endp$name, ...)
}

#' @rdname blob_endpoint
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



#' @rdname blob_endpoint
#' @export
delete_blob_container <- function(endpoint, ...)
{
    UseMethod("delete_blob_container")
}

#' @rdname blob_endpoint
#' @export
delete_blob_container.character <- function(endpoint, key=NULL, sas=NULL,
                                            api_version=getOption("azure_storage_api_version"),
                                            ...)
{
    endp <- generate_endpoint_container(endpoint, key, sas, api_version)
    delete_blob_container(endp$endpoint, endp$name, ...)
}

#' @rdname blob_endpoint
#' @export
delete_blob_container.blob_endpoint <- function(endpoint, name, confirm=TRUE, lease=NULL)
{
    if(confirm && interactive())
    {
        path <- paste0(endpoint$url, name, "/")
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


#' Operations on a blob container
#'
#' Upload, download, or delete a blob; list blobs in a container.
#'
#' @param container A blob container object.
#' @param blob A string naming a blob.
#' @param src,dest The source and destination filenames for uploading and downloading. Paths are allowed.
#' @param info For `list_blobs`, level of detail about each blob to return: a vector of names only; the name, size and last-modified date (default); or all information.
#' @param confirm Whether to ask for confirmation on deleting a blob.
#'
#' @return
#' For `list_blobs`, details on the blobs in the container.
#'
#' @seealso
#' [blob_container], [az_storage]
#'
#' @rdname blob_container
#' @export
list_blobs <- function(container, info=c("partial", "name", "all"))
{
    info <- match.arg(info)
    lst <- do_container_op(container, options=list(comp="list", restype="container"))
    if(info != "name")
    {
        rows <- lapply(lst$Blobs, function(blob)
        {
            props <- c(Name=blob$Name, blob$Properties)
            props <- data.frame(lapply(props, function(p) if(!is_empty(p)) unlist(p) else NA),
                                stringsAsFactors=FALSE, check.names=FALSE)
        })

        df <- do.call(rbind, rows)
        df$`Last-Modified` <- as.POSIXct(df$`Last-Modified`, format="%a, %d %b %Y %H:%M:%S", tz="GMT")
        row.names(df) <- NULL

        if(info == "partial")
            df[c("Name", "Last-Modified", "Content-Length")]
        else df
    }
    else unname(vapply(lst$Blobs, function(b) b$Name[[1]], FUN.VALUE=character(1)))
}

#' @rdname blob_container
#' @export
upload_blob <- function(container, src, dest, type="BlockBlob")
{
    # TODO: upload in chunks
    body <- readBin(src, "raw", file.info(src)$size)
    hash <- openssl::base64_encode(openssl::md5(body))

    headers <- list("content-length"=length(body),
                    "content-md5"=hash,
                    "content-type"="application/octet-stream",
                    "x-ms-blob-type"=type)

    do_container_op(container, dest, headers=headers, body=body,
                 http_verb="PUT")
}

#' @rdname blob_container
#' @export
download_blob <- function(container, src, dest, overwrite=FALSE)
{
    do_container_op(container, src, config=httr::write_disk(dest, overwrite))
}

#' @rdname blob_container
#' @export
delete_blob <- function(container, blob, confirm=TRUE)
{
    if(confirm && interactive())
    {
        endp <- container$endpoint
        path <- paste0(endp$url, endp$name, blob, "/")
        yn <- readline(paste0("Are you sure you really want to delete '", path, "'? (y/N) "))
        if(tolower(substr(yn, 1, 1)) != "y")
            return(invisible(NULL))
    }
    do_container_op(container, blob, http_verb="DELETE")
}


