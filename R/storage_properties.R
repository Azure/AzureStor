#' Get storage properties for an object
#'
#' @param object A storage object: an endpoint, blob container, file share, or ADLS filesystem.
#' @param filesystem An ADLS filesystem.
#' @param file The name of an individual blob, file or directory within a container.
#' @param isdir Whether the object is a file or directory.
#'
#' @details
#' The `get_storage_properties` generic returns a list of properties for the given storage object. There are methods defined for objects of class `storage_endpoint`, `blob_container`, `file_share` and `adls_filesystem`. Similar functions are defined for individual blobs, files and directories.
#'
#' @return
#' A list describing the object properties.
#'
#' @seealso
#' [storage_endpoint], [blob_container], [file_share]
#'
#' [Blob service properties reference[(https://docs.microsoft.com/en-us/rest/api/storageservices/get-blob-service-properties).
#' [File service properties reference](https://docs.microsoft.com/en-us/rest/api/storageservices/get-file-service-properties),
#' [Blob container properties reference](https://docs.microsoft.com/en-us/rest/api/storageservices/get-container-properties),
#' [File share properties reference](https://docs.microsoft.com/en-us/rest/api/storageservices/get-share-properties),
#' [ADLS filesystem properties reference](https://docs.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/filesystem/getproperties)
#'
#' @rdname properties
#' @export
get_storage_properties <- function(object, ...)
{
    UseMethod("get_storage_properties")
}


#' @rdname properties
#' @export
get_storage_properties.blob_endpoint <- function(object, ...)
{
    res <- call_storage_endpoint(object, "", options=list(restype="service", comp="properties"))
    tidy_list(res)
}


#' @rdname properties
#' @export
get_storage_properties.file_endpoint <- function(object, ...)
{
    res <- call_storage_endpoint(object, "", options=list(restype="service", comp="properties"))
    tidy_list(res)
}


#' @rdname properties
#' @export
get_storage_properties.blob_container <- function(object, blob, ...)
{
    # properties for container
    if(missing(blob))
        return(do_container_op(object, options=list(restype="container"), http_verb="HEAD"))

    # properties for blob
    do_container_op(object, blob, http_verb="HEAD")
}


#' @rdname properties
#' @export
get_storage_properties.file_share <- function(object, file, isdir, ...)
{
    # properties for container
    if(missing(file))
        return(do_container_op(object, options=list(restype="share"), http_verb="HEAD"))

    # properties for file/directory
    if(missing(isdir))
    {
        res <- tryCatch(Recall(object, file, FALSE), error=function(e) e)
        if(inherits(res, "error"))
            res <- tryCatch(Recall(object, file, TRUE), error=function(e) e)
        if(inherits(res, "error"))
            stop(e)
        return(res)
    }

    options <- if(isdir) list(restype="directory") else list()
    do_container_op(object, file, options=options, http_verb="HEAD")
}


#' @rdname properties
#' @export
get_storage_properties.adls_filesystem <- function(object, file, ...)
{
    # properties for container
    if(missing(file))
        return(do_container_op(object, options=list(resource="filesystem"), http_verb="HEAD"))

    # properties for file/directory
    do_container_op(object, file, http_verb="HEAD")
}


#' @rdname properties
#' @export
get_adls_file_acl <- function(filesystem, file)
{
    do_container_op(filesystem, file, options=list(action="getaccesscontrol"), http_verb="HEAD")[["x-ms-acl"]]
}


#' @rdname properties
#' @export
get_adls_file_status <- function(filesystem, file)
{
    do_container_op(filesystem, file, options=list(action="getstatus"), http_verb="HEAD")
}


#' Get/set user-defined metadata for a storage object
#'
#' @param object A blob container, file share or ADLS filesystem object.
#' @param file The name of an individual blob, file or directory within a container.
#' @param isdir For the file share method, whether the object is a file or directory.
#' @param ... For the metadata setters, name-value pairs to set as metadata for a blob or file.
#' @param keep_existing For the metadata setters, whether to retain existing metadata information.
#' @rdname metadata
#' @export
get_storage_metadata <- function(object, ...)
{
    UseMethod("get_storage_metadata")
}

#' @rdname metadata
#' @export
get_storage_metadata.blob_container <- function(object, blob, ...)
{
    res <- do_container_op(object, blob, options=list(comp="metadata"), http_verb="HEAD")
    get_classic_metadata_headers(res)
}


#' @rdname metadata
#' @export
get_storage_metadata.file_share <- function(object, file, isdir, ...)
{
    if(missing(isdir))
    {
        res <- tryCatch(Recall(object, file, FALSE), error=function(e) e)
        if(inherits(res, "error"))
            res <- tryCatch(Recall(object, file, TRUE), error=function(e) e)
        if(inherits(res, "error"))
            stop(res)
        return(res)
    }

    options <- if(isdir)
        list(restype="directory", comp="metadata")
    else list(comp="metadata")
    res <- do_container_op(object, file, options=options, http_verb="HEAD")
    get_classic_metadata_headers(res)
}


#' @rdname metadata
#' @export
get_storage_metadata.adls_filesystem <- function(object, file, ...)
{
    res <- get_storage_properties(object, file)
    get_adls_metadata_header(res)
}


#' @rdname metadata
#' @export
set_storage_metadata <- function(object, ...)
{
    UseMethod("set_storage_metadata")
}


#' @rdname metadata
#' @export
set_storage_metadata.blob_container <- function(object, blob, ..., keep_existing=TRUE)
{
    meta <- if(keep_existing)
        modifyList(get_storage_metadata(object, blob), list(...))
    else list(...)

    do_container_op(object, blob, options=list(comp="metadata"), headers=set_classic_metadata_headers(meta),
                    http_verb="PUT")
    invisible(meta)
}


#' @rdname metadata
#' @export
set_storage_metadata.file_share <- function(object, file, isdir, ..., keep_existing=TRUE)
{
    if(missing(isdir))
    {
        res <- tryCatch(Recall(object, file, ..., keep_existing=keep_existing, isdir=TRUE), error=function(e) e)
        if(inherits(res, "error"))
            res <- tryCatch(Recall(object, file, ..., keep_existing=keep_existing, isdir=FALSE), error=function(e) e)
        if(inherits(res, "error"))
            stop(res)
        return(res)
    }

    meta <- if(keep_existing)
        modifyList(get_storage_metadata(object, file, isdir=isdir), list(...))
    else list(...)
    options <- if(isdir)
        list(restype="directory", comp="metadata")
    else list(comp="metadata")
    do_container_op(object, file, options=options, headers=set_classic_metadata_headers(meta),
                    http_verb="PUT")
    invisible(meta)
}


#' @rdname metadata
#' @export
set_storage_metadata.adls_filesystem <- function(object, file, ..., keep_existing=TRUE)
{
    meta <- if(keep_existing)
        modifyList(get_storage_metadata(object, file), list(...))
    else list(...)

    do_container_op(object, file, options=list(action="setProperties"), headers=set_adls_metadata_header(meta),
                    http_verb="PATCH")
    invisible(meta)
}


# recursively tidy XML list: turn leaf nodes into scalars
tidy_list <- function(x)
{
    if(is_empty(x))
        return()
    else if(!is.list(x[[1]]))
    {
        x <- unlist(x)
        if(x %in% c("true", "false"))
            x <- as.logical(x)
        else if(!is.numeric(x) && !is.na(suppressWarnings(as.numeric(x))))
            x <- as.numeric(x)
        x
    }
    else lapply(x, tidy_list)
}


get_classic_metadata_headers <- function(res)
{
    res <- res[grepl("^x-ms-meta-", names(res))]
    names(res) <- substr(names(res), 11, nchar(names(res)))
    res
}


set_classic_metadata_headers <- function(metalist)
{
    if(is_empty(metalist))
        return(metalist)
    if(is.null(names(metalist)) || any(names(metalist) == ""))
        stop("All metadata values must be named")
    names(metalist) <- paste0("x-ms-meta-", names(metalist))
    metalist
}


get_adls_metadata_header <- function(res)
{
    meta <- strsplit(res[["x-ms-properties"]], ",")[[1]]
    pos <- regexpr("=", meta)
    if(any(pos == -1))
        stop("Error getting object metadata", call.=FALSE)

    metanames <- substr(meta, 1, pos - 1)
    metavals <- lapply(substr(meta, pos + 1, nchar(meta)),
                       function(x) rawToChar(openssl::base64_decode(x)))
    names(metavals) <- metanames
    metavals
}


set_adls_metadata_header <- function(metalist)
{
    if(is.null(names(metalist)) || any(names(metalist) == ""))
        stop("All metadata values must be named")

    metalist <- sapply(metalist, function(x) openssl::base64_encode(as.character(x)))
    list(`x-ms-properties`=paste(names(metalist), metalist, sep="=", collapse=","))
}
