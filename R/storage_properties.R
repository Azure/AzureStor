#' Get storage properties for an object
#'
#' @param object A storage object: an endpoint, blob container, file share, or ADLS filesystem.
#' @param filesystem An ADLS filesystem.
#' @param blob,file The name of an individual blob, file or directory within a container.
#' @param isdir For the file share method, whether the `file` argument is a file or directory. If omitted, `get_storage_properties` will auto-detect the type; however this can be slow, so supply this argument if possible.
#' @param ... For compatibility with the generic.
#' @return
#' `get_storage_properties` returns a list describing the object properties. If the `blob` or `file` argument is present, the properties will be for the blob/file specified. If this argument is omitted, the properties will be for the container itself.
#'
#' `get_adls_file_acl` returns a string giving the ACL for the file.
#'
#' `get_adls_file_status` returns a list of system properties for the file.
#'
#' @seealso
#' [storage_endpoint], [blob_container], [file_share], [adls_filesystem]
#'
#' [metadata] for getting and setting _user-defined_ properties (metadata)
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
            stop(res)
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
#' @param blob,file The name of an individual blob, file or directory within a container.
#' @param isdir For the file share method, whether the `file` argument is a file or directory. If omitted, `get_storage_metadata` will auto-detect the type; however this can be slow, so supply this argument if possible.
#' @param ... For the metadata setters, name-value pairs to set as metadata for a blob or file.
#' @param keep_existing For the metadata setters, whether to retain existing metadata information.
#' @details
#' These methods let you get and set user-defined properties (metadata) for storage objects.
#' @return
#' `get_storage_metadata` returns a named list of metadata properties. `set_storage_metadata` returns the same list after setting the object's metadata, invisibly.
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
