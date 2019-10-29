#' Get storage properties for an endpoint or container
#'
#' @param object A storage object.
#' @param container,share,filesystem A blob container, file share, or ADLS filesystem object.
#' @param blob,file,dir The name of an individual blob, file or directory.
#' @param ... For the metadata setters, name-value pairs to set as metadata for a blob or file.
#' @param keep_existing For the metadata setters, whether to retain existing metadata information.
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
get_storage_properties <- function(object)
{
    UseMethod("get_storage_properties")
}


#' @rdname properties
#' @export
get_storage_properties.blob_endpoint <- function(object)
{
    res <- call_storage_endpoint(object, "", options=list(restype="service", comp="properties"))
    tidy_list(res)
}


#' @rdname properties
#' @export
get_storage_properties.file_endpoint <- function(object)
{
    res <- call_storage_endpoint(object, "", options=list(restype="service", comp="properties"))
    tidy_list(res)
}


#' @rdname properties
#' @export
get_storage_properties.blob_container <- function(object)
{
    do_container_op(object, options=list(restype="container"), http_verb="HEAD")
}


#' @rdname properties
#' @export
get_storage_properties.file_share <- function(object)
{
    do_container_op(object, options=list(restype="share"), http_verb="HEAD")
}


#' @rdname properties
#' @export
get_storage_properties.adls_filesystem <- function(object)
{
    do_container_op(object, options=list(resource="filesystem"), http_verb="HEAD")
}


#' @rdname properties
#' @export
get_blob_properties <- function(container, blob)
{
    do_container_op(container, blob, http_verb="HEAD")
}


#' @rdname properties
#' @export
get_azure_file_properties <- function(share, file)
{
    do_container_op(share, file, http_verb="HEAD")
}


#' @rdname properties
#' @export
get_azure_dir_properties <- function(share, dir)
{
    do_container_op(share, dir, options=list(restype="directory"), http_verb="HEAD")
}


#' @rdname properties
#' @export
get_adls_file_properties <- function(filesystem, file)
{
    do_container_op(filesystem, file, http_verb="HEAD")
}


#' @rdname properties
#' @export
get_adls_dir_properties <- function(filesystem, dir)
{
    do_container_op(filesystem, dir, http_verb="HEAD")
}


#' @rdname properties
#' @export
get_adls_file_acls <- function(filesystem, file)
{
    do_container_op(filesystem, file, options=list(action="getaccesscontrol"), http_verb="HEAD")[["x-ms-acl"]]
}


#' @rdname properties
#' @export
get_adls_dir_acls <- function(filesystem, dir)
{
    do_container_op(filesystem, dir, options=list(action="getaccesscontrol"), http_verb="HEAD")[["x-ms-acl"]]
}

#' @rdname properties
#' @export
get_adls_file_status <- function(filesystem, file)
{
    do_container_op(filesystem, file, options=list(action="getstatus"), http_verb="HEAD")
}


#' @rdname properties
#' @export
get_adls_dir_status <- function(filesystem, dir)
{
    do_container_op(filesystem, dir, options=list(action="getstatus"), http_verb="HEAD")
}


#' @rdname properties
#' @export
get_blob_metadata <- function(container, blob)
{
    res <- do_container_op(container, blob, options=list(comp="metadata"), http_verb="HEAD")
    get_classic_metadata_headers(res)
}


#' @rdname properties
#' @export
get_azure_file_metadata <- function(share, file)
{
    res <- do_container_op(share, file, options=list(comp="metadata"), http_verb="HEAD")
    get_classic_metadata_headers(res)
}


#' @rdname properties
#' @export
get_azure_dir_metadata <- function(share, dir)
{
    res <- do_container_op(share, dir, options=list(restype="directory", comp="metadata"), http_verb="HEAD")
    get_classic_metadata_headers(res)
}


#' @rdname properties
#' @export
get_adls_file_metadata <- function(filesystem, file)
{
    res <- get_adls_file_properties(filesystem, file)
    get_adls_metadata_header(res)
}


#' @rdname properties
#' @export
get_adls_dir_metadata <- function(filesystem, dir)
{
    res <- get_adls_file_properties(filesystem, dir)
    get_adls_metadata_header(res)
}


#' @rdname properties
#' @export
set_blob_metadata <- function(container, blob, ..., keep_existing=TRUE)
{
    meta <- if(keep_existing)
        modifyList(get_blob_metadata(container, blob), list(...))
    else list(...)

    do_container_op(container, blob, options=list(comp="metadata"), headers=set_classic_metadata_headers(meta),
                    http_verb="PUT")
    invisible(meta)
}


#' @rdname properties
#' @export
set_azure_file_metadata <- function(share, file, ..., keep_existing=TRUE)
{
    meta <- if(keep_existing)
        modifyList(get_azure_file_metadata(share, file), list(...))
    else list(...)

    do_container_op(share, file, options=list(comp="metadata"), headers=set_classic_metadata_headers(meta),
                    http_verb="PUT")
    invisible(meta)
}


#' @rdname properties
#' @export
set_azure_dir_metadata <- function(share, dir, ..., keep_existing=TRUE)
{
    meta <- if(keep_existing)
        modifyList(get_azure_dir_metadata(share, dir), list(...))
    else list(...)

    do_container_op(share, dir, options=list(restype="directory", comp="metadata"),
                    headers=set_classic_metadata_headers(meta), http_verb="PUT")
    invisible(meta)
}


#' @rdname properties
#' @export
set_adls_file_metadata <- function(filesystem, file, ..., keep_existing=TRUE)
{
    meta <- if(keep_existing)
        modifyList(get_adls_file_metadata(filesystem, file), list(...))
    else list(...)

    do_container_op(filesystem, file, options=list(action="setProperties"),
                    headers=set_adls_metadata_header(meta), http_verb="PATCH")
    invisible(meta)
}


#' @rdname properties
#' @export
set_adls_dir_metadata <- function(filesystem, dir, ..., keep_existing=TRUE)
{
    meta <- if(keep_existing)
        modifyList(get_adls_dir_metadata(filesystem, dir), list(...))
    else list(...)

    do_container_op(filesystem, dir, options=list(action="setProperties"),
                    headers=set_adls_metadata_header(meta), http_verb="PATCH")
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
