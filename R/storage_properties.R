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
#' [get_storage_metadata] for getting and setting _user-defined_ properties (metadata)
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

