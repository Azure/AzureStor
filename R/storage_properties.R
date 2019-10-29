#' Get storage properties for an endpoint or container
#'
#' @param object An storage object.
#' @param container,share A blob container or file share.
#' @param blob,file,dir The name of an individual blob, file or directory.
#'
#' @details
#' The `get_storage_properties` generic returns a list of properties for the given storage object. There are methods defined for objects of class `storage_endpoint`, `blob_container` and `file_share`. Similar functions are defined for individual blobs, files and directories.
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
get_storage_properties.storage_endpoint <- function(object)
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
get_adls_file_acls <- function(filesystem, file)
{
    do_container_op(filesystem, file, options=list(action="getaccesscontrol"), http_verb="HEAD")[["x-ms-acl"]]
}


#' @rdname properties
#' @export
get_adls_file_status <- function(filesystem, file)
{
    do_container_op(filesystem, file, options=list(action="getstatus"), http_verb="HEAD")
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
