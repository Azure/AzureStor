#' Get storage properties for an endpoint or container
#'
#' @param object An object.
#' @param container A blob container.
#' @param share A file share.
#' @param blob,file,dir The name of an individual blob, file or directory.
#'
#' @details
#' The `get_storage_properties` generic returns a list of properties for the given storage object. There are methods defined for objects of class `storage_endpoint`, `blob_container` and `file_share`. Similar functions are defined for individual blobs, files and directories.
#'
#' @rdname properties
#' @export
get_storage_properties <- function(object, ...)
{
    UseMethod("get_storage_properties")
}


#' @rdname properties
#' @export
get_storage_properties.storage_endpoint <- function(object)
{
    do_storage_call(object$url, "", options=list(restype="service", comp="properties"),
                    key=object$key, sas=object$sas, api_version=object$api_version)
}


#' @rdname properties
#' @export
get_storage_properties.blob_container <- function(object)
{
    res <- do_container_op(object, options=list(restype="container"), http_status_handler="pass")
    httr::stop_for_status(res, storage_error_message(res))
    httr::headers(res)
}


#' @rdname properties
#' @export
get_storage_properties.file_share <- function(object)
{
    res <- do_container_op(object, options=list(restype="share"), http_status_handler="pass")
    httr::stop_for_status(res, storage_error_message(res))
    httr::headers(res)
}


#' @rdname properties
#' @export
get_blob_properties <- function(container, blob)
{
    res <- do_container_op(container, blob, http_verb="HEAD", http_status_handler="pass")
    httr::stop_for_status(res, storage_error_message(res))
    httr::headers(res)
}


#' @rdname properties
#' @export
get_azure_file_properties <- function(share, file)
{
    res <- do_container_op(share, file, http_verb="HEAD", http_status_handler="pass")
    httr::stop_for_status(res, storage_error_message(res))
    httr::headers(res)
}


#' @rdname properties
#' @export
get_azure_dir_properties <- function(share, dir)
{
    res <- do_container_op(share, dir, options=list(restype="directory"), http_verb="HEAD", http_status_handler="pass")
    httr::stop_for_status(res, storage_error_message(res))
    httr::headers(res)
}

