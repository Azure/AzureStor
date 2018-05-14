#' @export
get_storage_properties <- function(object, ...)
{
    UseMethod("get_storage_properties")
}


#' @export
get_storage_properties.storage_endpoint <- function(object)
{
    do_storage_call(object$url, "", options=list(restype="service", comp="properties"),
                    key=object$key, sas=object$sas, api_version=object$api_version)
}


#' @export
get_storage_properties.blob_container <- function(object)
{
    res <- do_container_op(object, options=list(restype="container"), http_status_handler="pass")
    httr::stop_for_status(res, )
    httr::headers(res)
}


#' @export
get_storage_properties.file_share <- function(object)
{
    res <- do_container_op(object, options=list(restype="share"), http_status_handler="pass")
    httr::stop_for_status(res)
    httr::headers(res)
}


#' @export
get_azure_blob_properties <- function(container, blob)
{
    res <- do_container_op(container, blob, http_verb="HEAD", http_status_handler="pass")
    httr::stop_for_status(res)
    httr::headers(res)
}


#' @export
get_azure_file_properties <- function(share, file)
{
    res <- do_container_op(share, file, http_verb="HEAD", http_status_handler="pass")
    httr::stop_for_status(res)
    httr::headers(res)
}


#' @export
get_azure_dir_properties <- function(share, dir)
{
    res <- do_container_op(share, dir, options=list(restype="directory"), http_verb="HEAD", http_status_handler="pass")
    httr::stop_for_status(res)
    httr::headers(res)
}

