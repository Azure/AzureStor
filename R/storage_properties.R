#' @export
get_storage_properties <- function(object, ...)
{
    UseMethod("get_storage_properties")
}


#' @export
get_storage_properties.storage_endpoint <- function(object)
{
    do_storage_op(object$url, "", options=list(restype="service", comp="properties"))
}


#' @export
get_storage_properties.blob_share <- function(object)
{
    do_container_op(object, options=list(restype="container"))
}


#' @export
get_storage_properties.file_share <- function(object)
{
    do_container_op(object, options=list(restype="share"))
}


#' @export
get_azure_blob_properties <- function(container, blob)
{
    do_container_op(container, blob, http_verb="HEAD")
}


#' @export
get_azure_file_properties <- function(share, file)
{
    do_container_op(share, file, http_verb="HEAD")
}


#' @export
get_azure_dir_properties <- function(share, dir)
{
    do_container_op(share, dir, options=list(restype="directory"), http_verb="HEAD")
}

