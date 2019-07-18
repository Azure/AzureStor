#' @details
#' `copy_url_to_storage` transfers the contents of the file at the specified HTTP\[S\] URL directly to storage, without requiring a temporary local copy to be made. Currently this is only implemented for blob storage.
#' @rdname file_transfer
#' @export
copy_url_to_storage <- function(container, src, dest, ...)
{
    UseMethod("copy_from_url")
}


#' @rdname file_transfer
#' @export
copy_url_to_storage.blob_container <- function(container, src, dest, ...)
{
    copy_url_to_blob(container, src, dest, ...)
}


#' @param async For `copy_url_to_blob`, whether the copy operation should be asynchronous (proceed in the background).
#' @details
#' `copy_url_to_blob` transfers the contents of the file at the specified HTTP\[S\] URL directly to blob storage, without requiring a temporary local copy to be made. This has a current file size limit of 256MB.
#' @rdname blob
#' @export
copy_url_to_blob <- function(container, src, dest, lease=NULL, async=FALSE)
{
    if(!is_url(src))
        stop("Source must be a HTTP[S] url", call.=FALSE)

    headers <- list(
        `x-ms-copy-source`=src,
        `x-ms-requires-sync`=!async
    )
    if(!is.null(lease))
        headers[["x-ms-lease-id"]] <- as.character(lease)

    do_container_op(container, dest, headers=headers, http_verb="PUT")
}
