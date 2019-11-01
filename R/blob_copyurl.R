#' @details
#' `copy_url_to_storage` transfers the contents of the file at the specified HTTP\[S\] URL directly to storage, without requiring a temporary local copy to be made. `multicopy_url_to_storage` does the same, for multiple URLs at once. Currently methods for these are only implemented for blob storage.
#' @rdname file_transfer
#' @export
copy_url_to_storage <- function(container, src, dest, ...)
{
    UseMethod("copy_url_to_storage")
}


#' @rdname file_transfer
#' @export
multicopy_url_to_storage <- function(container, src, dest, ...)
{
    UseMethod("multicopy_url_to_storage")
}

#' @rdname file_transfer
#' @export
copy_url_to_storage.blob_container <- function(container, src, dest, ...)
{
    copy_url_to_blob(container, src, dest, ...)
}


#' @rdname file_transfer
#' @export
multicopy_url_to_storage.blob_container <- function(container, src, dest, ...)
{
    multicopy_url_to_blob(container, src, dest, ...)
}

#' @param async For `copy_url_to_blob` and `multicopy_url_to_blob`, whether the copy operation should be asynchronous (proceed in the background).
#' @details
#' `copy_url_to_blob` transfers the contents of the file at the specified HTTP\[S\] URL directly to blob storage, without requiring a temporary local copy to be made. `multicopy_url_to_blob` does the same, for multiple URLs at once. These functions have a current file size limit of 256MB.
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
    invisible(NULL)
}


#' @rdname blob
#' @export
multicopy_url_to_blob <- function(container, src, dest, lease=NULL, async=FALSE, max_concurrent_transfers=10)
{
    if(missing(dest))
        dest <- basename(src)

    n_src <- length(src)
    n_dest <- length(dest)

    if(n_src == 0)
        stop("No files to transfer", call.=FALSE)

    if(n_dest != n_src)
        stop("'dest' must contain one name per file in 'src'", call.=FALSE)

    if(n_src == 1)
        return(copy_url_to_blob(container, src, dest, lease=lease, async=async))

    init_pool(max_concurrent_transfers)

    pool_export("container", envir=environment())
    pool_map(function(s, d, lease, async) AzureStor::copy_url_to_blob(container, s, d, lease=lease, async=async),
             src, dest, MoreArgs=list(lease=lease, async=async))
    invisible(NULL)
}
