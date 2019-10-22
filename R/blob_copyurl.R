#' @details
#' `copy_url_to_storage` transfers the contents of the file at the specified HTTP\[S\] URL directly to storage, without requiring a temporary local copy to be made. `multicopy_url_to_storage` does the same, for multiple URLs at once. Currently methods for these are only implemented for blob storage.
#' @rdname file_transfer
#' @export
copy_url_to_storage <- function(container, src, dest, ...)
{
    UseMethod("copy_from_url")
}


#' @rdname file_transfer
#' @export
multicopy_url_to_storage <- function(container, src, dest, ...)
{
    UseMethod("multicopy_from_url")
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
#' `copy_url_to_blob` transfers the contents of the file at the specified HTTP\[S\] URL directly to blob storage, without requiring a temporary local copy to be made. `multicopy_url_to_blob1 does the same, for multiple URLs at once. These functions have a current file size limit of 256MB.
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


#' @rdname blob
#' @export
multicopy_url_to_blob <- function(container, src, dest, lease=NULL, async=FALSE, max_concurrent_transfers=10)
{
    if(missing(dest))
        dest <- "/"

    if(length(dest) > 1)
        stop("'dest' must be a single directory", call.=FALSE)

    init_pool(max_concurrent_transfers)

    pool_export(c("container", "dest", "lease", "async"),
        envir=environment())
    pool_lapply(src, function(f)
    {
        dest <- if(dest == "/")
            basename(httr::parse_url(f)$path)
        else file.path(dest, basename(httr::parse_url(f)$path))
        AzureStor::copy_url_to_blob(container, f, dest, lease=lease, async=async)
    })
    invisible(NULL)
}
