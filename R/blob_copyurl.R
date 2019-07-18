copy_url_to_storage <- function(container, src, dest, ...)
{
    UseMethod("copy_from_url")
}


copy_url_to_storage.blob_container <- function(container, src, dest, ...)
{
    copy_url_to_blob(container, src, dest, ...)
}


copy_url_to_blob <- function(container, src, dest, blocksize=2^24, lease=NULL, async=FALSE)
{
    if(!is_url(src))
        stop("Source must be a HTTP[S] url", call.=FALSE)

    headers <- list(
        `x-ms-copy-source`=src,
        `x-ms-requires-sync`=!async
    )
    do_container_op(container, dest, headers=headers)
}
