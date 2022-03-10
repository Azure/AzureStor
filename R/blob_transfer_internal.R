upload_blob_internal <- function(container, src, dest, type, blocksize, lease=NULL, put_md5=FALSE, append=FALSE)
{
    src <- normalize_src(src, put_md5)
    on.exit(close(src$con))

    switch(type,
        "BlockBlob"=upload_block_blob(container, src, dest, blocksize, lease),
        "AppendBlob"=upload_append_blob(container, src, dest, blocksize, lease, append),
        stop("Unknown blob type: ", type, call.=FALSE)
    )

    invisible(NULL)
}


download_blob_internal <- function(container, src, dest, blocksize=2^24, overwrite=FALSE, lease=NULL,
                                   check_md5=FALSE, snapshot=NULL)
{
    headers <- list()
    if(!is.null(lease))
        headers[["x-ms-lease-id"]] <- as.character(lease)

    opts <- if(is.null(snapshot))
        list()
    else list(snapshot=snapshot)

    dest <- init_download_dest(dest, overwrite)
    on.exit(dispose_download_dest(dest))

    # get file size (for progress bar) and MD5 hash
    props <- get_storage_properties(container, src, snapshot=snapshot)
    size <- as.numeric(props[["content-length"]])
    src_md5 <- props[["content-md5"]]

    bar <- storage_progress_bar$new(size, "down")
    offset <- 0

    while(offset < size)
    {
        headers$Range <- sprintf("bytes=%.0f-%.0f", offset, offset + blocksize - 1)
        res <- do_container_op(container, src, options=opts, headers=headers, progress=bar$update(),
                               http_status_handler="pass")
        httr::stop_for_status(res, storage_error_message(res))
        writeBin(httr::content(res, as="raw"), dest)

        offset <- offset + blocksize
        bar$offset <- offset
    }

    bar$close()
    if(check_md5)
        do_md5_check(dest, src_md5)
    if(inherits(dest, "null_dest")) rawConnectionValue(dest) else invisible(NULL)
}
