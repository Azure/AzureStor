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


download_blob_internal <- function(container, src, dest, blocksize=2^24, overwrite=FALSE, lease=NULL)
{
    file_dest <- is.character(dest)
    null_dest <- is.null(dest)
    conn_dest <- inherits(dest, "rawConnection")

    if(!file_dest && !null_dest && !conn_dest)
        stop("Unrecognised dest argument", call.=FALSE)

    headers <- list()
    if(!is.null(lease))
        headers[["x-ms-lease-id"]] <- as.character(lease)

    if(file_dest)
    {
        if(!overwrite && file.exists(dest))
            stop("Destination file exists and overwrite is FALSE", call.=FALSE)
        if(!dir.exists(dirname(dest)))
            dir.create(dirname(dest), recursive=TRUE)
        dest <- file(dest, "w+b")
        on.exit(close(dest))
    }
    if(null_dest)
    {
        dest <- rawConnection(raw(0), "w+b")
        on.exit(close(dest))
    }
    if(conn_dest)
        on.exit(seek(dest, 0))

    # get file size (for progress bar)
    props <- get_storage_properties(container, src)
    size <- as.numeric(props[["content-length"]])

    bar <- storage_progress_bar$new(size, "down")
    offset <- 0

    while(offset < size)
    {
        headers$Range <- sprintf("bytes=%.0f-%.0f", offset, offset + blocksize - 1)
        res <- do_container_op(container, src, headers=headers, progress=bar$update(), http_status_handler="pass")
        httr::stop_for_status(res, storage_error_message(res))
        writeBin(httr::content(res, as="raw"), dest)

        offset <- offset + blocksize
        bar$offset <- offset
    }

    bar$close()
    if(null_dest) rawConnectionValue(dest) else invisible(NULL)
}
