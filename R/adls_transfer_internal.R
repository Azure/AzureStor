upload_adls_file_internal <- function(filesystem, src, dest, blocksize=2^24, lease=NULL)
{
    src <- normalize_src(src)
    on.exit(close(src$con))

    headers <- list(`x-ms-content-type`=src$content_type)
    if(!is.null(lease))
        headers[["x-ms-lease-id"]] <- as.character(lease)

    do_container_op(filesystem, dest, options=list(resource="file"), headers=headers, http_verb="PUT")

    bar <- storage_progress_bar$new(src$size, "up")

    # transfer the contents
    blocklist <- list()
    pos <- 0
    repeat
    {
        body <- readBin(src$con, "raw", blocksize)
        thisblock <- length(body)
        if(thisblock == 0)
            break

        headers <- list(
            `content-type`="application/octet-stream",
            `content-length`=sprintf("%.0f", thisblock)
        )
        opts <- list(action="append", position=sprintf("%.0f", pos))

        do_container_op(filesystem, dest, headers=headers, body=body, options=opts, progress=bar$update(),
                        http_verb="PATCH")

        bar$offset <- bar$offset + blocksize
        pos <- pos + thisblock
    }

    bar$close()

    # flush contents
    do_container_op(filesystem, dest,
                    options=list(action="flush", position=sprintf("%.0f", pos)),
                    http_verb="PATCH")
    invisible(NULL)
}


download_adls_file_internal <- function(filesystem, src, dest, blocksize=2^24, overwrite=FALSE)
{
    file_dest <- is.character(dest)
    null_dest <- is.null(dest)
    conn_dest <- inherits(dest, "rawConnection")

    if(!file_dest && !null_dest && !conn_dest)
        stop("Unrecognised dest argument", call.=FALSE)

    headers <- list()
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
        on.exit(seek(dest, 0))
    }
    if(conn_dest)
        on.exit(seek(dest, 0))

    # get file size (for progress bar)
    res <- do_container_op(filesystem, src, headers=headers, http_verb="HEAD", http_status_handler="pass")
    httr::stop_for_status(res, storage_error_message(res))
    size <- as.numeric(httr::headers(res)[["Content-Length"]])

    bar <- storage_progress_bar$new(size, "down")
    offset <- 0

    while(offset < size)
    {
        headers$Range <- sprintf("bytes=%.0f-%.0f", offset, offset + blocksize - 1)
        res <- do_container_op(filesystem, src, headers=headers, progress=bar$update(), http_status_handler="pass")
        httr::stop_for_status(res, storage_error_message(res))
        writeBin(httr::content(res, as="raw"), dest)

        offset <- offset + blocksize
        bar$offset <- offset
    }

    bar$close()
    if(null_dest) rawConnectionValue(dest) else invisible(NULL)
}
