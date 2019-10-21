upload_azure_file_internal <- function(share, src, dest, blocksize=2^22)
{
    src <- normalize_src(src)
    on.exit(close(src$con))

    # first, create the file
    # ensure content-length is never exponential notation
    headers <- list("x-ms-type"="file",
                    "x-ms-content-length"=sprintf("%.0f", src$size))
    do_container_op(share, dest, headers=headers, http_verb="PUT")

    # then write the bytes into it, one block at a time
    options <- list(comp="range")
    headers <- list("x-ms-write"="Update")

    bar <- storage_progress_bar$new(src$size, "up")

    # upload each block
    blocklist <- list()
    range_begin <- 0
    while(range_begin < src$size)
    {
        body <- readBin(src$con, "raw", blocksize)
        thisblock <- length(body)
        if(thisblock == 0)  # sanity check
            break

        # ensure content-length and range are never exponential notation
        headers[["content-length"]] <- sprintf("%.0f", thisblock)
        headers[["range"]] <- sprintf("bytes=%.0f-%.0f", range_begin, range_begin + thisblock - 1)

        do_container_op(share, dest, headers=headers, body=body, options=options, progress=bar$update(),
                        http_verb="PUT")

        bar$offset <- bar$offset + blocksize
        range_begin <- range_begin + thisblock
    }

    bar$close()

    do_container_op(share, dest, headers=list("x-ms-content-type"=src$content_type),
                    options=list(comp="properties"),
                    http_verb="PUT")
    invisible(NULL)
}


download_azure_file_internal <- function(share, src, dest, blocksize=2^22, overwrite=FALSE)
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
    res <- do_container_op(share, src, headers=headers, http_verb="HEAD", http_status_handler="pass")
    httr::stop_for_status(res, storage_error_message(res))
    size <- as.numeric(httr::headers(res)[["Content-Length"]])

    bar <- storage_progress_bar$new(size, "down")
    offset <- 0

    while(offset < size)
    {
        headers$Range <- sprintf("bytes=%.0f-%.0f", offset, offset + blocksize - 1)
        res <- do_container_op(share, src, headers=headers, progress=bar$update(), http_status_handler="pass")
        httr::stop_for_status(res, storage_error_message(res))
        writeBin(httr::content(res, as="raw"), dest)

        offset <- offset + blocksize
        bar$offset <- offset
    }

    bar$close()
    if(null_dest) dest else invisible(NULL)
}
