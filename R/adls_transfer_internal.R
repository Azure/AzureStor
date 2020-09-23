upload_adls_file_internal <- function(filesystem, src, dest, blocksize=2^24, lease=NULL, put_md5=FALSE)
{
    src <- normalize_src(src, put_md5)
    on.exit(close(src$con))

    headers <- list()
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

        opts <- list(action="append", position=sprintf("%.0f", pos))
        headers <- list(
            `content-length`=sprintf("%.0f", thisblock),
            `content-md5`=encode_md5(body)
        )
        do_container_op(filesystem, dest, headers=headers, body=body, options=opts, progress=bar$update(),
                        http_verb="PATCH")

        bar$offset <- bar$offset + blocksize
        pos <- pos + thisblock
    }

    bar$close()

    # flush contents
    headers <- list(`content-type`=src$content_type)
    if(!is.null(src$md5))
        headers$`x-ms-content-md5` <- src$md5
    do_container_op(filesystem, dest,
                    options=list(action="flush", position=sprintf("%.0f", pos)),
                    headers=headers,
                    http_verb="PATCH")
    invisible(NULL)
}


download_adls_file_internal <- function(filesystem, src, dest, blocksize=2^24, overwrite=FALSE, check_md5=FALSE)
{
    headers <- list()
    dest <- init_download_dest(dest, overwrite)
    on.exit(dispose_download_dest(dest))

    # get file size (for progress bar) and MD5 hash
    props <- get_storage_properties(filesystem, src)
    size <- as.numeric(props[["content-length"]])
    src_md5 <- props[["content-md5"]]

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
    if(check_md5)
        do_md5_check(dest, src_md5)
    if(inherits(dest, "null_dest")) rawConnectionValue(dest) else invisible(NULL)
}
