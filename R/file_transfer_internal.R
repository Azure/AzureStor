upload_azure_file_internal <- function(share, src, dest, create_dir=FALSE, blocksize=2^22, put_md5=FALSE)
{
    src <- normalize_src(src, put_md5)
    on.exit(close(src$con))

    # file API needs separate call(s) to create destination dir
    if(create_dir)
        try(create_azure_dir(share, dirname(dest), recursive=TRUE), silent=TRUE)

    # first, create the file
    # ensure content-length is never exponential notation
    headers <- list("x-ms-type"="file",
                    "x-ms-content-type"=src$content_type,
                    "x-ms-content-length"=sprintf("%.0f", src$size))
    if(!is.null(src$md5))
        headers <- c(headers, "x-ms-content-md5"=src$md5)
    headers <- c(headers, file_default_perms)
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
        headers[["content-md5"]] <- encode_md5(body)

        do_container_op(share, dest, headers=headers, body=body, options=options, progress=bar$update(),
                        http_verb="PUT")

        bar$offset <- bar$offset + blocksize
        range_begin <- range_begin + thisblock
    }

    bar$close()

    invisible(NULL)
}


download_azure_file_internal <- function(share, src, dest, blocksize=2^22, overwrite=FALSE, check_md5=FALSE)
{
    headers <- list()
    dest <- init_download_dest(dest, overwrite)
    on.exit(dispose_download_dest(dest))

    # get file size (for progress bar) and MD5 hash
    props <- get_storage_properties(share, src)
    size <- as.numeric(props[["content-length"]])
    src_md5 <- props[["content-md5"]]

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
    if(check_md5)
        do_md5_check(dest, src_md5)
    if(inherits(dest, "null_dest")) rawConnectionValue(dest) else invisible(NULL)
}
