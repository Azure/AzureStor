upload_blob_internal <- function(container, src, dest, type="BlockBlob", blocksize=2^24, lease=NULL)
{
    if(type != "BlockBlob")
        stop("Only block blobs currently supported")

    src <- normalize_src(src)
    on.exit(close(src$con))

    headers <- list("x-ms-blob-type"=type)
    if(!is.null(lease))
        headers[["x-ms-lease-id"]] <- as.character(lease)

    bar <- storage_progress_bar$new(src$size, "up")

    # upload each block
    blocklist <- list()
    base_id <- openssl::md5(dest)
    i <- 1
    repeat
    {
        body <- readBin(src$con, "raw", blocksize)
        thisblock <- length(body)
        if(thisblock == 0)
            break

        # ensure content-length is never exponential notation
        headers[["content-length"]] <- sprintf("%.0f", thisblock)
        id <- openssl::base64_encode(sprintf("%s-%010d", base_id, i))
        opts <- list(comp="block", blockid=id)

        do_container_op(container, dest, headers=headers, body=body, options=opts, progress=bar$update(),
                        http_verb="PUT")

        blocklist <- c(blocklist, list(Latest=list(id)))
        bar$offset <- bar$offset + blocksize
        i <- i + 1
    }

    bar$close()

    # update block list
    body <- as.character(xml2::as_xml_document(list(BlockList=blocklist)))
    headers <- list("content-length"=sprintf("%.0f", nchar(body)),
                    "x-ms-blob-content-type"=src$content_type)
    do_container_op(container, dest, headers=headers, body=body, options=list(comp="blocklist"),
                    http_verb="PUT")

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
        on.exit(seek(dest, 0))
    }
    if(conn_dest)
        on.exit(seek(dest, 0))

    # get file size (for progress bar)
    res <- do_container_op(container, src, headers=headers, http_verb="HEAD", http_status_handler="pass")
    httr::stop_for_status(res, storage_error_message(res))
    size <- as.numeric(httr::headers(res)[["Content-Length"]])

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
