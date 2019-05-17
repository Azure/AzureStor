multiupload_blob_internal <- function(container, src, dest, type="BlockBlob", blocksize=2^24, lease=NULL,
                                      max_concurrent_transfers=10)
{
    src_dir <- dirname(src)
    src_files <- glob2rx(basename(src))
    src <- dir(src_dir, pattern=src_files, full.names=TRUE)

    if(length(src) == 0)
        stop("No files to transfer", call.=FALSE)
    if(length(src) == 1)
        return(upload_blob(container, src, dest, type=type, blocksize=blocksize, lease=lease))

    if(missing(dest))
        dest <- "/"

    init_pool(max_concurrent_transfers)

    parallel::clusterExport(.AzureStor$pool,
        c("container", "dest", "type", "blocksize", "lease"),
        envir=environment())
    parallel::parLapply(.AzureStor$pool, src, function(f)
    {
        dest <- if(dest == "/")
            basename(f)
        else file.path(dest, basename(f))
        AzureStor::upload_blob(container, f, dest, type=type, blocksize=blocksize, lease=lease)
    })
    invisible(NULL)
}


upload_blob_internal <- function(container, src, dest, type="BlockBlob", blocksize=2^24, lease=NULL)
{
    if(type != "BlockBlob")
        stop("Only block blobs currently supported")
    content_type <- if(inherits(src, "connection"))
        "application/octet-stream"
    else mime::guess_type(src)

    headers <- list("x-ms-blob-type"=type)
    if(!is.null(lease))
        headers[["x-ms-lease-id"]] <- as.character(lease)

    con <- if(inherits(src, "textConnection"))
        rawConnection(charToRaw(paste0(readLines(src), collapse="\n")))
    else if(inherits(src, "rawConnection"))
        src
    else file(src, open="rb")
    on.exit(close(con))

    # upload each block
    blocklist <- list()
    base_id <- openssl::md5(dest)
    i <- 1
    repeat
    {
        body <- readBin(con, "raw", blocksize)
        thisblock <- length(body)
        if(thisblock == 0)
            break

        # ensure content-length is never exponential notation
        headers[["content-length"]] <- sprintf("%.0f", thisblock)
        id <- openssl::base64_encode(sprintf("%s-%010d", base_id, i))
        opts <- list(comp="block", blockid=id)

        do_container_op(container, dest, headers=headers, body=body, options=opts, http_verb="PUT")

        blocklist <- c(blocklist, list(Latest=list(id)))
        i <- i + 1
    }

    # update block list
    body <- as.character(xml2::as_xml_document(list(BlockList=blocklist)))
    headers <- list("content-length"=sprintf("%.0f", nchar(body)))
    do_container_op(container, dest, headers=headers, body=body, options=list(comp="blocklist"),
                    http_verb="PUT")

    # set content type
    do_container_op(container, dest, headers=list("x-ms-blob-content-type"=content_type),
                    options=list(comp="properties"),
                    http_verb="PUT")
}


multidownload_blob_internal <- function(container, src, dest, blocksize=2^24, overwrite=FALSE, lease=NULL,
                                        max_concurrent_transfers=10)
{
    files <- list_blobs(container, info="name")

    src_files <- glob2rx(sub("^/", "", src)) # strip leading slash if present, not meaningful
    src <- grep(src_files, files, value=TRUE)

    if(length(src) == 0)
        stop("No files to transfer", call.=FALSE)
    if(length(src) == 1)
        return(download_blob(container, src, dest, overwrite=overwrite, lease=lease))

    init_pool(max_concurrent_transfers)

    parallel::clusterExport(.AzureStor$pool,
        c("container", "dest", "overwrite", "lease"),
        envir=environment())
    parallel::parLapply(.AzureStor$pool, src, function(f)
    {
        dest <- file.path(dest, basename(f))
        AzureStor::download_blob(container, f, dest, overwrite=overwrite, lease=lease)
    })
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
    if(null_dest) dest else invisible(NULL)
}
