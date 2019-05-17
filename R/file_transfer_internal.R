multiupload_azure_file_internal <- function(share, src, dest, blocksize=2^22, max_concurrent_transfers=10)
{
    src_dir <- dirname(src)
    src_files <- glob2rx(basename(src))
    src <- dir(src_dir, pattern=src_files, full.names=TRUE)

    if(length(src) == 0)
        stop("No files to transfer", call.=FALSE)
    if(length(src) == 1)
        return(upload_azure_file(share, src, dest, blocksize=blocksize))

    init_pool(max_concurrent_transfers)

    parallel::clusterExport(.AzureStor$pool,
        c("share", "dest", "blocksize"),
        envir=environment())
    parallel::parLapply(.AzureStor$pool, src, function(f)
    {
        dest <- sub("//", "/", file.path(dest, basename(f))) # API too dumb to handle //'s
        AzureStor::upload_azure_file(share, f, dest, blocksize=blocksize)
    })
    invisible(NULL)
}


upload_azure_file_internal <- function(share, src, dest, blocksize=2^22)
{
    # set content type
    content_type <- if(inherits(src, "connection"))
        "application/octet-stream"
    else mime::guess_type(src)

    if(inherits(src, "textConnection"))
    {
        src <- charToRaw(paste0(readLines(src), collapse="\n"))
        nbytes <- length(src)
        con <- rawConnection(src)
    }
    else if(inherits(src, "rawConnection"))
    {
        con <- src
        # need to read the data to get object size (!)
        nbytes <- 0
        repeat
        {
            x <- readBin(con, "raw", n=blocksize)
            if(length(x) == 0)
                break
            nbytes <- nbytes + length(x)
        }
        seek(con, 0) # reposition connection after reading
    }
    else
    {
        con <- file(src, open="rb")
        nbytes <- file.info(src)$size
    }
    on.exit(close(con))

    # first, create the file
    # ensure content-length is never exponential notation
    headers <- list("x-ms-type"="file",
                    "x-ms-content-length"=sprintf("%.0f", nbytes))
    do_container_op(share, dest, headers=headers, http_verb="PUT")

    # then write the bytes into it, one block at a time
    options <- list(comp="range")
    headers <- list("x-ms-write"="Update")

    # upload each block
    blocklist <- list()
    range_begin <- 0
    while(range_begin < nbytes)
    {
        body <- readBin(con, "raw", blocksize)
        thisblock <- length(body)
        if(thisblock == 0)  # sanity check
            break

        # ensure content-length and range are never exponential notation
        headers[["content-length"]] <- sprintf("%.0f", thisblock)
        headers[["range"]] <- sprintf("bytes=%.0f-%.0f", range_begin, range_begin + thisblock - 1)

        do_container_op(share, dest, headers=headers, body=body, options=options, http_verb="PUT")

        range_begin <- range_begin + thisblock
    }

    do_container_op(share, dest, headers=list("x-ms-content-type"=content_type),
                    options=list(comp="properties"),
                    http_verb="PUT")
    invisible(NULL)
}


multidownload_azure_file_internal <- function(share, src, dest, blocksize=2^22, overwrite=FALSE,
                                              max_concurrent_transfers=10)
{
    src_files <- glob2rx(basename(src))
    src_dir <- dirname(src)
    if(src_dir == ".")
        src_dir <- "/"

    files <- list_azure_files(share, src_dir, info="name")
    src <- sub("//", "/", file.path(src_dir, grep(src_files, files, value=TRUE)))

    if(length(src) == 0)
        stop("No files to transfer", call.=FALSE)
    if(length(src) == 1)
        return(download_azure_file(share, src, dest, blocksize=blocksize, overwrite=overwrite))

    init_pool(max_concurrent_transfers)

    parallel::clusterExport(.AzureStor$pool,
        c("share", "dest", "overwrite"),
        envir=environment())
    parallel::parLapply(.AzureStor$pool, src, function(f)
    {
        dest <- file.path(dest, basename(f))
        AzureStor::download_azure_file(share, f, dest, blocksize=blocksize, overwrite=overwrite)
    })
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
