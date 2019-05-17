multiupload_adls_file_internal <- function(filesystem, src, dest, blocksize=2^22, lease=lease,
                                           max_concurrent_transfers=10)
{
    src_files <- glob2rx(basename(src))
    src_dir <- dirname(src)
    src <- dir(src_dir, pattern=src_files, full.names=TRUE)

    if(length(src) == 0)
        stop("No files to transfer", call.=FALSE)
    if(length(src) == 1)
        return(upload_adls_file(filesystem, src, dest, blocksize=blocksize, lease=lease))

    init_pool(max_concurrent_transfers)

    parallel::clusterExport(.AzureStor$pool,
        c("filesystem", "dest", "blocksize"),
        envir=environment())
    parallel::parLapply(.AzureStor$pool, src, function(f)
    {
        dest <- sub("//", "/", file.path(dest, basename(f))) # API too dumb to handle //'s
        AzureStor::upload_adls_file(filesystem, f, dest, blocksize=blocksize, lease=lease)
    })
    invisible(NULL)
}


upload_adls_file_internal <- function(filesystem, src, dest, blocksize=2^24, lease=NULL)
{
    con <- if(inherits(src, "textConnection"))
        rawConnection(charToRaw(paste0(readLines(src), collapse="\n")))
    else if(inherits(src, "rawConnection"))
        src
    else file(src, open="rb")
    on.exit(close(con))

    # create the file
    content_type <- if(inherits(src, "connection"))
        "application/octet-stream"
    else mime::guess_type(src)
    headers <- list(`x-ms-content-type`=content_type)
    #if(!is.null(lease))
        #headers[["x-ms-lease-id"]] <- as.character(lease)
    do_container_op(filesystem, dest, options=list(resource="file"), headers=headers, http_verb="PUT")

    # transfer the contents
    blocklist <- list()
    pos <- 0
    repeat
    {
        body <- readBin(con, "raw", blocksize)
        thisblock <- length(body)
        if(thisblock == 0)
            break

        headers <- list(
            `content-type`="application/octet-stream",
            `content-length`=sprintf("%.0f", thisblock)
        )
        opts <- list(action="append", position=sprintf("%.0f", pos))

        do_container_op(filesystem, dest, headers=headers, body=body, options=opts, http_verb="PATCH")

        pos <- pos + thisblock
    }

    # flush contents
    do_container_op(filesystem, dest,
        options=list(action="flush", position=sprintf("%.0f", pos)),
        http_verb="PATCH")
}


multidownload_adls_file_internal <- function(filesystem, src, dest, blocksize=2^24, overwrite=FALSE,
                                             max_concurrent_transfers=10)
{
    src_dir <- dirname(src)
    if(src_dir == ".")
        src_dir <- "/"

    files <- list_adls_files(filesystem, src_dir, info="name")
    src <- grep(glob2rx(src), files, value=TRUE) # file listing on ADLS includes directory name

    if(length(src) == 0)
        stop("No files to transfer", call.=FALSE)
    if(length(src) == 1)
        return(download_adls_file(filesystem, src, dest, blocksize=blocksize, overwrite=overwrite))

    init_pool(max_concurrent_transfers)

    parallel::clusterExport(.AzureStor$pool,
        c("filesystem", "dest", "overwrite"),
        envir=environment())
    parallel::parLapply(.AzureStor$pool, src, function(f)
    {
        dest <- file.path(dest, basename(f))
        AzureStor::download_adls_file(filesystem, f, dest, blocksize=blocksize, overwrite=overwrite)
    })
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
    if(null_dest) dest else invisible(NULL)
}
