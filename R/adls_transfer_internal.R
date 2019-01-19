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
    while(1)
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

        do_container_op(filesystem, dest, options=opts, headers=headers, body=body, http_verb="PATCH")
        pos <- pos + thisblock
    }

    # flush contents
    do_container_op(filesystem, dest,
        options=list(action="flush", position=sprintf("%.0f", pos)),
        http_verb="PATCH")
}


multidownload_adls_file_internal <- function(filesystem, src, dest, overwrite=FALSE, max_concurrent_transfers=10)
{
    src_dir <- dirname(src)
    if(src_dir == ".")
        src_dir <- "/"

    files <- list_adls_files(filesystem, src_dir, info="name")
    src <- grep(glob2rx(src), files, value=TRUE) # file listing on ADLS includes directory name

    if(length(src) == 0)
        stop("No files to transfer", call.=FALSE)
    if(length(src) == 1)
        return(download_adls_file(filesystem, src, dest, overwrite=overwrite))

    init_pool(max_concurrent_transfers)

    parallel::clusterExport(.AzureStor$pool,
        c("filesystem", "dest", "overwrite"),
        envir=environment())
    parallel::parLapply(.AzureStor$pool, src, function(f)
    {
        dest <- file.path(dest, basename(f))
        writeLines(dest, file.path("d:/misc/temp", basename(f)))
        AzureStor::download_adls_file(filesystem, f, dest, overwrite=overwrite)
    })
    invisible(NULL)
}


download_adls_file_internal <- function(filesystem, src, dest, overwrite=FALSE)
{
    if(is.character(dest))
        return(do_container_op(filesystem, src, config=httr::write_disk(dest, overwrite)))

    # if dest is NULL or a raw connection, return the transferred data in memory as raw bytes
    cont <- httr::content(do_container_op(filesystem, src, http_status_handler="pass"),
                          as="raw")
    if(is.null(dest))
        return(cont)

    if(inherits(dest, "rawConnection"))
    {
        writeBin(cont, dest)
        seek(dest, 0)
        invisible(NULL)
    }
    else stop("Unrecognised dest argument", call.=FALSE)
}
