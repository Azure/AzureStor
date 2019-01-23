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

    if(!missing(dest))
        warning("Internal multiupload_blob implementation does not use the 'dest' argument")

    init_pool(max_concurrent_transfers)

    parallel::clusterExport(.AzureStor$pool,
        c("container", "type", "blocksize", "lease"),
        envir=environment())
    parallel::parLapply(.AzureStor$pool, src, function(f)
    {
        AzureStor::upload_blob(container, f, basename(f), type=type, blocksize=blocksize, lease=lease)
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
    i <- 1
    while(1)
    {
        body <- readBin(con, "raw", blocksize)
        thisblock <- length(body)
        if(thisblock == 0)
            break

        # ensure content-length is never exponential notation
        headers[["content-length"]] <- sprintf("%.0f", thisblock)
        id <- openssl::base64_encode(sprintf("%s-%010d", dest, i))
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


multidownload_blob_internal <- function(container, src, dest, overwrite=FALSE, lease=NULL,
                                        max_concurrent_transfers=10)
{
    files <- list_blobs(container, info="name")

    src_files <- glob2rx(basename(src))
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


download_blob_internal <- function(container, src, dest, overwrite=FALSE, lease=NULL)
{
    headers <- list()
    if(!is.null(lease))
        headers[["x-ms-lease-id"]] <- as.character(lease)
    
    if(is.character(dest))
        return(do_container_op(container, src, headers=headers, config=httr::write_disk(dest, overwrite)))
    
    # if dest is NULL or a raw connection, return the transferred data in memory as raw bytes
    cont <- httr::content(do_container_op(container, src, headers=headers, http_status_handler="pass"),
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


call_azcopy_upload <- function(...)
{
    if(.AzureStor$azcopy == "")
        stop("azcopy version 10+ required but not found")
    else stop("Not yet implemented")
}


call_azcopy_download <- function(...)
{
    if(.AzureStor$azcopy == "")
        stop("azcopy version 10+ required but not found")
    else stop("Not yet implemented")
}
