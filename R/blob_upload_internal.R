upload_block_blob <- function(container, src, dest, blocksize, lease)
{
    bar <- storage_progress_bar$new(src$size, "up")

    headers <- list("x-ms-blob-type"="BlockBlob")
    if(!is.null(lease))
        headers[["x-ms-lease-id"]] <- as.character(lease)

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
}


upload_append_blob <- function(container, src, dest, blocksize, lease, append)
{
    bar <- storage_progress_bar$new(src$size, "up")

    headers <- list("x-ms-blob-type"="AppendBlob")
    if(!is.null(lease))
        headers[["x-ms-lease-id"]] <- as.character(lease)

    # initialise the blob
    do_container_op(container, dest, headers=headers, http_verb="PUT")

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
        opts <- list(comp="appendblock")

        do_container_op(container, dest, headers=headers, body=body, options=opts, progress=bar$update(),
                        http_verb="PUT")

        bar$offset <- bar$offset + blocksize
        i <- i + 1
    }

    bar$close()
}
