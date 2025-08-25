normalize_src <- function(src, put_md5=FALSE)
{
    UseMethod("normalize_src")
}


#' @exportS3Method
normalize_src.character <- function(src, put_md5=FALSE)
{
    content_type <- mime::guess_type(src)
    con <- file(src, open="rb")
    size <- file.size(src)
    if(put_md5)
    {
        md5 <- encode_md5(con)
        seek(con, 0)
    }
    else md5 <- NULL
    list(content_type=content_type, con=con, size=size, md5=md5)
}


#' @exportS3Method
normalize_src.textConnection <- function(src, put_md5=FALSE)
{
    # convert to raw connection
    content_type <- "application/octet-stream"
    src <- charToRaw(paste0(readLines(src), collapse="\n"))
    size <- length(src)
    md5 <- if(put_md5)
        encode_md5(src)
    else NULL
    con <- rawConnection(src)
    list(content_type=content_type, con=con, size=size, md5=md5)
}


#' @exportS3Method
normalize_src.rawConnection <- function(src, put_md5=FALSE)
{
    content_type <- "application/octet-stream"
    # need to read the data to get object size (!)
    size <- 0
    repeat
    {
        x <- readBin(src, "raw", n=1e6)
        if(length(x) == 0)
            break
        size <- size + length(x)
    }
    seek(src, 0) # reposition connection after reading
    if(put_md5)
    {
        md5 <- encode_md5(src)
        seek(src, 0)
    }
    else md5 <- NULL
    list(content_type=content_type, con=src, size=size, md5=md5)
}


multiupload_internal <- function(container, src, dest, recursive, ..., max_concurrent_transfers=10)
{
    src <- make_upload_set(src, recursive)
    wildcard_src <- !is.null(attr(src, "root"))
    if(missing(dest))
    {
        dest <- if(wildcard_src) "/" else basename(src)
    }

    n_src <- length(src)
    n_dest <- length(dest)

    if(n_src == 0)
        stop("No files to transfer", call.=FALSE)

    if(wildcard_src && n_dest > 1)
        stop("'dest' for a wildcard 'src' must be a single directory", call.=FALSE)

    if(!wildcard_src && n_dest != n_src)
        stop("'dest' must contain one name per file in 'src'", call.=FALSE)

    if(n_src == 1 && !wildcard_src)
        return(storage_upload(container, src, dest, ...))

    if(wildcard_src)
        dest <- sub("//", "/", file.path(dest, substr(src, nchar(attr(src, "root")) + 2, nchar(src))))

    init_pool(max_concurrent_transfers)
    pool_export("container", envir=environment())
    pool_map(function(s, d, ...) AzureStor::storage_upload(container, s, d, ...),
             src, dest, MoreArgs=list(...))

    invisible(NULL)
}


multidownload_internal <- function(container, src, dest, recursive, ..., max_concurrent_transfers=10)
{
    src <- make_download_set(container, src, recursive)
    wildcard_src <- !is.null(attr(src, "root"))
    if(missing(dest))
    {
        dest <- if(wildcard_src) "." else basename(src)
    }

    n_src <- length(src)
    n_dest <- length(dest)

    if(n_src == 0)
        stop("No files to transfer", call.=FALSE)

    if(wildcard_src && n_dest > 1)
        stop("'dest' for a wildcard 'src' must be a single directory", call.=FALSE)

    if(!wildcard_src && n_dest != n_src)
        stop("'dest' must contain one name per file in 'src'", call.=FALSE)

    if(n_src == 1 && !wildcard_src)
        return(storage_download(container, src, dest, ...))

    if(wildcard_src)
    {
        root <- attr(src, "root")
        destnames <- if(root != "/")
            substr(src, nchar(root) + 2, nchar(src))
        else src
        dest <- sub("//", "/", file.path(dest, destnames))
    }

    init_pool(max_concurrent_transfers)
    pool_export("container", envir=environment())
    pool_map(function(s, d, ...) AzureStor::storage_download(container, s, d, ...),
             src, dest, MoreArgs=list(...))

    invisible(NULL)
}


make_upload_set <- function(src, recursive)
{
    if(length(src) == 1)  # possible wildcard
    {
        src_dir <- dirname(src)
        src_name <- basename(src)
        src_spec <- glob2rx(src_name)

        if(src_spec != paste0("^", gsub("\\.", "\\\\.", src_name), "$"))  # a wildcard
        {
            fnames <- dir(src_dir, pattern=src_spec, full.names=TRUE, recursive=recursive,
                        ignore.case=(.Platform$OS.type == "windows"))
            dnames <- list.dirs(dirname(src), full.names=TRUE, recursive=recursive)
            src <- setdiff(fnames, dnames)
            # store original src dir of the wildcard
            attr(src, "root") <- src_dir
            return(src)
        }
    }

    src[file.exists(src)]
}


make_download_set <- function(container, src, recursive)
{
    src <- sub("^/", "", src) # strip leading slash if present, not meaningful

    if(length(src) == 1)  # possible wildcard
    {
        src_dir <- dirname(src)
        src_name <- basename(src)
        src_spec <- glob2rx(src_name)

        if(src_spec != paste0("^", gsub("\\.", "\\\\.", src_name), "$"))  # a wildcard
        {
            if(src_dir == ".")
                src_dir <- "/"

            src <- list_storage_files(container, src_dir, recursive=recursive)
            if(is.null(src$isdir))  # blob wart
                src$isdir <- FALSE
            src <- src$name[grepl(src_spec, basename(src$name)) & !(src$isdir)]
            # store original src dir of the wildcard
            attr(src, "root") <- src_dir
        }
    }

    src
}
