multiupload_internal <- function(container, src, dest, ..., max_concurrent_transfers=10, ulfunc)
{
    src <- make_upload_set(src)

    if(length(src) == 0)
        stop("No files to transfer", call.=FALSE)

    if(length(dest) != 1 && length(dest) != length(src))
        stop("'dest' must be either a single directory, or one name per file in 'src'", call.=FALSE)

    if(length(src) == 1)
    {
        ulfunc <- get(ulfunc, getNamespace("AzureStor"))
        return(ulfunc(container, src, dest, ...))
    }

    if(length(dest) == 1)
        dest <- sub("//", "/", file.path(dest, basename(src)))

    init_pool(max_concurrent_transfers)
    pool_export(c("container", "ulfunc"), envir=environment())
    pool_map(function(f, d, ...)
    {
        ulfunc <- get(ulfunc, getNamespace("AzureStor"))
        ulfunc(container, f, d, ...)
    }, src, dest, MoreArgs=list(...), RECYCLE=FALSE)

    invisible(NULL)
}


multidownload_internal <- function(container, src, dest, ..., files, max_concurrent_transfers=10, dlfunc)
{
    src <- make_download_set(src, files)

    if(length(src) == 0)
        stop("No files to transfer", call.=FALSE)

    if(length(dest) != 1 && length(dest) != length(src))
        stop("'dest' must be either a single directory, or one name per file in 'src'", call.=FALSE)

    if(length(src) == 1)
    {
        dlfunc <- get(dlfunc, getNamespace("AzureStor"))
        return(dlfunc(container, src, dest, ...))
    }

    if(length(dest) == 1)
        dest <- sub("//", "/", file.path(dest, basename(src)))

    init_pool(max_concurrent_transfers)
    pool_export(c("container", "dlfunc"), envir=environment())
    pool_map(function(f, d, ...)
    {
        dlfunc <- get(dlfunc, getNamespace("AzureStor"))
        dlfunc(container, f, d, ...)
    }, src, dest, MoreArgs=list(...), RECYCLE=FALSE)

    invisible(NULL)
}


normalize_src <- function(src)
{
    UseMethod("normalize_src")
}


normalize_src.character <- function(src)
{
    content_type <- mime::guess_type(src)
    con <- file(src, open="rb")
    size <- file.size(src)
    list(content_type=content_type, con=con, size=size)
}


normalize_src.textConnection <- function(src)
{
    content_type <- "application/octet-stream"
    # convert to raw connection
    src <- charToRaw(paste0(readLines(src), collapse="\n"))
    size <- length(src)
    con <- rawConnection(src)
    list(content_type=content_type, con=con, size=size)
}


normalize_src.rawConnection <- function(src)
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
    list(content_type=content_type, con=src, size=size)
}


make_upload_set <- function(src)
{
    src_files <- basename(src)
    src_spec <- glob2rx(src_files)
    fixed <- paste0("^", src_files, "$") == src_spec

    src_regex <- if(!all(fixed))
    {
        unlist(mapply(function(dname, fpat) dir(dname, pattern=fpat, full.names=TRUE),
                      dirname(src[!fixed]), src_spec[!fixed], SIMPLIFY=FALSE, USE.NAMES=FALSE))
    }
    else character(0)
    src_fixed <- if(any(fixed))
        (src[fixed])[file.exists(src[fixed])]
    else character(0)

    c(src_fixed, src_regex)
}


make_download_set <- function(src, files)
{
    # don't grep unnecessarily
    src_spec <- glob2rx(src)
    fixed <- paste0("^", src, "$") == src_spec

    src_regex <- if(!all(fixed))
        grep(paste0(src_spec[!fixed], collapse="|"), files, value=TRUE)
    else character(0)
    src_fixed <- if(any(fixed))
        (src[fixed])[src[fixed] %in% files]
    else character(0)

    c(src_fixed, src_regex)
}

