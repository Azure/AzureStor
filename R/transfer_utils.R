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


multiupload_internal <- function(container, src, dest, recursive, ..., max_concurrent_transfers=10)
{
    src <- make_upload_set(src, recursive)

    if(length(src) == 0)
        stop("No files to transfer", call.=FALSE)

    if(length(dest) != 1 && length(dest) != length(src))
        stop("'dest' must be either a single directory, or one name per file in 'src'", call.=FALSE)

    if(length(src) == 1)
        return(storage_upload(container, src, dest, ...))

    if(length(dest) == 1)
    {
        src_root <- attr(src, "root")
        dest <- sub("//", "/", file.path(dest, substr(src, nchar(src_root) + 2, nchar(src))))
    }

    init_pool(max_concurrent_transfers)
    pool_export("container", envir=environment())
    pool_map(function(f, d, ...) AzureStor::storage_upload(container, f, d, ...),
             src, dest, MoreArgs=list(...))

    invisible(NULL)
}


multidownload_internal <- function(container, src, dest, recursive, ..., max_concurrent_transfers=10)
{
    src <- make_download_set(container, src, recursive)

    if(length(src) == 0)
        stop("No files to transfer", call.=FALSE)

    if(length(dest) != 1 && length(dest) != length(src))
        stop("'dest' must be either a single directory, or one name per file in 'src'", call.=FALSE)

    if(length(src) == 1)
        return(storage_download(container, src, dest, ...))

    if(length(dest) == 1)
        dest <- sub("//", "/", file.path(dest, basename(src)))

    init_pool(max_concurrent_transfers)
    pool_export("container", envir=environment())
    pool_map(function(f, d, ...) AzureStor::storage_download(container, f, d, ...),
             src, dest, MoreArgs=list(...))

    invisible(NULL)
}


make_upload_set <- function(src, recursive=FALSE)
{
    if(length(src) == 1)  # possible wildcard
    {
        src_dir <- dirname(src)
        src_spec <- glob2rx(basename(src))
        fnames <- dir(dirname(src), pattern=src_spec, full.names=TRUE, recursive=recursive,
                      ignore.case=(.Platform$OS.type == "windows"))
        dnames <- list.dirs(dirname(src), full.names=TRUE, recursive=recursive)
        src <- setdiff(fnames, dnames)
        # store original src dir of the wildcard
        attr(src, "root") <- src_dir
        src
    }
    else src[file.exists(src)]
}


make_download_set <- function(container, src)
{
    UseMethod("make_download_set")
}


make_download_set.adls_filesystem <- function(container, src, recursive)
{
    src <- sub("^/", "", src) # strip leading slash if present, not meaningful
    src_dirs <- unique(dirname(src))
    src_dirs[src_dirs == "."] <- "/"

    # file listing on ADLS includes directory name
    files <- unlist(lapply(src_dirs, function(x) list_adls_files(filesystem, x, info="name", recursive=recursive)))

    make_download_set_internal(src, files)
}


make_download_set.blob_container <- function(container, src, recursive)
{
    src <- sub("^/", "", src) # strip leading slash if present, not meaningful
    files <- list_blobs(container, info="name")

    make_download_set_internal(src, files)
}


make_download_set.file_share <- function(container, src, recursive)
{
    src <- sub("^/", "", src) # strip leading slash if present, not meaningful
    src_dirs <- unique(dirname(src))
    src_dirs[src_dirs == "."] <- ""

    files <- unlist(lapply(src_dirs, function(dname)
    {
        fnames <- list_azure_files(share, dname, info="name")
        if(dname != "")
            file.path(dname, fnames)
        else fnames
    }))

    make_download_set_internal(src, files)
}


make_download_set_internal <- function(src, files)
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

    unique(c(src_fixed, src_regex))
}

