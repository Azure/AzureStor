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
