init_download_dest <- function(dest, overwrite)
{
    UseMethod("init_download_dest")
}


init_download_dest.character <- function(dest, overwrite)
{
    if(!overwrite && file.exists(dest))
        stop("Destination file exists and overwrite is FALSE", call.=FALSE)
    if(!dir.exists(dirname(dest)))
        dir.create(dirname(dest), recursive=TRUE)
    f <- file(dest, "w+b")
    structure(f, class=c("file_dest", class(f)))
}


init_download_dest.rawConnection <- function(dest, overwrite)
{
    structure(dest, class=c("conn_dest", class(dest)))
}


init_download_dest.NULL <- function(dest, overwrite)
{
    con <- rawConnection(raw(0), "w+b")
    structure(con, class=c("null_dest", class(con)))
}


dispose_download_dest <- function(dest)
{
    UseMethod("dispose_download_dest")
}


dispose_download_dest.file_dest <- function(dest)
{
    close(dest)
}


dispose_download_dest.conn_dest <- function(dest)
{
    seek(dest, 0)
}


dispose_download_dest.null_dest <- function(dest)
{
    close(dest)
}


do_md5_check <- function(dest, src_md5)
{
    if(is.null(src_md5))
    {
        warning("Source file MD5 hash not set", call.=FALSE)
        return()
    }
    seek(dest, 0)
    dest_md5 <- encode_md5(dest)
    if(dest_md5 != src_md5)
        stop("Destination and source MD5 hashes do not match", call.=FALSE)
}


