#' Call the azcopy file transfer utility
#'
#' @param ... Arguments to pass to azcopy. If no arguments are supplied, azcopy will print a help screen.
#' @param force For `azcopy_login`, whether to force azcopy to relogin.
#'
#' @rdname azcopy
#' @export
call_azcopy <- function(...)
{
    azcopy <- get_azcopy_path()
    args <- sapply(list(...), as.character)
    system2(azcopy, args)
}


#' @rdname azcopy
#' @export
azcopy_login <- function(force=FALSE)
{
    if(exists("azcopy_logged_in", envir=.AzureStor) && isTRUE(.AzureStor$azcopy_logged_in) && !force)
        return(invisible(NULL))
    res <- call_azcopy("login")
    if(res == 0)
        .AzureStor$azcopy_logged_in <- TRUE
    invisible(NULL)
}


# azcopy unset/NULL -> not initialized
# azcopy = NA -> binary not found, or version < 10 (not usable)
# azcopy = path -> usable
get_azcopy_path <- function()
{
    if(exists("azcopy", envir=.AzureStor))
    {
        if(!is.na(.AzureStor$azcopy))
            return(.AzureStor$azcopy)
        else stop("azcopy version 10+ required but not found", call.=FALSE)
    }
    else
    {
        set_azcopy_path()
        Recall()
    }
}


set_azcopy_path <- function(path="azcopy")
{
    path <- Sys.which(path)
    if(is.na(path) || path == "")
    {
        .AzureStor$azcopy <- NA
        return(NULL)
    }

    # both stdout=TRUE and stderr=TRUE could result in jumbled output;
    # assume only one stream will actually have data for a given invocation
    ver <- suppressWarnings(system2(path, "--version", stdout=TRUE, stderr=TRUE))
    if(!grepl("version 1[[:digit:]]", ver, ignore.case=TRUE))
    {
        .AzureStor$azcopy <- NA
        return(NULL)
    }

    .AzureStor$azcopy <- unname(path)
    message("Using azcopy binary ", path)
    invisible(NULL)
}


azcopy_upload <- function(container, src, dest, ...)
{
    UseMethod("azcopy_upload")
}

azcopy_upload.blob_container <- function(container, src, dest, type="BlockBlob", blocksize=2^24, lease=NULL, ...)
{
    opts <- paste("--blobType", type, "--block-size", sprintf("%.0f", blocksize))
    azcopy_upload_internal(src, dest, opts)
}

azcopy_upload.file_share <- function(container, src, dest, blocksize=2^24, ...)
{
    opts <- sprintf("--block-size %.0f", blocksize)
    azcopy_upload_internal(container, src, dest, opts)
}

azcopy_upload.adls_filesystem <- function(container, src, dest, blocksize=2^24, lease=NULL, ...)
{
    opts <- sprintf("--block-size %.0f", blocksize)
    azcopy_upload_internal(container, src, dest, opts)
}

azcopy_upload_internal <- function(container, src, dest, opts)
{
    auth <- check_azcopy_auth(container)

    if(attr(auth, "method") == "key")
    {
        acctname <- sub("\\..*$", "", httr::parse_url(container$endpoint$url)$host)
        Sys.setenv(ACCOUNT_NAME=acctname, ACCOUNT_KEY=auth)
        on.exit(Sys.unsetenv(c("ACCOUNT_NAME", "ACCOUNT_KEY")))
    }
    else if(attr(auth, "method") == "token")
        azcopy_login()
    else if(attr(auth, "method") == "sas")
        dest <- paste0(dest, "?", auth)

    call_azcopy("copy", src, dest, opts)
}


azcopy_download <- function(container, src, dest, ...)
{
    UseMethod("azcopy_download")
}

# currently all azcopy_download methods are the same
azcopy_download.blob_container <- function(container, src, dest, overwrite=FALSE, ...)
{
    opts <- paste("--overwrite", tolower(as.character(overwrite)))
    azcopy_download_internal(container, src, dest, opts)
}

azcopy_download.file_share  <- function(container, src, dest, overwrite=FALSE, ...)
{
    opts <- paste("--overwrite", tolower(as.character(overwrite)))
    azcopy_download_internal(container, src, dest, opts)
}

azcopy_download.adls_filesystem <- function(container, src, dest, overwrite=FALSE, ...)
{
    opts <- paste("--overwrite", tolower(as.character(overwrite)))
    azcopy_download_internal(container, src, dest, opts)
}

azcopy_download_internal <- function(container, src, dest, opts)
{
    auth <- check_azcopy_auth(container)

    if(attr(auth, "method") == "key")
    {
        acctname <- sub("\\..*$", "", httr::parse_url(container$endpoint$url)$host)
        Sys.setenv(ACCOUNT_NAME=acctname, ACCOUNT_KEY=auth)
        on.exit(Sys.unsetenv(c("ACCOUNT_NAME", "ACCOUNT_KEY")))
    }
    else if(attr(auth, "method") == "token")
        azcopy_login()
    else if(attr(auth, "method") == "sas")
        src <- paste0(src, "?", auth)

    call_azcopy("copy", src, dest, opts)
}


check_azcopy_auth <- function(container)
{
    UseMethod("check_azcopy_auth")
}

check_azcopy_auth.blob_container <- function(container)
{
    endpoint <- container$endpoint

    if(!is.null(endpoint$token))
        return(structure(0, method="token"))
    if(!is.null(endpoint$sas))
        return(structure(endpoint$sas, method="sas"))

    warning("No supported authentication method found for blob storage; defaulting to public", call.=FALSE)
    return(structure(0, method="public"))
}

check_azcopy_auth.file_share <- function(container)
{
    endpoint <- container$endpoint

    if(!is.null(endpoint$sas))
        return(structure(endpoint$sas, method="sas"))
    stop("No supported authentication method found for file storage", call.=FALSE)
}

check_azcopy_auth.adls_filesystem <- function(container)
{
    endpoint <- container$endpoint

    if(!is.null(endpoint$key))
    {
        warning("Authenticating with a shared key is discouraged")
        return(structure(endpoint$key, method="key"))
    }
    if(!is.null(endpoint$token))
        return(structure(0, method="token"))

    stop("No supported authentication method found for ADLSgen2", call.=FALSE)
}

check_azcopy_auth.default <- function(container)
{
    stop("Unknown or unsupported container type: ", class(container)[1], call.=FALSE)
}
