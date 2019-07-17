#' Call the azcopy file transfer utility
#'
#' @param ... Arguments to pass to AzCopy on the commandline. If no arguments are supplied, a help screen is printed.
#' @param force For `azcopy_login`, whether to force AzCopy to relogin. If `FALSE` (the default), and AzureStor has detected that AzCopy has already logged in, this has no effect.
#'
#' @details
#' AzureStor has the ability to use the Microsoft AzCopy commandline utility to transfer files. To enable this, set the argument `use_azcopy=TRUE` in any call to an upload or download function; AzureStor will then call AzCopy to perform the file transfer rather than relying on its own code. You can also call AzCopy directly with the `call_azcopy` function, passing it any arguments as required.
#'
#' AzureStor requires version 10 or later of AzCopy. The first time you try to run it, AzureStor will check that the version of AzCopy is correct, and throw an error if it is version 8 or earlier.
#'
#' The AzCopy utility must be in your path for AzureStor to find it. Note that unlike earlier versions, Azcopy 10 is a single, self-contained binary file that can be placed in any directory.
#'
#' AzCopy uses its own mechanisms for authenticating with Azure Active Directory, which is independent of the OAuth tokens used by AzureStor. AzureStor will try to ensure that AzCopy has previously authenticated before trying to transfer a file with a token, but this may not always succeed. You can run `azcopy_login(force=TRUE)` to force it to authenticate.
#'
#' @seealso
#' [AzCopy page on Microsoft Docs](https://docs.microsoft.com/en-us/azure/storage/common/storage-use-azcopy-v10)
#'
#' [AzCopy GitHub repo](https://github.com/Azure/azure-storage-azcopy)
#' @aliases azcopy
#' @rdname azcopy
#' @export
call_azcopy <- function(...)
{
    azcopy <- get_azcopy_path()
    args <- paste(sapply(list(...), as.character), collapse=" ")
    cat("Command: azcopy", args, "\n")
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
    azcopy_upload_internal(container, src, dest, opts)
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

    dest_uri <- httr::parse_url(container$endpoint$url)
    dest_uri$path <- gsub("//", "/", file.path(container$name, dest))
    dest <- httr::build_url(dest_uri)

    call_azcopy("copy", shQuote(src), shQuote(dest), opts)
}


azcopy_download <- function(container, src, dest, ...)
{
    UseMethod("azcopy_download")
}

# currently all azcopy_download methods are the same
azcopy_download.blob_container <- function(container, src, dest, overwrite=FALSE, ...)
{
    opts <- paste0("--overwrite=", tolower(as.character(overwrite)))
    azcopy_download_internal(container, src, dest, opts)
}

azcopy_download.file_share  <- function(container, src, dest, overwrite=FALSE, ...)
{
    opts <- paste0("--overwrite=", tolower(as.character(overwrite)))
    azcopy_download_internal(container, src, dest, opts)
}

azcopy_download.adls_filesystem <- function(container, src, dest, overwrite=FALSE, ...)
{
    opts <- paste0("--overwrite=", tolower(as.character(overwrite)))
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

    src_uri <- httr::parse_url(container$endpoint$url)
    src_uri$path <- gsub("//", "/", file.path(container$name, src))
    src <- httr::build_url(src_uri)

    call_azcopy("copy", shQuote(src), shQuote(dest), opts)
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
        return(structure(endpoint$key, method="key"))
    if(!is.null(endpoint$token))
        return(structure(0, method="token"))

    stop("No supported authentication method found for ADLSgen2", call.=FALSE)
}

check_azcopy_auth.default <- function(container)
{
    stop("Unknown or unsupported container type: ", class(container)[1], call.=FALSE)
}
