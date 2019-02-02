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
    auth <- check_azcopy_auth(container)

    if(attr(auth, "method") == "key")
    {
        acctname <- sub("\\..*$", "", httr::parse_url(container$endpoint$url)$host)
        Sys.setenv(ACCOUNT_NAME=acctname, ACCOUNT_KEY=auth)
        on.exit(Sys.unsetenv(c("ACCOUNT_NAME", "ACCOUNT_KEY")))
    }
    else if(attr(auth, "method") == "token")
        azcopy_login()
    else if(attr(auth, method) == "sas")
        dest <- paste0(dest, "?", auth)

    call_azcopy("copy", src, dest, ...)
}


azcopy_download <- function(container, src, dest, ...)
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
    else if(attr(auth, method) == "sas")
        src <- paste0(src, "?", auth)

    call_azcopy("copy", src, dest, ...)
}


check_azcopy_auth <- function(container, key, token, sas)
{
    endpoint <- container$endpoint

    if(inherits(container, "blob_container"))
    {
        if(!is.null(endpoint$token))
            return(structure(0, method="token"))
        if(!is.null(endpoint$sas))
            return(structure(endpoint$sas, method="sas"))
        warning("No supported authentication method found for blob; defaulting to public", call.=FALSE)
        return(structure(0, method="public"))
    }

    if(inherits(container, "file_share") && !is.null(endpoint$sas))
        return(structure(endpoint$sas, method="sas"))

    if(inherits(container, "adls_filesystem"))
    {
        if(!is.null(endpoint$key))
        {
            warning("Authenticating with a shared key is discouraged")
            return(structure(endpoint$key, method="key"))
        }
        if(!is.null(endpoint$token))
            return(structure(0, method="token"))
    }

    stop("No supported authentication method", call.=FALSE)
}
