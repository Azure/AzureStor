#' Call the azcopy file transfer utility
#'
#' @param ... Arguments to pass to AzCopy on the commandline. If no arguments are supplied, a help screen is printed.
#' @param env Environment variables to pass to AzCopy. Typically these will be for authentication.
#' @param endpoint For `azcopy_key_creds` and `azcopy_token_creds`, an AzureStor endpoint object (of class `storage_endpoint`).
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
call_azcopy <- function(..., env=NULL)
{
    if(!requireNamespace("processx"))
        stop("The processx package must be installed to use azcopy", call.=FALSE)
    azcopy <- get_azcopy_path()

    args <- as.character(unlist(list(...)))

    invisible(processx::run(azcopy, args, echo=TRUE, echo_cmd=TRUE, env=env))
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

azcopy_upload.blob_container <- function(container, src, dest, type="BlockBlob", blocksize=2^24, recursive=FALSE,
                                         lease=NULL, ...)
{
    opts <- c("--blobType", type, "--block-size", sprintf("%.0f", blocksize), if(recursive) "--recursive")
    azcopy_upload_internal(container, src, dest, opts, ...)
}

azcopy_upload.file_share <- function(container, src, dest, blocksize=2^24, recursive=FALSE, ...)
{
    opts <- sprintf("--block-size %.0f", blocksize, if(recursive) "--recursive")
    azcopy_upload_internal(container, src, dest, opts, ...)
}

azcopy_upload.adls_filesystem <- function(container, src, dest, blocksize=2^24, recursive=FALSE, lease=NULL, ...)
{
    opts <- sprintf("--block-size %.0f", blocksize, if(recursive) "--recursive")
    azcopy_upload_internal(container, src, dest, opts, ...)
}

azcopy_upload_internal <- function(container, src, dest, opts, recursive=FALSE, ...)
{
    env <- character(0)
    endp <- container$endpoint
    if(!is.null(endp$key))
        env <- azcopy_key_creds(endp)
    else if(!is.null(endp$token))
        env <- azcopy_token_creds(endp)
    else if(!is.null(endp$sas))
        dest <- paste0(dest, "?", sub("^\\?", "", endp$sas))

    dest_uri <- httr::parse_url(container$endpoint$url)
    dest_uri$path <- gsub("//", "/", file.path(container$name, dest))
    dest <- httr::build_url(dest_uri)

    call_azcopy("copy", src, dest, opts, ..., env=env)
}


azcopy_download <- function(container, src, dest, ...)
{
    UseMethod("azcopy_download")
}

# currently all azcopy_download methods are the same
azcopy_download.blob_container <- function(container, src, dest, overwrite=FALSE, ...)
{
    opts <- paste0("--overwrite=", tolower(as.character(overwrite)))
    azcopy_download_internal(container, src, dest, opts, ...)
}

azcopy_download.file_share  <- function(container, src, dest, overwrite=FALSE, ...)
{
    opts <- paste0("--overwrite=", tolower(as.character(overwrite)))
    azcopy_download_internal(container, src, dest, opts, ...)
}

azcopy_download.adls_filesystem <- function(container, src, dest, overwrite=FALSE, ...)
{
    opts <- paste0("--overwrite=", tolower(as.character(overwrite)))
    azcopy_download_internal(container, src, dest, opts, ...)
}

azcopy_download_internal <- function(container, src, dest, opts, ...)
{
    env <- character(0)
    endp <- container$endpoint
    if(!is.null(endp$key))
        env <- azcopy_key_creds(endp)
    else if(!is.null(endp$token))
        env <- azcopy_token_creds(endp)
    else if(!is.null(endp$sas))
        src <- paste0(src, "?", sub("^\\?", "", endp$sas))

    src_uri <- httr::parse_url(container$endpoint$url)
    src_uri$path <- gsub("//", "/", file.path(container$name, src))
    src <- httr::build_url(src_uri)

    call_azcopy("copy", src, dest, opts, ..., env=env)
}


#' @export
#' @rdname azcopy
azcopy_key_creds <- function(endpoint)
{
    if(is.null(endpoint$key))
        return(NULL)
    acctname <- sub("\\..*$", "", httr::parse_url(endpoint$url)$hostname)
    c(AZCOPY_ACCOUNT_NAME=acctname, AZCOPY_ACCOUNT_KEY=unname(endpoint$key))
}


#' @export
#' @rdname azcopy
azcopy_token_creds <- function(endpoint)
{
    token <- endpoint$token
    if(is.null(token))
        return(NULL)
    creds <- list(
        access_token=token$credentials$access_token,
        refresh_token=token$credentials$refresh_token,
        token_type=token$credentials$token_type,
        resource=token$credentials$resource,
        scope=token$credentials$scope,
        not_before=token$credentials$not_before,
        expires_on=token$credentials$expires_on,
        expires_in=token$credentials$expires_in,
        `_tenant`=token$tenant,
        `_ad_endpoint`=token$aad_host
    )
    c(AZCOPY_OAUTH_TOKEN_INFO=jsonlite::toJSON(creds[!sapply(creds, is.null)], auto_unbox=TRUE))
}

