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

    ver <- suppressWarnings(processx::run(path, "--version"))
    if(!grepl("version 1[[:digit:]]", ver$stdout, ignore.case=TRUE))
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
    opts <- azcopy_upload_opts(container, ...)
    dest_uri <- httr::parse_url(container$endpoint$url)
    dest_uri$path <- gsub("//", "/", file.path(container$name, dest))
    dest <- httr::build_url(dest_uri)

    auth <- azcopy_auth(container)
    azcopy_login(auth)
    dest <- azcopy_add_sas(auth, dest)

    call_azcopy("copy", src, dest, opts, env=auth$env)
}


azcopy_upload_opts <- function(container, ...)
{
    UseMethod("azcopy_upload_opts")
}

azcopy_upload_opts.blob_container <- function(container, type="BlockBlob", blocksize=2^24, recursive=FALSE,
                                              lease=NULL, ...)
{
    c("--blobType", type, "--block-size", sprintf("%.0f", blocksize), if(recursive) "--recursive")
}

azcopy_upload_opts.file_share <- function(container, blocksize=2^22, recursive=FALSE, ...)
{
    c("--block-size", sprintf("%.0f", blocksize), if(recursive) "--recursive")
}

azcopy_upload_opts.adls_filesystem <- function(container, blocksize=2^24, recursive=FALSE, lease=NULL, ...)
{
    c("--block-size", sprintf("%.0f", blocksize), if(recursive) "--recursive")
}


azcopy_download <- function(container, src, dest, ...)
{
    opts <- azcopy_download_opts(container, ...)
    src_uri <- httr::parse_url(container$endpoint$url)
    src_uri$path <- gsub("//", "/", file.path(container$name, src))
    src <- httr::build_url(src_uri)

    auth <- azcopy_auth(container)
    azcopy_login(auth)
    src <- azcopy_add_sas(auth, src)

    call_azcopy("copy", src, dest, opts, env=auth$env)
}


azcopy_download_opts <- function(container, ...)
{
    UseMethod("azcopy_download_opts")
}

# currently all azcopy_download_opts methods are the same
azcopy_download_opts.blob_container <- function(container, overwrite=FALSE, ...)
{
    paste0("--overwrite=", tolower(as.character(overwrite)))
}

azcopy_download.file_share  <- function(container, overwrite=FALSE, ...)
{
    paste0("--overwrite=", tolower(as.character(overwrite)))
}

azcopy_download.adls_filesystem <- function(container, overwrite=FALSE, ...)
{
    paste0("--overwrite=", tolower(as.character(overwrite)))
}

