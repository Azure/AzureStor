#' Call the azcopy file transfer utility
#'
#' @param ... Arguments to pass to AzCopy on the commandline. If no arguments are supplied, a help screen is printed.
#' @param env A named character vector of environment variables to set for AzCopy.
#' @param silent Whether to print the output from AzCopy to the screen; also sets whether an error return code from AzCopy will be propagated to an R error. Defaults to the value of the `azure_storage_azcopy_silent` option, or FALSE if this is unset.
#'
#' @details
#' AzureStor has the ability to use the Microsoft AzCopy commandline utility to transfer files. To enable this, ensure the processx package is installed and set the argument `use_azcopy=TRUE` in any call to an upload or download function; AzureStor will then call AzCopy to perform the file transfer rather than relying on its own code. You can also call AzCopy directly with the `call_azcopy` function.
#'
#' AzureStor requires version 10 or later of AzCopy. The first time you try to run it, AzureStor will check that the version of AzCopy is correct, and throw an error if it is version 8 or earlier.
#'
#' The AzCopy utility must be in your path for AzureStor to find it. Note that unlike earlier versions, Azcopy 10 is a single, self-contained binary file that can be placed in any directory.
#'
#' @return
#' A list, invisibly, with the following components:
#' - `status`: The exit status of the AzCopy command. If this is NA, then the process was killed and had no exit status.
#' - `stdout`: The standard output of the command.
#' - `stderr`: The standard error of the command.
#' - `timeout`: Whether AzCopy was killed because of a timeout.
#' @seealso
#' [processx::run], [download_blob], [download_azure_file], [download_adls_file]
#'
#' [AzCopy page on Microsoft Docs](https://docs.microsoft.com/en-us/azure/storage/common/storage-use-azcopy-v10)
#'
#' [AzCopy GitHub repo](https://github.com/Azure/azure-storage-azcopy)
#' @examples
#' \dontrun{
#'
#' endp <- storage_endpoint("https://mystorage.blob.core.windows.net", sas="mysas")
#' cont <- storage_container(endp, "mycontainer")
#'
#' # print various help screens
#' call_azcopy("help")
#' call_azcopy("help", "copy")
#'
#' # calling azcopy to download a blob
#' storage_download(cont, "myblob.csv", use_azcopy=TRUE)
#'
#' # calling azcopy directly (must specify the SAS explicitly in the source URL)
#' call_azcopy("copy",
#'             "https://mystorage.blob.core.windows.net/mycontainer/myblob.csv?mysas",
#'             "myblob.csv")
#'
#' }
#' @aliases azcopy
#' @rdname azcopy
#' @export
call_azcopy <- function(..., env=NULL, silent=getOption("azure_storage_azcopy_silent", FALSE))
{
    silent <- as.logical(silent)
    args <- as.character(unlist(list(...)))
    invisible(processx::run(get_azcopy_path(), args, env=env, echo_cmd=!silent, echo=!silent, error_on_status=!silent))
}


call_azcopy_from_storage <- function(object, ...)
{
    if(!requireNamespace("processx"))
        stop("The processx package must be installed to use azcopy", call.=FALSE)

    auth <- azcopy_auth(object)
    if(auth$login)
        on.exit(call_azcopy("logout", silent=TRUE))
    invisible(call_azcopy(..., env=auth$env))
}


azcopy_upload <- function(container, src, dest, ...)
{
    opts <- azcopy_upload_opts(container, ...)

    dest_uri <- httr::parse_url(container$endpoint$url)
    dest_uri$path <- gsub("//", "/", file.path(container$name, dest))
    dest <- azcopy_add_sas(container$endpoint, httr::build_url(dest_uri))

    call_azcopy_from_storage(container$endpoint, "copy", src, dest, opts)
}

azcopy_upload_opts <- function(container, ...)
{
    UseMethod("azcopy_upload_opts")
}

azcopy_upload_opts.blob_container <- function(container, type="BlockBlob", blocksize=2^24, recursive=FALSE,
                                              lease=NULL, put_md5=FALSE, ...)
{
    c("--blob-type", type, "--block-size-mb", sprintf("%.0f", blocksize/1048576), if(recursive) "--recursive",
      if(put_md5) "--put-md5")
}

azcopy_upload_opts.file_share <- function(container, blocksize=2^22, recursive=FALSE, put_md5=FALSE, ...)
{
    c("--block-size-mb", sprintf("%.0f", blocksize/1048576), if(recursive) "--recursive",
      if(put_md5) "--put-md5")
}

azcopy_upload_opts.adls_filesystem <- function(container, blocksize=2^24, recursive=FALSE, lease=NULL,
                                               put_md5=FALSE, ...)
{
    c("--block-size-mb", sprintf("%.0f", blocksize/1048576), if(recursive) "--recursive",
      if(put_md5) "--put-md5")
}


azcopy_download <- function(container, src, dest, ...)
{
    opts <- azcopy_download_opts(container, ...)

    src_uri <- httr::parse_url(container$endpoint$url)
    src_uri$path <- gsub("//", "/", file.path(container$name, src))
    src <- azcopy_add_sas(container$endpoint, httr::build_url(src_uri))

    call_azcopy_from_storage(container$endpoint, "copy", src, dest, opts)
}

azcopy_download_opts <- function(container, ...)
{
    UseMethod("azcopy_download_opts")
}

# currently all azcopy_download_opts methods are the same
azcopy_download_opts.blob_container <- function(container, overwrite=FALSE, recursive=FALSE, check_md5=FALSE, ...)
{
    c(paste0("--overwrite=", tolower(as.character(overwrite))), if(recursive) "--recursive",
      if(check_md5) c("--check-md5", "FailIfDifferent"))
}

azcopy_download_opts.file_share  <- function(container, overwrite=FALSE, recursive=FALSE, ...)
{
    c(paste0("--overwrite=", tolower(as.character(overwrite))), if(recursive) "--recursive",
      if(check_md5) c("--check-md5", "FailIfDifferent"))
}

azcopy_download_opts.adls_filesystem <- function(container, overwrite=FALSE, recursive=FALSE, ...)
{
    c(paste0("--overwrite=", tolower(as.character(overwrite))), if(recursive) "--recursive",
      if(check_md5) c("--check-md5", "FailIfDifferent"))
}

