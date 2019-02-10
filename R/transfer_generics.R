#' Upload and download generics
#'
#' @param container A storage container object.
#' @param src,dest The source and destination files to transfer.
#' @param key,token,sas Authentication arguments: an access key, Azure Active Directory (AAD) token or a shared access signature (SAS). If multiple arguments are supplied, a key takes priority over a token, which takes priority over a SAS. For `upload_to_url` and `download_to_url`, you can also provide a SAS as part of the URL itself.
#' @param ... Further arguments to pass to lower-level functions.
#' @param overwrite For downloading, whether to overwrite any destination files that exist.
#'
#' @details
#' These functions allow you to transfer files to and from a storage account.
#'
#' `storage_upload`, `storage_download`, `storage_multiupload` and `storage_multidownload` take as first argument a storage container, either for blob storage, file storage, or ADLSgen2. They dispatch to the corresponding file transfer functions for the given storage type.
#'
#' `upload_to_url` and `download_to_url` allow you to transfer a file to or from Azure storage, given the URL of the source or destination. The storage details (endpoint, container name, and so on) are obtained from the URL.
#'
#' @seealso
#' [storage_container], [blob_container], [file_share], [adls_filesystem]
#'
#' [download_blob], [download_azure_file], [download_adls_file], [call_azcopy]
#'
#' @examples
#' \dontrun{
#'
#' # download from blob storage
#' bl <- storage_endpoint("https://mystorage.blob.core.windows.net/", key="access_key")
#' cont <- storage_container(bl, "mycontainer")
#' storage_download(cont, "bigfile.zip", "~/bigfile.zip")
#'
#' # same download but directly from the URL
#' download_from_url("https://mystorage.blob.core.windows.net/mycontainer/bigfile.zip",
#'                   "~/bigfile.zip",
#'                   key="access_key")
#'
#' # upload to ADLSgen2
#' ad <- storage_endpoint("https://myadls.dfs.core.windows.net/", token=mytoken)
#' cont <- storage_container(ad, "myfilesystem")
#' create_storage_dir(cont, "newdir")
#' storage_upload(cont, "files.zip", "newdir/files.zip")
#'
#' # same upload but directly to the URL
#' upload_to_url("files.zip",
#'               "https://myadls.dfs.core.windows.net/myfilesystem/newdir/files.zip",
#'               token=mytoken)
#'
#' }
#' @rdname file_transfer
#' @export
storage_upload <- function(container, ...)
UseMethod("storage_upload")

#' @rdname file_transfer
#' @export
storage_upload.blob_container <- function(container, src, dest, ...)
upload_blob(container, src, dest, ...)

#' @rdname file_transfer
#' @export
storage_upload.file_share <- function(container, src, dest, ...)
upload_azure_file(container, src, dest, ...)

#' @rdname file_transfer
#' @export
storage_upload.adls_filesystem <- function(container, src, dest, ...)
upload_adls_file(container, src, dest, ...)

#' @rdname file_transfer
#' @export
storage_multiupload <- function(container, ...)
UseMethod("storage_multiupload")

#' @rdname file_transfer
#' @export
storage_multiupload.blob_container <- function(container, src, dest, ...)
multiupload_blob(container, src, dest, ...)

#' @rdname file_transfer
#' @export
storage_multiupload.file_share <- function(container, src, dest, ...)
multiupload_azure_file(container, src, dest, ...)

#' @rdname file_transfer
#' @export
storage_multiupload.adls_filesystem <- function(container, src, dest, ...)
multiupload_adls_file(container, src, dest, ...)


# download

#' @rdname file_transfer
#' @export
storage_download <- function(container, ...)
UseMethod("storage_download")

#' @rdname file_transfer
#' @export
storage_download.blob_container <- function(container, src, dest, ...)
download_blob(container, src, dest, ...)

#' @rdname file_transfer
#' @export
storage_download.file_share <- function(container, src, dest, ...)
download_azure_file(container, src, dest, ...)

#' @rdname file_transfer
#' @export
storage_download.adls_filesystem <- function(container, src, dest, ...)
download_adls_file(container, src, dest, ...)

#' @rdname file_transfer
#' @export
storage_multidownload <- function(container, ...)
UseMethod("storage_multidownload")

#' @rdname file_transfer
#' @export
storage_multidownload.blob_container <- function(container, src, dest, ...)
multidownload_blob(container, src, dest, ...)

#' @rdname file_transfer
#' @export
storage_multidownload.file_share <- function(container, src, dest, ...)
multidownload_azure_file(container, src, dest, ...)

#' @rdname file_transfer
#' @export
storage_multidownload.adls_filesystem <- function(container, src, dest, ...)
multidownload_adls_file(container, src, dest, ...)


#' @rdname file_transfer
#' @export
download_from_url <- function(src, dest, key=NULL, token=NULL, sas=NULL, ..., overwrite=FALSE)
{
    az_path <- parse_storage_url(src)
    if(is.null(sas))
        sas <- find_sas(src)

    endpoint <- storage_endpoint(az_path[1], key=key, token=token, sas=sas, ...)
    cont <- storage_container(endpoint, az_path[2])
    storage_download(cont, az_path[3], dest, overwrite=overwrite)
}


#' @rdname file_transfer
#' @export
upload_to_url <- function(src, dest, key=NULL, token=token, sas=NULL, ...)
{
    az_path <- parse_storage_url(dest)
    if(is.null(sas))
        sas <- find_sas(dest)

    endpoint <- storage_endpoint(az_path[1], key=key, token=token, sas=sas, ...)
    cont <- storage_container(endpoint, az_path[2])
    storage_upload(cont, src, az_path[3])
}


find_sas <- function(url)
{
    querymark <- regexpr("\\?sv", url)
    if(querymark == -1)
        NULL
    else substr(url, querymark + 1, nchar(url))
}

