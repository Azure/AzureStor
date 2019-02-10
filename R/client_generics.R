#' Storage client generics
#'
#' @param endpoint A storage endpoint object, or for the character methods, a string giving the full URL to the container.
#' @param container A storage container object.
#' @param key,token,sas For the character methods, authentication credentials for the container: either an access key, an Azure Active Directory (AAD) token, or a SAS. If multiple arguments are supplied, a key takes priority over a token, which takes priority over a SAS.
#' @param name For the storage container management methods, a container name.
#' @param file,dir For the storage object management methods, a file or directory name.
#' @param confirm For the deletion methods, whether to ask for confirmation first.
#' @param ... Further arguments to pass to lower-level functions.
#'
#' @details
#' These methods provide a framework for all storage management tasks supported by AzureStor. They dispatch to the appropriate functions for each type of storage.
#'
#' Storage container management methods:
#' - `storage_container` dispatches to `blob_container`, `file_share` or `adls_filesystem`
#' - `create_storage_container` dispatches to `create_blob_container`, `create_file_share` or `create_adls_filesystem`
#' - `delete_storage_container` dispatches to `delete_blob_container`, `delete_file_share` or `delete_adls_filesystem`
#' - `list_storage_containers` dispatches to `list_blob_containers`, `list_file_shares` or `list_adls_filesystems`
#'
#' Storage object management methods:
#' - `list_storage_files` dispatches to `list_blobs`, `list_azure_files` or `list_adls_files`
#' - `create_storage_dir` dispatches to `create_azure_dir` or `create_adls_dir`; throws an error if passed a blob container
#' - `delete_storage_dir` dispatches to `delete_azure_dir` or `delete_adls_dir`; throws an error if passed a blob container
#' - `delete_storage_file` dispatches to `delete_blob`, `delete_azure_file` or `delete_adls_file`
#'
#' @seealso
#' [storage_endpoint], [blob_container], [file_share], [adls_filesystem]
#'
#' [list_blobs], [list_azure_files], [list_adls_files]
#'
#' Similar generics exist for file transfer methods; see the page for [storage_download].
#'
#' @examples
#' \dontrun{
#'
#' # storage endpoints for the one account
#' bl <- storage_endpoint("https://mystorage.blob.core.windows.net/", key="access_key")
#' fl <- storage_endpoint("https://mystorage.file.core.windows.net/", key="access_key")
#'
#' list_storage_containers(bl)
#' list_storage_containers(fl)
#'
#' # creating containers
#' cont <- create_storage_container(bl, "newblobcontainer")
#' fs <- create_storage_container(fl, "newfileshare")
#'
#' # creating directories (if possible)
#' create_storage_dir(cont, "newdir")  # will error out
#' create_storage_dir(fs, "newdir")
#'
#' # transfer a file
#' storage_upload(bl, "~/file.txt", "storage_file.txt")
#' storage_upload(cont, "~/file.txt", "newdir/storage_file.txt")
#'
#' }
#' @aliases storage_generics
#' @rdname generics
#' @export
storage_container <- function(endpoint, ...)
UseMethod("storage_container")

#' @rdname generics
#' @export
storage_container.blob_endpoint <- function(endpoint, name, ...)
blob_container(endpoint, name, ...)

#' @rdname generics
#' @export
storage_container.file_endpoint <- function(endpoint, name, ...)
file_share(endpoint, name, ...)

#' @rdname generics
#' @export
storage_container.adls_endpoint <- function(endpoint, name, ...)
adls_filesystem(endpoint, name, ...)

#' @rdname generics
#' @export
storage_container.character <- function(endpoint, key=NULL, token=NULL, sas=NULL, ...)
{
    lst <- parse_storage_url(endpoint)
    endpoint <- storage_endpoint(lst[[1]], key=key, token=token, sas=sas, ...)
    storage_container(endpoint, lst[[2]])
}


# create container

#' @rdname generics
#' @export
create_storage_container <- function(endpoint, ...)
UseMethod("create_storage_container")

#' @rdname generics
#' @export
create_storage_container.blob_endpoint <- function(endpoint, name, ...)
create_blob_container(endpoint, name, ...)

#' @rdname generics
#' @export
create_storage_container.file_endpoint <- function(endpoint, name, ...)
create_file_share(endpoint, name, ...)

#' @rdname generics
#' @export
create_storage_container.adls_endpoint <- function(endpoint, name, ...)
create_adls_filesystem(endpoint, name, ...)

#' @rdname generics
#' @export
create_storage_container.storage_container <- function(endpoint, ...)
create_storage_container(endpoint$endpoint, endpoint$name, ...)

#' @rdname generics
#' @export
create_storage_container.character <- function(endpoint, key=NULL, token=NULL, sas=NULL, ...)
{
    lst <- parse_storage_url(endpoint)
    endpoint <- storage_endpoint(lst[[1]], key=key, token=token, sas=sas, ...)
    create_storage_container(endpoint, lst[[2]])
}


# delete container

#' @rdname generics
#' @export
delete_storage_container <- function(endpoint, ...)
UseMethod("delete_storage_container")

#' @rdname generics
#' @export
delete_storage_container.blob_endpoint <- function(endpoint, name, ...)
delete_blob_container(endpoint, name, ...)

#' @rdname generics
#' @export
delete_storage_container.file_endpoint <- function(endpoint, name, ...)
delete_file_share(endpoint, name, ...)

#' @rdname generics
#' @export
delete_storage_container.adls_endpoint <- function(endpoint, name, ...)
delete_adls_filesystem(endpoint, name, ...)

#' @rdname generics
#' @export
delete_storage_container.storage_container <- function(endpoint, ...)
delete_storage_container(endpoint$endpoint, endpoint$name, ...)

#' @rdname generics
#' @export
delete_storage_container.character <- function(endpoint, key=NULL, token=NULL, sas=NULL, confirm=TRUE, ...)
{
    lst <- parse_storage_url(endpoint)
    endpoint <- storage_endpoint(lst[[1]], key=key, token=token, sas=sas, ...)
    delete_storage_container(endpoint, lst[[2]], confirm=confirm)
}


# list containers

#' @rdname generics
#' @export
list_storage_containers <- function(endpoint, ...)
UseMethod("list_storage_containers")

#' @rdname generics
#' @export
list_storage_containers.blob_endpoint <- function(endpoint, ...)
list_blob_containers(endpoint, ...)

#' @rdname generics
#' @export
list_storage_containers.file_endpoint <- function(endpoint, ...)
list_file_shares(endpoint, ...)

#' @rdname generics
#' @export
list_storage_containers.adls_endpoint <- function(endpoint, ...)
list_adls_filesystems(endpoint, ...)

#' @rdname generics
#' @export
list_storage_containers.character <- function(endpoint, key=NULL, token=NULL, sas=NULL, ...)
{
    lst <- parse_storage_url(endpoint)
    endpoint <- storage_endpoint(lst[[1]], key=key, token=token, sas=sas, ...)
    list_storage_containers(endpoint, lst[[2]])
}


# list files

#' @rdname generics
#' @export
list_storage_files <- function(container, ...)
UseMethod("list_storage_files")

#' @rdname generics
#' @export
list_storage_files.blob_container <- function(container, ...)
list_blobs(container, ...)

#' @rdname generics
#' @export
list_storage_files.file_share <- function(container, ...)
list_azure_files(container, ...)

#' @rdname generics
#' @export
list_storage_files.adls_filesystem <- function(container, ...)
list_adls_files(container, ...)


# create directory

#' @rdname generics
#' @export
create_storage_dir <- function(container, ...)
UseMethod("create_storage_dir")

#' @rdname generics
#' @export
create_storage_dir.blob_container <- function(container, ...)
stop("Blob storage does not support directories")

#' @rdname generics
#' @export
create_storage_dir.file_share <- function(container, dir, ...)
create_azure_dir(container, dir, ...)

#' @rdname generics
#' @export
create_storage_dir.adls_filesystem <- function(container, dir, ...)
create_adls_dir(container, dir, ...)


# delete directory

#' @rdname generics
#' @export
delete_storage_dir <- function(container, ...)
UseMethod("delete_storage_dir")

#' @rdname generics
#' @export
delete_storage_dir.blob_container <- function(container, ...)
stop("Blob storage does not support directories")

#' @rdname generics
#' @export
delete_storage_dir.file_share <- function(container, dir, ...)
delete_azure_dir(container, dir, ...)

#' @rdname generics
#' @export
delete_storage_dir.adls_filesystem <- function(container, dir, confirm=TRUE, ...)
delete_adls_dir(container, dir, confirm=confirm, ...)


# delete file

#' @rdname generics
#' @export
delete_storage_file <- function(container, ...)
UseMethod("delete_storage_file")

#' @rdname generics
#' @export
delete_storage_file.blob_container <- function(container, file, ...)
delete_blob(container, file, ...)

#' @rdname generics
#' @export
delete_storage_file.file_share <- function(container, file, ...)
delete_azure_file(container, file, ...)

#' @rdname generics
#' @export
delete_storage_file.adls_filesystem <- function(container, file, confirm=TRUE, ...)
delete_adls_file(container, file, confirm=confirm, ...)

