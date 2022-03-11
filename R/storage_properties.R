#' Get storage properties for an object
#'
#' @param object A blob container, file share, or ADLS filesystem object.
#' @param filesystem An ADLS filesystem.
#' @param blob,file Optionally the name of an individual blob, file or directory within a container.
#' @param isdir For the file share method, whether the `file` argument is a file or directory. If omitted, `get_storage_properties` will auto-detect the type; however this can be slow, so supply this argument if possible.
#' @param snapshot,version For the blob method of `get_storage_properties`, optional snapshot and version identifiers. These should be datetime strings, in the format "yyyy-mm-ddTHH:MM:SS.SSSSSSSZ". Ignored if `blob` is omitted.
#' @param ... For compatibility with the generic.
#' @return
#' `get_storage_properties` returns a list describing the object properties. If the `blob` or `file` argument is present for the container methods, the properties will be for the blob/file specified. If this argument is omitted, the properties will be for the container itself.
#'
#' `get_adls_file_acl` returns a string giving the ADLSgen2 ACL for the file.
#'
#' `get_adls_file_status` returns a list of ADLSgen2 system properties for the file.
#'
#' @seealso
#' [blob_container], [file_share], [adls_filesystem]
#'
#' [get_storage_metadata] for getting and setting _user-defined_ properties (metadata)
#'
#' [list_blob_snapshots] to obtain the snapshots for a blob
#' @examples
#' \dontrun{
#'
#' fs <- storage_container("https://mystorage.dfs.core.windows.net/myshare", key="access_key")
#' create_storage_dir("newdir")
#' storage_upload(share, "iris.csv", "newdir/iris.csv")
#'
#' get_storage_properties(fs)
#' get_storage_properties(fs, "newdir")
#' get_storage_properties(fs, "newdir/iris.csv")
#'
#' # these are ADLS only
#' get_adls_file_acl(fs, "newdir/iris.csv")
#' get_adls_file_status(fs, "newdir/iris.csv")
#'
#' }
#' @rdname properties
#' @export
get_storage_properties <- function(object, ...)
{
    UseMethod("get_storage_properties")
}


#' @rdname properties
#' @export
get_storage_properties.blob_container <- function(object, blob, snapshot=NULL, version=NULL, ...)
{
    # properties for container
    if(missing(blob))
        return(do_container_op(object, options=list(restype="container"), http_verb="HEAD"))

    # properties for blob
    opts <- list()
    if(!is.null(snapshot))
        opts$snapshot <- snapshot
    if(!is.null(version))
        opts$versionid <- version

    do_container_op(object, blob, options=opts, http_verb="HEAD")
}


#' @rdname properties
#' @export
get_storage_properties.file_share <- function(object, file, isdir, ...)
{
    # properties for container
    if(missing(file))
        return(do_container_op(object, options=list(restype="share"), http_verb="HEAD"))

    # properties for file/directory
    if(missing(isdir))
    {
        res <- tryCatch(Recall(object, file, FALSE), error=function(e) e)
        if(inherits(res, "error"))
            res <- tryCatch(Recall(object, file, TRUE), error=function(e) e)
        if(inherits(res, "error"))
            stop(res)
        return(res)
    }

    options <- if(isdir) list(restype="directory") else list()
    do_container_op(object, file, options=options, http_verb="HEAD")
}


#' @rdname properties
#' @export
get_storage_properties.adls_filesystem <- function(object, file, ...)
{
    # properties for container
    if(missing(file))
        return(do_container_op(object, options=list(resource="filesystem"), http_verb="HEAD"))

    # properties for file/directory
    do_container_op(object, file, http_verb="HEAD")
}


#' @rdname properties
#' @export
get_adls_file_acl <- function(filesystem, file)
{
    do_container_op(filesystem, file, options=list(action="getaccesscontrol"), http_verb="HEAD")[["x-ms-acl"]]
}


#' @rdname properties
#' @export
get_adls_file_status <- function(filesystem, file)
{
    do_container_op(filesystem, file, options=list(action="getstatus"), http_verb="HEAD")
}

