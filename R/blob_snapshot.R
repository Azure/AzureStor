#' Create, list and delete blob snapshots
#'
#' @param container A blob container.
#' @param blob The path/name of a blob.
#' @param ... For `create_blob_snapshot`, an optional list of name-value pairs that will be treated as the metadata for the snapshot. If no metadata is supplied, the metadata for the base blob is copied to the snapshot.
#' @param snapshot For `delete_blob_snapshot`, the specific snapshot to delete. This should be a datetime string, in the format "yyyy-mm-ddTHH:MM:SS.SSSSSSSZ". To delete _all_ snapshots for the blob, set this to `"all"`.
#' @param confirm Whether to ask for confirmation on deleting a blob's snapshots.
#'#' @details
#' Blobs can have _snapshots_ associated with them, which are the contents and optional metadata for the blob at a given point in time. A snapshot is identified by the date and time on which it was created.
#'
#' `create_blob_snapshot` creates a new snapshot, `list_blob_snapshots` lists all the snapshots, and `delete_blob_snapshot` deletes a given snapshot or all snapshots for a blob.
#'
#' Note that snapshots are only supported if the storage account does NOT have hierarchical namespaces enabled.
#' @return
#' For `create_blob_snapshot`, the datetime string that identifies the snapshot.
#'
#' For `list_blob_snapshots` a vector of such strings, or NULL if the blob has no snapshots.
#' @seealso
#' Other AzureStor functions that support blob snapshots by passing a `snapshot` argument: [download_blob], [get_storage_properties], [get_storage_metadata]
#' @examples
#' \dontrun{
#'
#' cont <- blob_container("https://mystorage.blob.core.windows.net/mycontainer", key="access_key")
#'
#' snap_id <- create_blob_snapshot(cont, "myfile", tag1="value1", tag2="value2")
#'
#' list_blob_snapshots(cont, "myfile")
#'
#' get_storage_properties(cont, "myfile", snapshot=snap_id)
#'
#' # returns list(tag1="value1", tag2="value2")
#' get_storage_metadata(cont, "myfile", snapshot=snap_id)
#'
#' download_blob(cont, "myfile", snapshot=snap_id)
#'
#' # delete all snapshots
#' delete_blob_snapshots(cont, "myfile", snapshot="all")
#'
#' }
#' @rdname snapshot
#' @export
create_blob_snapshot <- function(container, blob, ...)
{
    opts <- list(comp="snapshot")
    meta <- list(...)
    if(!is_empty(meta))
        hdrs <- set_classic_metadata_headers(meta)

    res <- do_container_op(container, blob, options=opts, headers=hdrs, http_verb="PUT", return_headers=TRUE)
    res$`x-ms-snapshot`
}


#' @rdname snapshot
#' @export
list_blob_snapshots <- function(container, blob)
{
    opts <- list(comp="list", restype="container", include="snapshots", prefix=as.character(blob))

    res <- do_container_op(container, options=opts)
    lst <- res$Blobs
    while(length(res$NextMarker) > 0)
    {
        opts$marker <- res$NextMarker[[1]]
        res <- do_container_op(container, options=opts)
        lst <- c(lst, res$Blobs)
    }

    unname(unlist(lapply(lst, function(bl) bl$Snapshot[[1]])))
}


#' @rdname snapshot
#' @export
delete_blob_snapshot <- function(container, blob, snapshot, confirm=TRUE)
{
    if(!delete_confirmed(confirm, snapshot, "blob snapshot"))
        return(invisible(NULL))

    hdrs <- opts <- list()
    if(snapshot == "all")
        hdrs <- list(`x-ms-delete-snapshots`="only")
    else opts <- list(snapshot=snapshot)

    invisible(do_container_op(container, blob, options=opts, headers=hdrs, http_verb="DELETE"))
}
