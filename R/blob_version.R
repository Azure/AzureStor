#' List and delete blob versions
#'
#' @param container A blob container.
#' @param blob The path/name of a blob.
#' @param version For `delete_blob_version`, the specific version to delete. This should be a datetime string, in the format `yyyy-mm-ddTHH:MM:SS.SSSSSSSZ`.
#' @param confirm Whether to ask for confirmation on deleting a blob version.
#' @details
#' A version captures the state of a blob at a given point in time. Each version is identified with a version ID. When blob versioning is enabled for a storage account, Azure Storage automatically creates a new version with a unique ID when a blob is first created and each time that the blob is subsequently modified.
#'
#' A version ID can identify the current version or a previous version. A blob can have only one current version at a time.
#'
#' When you create a new blob, a single version exists, and that version is the current version. When you modify an existing blob, the current version becomes a previous version. A new version is created to capture the updated state, and that new version is the current version. When you delete a blob, the current version of the blob becomes a previous version, and there is no longer a current version. Any previous versions of the blob persist.
#'
#' Versions are different to [snapshots][list_blob_snapshots]:
#' - A new snapshot has to be explicitly created via `create_blob_snapshot`. A new blob version is automatically created whenever the base blob is modified (and hence there is no `create_blob_version` function).
#' - Deleting the base blob will also delete all snapshots for that blob, while blob versions will be retained (but will typically be inaccessible).
#' - Snapshots are only available for storage accounts with hierarchical namespaces disabled, while versioning can be used with any storage account.
#'
#' @return
#' For `list_blob_versions`, a vector of datetime strings which are the IDs of each version.
#' @rdname version
#' @export
list_blob_versions <- function(container, blob)
{
    opts <- list(comp="list", restype="container", include="versions", prefix=as.character(blob))

    res <- do_container_op(container, options=opts)
    lst <- res$Blobs
    while(length(res$NextMarker) > 0)
    {
        opts$marker <- res$NextMarker[[1]]
        res <- do_container_op(container, options=opts)
        lst <- c(lst, res$Blobs)
    }

    unname(unlist(lapply(lst, function(bl) bl$VersionId[[1]])))
}


#' @rdname version
#' @export
delete_blob_version <- function(container, blob, version, confirm=TRUE)
{
    if(!delete_confirmed(confirm, version, "blob version"))
        return(invisible(NULL))

    opts <- list(versionid=version)
    invisible(do_container_op(container, blob, options=opts, http_verb="DELETE"))
}
