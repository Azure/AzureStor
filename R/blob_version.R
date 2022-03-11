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


delete_blob_version <- function(container, blob, version, confirm=TRUE)
{
    if(!delete_confirmed(confirm, version, "blob version"))
        return(invisible(NULL))

    opts <- list(versionid=version)
    invisible(do_container_op(container, blob, options=opts, http_verb="DELETE"))
}
