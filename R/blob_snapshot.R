create_snapshot <- function(container, blob)
{
    opts <- list(comp="snapshot")
    res <- do_container_op(container, blob, options=opts, http_verb="PUT", return_headers=TRUE)
    res$`x-ms-snapshot`
}


list_snapshots <- function(container, blob)
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


delete_snapshot <- function(container, blob, snapshot, confirm=TRUE)
{
    if(!delete_confirmed(confirm, paste0(container$endpoint$url, container$name, "/", blob), "blob snapshot"))
        return(invisible(NULL))

    hdrs <- opts <- list()
    if(snapshot == "all")
        hdrs <- list(`x-ms-delete-snapshots`="only")
    else opts <- list(snapshot=snapshot)

    invisible(do_container_op(container, blob, options=opts, headers=hdrs, http_verb="DELETE"))
}
