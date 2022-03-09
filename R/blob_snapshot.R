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


