#' Operations on blob leases
#'
#' Manage leases for blobs and blob containers.
#'
#' @param container A blob container object.
#' @param blob The name of an individual blob. If not supplied, the lease applies to the entire container.
#' @param duration For `acquire_lease`, The duration of the requested lease. For an indefinite duration, set this to -1.
#' @param lease For `acquire_lease` an optional proposed name of the lease; for `release_lease`, `renew_lease` and `change_lease`, the name of the existing lease.
#' @param period For `break_lease`, the period for which to break the lease.
#' @param new_lease For `change_lease`, the proposed name of the lease.
#'
#' @details
#' Leasing is a way to prevent a blob or container from being accidentally deleted. The duration of a lease can range from 15 to 60 seconds, or be indefinite.
#'
#' @return
#' For `acquire_lease` and `change_lease`, a string containing the lease ID.
#'
#' @seealso
#' [blob_container],
#' [Leasing a blob](https://docs.microsoft.com/en-us/rest/api/storageservices/lease-blob),
#' [Leasing a container](https://docs.microsoft.com/en-us/rest/api/storageservices/lease-container)
#'
#' @rdname lease
#' @export
acquire_lease <- function(container, blob="", duration=60, lease=NULL)
{
    headers <- list("x-ms-lease-action"="acquire", "x-ms-lease-duration"=duration)
    if(!is_empty(lease))
        headers <- c(headers, list("x-ms-proposed-lease-id"=lease))
    res <- do_container_op(container, blob, options=list(comp="lease", restype="container"), headers=headers,
                           http_verb="PUT", http_status_handler="pass")
    httr::stop_for_status(res, storage_error_message(res))
    httr::headers(res)[["x-ms-lease-id"]]
}


#' @rdname lease
#' @export
break_lease <- function(container, blob="", period=NULL)
{
    headers <- list("x-ms-lease-action"="break")
    if(!is_empty(period))
        headers=c(headers, list("x-ms-lease-break-period"=period))
    do_container_op(container, blob, options=list(comp="lease", restype="container"), headers=headers,
                    http_verb="PUT")
}


#' @rdname lease
#' @export
release_lease <- function(container, blob="", lease)
{
    headers <- list("x-ms-lease-id"=lease, "x-ms-lease-action"="release")
    do_container_op(container, blob, options=list(comp="lease", restype="container"), headers=headers,
                    http_verb="PUT")
}


#' @rdname lease
#' @export
renew_lease <- function(container, blob="", lease)
{
    headers <- list("x-ms-lease-id"=lease, "x-ms-lease-action"="renew")
    do_container_op(container, blob, options=list(comp="lease", restype="container"), headers=headers,
                    http_verb="PUT")
}


#' @rdname lease
#' @export
change_lease <- function(container, blob="", lease, new_lease)
{
    headers <- list("x-ms-lease-id"=lease, "x-ms-lease-action"="change", "x-ms-proposed-lease-id"=new_lease)
    res <- do_container_op(container, blob, options=list(comp="lease", restype="container"), headers=headers,
                           http_verb="PUT", http_status_handler="pass")
    httr::stop_for_status(res, storage_error_message(res))
    httr::headers(res)[["x-ms-lease-id"]]
}
