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


#' @export
break_lease <- function(container, blob="", period=NULL)
{
    headers <- list("x-ms-lease-action"="break")
    if(!is_empty(period))
        headers=c(headers, list("x-ms-lease-break-period"=period))
    do_container_op(container, blob, options=list(comp="lease", restype="container"), headers=headers,
                    http_verb="PUT")
}


#' @export
release_lease <- function(container, blob="", lease)
{
    headers <- list("x-ms-lease-id"=lease, "x-ms-lease-action"="release")
    do_container_op(container, blob, options=list(comp="lease", restype="container"), headers=headers,
                    http_verb="PUT")
}


#' @export
renew_lease <- function(container, blob="", lease)
{
    headers <- list("x-ms-lease-id"=lease, "x-ms-lease-action"="renew")
    do_container_op(container, blob, options=list(comp="lease", restype="container"), headers=headers,
                    http_verb="PUT")
}


#' @export
change_lease <- function(container, blob="", lease, new_lease)
{
    headers <- list("x-ms-lease-id"=lease, "x-ms-lease-action"="change", "x-ms-proposed-lease-id"=new_lease)
    res <- do_container_op(container, blob, options=list(comp="lease", restype="container"), headers=headers,
                           http_verb="PUT", http_status_handler="pass")
    httr::stop_for_status(res, storage_error_message(res))
    httr::headers(res)[["x-ms-lease-id"]]
}
