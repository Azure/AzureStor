#' @import AzureRMR
NULL

.onLoad <- function(libname, pkgname)
{
    api="2017-07-29"
    options(azure_storage_api_version=api)
    invisible(NULL)
}

