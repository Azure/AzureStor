#' @import AzureRMR
#' @importFrom utils URLencode modifyList packageVersion glob2rx
NULL

globalVariables(c("self", "pool"), "AzureStor")

.AzureStor <- new.env()


.onLoad <- function(libname, pkgname)
{
    options(azure_storage_api_version="2018-03-28")
    options(azure_adls_api_version="2018-06-17")
    options(azure_dl_progress_bar=TRUE)
    options(azure_storage_retries=10)

    # all methods extending classes in external package must be run from .onLoad
    add_methods()
}


.onUnload <- function(libpath)
{
    if(exists("pool", envir=.AzureStor))
        try(parallel::stopCluster(.AzureStor$pool), silent=TRUE)
}


