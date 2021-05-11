#' @import AzureRMR
#' @importFrom utils URLencode modifyList packageVersion glob2rx
NULL

globalVariables(c("self", "pool"), "AzureStor")

.AzureStor <- new.env()


.onLoad <- function(libname, pkgname)
{
    options(azure_storage_api_version="2020-04-08")
    options(azure_storage_progress_bar=TRUE)
    options(azure_storage_retries=10)

    # all methods extending classes in external package must be run from .onLoad
    add_methods()
}



