#' @import AzureRMR
#' @importFrom utils URLencode modifyList packageVersion glob2rx
NULL

globalVariables(c("self", "pool"), "AzureStor")

.AzureStor <- new.env()


.onLoad <- function(libname, pkgname)
{
    # .AzureStor$azcopy <- find_azcopy()

    # all methods extending classes in external package must go in .onLoad
    add_methods()
}


.onUnload <- function(libpath)
{
    if(exists("pool", envir=.AzureStor))
        try(parallel::stopCluster(.AzureStor$pool), silent=TRUE)
}


# .onAttach <- function(libname, pkgname)
# {
#     if(.AzureStor$azcopy != "")
#         packageStartupMessage("azcopy version 10+ binary found at ", .AzureStor$azcopy)
# }


# find_azcopy <- function()
# {
#     path <- Sys.which("azcopy")
#     if(path != "")
#     {
#         # we need version 10 or later
#         ver <- system2(path, "--version", stdout=TRUE)
#         if(!grepl("version 1[[:digit:]]", ver, ignore.case=TRUE))
#             path <- ""
#     }
#     unname(path)
# }


# set_azcopy_path <- function(path)
# {
#     if(Sys.which(path) == "")
#         stop("azcopy binary not found")
    
#     ver <- system2(path, "--version", stdout=TRUE)
#     if(!grepl("version 1[[:digit:]]", ver, ignore.case=TRUE))
#         stop("azcopy version 10+ required but not found")

#     .AzureStor$azcopy <- path
#     invisible(path)
# }
