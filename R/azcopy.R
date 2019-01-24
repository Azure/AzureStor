# azcopy unset/NULL -> not initialized
# azcopy = NA -> binary not found, or version < 10 (not usable)
# azcopy = path -> usable
get_azcopy_path <- function()
{
    if(exists("azcopy", envir=.AzureStor))
    {
        if(!is.na(.AzureStor$azcopy))
            return(.AzureStor$azcopy)
        else stop("azcopy version 10+ required but not found", call.=FALSE)
    }
    else
    {
        set_azcopy_path()
        Recall()
    }
}


set_azcopy_path <- function(path="azcopy")
{
    path <- Sys.which(path)
    if(is.na(path) || path == "")
    {
        .AzureStor$azcopy <- NA
        return(NULL)
    }

    # both stdout=TRUE and stderr=TRUE could result in jumbled output;
    # assume only one stream will actually have data for a given invocation
    ver <- suppressWarnings(system2(path, "--version", stdout=TRUE, stderr=TRUE))
    if(!grepl("version 1[[:digit:]]", ver, ignore.case=TRUE))
    {
        .AzureStor$azcopy <- NA
        return(NULL)
    }

    .AzureStor$azcopy <- unname(path)
    invisible(NULL)
}


azcopy_upload <- function(container, src, dest, ...)
{
    call_azcopy(...)
}


azcopy_download <- function(container, src, dest, ...)
{
    call_azcopy(...)
}


call_azcopy <- function(...)
{
    azcopy <- get_azcopy_path()
    stop("Not yet implemented")
}
