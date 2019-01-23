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
    if(path == "")
    {
        .AzureStor$azcopy <- NA
        return(NULL)
    }

    ver <- system2(path, "--version", stdout=TRUE)
    if(!grepl("version 1[[:digit:]]", ver, ignore.case=TRUE))
    {
        .AzureStor$azcopy <- NA
        return(NULL)
    }

    .AzureStor$azcopy <- unname(path)
    invisible(NULL)
}


azcopy_upload <- function(...)
{
    call_azcopy(...)
}


azcopy_download <- function(...)
{
    call_azcopy(...)
}


call_azcopy <- function(...)
{
    azcopy <- get_azcopy_path()
    stop("Not yet implemented")
}
