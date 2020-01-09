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

    ver <- suppressWarnings(processx::run(path, "--version"))
    if(!grepl("version 1[[:digit:]]", ver$stdout, ignore.case=TRUE))
    {
        .AzureStor$azcopy <- NA
        return(NULL)
    }

    .AzureStor$azcopy <- unname(path)
    message("Using azcopy binary ", path)
    invisible(NULL)
}


'azcopy copy "https://hongstor2.blob.core.windows.net/cont1/script1.R?sv=2015-04-05&ss=bqtf&srt=sco&sp=rwld&st=2020-01-09T18%3A24%3A25.0000000Z&se=2020-01-10T02%3A24%3A25.0000000Z&sig=Ila7p6Pi9mA5WN5AfiXEW5HQYbbgisorumKg8PuBDn4%3D" misc/copy'