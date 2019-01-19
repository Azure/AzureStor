#' Parallelise multiple file transfers in the background
#'
#' @param max_concurrent_transfers The maximum number of concurrent file transfers to support, which translates into the number of background R processes to create. Each concurrent transfer requires a separate R process, so limit this is you are low on memory.
#' @param restart For `init_pool`, whether to terminate an already running pool first.
#' @param ... Other arguments passed on to `parallel::makeCluster`.
#'
#' @details
#' AzureStor can parallelise file transfers by utilizing a pool of R processes in the background. This often leads to significant speedups when transferring multiple small files. The pool is created by calling `init_pool`, or automatically the first time that a multiple file transfer is begun. It remains persistent for the session or until terminated by `delete_pool`.
#'
#' If `init_pool` is called and the current pool is smaller than `max_concurrent_transfers`, it is resized.
#'
#' @seealso
#' [multiupload_blob], [multidownload_blob], [parallel::makeCluster]
#' @rdname pool
#' @export
init_pool <- function(max_concurrent_transfers=10, restart=FALSE, ...)
{
    if(restart)
        delete_pool()

    if(!exists("pool", envir=.AzureStor) || length(.AzureStor$pool) < max_concurrent_transfers)
    {
        delete_pool()
        message("Creating background pool")
        .AzureStor$pool <- parallel::makeCluster(max_concurrent_transfers)
        parallel::clusterEvalQ(.AzureStor$pool, loadNamespace("AzureStor"))
    }
    else
    {
        # restore original state, set working directory to master working directory
        parallel::clusterCall(.AzureStor$pool, function(wd)
        {
            setwd(wd)
            rm(list=ls(all.names=TRUE), envir=.GlobalEnv)
        }, wd=getwd())
    }

    invisible(NULL)
}


#' @rdname pool
#' @export
delete_pool <- function()
{
    if(!exists("pool", envir=.AzureStor))
        return()
    
    message("Deleting background pool")
    parallel::stopCluster(.AzureStor$pool)
    rm(pool, envir=.AzureStor)
}
