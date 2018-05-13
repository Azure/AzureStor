az_blob_client <- R6::R6Class("az_blob_client",

public=list(
    endpoint=NULL,
    key=NULL,
    sas=NULL,
    api_version=NULL,

    initialize=function(endpoint, key=NULL, sas=NULL, api_version=getOption("azure_storage_api_version"))
    {
        self$endpoint <- endpoint
        self$key <- key
        self$sas <- sas
        self$api_version <- api_version
        NULL
    },

    list_containers=function()
    {
        lst <- do_storage_call(self$endpoint, "/", options=list(comp="list"),
                        key=self$key, sas=self$sas, api_version=self$api_version)

        lst <- lapply(lst$Containers, function(cont)
        {
            az_blob_container$new(cont$Name[[1]], self$endpoint, self$key, self$sas, self$api_version)
        })
        named_list(lst)
    },

    get_container=function(container)
    {
        az_blob_container$new(container, self$endpoint, self$key, self$sas, self$api_version)
    },

    create_container=function(container, public_access=NULL)
    {
        az_blob_container$new(container, self$endpoint, self$key, self$sas, self$api_version,
                              public_access=public_access, create=TRUE)
    },

    delete_container=function(container, confirm=TRUE)
    {
        self$get_container(container)$delete(confirm=confirm)
    }
))


az_blob_container <- R6::R6Class("az_blob_container",

public=list(
    endpoint=NULL,
    name=NULL,
    key=NULL,
    sas=NULL,
    api_version=NULL,

    initialize=function(name, endpoint, key, sas, api_version, public_access=NULL, create=FALSE)
    {
        # allow passing full URL to constructor
        if(missing(endpoint))
        {
            url <- parse_url(name)
            if(url$path == "")
                stop("Must supply container name", call.=FALSE)
            self$endpoint <- get_hostroot(name)
            self$name <- sub("/$", "", url$path) # strip trailing /
        }
        else
        {
            self$endpoint <- endpoint
            self$name <- name
        }

        self$key <- key
        self$sas <- sas
        self$api_version <- api_version

        if(create)
        {
            headers <- if(!is_empty(public_access))
                list("x-ms-blob-public-access"=public_access)
            else list()

            private$container_op(options=list(restype="container"), headers=headers, http_verb="PUT")
        }
        NULL
    },

    delete=function(confirm=TRUE)
    {
        if(confirm && interactive())
        {
            path <- paste0(self$endpoint, self$name, "/")
            yn <- readline(paste0("Are you sure you really want to delete blob container '", path, "'? (y/N) "))
            if(tolower(substr(yn, 1, 1)) != "y")
                return(invisible(NULL))
        }

        private$container_op(options=list(restype="container"), http_verb="DELETE")
        invisible(NULL)
    },

    list_blobs=function()
    {
        lst <- private$container_op(options=list(comp="list", restype="container"))
        lst <- sapply(lst$Blobs, function(b) b$Name[[1]])
        unname(lst)
    },

    upload_blob=function(...) { },
    download_blob=function(...) { },
    delete_blob=function(...) { }
),

private=list(

    container_op=function(blob="", options=list(), headers=list(), http_verb="GET", ...)
    {
        path <- paste0(self$name, "/", blob)
        do_storage_call(self$endpoint, path, options=options, headers=headers,
                        key=self$key, sas=self$sas, api_version=self$api_version,
                        http_verb=http_verb, ...)
    }
))


#' @export
download_azure_blob <- function(src, dest, key=NULL, sas=NULL, api_version=getOption("azure_storage_api_version"))
{
    if(is.null(key) && is.null(sas))
        return(curl::curl_download(src, dest))

    az_blob_container$new(src, key=key, sas=sas, api_version=api_version)$download_blob(basename(src), dest)
}


#' @export
upload_azure_blob <- function(src, dest, key=NULL, sas=NULL, api_version=getOption("azure_storage_api_version"))
{
    az_blob_container$new(dest, key=key, sas=sas, api_version=api_version)$upload_blob(src, basename(dest))
}

