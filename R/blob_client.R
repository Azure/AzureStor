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
            az_blob_container$new(self$endpoint, cont$Name[[1]], self$key, self$sas, self$api_version)
        })
        named_list(lst)
    },

    get_container=function(container)
    {
        container <- sub("/$", "", container)
        az_blob_container$new(self$endpoint, container, self$key, self$sas, self$api_version)
    },

    create_container=function(container, public_access=NULL)
    {
        if(substr(container, nchar(container), nchar(container)) != "/")
            container <- paste0(container, "/")

        headers <- if(!is_empty(public_access))
            list("x-ms-blob-public-access"=public_access)
        else list()
        res <- do_storage_call(self$endpoint, container, options=list(restype="container"), headers=headers,
                               key=self$key, sas=self$sas, api_version=self$api_version,
                               http_verb="PUT")
        az_blob_container$new(self$endpoint, container, self$key, self$sas, self$api_version)
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

    initialize=function(endpoint, name, key, sas, api_version)
    {
        # allow passing full URL to constructor
        if(missing(endpoint))
        {
            url <- parse_url(container)
            self$endpoint <- get_hostroot(container)
            self$name <- url$path
        }
        else
        {
            self$endpoint <- endpoint
            self$name <- name
        }

        self$key <- key
        self$sas <- sas
        self$api_version <- api_version
        NULL
    },

    delete=function(...) { },

    list_blobs=function(...) { },

    upload_blob=function(...) { },
    download_blob=function(...) { },
    delete_blob=function(...) { }
))


download_azure_blob <- function(src, dest, key=NULL, sas=NULL)
{
    if(is.null(key) && is.null(sas))
        return(curl::curl_download(src, dest))

    src <- httr::parse_url(src)
    az_blob_client$new(get_hostroot(src), key=key, sas=sas)$
        get_container(dirname(src$path))$
        download_blob(basename(src$path), dest)
}


upload_azure_blob <- function(src, dest, key=NULL, sas=NULL)
{
    dest <- httr::parse_url(dest)
    az_blob_client$new(get_hostroot(dest), key=key, sas=sas)$
        get_container(dirname(dest$path))$
        upload_blob(src, basename(dest$path))
}

