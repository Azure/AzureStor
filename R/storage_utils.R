do_container_op <- function(container, path="", options=list(), headers=list(), http_verb="GET", ...)
{
    endp <- container$endpoint
    path <- sub("//", "/", paste0(container$name, "/", path))
    invisible(do_storage_call(endp$url, path, options=options, headers=headers,
                              key=endp$key, sas=endp$sas, api_version=endp$api_version,
                              http_verb=http_verb, ...))
}


do_storage_call <- function(endpoint_url, path, options=list(), headers=list(), body=NULL, ...,
                            key=NULL, sas=NULL,
                            api_version=getOption("azure_storage_api_version"),
                            http_verb=c("GET", "DELETE", "PUT", "POST", "HEAD", "PATCH"),
                            http_status_handler=c("stop", "warn", "message", "pass"))
{
    verb <- match.arg(http_verb)

    # use shared access signature if provided, otherwise key if provided, otherwise anonymous access
    if(!is.null(sas))
    {
        url <- paste0(endpoint_url, path, "/") # don't use file.path because it strips trailing / on Windows
        url <- paste0(url, "?", sas)
        url <- httr::parse_url(url)
        headers <- httr::add_headers(.headers=unlist(headers))
    }
    else
    {
        url <- httr::parse_url(endpoint_url)
        url$path <- path
        if(!is_empty(options))
            url$query <- options[order(names(options))]

        headers <- if(!is.null(key))
            sign_request(key, verb, url, headers, api_version)
        else httr::add_headers(.headers=unlist(headers))
    }

    verb <- get(verb, getNamespace("httr"))
    response <- verb(url, headers, body=body, ...)

    handler <- match.arg(http_status_handler)
    if(handler != "pass")
    {
        handler <- get(paste0(handler, "_for_status"), getNamespace("httr"))
        handler(response, paste0("complete Storage Services operation. Message:\n",
                                 sub("\\.$", "", storage_error_message(response))))

        # if file was written to disk, printing content(*) will read it back into memory!
        if(inherits(response$content, "path"))
            return(NULL)

        # silence message about missing encoding
        cont <- suppressMessages(httr::content(response))
        if(is_empty(cont))
            NULL
        else if(inherits(cont, "xml_node"))
            xml2::as_list(cont)
        else cont
    }
    else response
}


storage_error_message <- function(response)
{
    cont <- suppressMessages(httr::content(response))
    if(inherits(cont, "xml_node"))
    {
        cont <- xml2::as_list(cont)
        paste0(unlist(cont), collapse="\n")
    }
    else NULL
}


sign_request <- function(key, verb, url, headers, api)
{
    acct_name <- sub("\\..+$", "", url$host)
    resource <- paste0("/", acct_name, "/", url$path) # don't use file.path because it strips trailing / on Windows
    # sanity check
    resource <- gsub("//", "/", resource)

    if(is.null(headers$date) || is.null(headers$Date))
        headers$date <- httr::http_date(Sys.time())
    if(is.null(headers$`x-ms-version`))
        headers$`x-ms-version` <- api

    sig <- make_signature(key, verb, acct_name, resource, url$query, headers)

    httr::add_headers(Host=url$host, Authorization=sig, .headers=unlist(headers))
}


make_signature <- function(key, verb, acct_name, resource, options, headers)
{
    names(headers) <- tolower(names(headers))

    ms_headers <- headers[grepl("^x-ms", names(headers))]
    ms_headers <- ms_headers[order(names(ms_headers))]
    ms_headers <- paste(names(ms_headers), ms_headers, sep=":", collapse="\n")
    options <- paste(names(options), options, sep=":", collapse="\n")

    sig <- paste(verb,
                 as.character(headers[["content-encoding"]]),
                 as.character(headers[["content-language"]]),
                 as.character(headers[["content-length"]]),
                 as.character(headers[["content-md5"]]),
                 as.character(headers[["content-type"]]),
                 as.character(headers[["date"]]),
                 as.character(headers[["if-modified-since"]]),
                 as.character(headers[["if-match"]]),
                 as.character(headers[["if-none-match"]]),
                 as.character(headers[["if-unmodified-since"]]),
                 as.character(headers[["range"]]),
                 ms_headers,
                 resource,
                 options, sep="\n")
    sig <- sub("\n$", "", sig) # undocumented, found thanks to Tsuyoshi Matsuzaki's blog post

    hash <- openssl::sha256(charToRaw(sig), openssl::base64_decode(key))
    paste0("SharedKey ", acct_name, ":", openssl::base64_encode(hash))
}


# keep only the scheme and host parts of a URL
get_hostroot <- function(url)
{
    parse_storage_url(url)[1]
}


parse_storage_url <- function(url)
{
    url <- httr::parse_url(url)
    endpoint <- paste0(url$scheme, "://", url$host, "/")
    store <- sub("/.*$", "", url$path)
    path <- sub("^[^/]+/", "", url$path)
    c(endpoint, store, path)
}


is_endpoint_url <- function(url, type)
{
    type <- sprintf("://[a-z0-9]+\\.%s\\.", type)
    grepl(type, url)
}


