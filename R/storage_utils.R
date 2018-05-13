do_storage_call <- function(endpoint, path, options=list(), headers=list(), body=NULL, ...,
                            key=NULL, sas=NULL,
                            api_version=getOption("azure_storage_api_version"),
                            http_verb=c("GET", "DELETE", "PUT", "POST", "HEAD", "PATCH"),
                            http_status_handler=c("stop", "warn", "message", "pass"))
{
    verb <- match.arg(http_verb)

    # use shared access signature if provided, otherwise key if provided, otherwise anonymous access
    if(!is.null(sas))
    {
        url <- paste0(endpoint, path, sep="/") # don't use file.path because it strips trailing / on Windows
        url <- paste0(url, "?", sas)
        url <- httr::parse_url(url)
        headers <- httr::add_headers(.headers=unlist(headers))
    }
    else
    {
        url <- httr::parse_url(endpoint)
        url$path <- path
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
        handler(response)
        # silence message about missing encoding
        cont <- suppressMessages(httr::content(response))

        if(is_empty(cont))
            NULL
        else xml2::as_list(cont)
    }
    else response
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
    if(!inherits(url, "url"))
        url <- parse_url(url)
    url$port <- url$path <- url$params <- url$fragment <- url$query <- url$username <- url$password <- NULL
    build_url(url)
}

