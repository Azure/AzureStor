do_container_op <- function(container, path="", options=list(), headers=list(), http_verb="GET", ...)
{
    endp <- container$endpoint

    # don't add trailing / if no within-container path supplied: ADLS will complain
    path <- if(nchar(path) > 0)
        sub("//", "/", paste0(container$name, "/", path))
    else container$name

    invisible(do_storage_call(endp$url, path, options=options, headers=headers,
                              key=endp$key, token=endp$token, sas=endp$sas, api_version=endp$api_version,
                              http_verb=http_verb, ...))
}


do_storage_call <- function(endpoint_url, path, options=list(), headers=list(), body=NULL, ...,
                            key=NULL, token=NULL, sas=NULL,
                            api_version=getOption("azure_storage_api_version"),
                            http_verb=c("GET", "DELETE", "PUT", "POST", "HEAD", "PATCH"),
                            http_status_handler=c("stop", "warn", "message", "pass"),
                            progress=NULL)
{
    verb <- match.arg(http_verb)
    url <- httr::parse_url(endpoint_url)
    url$path <- URLencode(path)
    if(!is_empty(options))
        url$query <- options[order(names(options))] # must be sorted for access key signing

    # use key if provided, otherwise AAD token if provided, otherwise sas if provided, otherwise anonymous access
    if(!is.null(key))
        headers <- sign_request(key, verb, url, headers, api_version)
    else if(!is.null(token))
        headers <- add_token(token, headers, api_version)
    else if(!is.null(sas))
        url <- add_sas(sas, url)

    headers <- do.call(httr::add_headers, headers)
    retries <- as.numeric(getOption("azure_storage_retries"))
    for(r in seq_len(retries + 1))
    {
        # retry on curl errors, not on httr errors
        response <- tryCatch(httr::VERB(verb, url, headers, body=body, progress, ...), error=function(e) e)
        if(!retry_transfer(response))
            break
    }
    if(inherits(response, "error"))
        stop(response)

    handler <- match.arg(http_status_handler)
    if(handler != "pass")
    {
        handler <- get(paste0(handler, "_for_status"), getNamespace("httr"))
        handler(response, storage_error_message(response))

        # if file was written to disk, printing content(*) will read it back into memory!
        if(inherits(response$content, "path"))
            return(NULL)

        # silence message about missing encoding
        cont <- suppressMessages(httr::content(response, simplifyVector=TRUE))
        if(is_empty(cont))
            NULL
        else if(inherits(cont, "xml_node"))
            xml_to_list(cont)
        else cont
    }
    else response
}


add_token <- function(token, headers, api)
{
    if(is.null(headers$`x-ms-version`))
        headers$`x-ms-version` <- api

    if(inherits(token, "AzureToken") || inherits(token, "Token2.0"))
    {
        # if token has expired, renew it
        if(!token$validate())
        {
            message("Access token has expired or is no longer valid; refreshing")
            token$refresh()
        }
        type <- token$credentials$token_type
        token <- token$credentials$access_token
    }
    else
    {
        if(!is.character(token) || length(token) != 1)
            stop("Token must be a string, or an object of class AzureRMR::AzureToken", call.=FALSE)
        type <- "Bearer"
    }
    c(headers, Authorization=paste(type, token))
}


add_sas <- function(sas, url)
{
    full_url <- httr::build_url(url)
    paste0(full_url, if(is.null(url$query)) "?" else "&", sas)
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

    c(Host=url$host, Authorization=sig, headers)
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


storage_error_message <- function(response, for_httr=TRUE)
{
    cont <- suppressMessages(httr::content(response))
    msg <- if(inherits(cont, "xml_node"))
    {
        cont <- xml_to_list(cont)
        paste0(unlist(cont), collapse="\n")
    }
    else if(is.character(cont))
        cont
    else if(is.list(cont) && is.character(cont$message))
        cont$message
    else if(is.list(cont) && is.list(cont$error) && is.character(cont$error$message))
        cont$error$message
    else ""

    if(for_httr)
        paste0("complete Storage Services operation. Message:\n", sub("\\.$", "", msg))
    else msg
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
    # handle cases where type != uri string
    if(type == "adls")
        type <- "dfs"
    else if(type == "web")
        type <- "z26\\.web"

    # endpoint URL must be of the form {scheme}://{acctname}.{type}.{etc}
    type <- sprintf("^https?://[a-z0-9]+\\.%s\\.", type)

    is_url(url) && grepl(type, url)
}


generate_endpoint_container <- function(url, key, token, sas, api_version)
{
    stor_path <- parse_storage_url(url)
    endpoint <- storage_endpoint(stor_path[1], key, token, sas, api_version)
    name <- stor_path[2]
    list(endpoint=endpoint, name=name)
}


xml_to_list <- function(x)
{
    # work around breaking change in xml2 1.2
    if(packageVersion("xml2") < package_version("1.2"))
        xml2::as_list(x)
    else (xml2::as_list(x))[[1]]
}


# check whether to retry a failed file transfer
# retry on curl error (not any other kind of error)
# don't retry on host not found
retry_transfer <- function(res)
{
    inherits(res, "error") &&
        grepl("curl", deparse(res$call[[1]]), fixed=TRUE) &&
        !grepl("Could not resolve host", res$message, fixed=TRUE)
}


