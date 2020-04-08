#' Signs a request to the storage REST endpoint with a shared key
#' @param endpoint An endpoint object.
#' @param ... Further arguments to pass to individual methods.
#' @details
#' This is a generic method to allow for variations in how the different storage services handle key authorisation. The default method works with blob, file and ADLSgen2 storage.
#' @return
#' A named list of request headers. One of these should be the `Authorization` header containing the request signature.
#' @export
sign_request <- function(endpoint, ...)
{
    UseMethod("sign_request")
}


sign_request.default <- function(endpoint, verb, url, headers, api, ...)
{
    acct_name <- sub("\\..+$", "", url$host)
    resource <- paste0("/", acct_name, "/", url$path) # don't use file.path because it strips trailing / on Windows
    # sanity check
    resource <- gsub("//", "/", resource)

    if(is.null(headers$date) || is.null(headers$Date))
        headers$date <- httr::http_date(Sys.time())
    if(is.null(headers$`x-ms-version`))
        headers$`x-ms-version` <- api

    sig <- make_signature(endpoint$key, verb, acct_name, resource, url$query, headers)

    modifyList(headers, list(Host=url$host, Authorization=sig))
}


make_signature <- function(key, verb, acct_name, resource, options, headers)
{
    names(headers) <- tolower(names(headers))

    ms_headers <- headers[grepl("^x-ms", names(headers))]
    ms_headers <- ms_headers[order(names(ms_headers))]
    ms_headers <- paste(names(ms_headers), ms_headers, sep=":", collapse="\n")
    options <- options[!sapply(options, is.null)]
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

    paste0("SharedKey ", acct_name, ":", sign_sha256(sig, key))
}


