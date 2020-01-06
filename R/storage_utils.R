#' Carry out operations on a storage account container or endpoint
#'
#' @param container,endpoint For `do_container_op`, a storage container object (inheriting from `storage_container`). For `call_storage_endpoint`, a  storage endpoint object (inheriting from `storage_endpoint`).
#' @param operation The container operation to perform, which will form part of the URL path.
#' @param path The path component of the endpoint call.
#' @param options A named list giving the query parameters for the operation.
#' @param headers A named list giving any additional HTTP headers to send to the host. Note that AzureStor will handle authentication details, so you don't have to specify these here.
#' @param body The request body for a `PUT/POST/PATCH` call.
#' @param ... Any additional arguments to pass to `httr::VERB`.
#' @param http_verb The HTTP verb as a string, one of `GET`, `DELETE`, `PUT`, `POST`, `HEAD` or `PATCH`.
#' @param http_status_handler The R handler for the HTTP status code of the response. `"stop"`, `"warn"` or `"message"` will call the corresponding handlers in httr, while `"pass"` ignores the status code. The latter is primarily useful for debugging purposes.
#' @param progress Used by the file transfer functions, to display a progress bar.
#' @param return_headers Whether to return the (parsed) response headers, rather than the body. Ignored if `http_status_handler="pass"`.
#' @details
#' These functions form the low-level interface between R and the storage API. `do_container_op` constructs a path from the operation and the container name, and passes it and the other arguments to `call_storage_endpoint`.
#' @return
#' Based on the `http_status_handler` and `return_headers` arguments. If `http_status_handler` is `"pass"`, the entire response is returned without modification.
#'
#' If `http_status_handler` is one of `"stop"`, `"warn"` or `"message"`, the status code of the response is checked, and if an error is not thrown, the parsed headers or body of the response is returned. An exception is if the response was written to disk, as part of a file download; in this case, the return value is NULL.
#'
#' @seealso
#' [blob_endpoint], [file_endpoint], [adls_endpoint]
#'
#' [blob_container], [file_share], [adls_filesystem]
#'
#' [httr::GET], [httr::PUT], [httr::POST], [httr::PATCH], [httr::HEAD], [httr::DELETE]
#' @examples
#' \dontrun{
#'
#' # get the metadata for a blob
#' bl_endp <- blob_endpoint("storage_acct_url", key="key")
#' cont <- storage_container(bl_endp, "containername")
#' do_container_op(cont, "filename.txt", options=list(comp="metadata"), http_verb="HEAD")
#'
#' }
#' @rdname storage_call
#' @export
do_container_op <- function(container, operation="", options=list(), headers=list(), http_verb="GET", ...)
{
    # don't add trailing / if no within-container path supplied: ADLS will complain
    operation <- if(nchar(operation) > 0)
        sub("//", "/", paste0(container$name, "/", operation))
    else container$name

    call_storage_endpoint(container$endpoint, operation, options=options, headers=headers, http_verb=http_verb, ...)
}


#' @rdname storage_call
#' @export
call_storage_endpoint <- function(endpoint, path, options=list(), headers=list(), body=NULL, ...,
                                  http_verb=c("GET", "DELETE", "PUT", "POST", "HEAD", "PATCH"),
                                  http_status_handler=c("stop", "warn", "message", "pass"),
                                  progress=NULL, return_headers=(http_verb == "HEAD"))
{
    http_verb <- match.arg(http_verb)
    url <- httr::parse_url(endpoint$url)
    url$path <- URLencode(path)
    if(!is_empty(options))
        url$query <- options[order(names(options))] # must be sorted for access key signing

    # use key if provided, otherwise AAD token if provided, otherwise sas if provided, otherwise anonymous access
    if(!is.null(endpoint$key))
        headers <- sign_request(endpoint$key, http_verb, url, headers, endpoint$api_version)
    else if(!is.null(endpoint$token))
        headers$`x-ms-version` <- endpoint$api_version
    else if(!is.null(endpoint$sas))
        url <- add_sas(endpoint$sas, url)

    retries <- as.numeric(getOption("azure_storage_retries"))
    r <- 0
    repeat
    {
        r <- r + 1
        if(!is.null(endpoint$token))
            headers$Authorization <- paste("Bearer", validate_token(endpoint$token))

        # retry on curl errors, not on httr errors
        response <- tryCatch(httr::VERB(http_verb, url, do.call(httr::add_headers, headers), body=body, progress, ...),
                             error=function(e) e)
        if(retry_transfer(response) && r <= retries)
            message("Connection error, retrying (", r, " of ", retries, ")")
        else break
    }
    if(inherits(response, "error"))
        stop(response)

    process_storage_response(response, match.arg(http_status_handler), return_headers)
}


validate_token <- function(token)
{
    if(inherits(token, "AzureToken") || inherits(token, "Token2.0"))
    {
        # if token has expired, renew it
        if(!token$validate())
        {
            message("Access token has expired or is no longer valid; refreshing")
            token$refresh()
        }
        return(token$credentials$access_token)
    }
    else
    {
        if(!is.character(token) || length(token) != 1)
            stop("Token must be a string, or an object of class AzureRMR::AzureToken", call.=FALSE)
        return(token)
    }
}


add_sas <- function(sas, url)
{
    full_url <- httr::build_url(url)
    paste0(full_url, if(is.null(url$query)) "?" else "&", sub("^\\?", "", sas))
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


process_storage_response <- function(response, handler, return_headers)
{
    if(handler == "pass")
        return(response)

    handler <- get(paste0(handler, "_for_status"), getNamespace("httr"))
    handler(response, storage_error_message(response))

    if(return_headers)
        return(unclass(httr::headers(response)))

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


as_datetime <- function(x, format="%a, %d %b %Y %H:%M:%S", tz="GMT")
{
    as.POSIXct(x, format=format, tz=tz)
}


delete_confirmed <- function(confirm, name, type)
{
    if(!interactive() || !confirm)
        return(TRUE)

    msg <- sprintf("Are you sure you really want to delete the %s '%s'?", type, name)

    ok <- if(getRversion() < numeric_version("3.5.0"))
    {
        msg <- paste(msg, "(yes/No/cancel) ")
        yn <- readline(msg)
        if(nchar(yn) == 0)
            FALSE
        else tolower(substr(yn, 1, 1)) == "y"
    }
    else utils::askYesNo(msg, FALSE)
    isTRUE(ok)
}

