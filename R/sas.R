get_account_sas <- function(account, ...)
{
    UseMethod("get_account_sas")
}

get_account_sas.az_storage <- function(account, key=account$list_keys()[1], ...)
{
    get_account_sas(account$name, key, ...)
}

get_account_sas.storage_endpoint <- function(account, ...)
{
    if(is.null(account$key))
        stop("Must have access key to generate SAS", call.=FALSE)
    acctname <- sub("\\..*)", "", httr::parse_url(account$url)$hostname)
    get_account_sas(acctname, account$key, ...)
}

get_account_sas.default <- function(account, key, start=NULL, expiry=NULL, services="bqtf", permissions="rl",
                                    resource_types="sco", ip=NULL, protocol=NULL,
                                    auth_api_version=getOption("azure_storage_api_version"), ...)
{
    dates <- make_sas_dates(start, expiry)
    sig_str <- paste(
        account,
        permissions,
        services,
        resource_types,
        dates$start,
        dates$expiry,
        ip,
        protocol,
        auth_api_version,
        "",  # to ensure string always ends with newline
        sep="\n"
    )
    sig <- sign_sha256(sig_str, key)

    parts <- list(
        sv=auth_api_version,
        ss=services,
        srt=resource_types,
        sp=permissions,
        st=dates$start,
        se=dates$expiry,
        sip=ip,
        spr=protocol,
        sig=sig
    )
    parts <- parts[!sapply(parts, is_empty)]
    parts <- sapply(parts, utils::URLencode, reserved=TRUE)
    paste(names(parts), parts, sep="=", collapse="&")
}


get_user_delegation_key <- function(account, ...)
{
    UseMethod("get_user_delegation_key")
}

get_user_delegation_key.az_resource <- function(account, token=account$token, ...)
{
    endp <- account$get_blob_endpoint(key=NULL, token=token)
    get_user_delegation_key(endp, ...)
}

get_user_delegation_key.blob_endpoint <- function(account, key_start, key_expiry)
{
    if(is.null(account$token))
        stop("Must have AAD token to get user delegation key", call.=FALSE)

    account$key <- account$sas <- NULL
    dates <- make_sas_dates(key_start, key_expiry)
    body <- list(KeyInfo=list(
        Start=list(dates$start),
        Expiry=list(dates$expiry)
    ))
    res <- call_storage_endpoint(account, "", options=list(restype="service", comp="userdelegationkey"),
                                 body=render_xml(body), http_verb="POST")

    res <- unlist(res, recursive=FALSE)
    class(res) <- "user_delegation_key"
    res
}


revoke_user_delegation_keys <- function(account)
{
    UseMethod("revoke_user_delegation_key")
}

revoke_user_delegation_keys.az_storage <- function(account)
{
    account$do_operation("revokeUserDelegationKeys", http_verb="POST")
    invisible(NULL)
}


get_user_delegation_sas <- function(account, ...)
{
    UseMethod("get_user_delegation_sas")
}

get_user_delegation_sas.blob_endpoint <- function(account, key, ...)
{
    acctname <- sub("\\..*)", "", httr::parse_url(account$url)$hostname)
    get_account_sas(acctname, key, ...)
}

get_user_delegation_sas.default <- function(account, key, resource, start=NULL, expiry=NULL, permissions="rl",
                                            resource_types="c", ip=NULL, protocol=NULL, snapshot_time=NULL,
                                            auth_api_version=getOption("azure_storage_api_version"), ...)
{
    dates <- make_sas_dates(start, expiry)
    resource <- file.path("blob", account, resource)
    sig_str <- paste(
        permissions,
        dates$start,
        dates$expiry,
        resource,
        key$SignedOid,
        key$SignedTid,
        key$SignedStart,
        key$SignedExpiry,
        key$SignedService,
        key$SignedVersion,
        ip,
        protocol,
        auth_api_version,
        resource_types,
        snapshot_time,
        "",
        "",
        "",
        "",
        "Application/octet-stream",
        "",
        sep="\n"
    )
    cat(sig_str, ".\n")
    print(key$Value)
    #sig <- openssl::base64_encode(openssl::sha256(charToRaw(sig_str), openssl::base64_decode(key$Value)))
    sig <- sign_sha256(sig_str, key$Value)

    parts <- list(
        sv=auth_api_version,
        sr=resource_types,
        st=dates$start,
        se=dates$expiry,
        sp=permissions,
        sip=ip,
        spr=protocol,
        skoid=key$SignedOid,
        sktid=key$SignedTid,
        skt=key$SignedStart,
        ske=key$SignedExpiry,
        sks=key$SignedService,
        skv=key$SignedVersion,
        rsct="Application/octet-stream",
        sig=sig
    )
    parts <- parts[!sapply(parts, is_empty)]
    parts <- sapply(parts, utils::URLencode, reserved=TRUE)
    paste(names(parts), parts, sep="=", collapse="&")
}


make_sas_dates <- function(start=NULL, expiry=NULL)
{
    if(is.null(start))
        start <- Sys.time()
    else if(!inherits(start, "POSIXt"))
        start <- as.POSIXct(start, origin="1970-01-01")

    if(is.null(expiry)) # by default, 8 hours after start
        expiry <- start + 8 * 60 * 60
    else if(!inherits(expiry, "POSIXt"))
        expiry <- as.POSIXct(expiry, origin="1970-01-01")

    if(inherits(start, c("POSIXt", "Date")))
        start <- strftime(start, "%Y-%m-%dT%H:%M:%SZ", tz="UTC")
    if(inherits(expiry, c("POSIXt", "Date")))
        expiry <- strftime(expiry, "%Y-%m-%dT%H:%M:%SZ", tz="UTC")

    list(start=start, expiry=expiry)
}


