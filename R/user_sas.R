get_user_key <- function(endpoint, start=NULL, expiry=NULL)
{
    # must use AAD auth
    endpoint$key <- endpoint$sas <- NULL
    if(is.null(endpoint$token))
        stop("Endpoint must be authenticated with an AAD token", call.=FALSE)

    dates <- set_sas_dates(start, expiry)
    body <- list(KeyInfo=list(Start=list(dates$start), Expiry=list(dates$expiry)))
    con <- rawConnection(raw(0), "w")
    on.exit(close(con))
    xml2::write_xml(xml2::as_xml_document(body), con)

    res <- call_storage_endpoint(endpoint, "", options=list(restype="service", comp="userdelegationkey"),
                                 body=rawConnectionValue(con), http_verb="POST")
    # saner format
    structure(as.list(unlist(res)), class="user_delegation_key")
}


get_user_sas <- function(resource, start=NULL, expiry=NULL, permissions="r",
                         resource_types="b", ip=NULL, protocol=NULL, key)
{
    dates <- set_sas_dates(start, expiry)
    sas_opts <- list(
        sv=key$SignedVersion,
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
        skv=key$SignedVersion
    )

    sas_opts$sig <- sign_user_sas(key, sas_opts, resource)

    dummy_url <- httr::parse_url("")
    dummy_url$query <- sas_opts
    grep("\\?.+$", httr::build_url(dummy_url), value=TRUE)
}


set_sas_dates <- function(start, expiry)
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
