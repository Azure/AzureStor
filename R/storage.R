#' @export
az_storage <- R6::R6Class("az_storage", inherit=AzureRMR::az_resource,

public=list(

    initialize=function(token, subscription, resource_group, name, location,
        kind="Storage", sku=list(name="Standard_LRS", tier="Standard"), ...)
    {
        if(missing(location) && missing(kind) && missing(sku))
            super$initialize(token, subscription, resource_group, type="Microsoft.Storage/storageAccounts", name=name)
        else super$initialize(token, subscription, resource_group, type="Microsoft.Storage/storageAccounts", name=name,
                              location=location, kind=kind, sku=sku, ...)
    },

    list_keys=function()
    {
        named_list(private$res_op("listKeys", http_verb="POST")$keys, "keyName")
    },

    get_account_sas=function(start=NULL, expiry=NULL, services="bqtf", permissions="r",
                             resource_types="sco", ip=NULL, protocol=NULL, key=NULL)
    {
        if(is.null(start))
            start <- as.POSIXct(Sys.Date())
        else if(is.numeric(start))
            start <- as.POSIXct(start)

        if(inherits(start, c("POSIXt", "Date")))
            start <- strftime(start, "%Y-%m-%dT%H:%M:%SZ")

        if(is.null(expiry))
        {
            expiry <- as.POSIXlt(start)
            expiry$year <- expiry$year + 1
            expiry <- as.POSIXct(expiry)
        }
        else if(is.numeric(expiry))
            expiry <- as.POSIXct(expiry, origin="1970-01-01")

        if(inherits(expiry, c("POSIXt", "Date")))
            expiry <- strftime(expiry, "%Y-%m-%dT%H:%M:%SZ")

        parms <- list(keyToSign=key,
                      signedExpiry=expiry, signedIp=ip, signedPermission=permissions, signedProtocol=protocol,
                      signedResourceTypes=resource_types, signedServices=services, signedStart=start)

        private$res_op("listAccountSas", body=parms, encode="json", http_verb="POST")$accountSasToken
    },

    get_service_sas=function(start=NULL, expiry=NULL, path=NULL, service=NULL, permissions="r",
                             ip=NULL, protocol=NULL, key=NULL)
    {
        if(is.null(start))
            start <- as.POSIXct(Sys.Date())
        else if(is.numeric(start))
            start <- as.POSIXct(start)

        if(inherits(start, c("POSIXt", "Date")))
            start <- strftime(start, "%Y-%m-%dT%H:%M:%SZ")

        if(is.null(expiry))
        {
            expiry <- as.POSIXlt(start)
            expiry$year <- expiry$year + 1
            expiry <- as.POSIXct(expiry)
        }
        else if(is.numeric(expiry))
            expiry <- as.POSIXct(expiry, origin="1970-01-01")

        if(inherits(expiry, c("POSIXt", "Date")))
            expiry <- strftime(expiry, "%Y-%m-%dT%H:%M:%SZ")

        parms <- list(keyToSign=key, canonicalizedResource=path,
                      signedExpiry=expiry, signedIp=ip, signedPermission=permissions, signedProtocol=protocol,
                      signedResource=service, signedStart=start)

        private$res_op("listServiceSas", body=parms, encode="json", http_verb="POST")$serviceSasToken
    }
))
