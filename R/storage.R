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
        keys <- named_list(private$res_op("listKeys", http_verb="POST")$keys, "keyName")
        sapply(keys, `[[`, "value")
    },

    get_account_sas=function(start=NULL, expiry=NULL, services="bqtf", permissions="r",
                             resource_types="sco", ip=NULL, protocol=NULL, key=NULL)
    {
        dates <- private$set_sas_dates(start, expiry)
        parms <- list(keyToSign=key,
                      signedExpiry=dates$expiry, signedIp=ip, signedPermission=permissions, signedProtocol=protocol,
                      signedResourceTypes=resource_types, signedServices=services, signedStart=dates$start)

        self$do_operation("POST", "listAccountSas", body=parms, encode="json")$accountSasToken
    },

    get_service_sas=function(start=NULL, expiry=NULL, path=NULL, service=NULL, permissions="r",
                             ip=NULL, protocol=NULL, key=NULL)
    {
        dates <- private$set_sas_dates(start, expiry)
        parms <- list(keyToSign=key, canonicalizedResource=path,
                      signedExpiry=dates$expiry, signedIp=ip, signedPermission=permissions, signedProtocol=protocol,
                      signedResource=service, signedStart=dates$start)

        self$do_operation("POST", "listServiceSas", body=parms, encode="json")$serviceSasToken
    },

    get_blob_client=function(key=self$list_keys()[1])
    {
        az_blob_client$new(self$properties$primaryEndpoints$blob, key=key)
    },

    get_file_client=function(key=self$list_keys()[1])
    {
        az_file_client$new(self$properties$primaryEndpoints$file, key=key)
    },

    get_blob_endpoint=function(key=self$list_keys()[1])
    {
        az_blob_endpoint(self$properties$primaryEndpoints$blob, key=key)
    },

    get_file_endpoint=function(key=self$list_keys()[1])
    {
        az_file_endpoint(self$properties$primaryEndpoints$file, key=key)
    }
),

private=list(

    set_sas_dates=function(start, expiry)
    {
        if(is.null(start))
            start <- as.POSIXct(Sys.Date())
        else if(is.numeric(start))
            start <- as.POSIXct(start, origin="1970-01-01")
        if(inherits(start, c("POSIXt", "Date")))
            start <- strftime(start, "%Y-%m-%dT%H:%M:%SZ", tz="UTC")

        if(is.null(expiry)) # by default, 1 year after start
        {
            expiry <- as.POSIXlt(start, tz="UTC")
            expiry$year <- expiry$year + 1
            expiry <- as.POSIXct(expiry, tz="UTC")
        }
        else if(is.numeric(expiry))
            expiry <- as.POSIXct(expiry, origin="1970-01-01")
        if(inherits(expiry, c("POSIXt", "Date")))
            expiry <- strftime(expiry, "%Y-%m-%dT%H:%M:%SZ", tz="UTC")

        list(start=start, expiry=expiry)
    }
))
