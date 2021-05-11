#' Generate shared access signatures
#'
#' The simplest way for a user to access files and data in a storage account is to give them the account's access key. This gives them full control of the account, and so may be a security risk. An alternative is to provide the user with a _shared access signature_ (SAS), which limits access to specific resources and only for a set length of time. There are three kinds of SAS: account, service and user delegation.
#'
#' Listed here are S3 generics and methods to obtain a SAS for accessing storage; in addition, the [`az_storage`] resource class has R6 methods for `get_account_sas`, `get_service_sas`, `get_user_delegation_key` and `revoke_user_delegation_keys` which simply call the corresponding S3 method.
#'
#' Note that you don't need to worry about these methods if you have been _given_ a SAS, and only want to use it to access a storage account.
#'
#' @param account An object representing a storage account. Depending on the generic, this can be one of the following: an Azure resource object (of class `az_storage`); a client storage endpoint (of class `storage_endpoint`); a _blob_ storage endpoint (of class `blob_endpoint`); or a string with the name of the account.
#' @param key For `get_account_sas`, the _account_ key, which controls full access to the storage account. For `get_user_delegation_sas`, a _user delegation_ key, as obtained from `get_user_delegation_key`.
#' @param token For `get_user_delegation_key`, an AAD token from which to obtain user details. The token must have `https://storage.azure.com` as its audience.
#' @param resource For `get_user_delegation_sas` and `get_service_sas`, the resource for which the SAS is valid. For a user delegation SAS, this can be either the name of a blob container or an individual blob; if the latter, it should include the container as well (`containername/blobname`). A service SAS is similar but can also be used with file shares and files.
#' @param start,expiry The start and end dates for the account or user delegation SAS. These should be `Date` or `POSIXct` values, or strings coercible to such. If not supplied, the default is to generate start and expiry values for a period of 8 hours, starting from 15 minutes before the current time.
#' @param key_start,key_expiry For `get_user_delegation_key`, the start and end dates for the user delegation key.
#' @param services For `get_account_sas`, the storage service(s) for which the SAS is valid. Defaults to `bqtf`, meaning blob (including ADLS2), queue, table and file storage.
#' @param permissions The permissions that the SAS grants. The default value of `rl` (read and list) essentially means read-only access.
#' @param resource_types For an account or user delegation SAS, the resource types for which the SAS is valid. For `get_account_sas` the default is `sco` meaning service, container and object. For `get_user_delegation_sas` the default is `c` meaning container-level access (including blobs within the container).
#' @param ip The IP address(es) or IP address range(s) for which the SAS is valid. The default is not to restrict access by IP.
#' @param protocol The protocol required to use the SAS. Possible values are `https` meaning HTTPS-only, or `https,http` meaning HTTP is also allowed. Note that the storage account itself may require HTTPS, regardless of what the SAS allows.
#' @param snapshot_time For a user delegation or service SAS, the blob snapshot for which the SAS is valid. Only required if `resource_type[s]="bs"`.
#' @param auth_api_version The storage API version to use for authenticating.
#' @param ... Arguments passed to lower-level functions.
#'
#' @details
#' An **account SAS** is secured with the storage account key. An account SAS delegates access to resources in one or more of the storage services. All of the operations available via a user delegation SAS are also available via an account SAS. You can also delegate access to read, write, and delete operations on blob containers, tables, queues, and file shares. To obtain an account SAS, call `get_account_sas`.
#'
#' A **service SAS** is like an account SAS, but allows finer-grained control of access. You can create a service SAS that allows access only to specific blobs in a container, or files in a file share. To obtain a service SAS, call `get_service_sas`.
#'
#' A **user delegation SAS** is a SAS secured with Azure AD credentials. It's recommended that you use Azure AD credentials when possible as a security best practice, rather than using the account key, which can be more easily compromised. When your application design requires shared access signatures, use Azure AD credentials to create a user delegation SAS for superior security.
#'
#' Every SAS is signed with a key. To create a user delegation SAS, you must first request a **user delegation key**, which is then used to sign the SAS. The user delegation key is analogous to the account key used to sign a service SAS or an account SAS, except that it relies on your Azure AD credentials. To request the user delegation key, call `get_user_delegation_key`. With the user delegation key, you can then create the SAS with `get_user_delegation_sas`.
#'
#' To invalidate all user delegation keys, as well as the SAS's generated with them, call `revoke_user_delegation_keys`.
#'
#' See the examples and Microsoft Docs pages below for how to specify arguments like the services, permissions, and resource types. Also, while not explicitly mentioned in the documentation, ADLSgen2 storage can use any SAS that is valid for blob storage.
#' @seealso
#' [blob_endpoint], [file_endpoint],
#' [Date], [POSIXt]
#'
#' [Azure Storage Provider API reference](https://docs.microsoft.com/en-us/rest/api/storagerp/),
#' [Azure Storage Services API reference](https://docs.microsoft.com/en-us/rest/api/storageservices/)
#'
#' [Create an account SAS](https://docs.microsoft.com/en-us/rest/api/storageservices/create-account-sas),
#' [Create a user delegation SAS](https://docs.microsoft.com/en-us/rest/api/storageservices/create-user-delegation-sas),
#' [Create a service SAS](https://docs.microsoft.com/en-us/rest/api/storageservices/create-service-sas)
#'
#' @examples
#' # account SAS valid for 7 days
#' get_account_sas("mystorage", "access_key", start=Sys.Date(), expiry=Sys.Date() + 7)
#'
#' # SAS with read/write/create/delete permissions
#' get_account_sas("mystorage", "access_key", permissions="rwcd")
#'
#' # SAS limited to blob (+ADLS2) and file storage
#' get_account_sas("mystorage", "access_key", services="bf")
#'
#' # SAS for file storage, allows access to files only (not shares)
#' get_account_sas("mystorage", "access_key", services="f", resource_types="o")
#'
#' # getting the key from an endpoint object
#' endp <- storage_endpoint("https://mystorage.blob.core.windows.net", key="access_key")
#' get_account_sas(endp, permissions="rwcd")
#'
#' # service SAS for a container
#' get_service_sas(endp, "containername")
#'
#' # read/write service SAS for a blob
#' get_service_sas(endp, "containername/blobname", permissions="rw")
#'
#' \dontrun{
#'
#' # user delegation key valid for 24 hours
#' token <- AzureRMR::get_azure_token("https://storage.azure.com", "mytenant", "app_id")
#' endp <- storage_endpoint("https://mystorage.blob.core.windows.net", token=token)
#' userkey <- get_user_delegation_key(endp, start=Sys.Date(), expiry=Sys.Date() + 1)
#'
#' # user delegation SAS for a container
#' get_user_delegation_sas(endp, userkey, resource="mycontainer")
#'
#' # user delegation SAS for a specific file, read/write/create/delete access
#' # (order of permissions is important!)
#' get_user_delegation_sas(endp, userkey, resource="mycontainer/myfile",
#'                         resource_types="b", permissions="rcwd")
#'
#' }
#' @aliases sas shared-access-signature shared_access_signature
#' @rdname sas
#' @export
get_account_sas <- function(account, ...)
{
    UseMethod("get_account_sas")
}

#' @rdname sas
#' @export
get_account_sas.az_storage <- function(account, key=account$list_keys()[1], ...)
{
    get_account_sas(account$name, key, ...)
}

#' @rdname sas
#' @export
get_account_sas.storage_endpoint <- function(account, key=account$key, ...)
{
    if(is.null(key))
        stop("Must have access key to generate SAS", call.=FALSE)
    acctname <- sub("\\..*", "", httr::parse_url(account$url)$hostname)
    get_account_sas(acctname, key=key, ...)
}

#' @rdname sas
#' @export
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
    parts <- sapply(parts, url_encode, reserved=TRUE)
    paste(names(parts), parts, sep="=", collapse="&")
}


#' @rdname sas
#' @export
get_user_delegation_key <- function(account, ...)
{
    UseMethod("get_user_delegation_key")
}

#' @rdname sas
#' @export
get_user_delegation_key.az_resource <- function(account, token=account$token, ...)
{
    endp <- account$get_blob_endpoint(key=NULL, token=token)
    get_user_delegation_key(endp, token=endp$token, ...)
}

#' @rdname sas
#' @export
get_user_delegation_key.blob_endpoint <- function(account, token=account$token, key_start=NULL, key_expiry=NULL, ...)
{
    if(is.null(token))
        stop("Must have AAD token to get user delegation key", call.=FALSE)

    account$key <- account$sas <- NULL
    account$token <- token
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


#' @rdname sas
#' @export
revoke_user_delegation_keys <- function(account)
{
    UseMethod("revoke_user_delegation_keys")
}

#' @rdname sas
#' @export
revoke_user_delegation_keys.az_storage <- function(account)
{
    account$do_operation("revokeUserDelegationKeys", http_verb="POST")
    invisible(NULL)
}


#' @rdname sas
#' @export
get_user_delegation_sas <- function(account, ...)
{
    UseMethod("get_user_delegation_sas")
}

#' @rdname sas
#' @export
get_user_delegation_sas.az_storage <- function(account, key, ...)
{
    get_user_delegation_sas(account$name, key, ...)
}

#' @rdname sas
#' @export
get_user_delegation_sas.blob_endpoint <- function(account, key, ...)
{
    acctname <- sub("\\..*", "", httr::parse_url(account$url)$hostname)
    get_user_delegation_sas(acctname, key, ...)
}

#' @rdname sas
#' @export
get_user_delegation_sas.default <- function(account, key, resource, start=NULL, expiry=NULL, permissions="rl",
                                            resource_types="c", ip=NULL, protocol=NULL, snapshot_time=NULL,
                                            auth_api_version=getOption("azure_storage_api_version"), ...)
{
    stopifnot(inherits(key, "user_delegation_key"))
    dates <- make_sas_dates(start, expiry)
    resource <- file.path("/blob", account, resource)
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
        "",
        "",
        "",
        ip,
        protocol,
        auth_api_version,
        resource_types,
        snapshot_time,
        "",
        "",
        "",
        "",
        "",
        sep="\n"
    )
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
        sig=sig
    )
    parts <- parts[!sapply(parts, is_empty)]
    parts <- sapply(parts, url_encode, reserved=TRUE)
    paste(names(parts), parts, sep="=", collapse="&")
}


#' @rdname sas
#' @export
get_service_sas <- function(account, ...)
{
    UseMethod("get_service_sas")
}

#' @rdname sas
#' @export
get_service_sas.az_storage <- function(account, resource, service=c("blob", "file"), key=account$list_keys()[1],
                                       ...)
{
    service <- match.arg(service)
    endp <- if(service == "blob")
        account$get_blob_endpoint(key=key)
    else account$get_file_endpoint(key=key)
    get_service_sas(endp, resource=resource, key=key, ...)
}

#' @rdname sas
#' @export
get_service_sas.storage_endpoint <- function(account, resource, key=account$key, ...)
{
    if(is.null(key))
        stop("Must have access key to generate SAS", call.=FALSE)

    service <- sub("_endpoint$", "", class(account)[1])
    acctname <- sub("\\..*", "", httr::parse_url(account$url)$hostname)
    get_service_sas(acctname, resource=resource, key=key, service=service, ...)
}


#' @param service For a service SAS, the storage service for which the SAS is valid: either "blob" or "file". Currently AzureStor does not support creating a service SAS for queue or table storage.
#' @param resource_type For a service SAS, the type of resource for which the SAS is valid. For blob storage, the default value is "b" meaning a single blob. For file storage, the default value is "f" meaning a single file. Other possible values include "bs" (a blob snapshot), "c" (a blob container), or "s" (a file share).
#' @param policy For a service SAS, optionally the name of a stored access policy to correlate the SAS with. Revoking the policy will also invalidate the SAS.
#' @rdname sas
#' @export
get_service_sas.default <- function(account, resource, key, service, start=NULL, expiry=NULL, permissions="rl",
                                    resource_type=NULL, ip=NULL, protocol=NULL, policy=NULL, snapshot_time=NULL,
                                    auth_api_version=getOption("azure_storage_api_version"), ...)
{
    if(!(service %in% c("blob", "file")))
        stop("Generating a service SAS currently only supported for blob and file storage", call.=FALSE)

    dates <- make_sas_dates(start, expiry)
    if(is.null(resource_type))
        resource_type <- if(service == "blob") "b" else "f"
    resource <- file.path("", service, account, resource)  # canonicalized resource starts with /

    sig_str <- paste(
        permissions,
        dates$start,
        dates$expiry,
        resource,
        policy,
        ip,
        protocol,
        auth_api_version,
        resource_type,
        snapshot_time,
        "",  # rscc, rscd, rsce, rscl, rsct not yet implemented
        "",
        "",
        "",
        "",
        sep="\n"
    )
    sig <- sign_sha256(sig_str, key)

    parts <- list(
        sv=auth_api_version,
        sr=resource_type,
        sp=permissions,
        st=dates$start,
        se=dates$expiry,
        sip=ip,
        spr=protocol,
        si=policy,
        # sdd=directory_depth,
        sig=sig
    )
    parts <- parts[!sapply(parts, is_empty)]
    parts <- sapply(parts, url_encode, reserved=TRUE)
    paste(names(parts), parts, sep="=", collapse="&")
}


make_sas_dates <- function(start=NULL, expiry=NULL)
{
    if(is.null(start))
        start <- Sys.time() - 15*60
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


