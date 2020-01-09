# multiple code paths for authenticating, return an object with necessary info
# key: set AZCOPY_ACCOUNT_NAME and AZCOPY_ACCOUNT_KEY envvars
# sas: append sas to URL
# token:
# - client creds: run azcopy login, pass client secret in AZCOPY_SPA_CLIENT_SECRET envvar
# - auth code: set AZCOPY_OAUTH_TOKEN_INFO envvar
azcopy_auth <- function(container)
{
    env <- Sys.getenv()
    endp <- container$endpoint
    obj <- list()

    if(!is.null(endp$key))
    {
        env["AZCOPY_ACCOUNT_NAME"] <- sub("\\..*$", "", httr::parse_url(endp$url)$hostname)
        env["AZCOPY_ACCOUNT_KEY"] <- unname(endp$key)
        obj$env <- env
    }
    else if(!is.null(endp$token))
    {
        token <- endp$token
        if(inherits(token, "AzureTokenClientCredentials"))
        {
            env["AZCOPY_SPA_CLIENT_SECRET"] <- token$client$client_secret
            obj$tenant <- token$tenant
            obj$app <- token$client$client_id
            obj$sp_login <- TRUE
        }
        else if(inherits(token, c("AzureTokenAuthCode", "AzureTokenDeviceCode")))
        {
            creds <- list(
                access_token=token$credentials$access_token,
                refresh_token=token$credentials$refresh_token,
                token_type=token$credentials$token_type,
                resource=token$credentials$resource,
                scope=token$credentials$scope,
                not_before=token$credentials$not_before,
                expires_on=token$credentials$expires_on,
                expires_in=token$credentials$expires_in,
                `_tenant`=token$tenant,
                `_ad_endpoint`=token$aad_host
            )
            env["AZCOPY_OAUTH_TOKEN_INFO"] <- jsonlite::toJSON(creds[!sapply(creds, is.null)], auto_unbox=TRUE)
        }
        else if(inherits(token, "AzureTokenManaged"))
            obj$managed_login <- TRUE
        else stop(
            "Only client_credentials, authorization_code, device_code and managed_identity flows supported for azcopy",
            call.=FALSE
        )
        obj$env <- env
    }
    else if(!is.null(endp$sas))
        obj$sas <- endp$sas
    obj
}


azcopy_login <- function(auth)
{
    if(!is.null(auth$sp_login))
    {
        args <- c("login", "--service-principal", "--tenant-id", auth$tenant, "--application-id", auth$app)
        call_azcopy(args, env=auth$env)
    }
    else if(!is.null(auth$managed_login))
        call_azcopy("login", "--identity")
    else invisible(NULL)
}


azcopy_add_sas <- function(auth, url)
{
    if(!is.null(auth$sas))
        url <- paste0(url, "?", sub("^\\?", "", auth$sas))
    url
}
