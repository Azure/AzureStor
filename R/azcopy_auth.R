# multiple code paths for authenticating
# key: set AZCOPY_ACCOUNT_NAME and AZCOPY_ACCOUNT_KEY envvars
# sas: append sas to URL (handled separately)
# token:
# - client creds: run azcopy login, pass client secret in AZCOPY_SPA_CLIENT_SECRET envvar
# - auth code: set AZCOPY_OAUTH_TOKEN_INFO envvar
# managed: run azcopy login --identity
azcopy_auth <- function(endpoint)
{
    env <- Sys.getenv()
    obj <- list()

    if(!is.null(endpoint$key))
    {
        env["AZCOPY_ACCOUNT_NAME"] <- sub("\\..*$", "", httr::parse_url(endpoint$url)$hostname)
        env["AZCOPY_ACCOUNT_KEY"] <- unname(endpoint$key)
    }
    else if(!is.null(endpoint$token))
    {
        token <- endpoint$token
        if(inherits(token, "AzureTokenClientCredentials"))
        {
            env["AZCOPY_SPA_CLIENT_SECRET"] <- token$client$client_secret
            args <- c("login", "--service-principal", "--tenant-id", token$tenant,
                      "--application-id", token$client$client_id)
            call_azcopy_internal(args, env)
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
        {
            args <- c("login", "--identity")
            call_azcopy_internal(args, env)
        }
        else stop(
            "Only client_credentials, authorization_code, device_code and managed_identity flows supported for azcopy",
            call.=FALSE
        )
    }
    obj$env <- env
    obj
}


azcopy_add_sas <- function(endpoint, url)
{
    if(!is.null(endpoint$sas))
        url <- paste0(url, "?", sub("^\\?", "", endpoint$sas))
    url
}