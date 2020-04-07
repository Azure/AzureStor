make_account_sas <- function(account, dates, permissions, resource_types, services, ip, protocol, key,
                             auth_api_version=getOption("azure_storage_api_version"))
{
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
    sig <- openssl::sha256(charToRaw(sig_str), openssl::base64_decode(key))

    parts <- list(
        sv=auth_api_version,
        ss=services,
        srt=resource_types,
        sp=permissions,
        st=dates$start,
        se=dates$expiry,
        sip=ip,
        spr=protocol,
        sig=openssl::base64_encode(sig)
    )
    parts <- parts[!sapply(parts, is_empty)]
    parts <- sapply(parts, utils::URLencode, reserved=TRUE)
    paste(names(parts), parts, sep="=", collapse="&")
}
