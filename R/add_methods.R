# documentation is separate from implementation because roxygen still doesn't know how to handle R6

#' Create Azure storage account
#'
#' Method for the [AzureRMR::az_resource_group] class.
#'
#' @rdname create_storage_account
#' @name create_storage_account
#' @aliases create_storage_account
#' @section Usage:
#' ```
#' create_storage_account(name, location, kind = "StorageV2", replication = "Standard_LRS",
#'                        access_tier = "hot"), https_only = TRUE, 
#'                        hierarchical_namespace_enabled = FALSE, properties = list(), ...)
#' ```
#' @section Arguments:
#' - `name`: The name of the storage account.
#' - `location`: The location/region in which to create the account.
#' - `kind`: The type of account, either `"StorageV2"` (the default), `"FileStorage"` or `"BlobStorage"`.
#' - `replication`: The replication strategy for the account. The default is locally-redundant storage (LRS).
#' - `access_tier`: The access tier, either `"hot"` or `"cool"`, for blobs.
#' - `https_only`: Whether a HTTPS connection is required to access the storage.
#' - `hierarchical_namespace_enabled`: Whether to enable hierarchical namespaces, which are a feature of Azure Data Lake Storage Gen 2. If TRUE, this allows you to create ADLS Gen2 containers in the account. Note that ADLS Gen2 is currently (as of November 2018) in limited-access public preview.
#' - `properties`: A list of other properties for the storage account.
#' - ... Other named arguments to pass to the [az_storage] initialization function.
#'
#' @section Details:
#' This method deploys a new storage account resource, with parameters given by the arguments. A storage account can host multiple types of storage:
#' - blob storage
#' - file storage 
#' - table storage
#' - queue storage
#'
#' Accounts created with `kind = "BlobStorage"` can only host blob storage, while those with `kind = "FileStorage"` can only host file storage. Accounts with `kind = "StorageV2"` can host all types of storage. Currently, AzureStor provides an R interface only to blob and file storage.
#'
#' @section Value:
#' An object of class `az_storage` representing the created storage account.
#'
#' @seealso
#' [get_storage_account], [delete_storage_account], [az_storage],
#' [Azure Storage Provider API reference](https://docs.microsoft.com/en-us/rest/api/storagerp/)
NULL


#' Get existing Azure storage account(s)
#'
#' Methods for the [AzureRMR::az_resource_group] and [AzureRMR::az_subscription] classes.
#'
#' @rdname get_storage_account
#' @name get_storage_account
#' @aliases get_storage_account list_storage_accounts
#'
#' @section Usage:
#' ```
#' get_storage_account(name)
#' list_storage_accounts()
#' ```
#' @section Arguments:
#' - `name`: For `get_storage_account()`, the name of the storage account.
#'
#' @section Details:
#' The `AzureRMR::az_resource_group` class has both `get_storage_account()` and `list_storage_accounts()` methods, while the `AzureRMR::az_subscription` class only has the latter.
#'
#' @section Value:
#' For `get_storage_account()`, an object of class `az_storage` representing the storage account.
#'
#' For `list_storage_accounts()`, a list of such objects.
#'
#' @seealso
#' [create_storage_account], [delete_storage_account], [az_storage],
#' [Azure Storage Provider API reference](https://docs.microsoft.com/en-us/rest/api/storagerp/)
NULL


#' Delete an Azure storage account
#'
#' Method for the [AzureRMR::az_resource_group] class.
#'
#' @rdname delete_storage_account
#' @name delete_storage_account
#' @aliases delete_storage_account
#'
#' @section Usage:
#' ```
#' delete_storage_account(name, confirm=TRUE, wait=FALSE)
#' ```
#' @section Arguments:
#' - `name`: The name of the storage account.
#' - `confirm`: Whether to ask for confirmation before deleting.
#' - `wait`: Whether to wait until the deletion is complete.
#'
#' @section Value:
#' NULL on successful deletion.
#'
#' @seealso
#' [create_storage_account], [get_storage_account], [az_storage],
#' [Azure Storage Provider API reference](https://docs.microsoft.com/en-us/rest/api/storagerp/)
NULL


# all methods extending classes in external package must go in .onLoad
.onLoad <- function(libname, pkgname)
{
    api <- "2018-03-28"
    options(azure_storage_api_version=api)

    ## extending AzureRMR classes

    AzureRMR::az_resource_group$set("public", "create_storage_account", overwrite=TRUE,
    function(name, location=self$location,
             kind="StorageV2", replication="Standard_LRS",
             access_tier="hot", https_only=TRUE, hierarchical_namespace_enabled=FALSE,
             properties=list(), ...)
    {
        properties <- modifyList(properties,
            list(accessTier=access_tier, supportsHttpsTrafficOnly=https_only))
        if(isTRUE(hierarchical_namespace_enabled))
            properties <- modifyList(properties, list(isHnsEnabled=TRUE))

        az_storage$new(self$token, self$subscription, self$name,
                       type="Microsoft.Storage/storageAccounts", name=name, location=location,
                       kind=kind, sku=list(name=replication),
                       properties=properties, ...)
    })


    AzureRMR::az_resource_group$set("public", "get_storage_account", overwrite=TRUE,
    function(name)
    {
        az_storage$new(self$token, self$subscription, self$name,
                       type="Microsoft.Storage/storageAccounts", name=name)
    })


    AzureRMR::az_resource_group$set("public", "delete_storage_account", overwrite=TRUE,
    function(name, confirm=TRUE, wait=FALSE)
    {
        self$get_storage_account(name)$delete(confirm=confirm, wait=wait)
    })


    AzureRMR::az_resource_group$set("public", "list_storage_accounts", overwrite=TRUE,
    function(name)
    {
        provider <- "Microsoft.Storage"
        path <- "storageAccounts"
        api_version <- az_subscription$
            new(self$token, self$subscription)$
            get_provider_api_version(provider, path)

        op <- file.path("resourceGroups", self$name, "providers", provider, path)

        cont <- call_azure_rm(self$token, self$subscription, op, api_version=api_version)
        lst <- lapply(cont$value,
            function(parms) az_storage$new(self$token, self$subscription, deployed_properties=parms))

        # keep going until paging is complete
        while(!is_empty(cont$nextLink))
        {
            cont <- call_azure_url(self$token, cont$nextLink)
            lst <- lapply(cont$value,
                function(parms) az_storage$new(self$token, self$subscription, deployed_properties=parms))
        }
        named_list(lst)
    })


    AzureRMR::az_subscription$set("public", "list_storage_accounts", overwrite=TRUE,
    function(name)
    {
        provider <- "Microsoft.Storage"
        path <- "storageAccounts"
        api_version <- self$get_provider_api_version(provider, path)

        op <- file.path("providers", provider, path)

        cont <- call_azure_rm(self$token, self$id, op, api_version=api_version)
        lst <- lapply(cont$value,
            function(parms) az_storage$new(self$token, self$id, deployed_properties=parms))

        # keep going until paging is complete
        while(!is_empty(cont$nextLink))
        {
            cont <- call_azure_url(self$token, cont$nextLink)
            lst <- lapply(cont$value,
                function(parms) az_storage$new(self$token, self$id, deployed_properties=parms))
        }
        named_list(lst)
    })
}
