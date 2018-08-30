# documentation is separate from implementation because roxygen still doesn't know how to handle R6

#' Create Azure storage account
#'
#' Method for the [AzureRMR::az_resource_group] class.
#'
#' @rdname create_storage_account
#' @name create_storage_account
#' @usage
#' create_storage_account(name, location, kind="Storage", sku=list(name="Standard_LRS", tier="Standard"), ...)
#'
#' @param name The name of the storage account.
#' @param location The location/region in which to create the account.
#' @param kind The type of account, either `"Storage"` or `"BlobStorage"`.
#' @param sku The SKU. This is a named list specifying various configuration options for the account.
#' @param ... Other named arguments to pass to the [az_storage] initialization function.
#'
#' @details
#' This method deploys a new storage account resource, with parameters given by the arguments. A storage account can host multiple types of storage:
#' - blob storage
#' - file storage 
#' - table storage
#' - queue storage
#'
#' Accounts created with `kind = "BlobStorage"` can only host blob and table storage, while those with `kind = "Storage"` can host all four.
#'
#' @return
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
#' @aliases list_storage_accounts
#'
#' @usage
#' get_storage_account(name)
#' list_storage_accounts()
#'
#' @param name For `get_storage_account()`, the name of the storage account.
#'
#' @details
#' The `AzureRMR::az_resource_group` class has both `get_storage_account()` and `list_storage_accounts()` methods, while the `AzureRMR::az_subscription` class only has the latter.
#'
#' @return
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
#'
#' @usage
#' delete_storage_account(name, confirm=TRUE, wait=FALSE)
#'
#' @param name The name of the storage account.
#' @param confirm Whether to ask for confirmation before deleting.
#' @param wait Whether to wait until the deletion is complete.
#'
#' @return
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
    invisible(NULL)

    ## extending AzureRMR classes

    AzureRMR::az_resource_group$set("public", "create_storage_account", overwrite=TRUE,
                                    function(name, location, ...)
    {
        az_storage$new(self$token, self$subscription, self$name, name, location=location, ...)
    })


    AzureRMR::az_resource_group$set("public", "get_storage_account", overwrite=TRUE,
                                    function(name)
    {
        az_storage$new(self$token, self$subscription, self$name, name)
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
