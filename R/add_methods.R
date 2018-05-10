## extend resource group methods
AzureRMR::az_resource_group$set("public", "create_storage", function(name, location, ...)
{
    az_storage$new(self$token, self$subscription, self$name, name, location=location, ...)
})


AzureRMR::az_resource_group$set("public", "get_storage", function(name)
{
    az_storage$new(self$token, self$subscription, self$name, name)
})


AzureRMR::az_resource_group$set("public", "delete_storage", function(name, confirm=TRUE, wait=FALSE)
{
    self$get_storage(name)$delete(confirm=confirm, wait=wait)
})
