az_blob_client <- R6::R6Class("az_blob_client",

public=list(

    initialize=function(...){},

    list_containers=function(...) { },
    create_container=function(...) { },
    get_container=function(...) { },
    delete_container=function(...) { },
))


az_blob_container <- R6::R6Class("az_blob_container",

public=list(

    initialize=function(...) { },

    delete=function(...) { },

    list_blobs=function(...) { },

    upload_blob=function(...) { },
    download_blob=function(...) { },
    delete_blob=function(...) { }
))
