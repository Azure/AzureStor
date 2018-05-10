az_file_client <- R6::R6Class("az_file_client",

public=list(

    initialize=function(...) { },

    list_shares=function(...) { },
    create_share=function(...) { },
    get_share=function(...) { },
    delete_share=function(...) { }
))


az_file_share <- R6::R6Class("az_file_share",

public=list(

    initialize=function(...) { },

    delete=function(...) { },

    list_files=function(...) { },

    upload_file=function(...) { },
    download_file=function(...) { },
    delete_file=function(...) { }
))

