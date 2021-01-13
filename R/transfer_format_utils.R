#' Save and load R objects to/from a storage account
#'
#' @param object An R object to save to storage.
#' @param container An Azure storage container object.
#' @param file The name of a file in storage.
#' @param envir For `storage_save_rdata` and `storage_load_rdata`, the environment from which to get objects to save, or in which to restore objects, respectively.
#' @param ... Further arguments passed to `serialize`, `unserialize`, `save` and `load` as appropriate.
#' @details
#' These are equivalents to `saveRDS`, `readRDS`, `save` and `load` for saving and loading R objects to a storage account. With the exception of `storage_save_rdata`, they work via connections and so do not create temporary files. `storage_save_rdata` uses a temporary file so that compression of the resulting image is enabled.
#'
#' @seealso
#' [storage_download], [download_blob], [download_azure_file], [download_adls_file], [save], [load], [saveRDS]
#' @examples
#' \dontrun{
#'
#' bl <- storage_endpoint("https://mystorage.blob.core.windows.net/", key="access_key")
#' cont <- storage_container(bl, "mycontainer")
#'
#' storage_save_rds(iris, cont, "iris.rds")
#' irisnew <- storage_load_rds(iris, "iris.rds")
#' identical(iris, irisnew)  # TRUE
#'
#' storage_save_rdata(iris, mtcars, container=cont, file="dataframes.rdata")
#' storage_load_rdata(cont, "dataframes.rdata")
#'
#' }
#' @rdname storage_save
#' @export
storage_save_rds <- function(object, container, file, ...)
{
    conn <- rawConnection(serialize(object, NULL, ...), open="rb")
    storage_upload(container, conn, file)
}


#' @rdname storage_save
#' @export
storage_load_rds <- function(container, file, ...)
{
    conn <- storage_download(container, file, NULL)
    unserialize(conn, ...)
}


#' @rdname storage_save
#' @export
storage_save_rdata <- function(..., container, file, envir=parent.frame())
{
    # save to a temporary file as saving to a connection disables compression
    tmpsave <- tempfile(fileext=".rdata")
    on.exit(unlink(tmpsave))
    save(..., file=tmpsave, envir=envir)
    storage_upload(container, tmpsave, file)
}


#' @rdname storage_save
#' @export
storage_load_rdata <- function(container, file, envir=parent.frame(), ...)
{
    conn <- storage_download(container, file, NULL)
    load(rawConnection(conn, open="rb"), envir=envir, ...)
}


#' Read and write a data frame to/from a storage account
#'
#' @param object A data frame to write to storage.
#' @param container An Azure storage container object.
#' @param file The name of a file in storage.
#' @param delim For `storage_write_delim` and `storage_read_delim`, the field delimiter. Defaults to `\t` (tab).
#' @param ... Optional arguments passed to the file reading/writing functions. See 'Details'.
#' @details
#' These functions let you read and write data frames to storage. `storage_read_delim` and `write_delim` are for reading and writing arbitrary delimited files. `storage_read_csv` and `write_csv` are for comma-delimited (CSV) files. `storage_read_csv2` and `write_csv2` are for files with the semicolon `;` as delimiter and comma `,` as the decimal point, as used in some European countries.
#'
#' If the readr package is installed, they call down to `read_delim`, `write_delim`, `read_csv2` and `write_csv2`. Otherwise, they use `read_delim` and `write.table`.
#' @seealso
#' [storage_download], [download_blob], [download_azure_file], [download_adls_file],
#' [write.table], [read.csv], [readr::write_delim], [readr::read_delim]
#' @examples
#' \dontrun{
#'
#' bl <- storage_endpoint("https://mystorage.blob.core.windows.net/", key="access_key")
#' cont <- storage_container(bl, "mycontainer")
#'
#' storage_write_csv(iris, cont, "iris.csv")
#' # if readr is not installed
#' irisnew <- storage_read_csv(cont, "iris.csv", stringsAsFactors=TRUE)
#' # if readr is installed
#' irisnew <- storage_read_csv(cont, "iris.csv", col_types="nnnnf")
#'
#' all(mapply(identical, iris, irisnew))  # TRUE
#'
#' }
#' @rdname storage_write
#' @export
storage_write_delim <- function(object, container, file, delim="\t", ...)
{
    func <- if(requireNamespace("readr"))
        storage_write_delim_readr
    else storage_write_delim_base
    func(object, container, file, delim=delim, ...)
}


storage_write_delim_readr <- function(object, container, file, delim="\t", ...)
{
    conn <- rawConnection(raw(0), open="r+b")
    readr::write_delim(object, conn, delim=delim, ...)
    seek(conn, 0)
    storage_upload(container, conn, file)
}


storage_write_delim_base <- function(object, container, file, delim="\t", ...)
{
    conn <- rawConnection(raw(0), open="r+b")
    write.table(object, conn, sep=delim, ...)
    seek(conn, 0)
    storage_upload(container, conn, file)
}


#' @rdname storage_write
#' @export
storage_write_csv <- function(object, container, file, ...)
{
    func <- if(requireNamespace("readr"))
        storage_write_csv_readr
    else storage_write_csv_base
    func(object, container, file, ...)
}


storage_write_csv_readr <- function(object, container, file, ...)
{
    storage_write_delim_readr(object, container, file, delim=",", ...)
}


storage_write_csv_base <- function(object, container, file, ...)
{
    storage_write_delim_base(object, container, file, delim=",", ...)
}


#' @rdname storage_write
#' @export
storage_write_csv2 <- function(object, container, file, ...)
{
    func <- if(requireNamespace("readr"))
        storage_write_csv2_readr
    else storage_write_csv2_base
    func(object, container, file, ...)
}


storage_write_csv2_readr <- function(object, container, file, ...)
{
    conn <- rawConnection(raw(0), open="r+b")
    readr::write_csv2(object, conn, ...)
    seek(conn, 0)
    storage_upload(container, conn, file)
}


storage_write_csv2_base <- function(object, container, file, ...)
{
    storage_write_delim_base(object, container, file, delim=";", dec=",", ...)
}


#' @rdname storage_write
#' @export
storage_read_delim <- function(container, file, delim="\t", ...)
{
    func <- if(requireNamespace("readr"))
        storage_read_delim_readr
    else storage_read_delim_base
    func(container, file, delim=delim, ...)
}


storage_read_delim_readr <- function(container, file, delim="\t", ...)
{
    txt <- storage_download(container, file, NULL)
    readr::read_delim(txt, delim=delim, ...)
}


storage_read_delim_base <- function(container, file, delim="\t", ...)
{
    txt <- storage_download(container, file, NULL)
    read.delim(text=rawToChar(txt), sep=delim, ...)
}


#' @rdname storage_write
#' @export
storage_read_csv <- function(container, file, ...)
{
    func <- if(requireNamespace("readr"))
        storage_read_csv_readr
    else storage_read_csv_base
    func(container, file, ...)
}


storage_read_csv_readr <- function(container, file, ...)
{
    storage_read_delim_readr(container, file, delim=",", ...)
}


storage_read_csv_base <- function(container, file, ...)
{
    storage_read_delim_base(container, file, delim=",", ...)
}


#' @rdname storage_write
#' @export
storage_read_csv2 <- function(container, file, ...)
{
    func <- if(requireNamespace("readr"))
        storage_read_csv2_readr
    else storage_read_csv2_base
    func(container, file, ...)
}


storage_read_csv2_readr <- function(container, file, ...)
{
    txt <- storage_download(container, file, NULL)
    readr::read_csv2(txt, ...)
}


storage_read_csv2_base <- function(container, file, ...)
{
    storage_read_delim_base(container, file, delim=";", dec=",", ...)
}

