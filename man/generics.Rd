% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/client_generics.R
\name{storage_container}
\alias{storage_container}
\alias{storage_generics}
\alias{storage_container.blob_endpoint}
\alias{storage_container.file_endpoint}
\alias{storage_container.adls_endpoint}
\alias{storage_container.character}
\alias{create_storage_container}
\alias{create_storage_container.blob_endpoint}
\alias{create_storage_container.file_endpoint}
\alias{create_storage_container.adls_endpoint}
\alias{create_storage_container.storage_container}
\alias{create_storage_container.character}
\alias{delete_storage_container}
\alias{delete_storage_container.blob_endpoint}
\alias{delete_storage_container.file_endpoint}
\alias{delete_storage_container.adls_endpoint}
\alias{delete_storage_container.storage_container}
\alias{delete_storage_container.character}
\alias{list_storage_containers}
\alias{list_storage_containers.blob_endpoint}
\alias{list_storage_containers.file_endpoint}
\alias{list_storage_containers.adls_endpoint}
\alias{list_storage_containers.character}
\alias{list_storage_files}
\alias{list_storage_files.blob_container}
\alias{list_storage_files.file_share}
\alias{list_storage_files.adls_filesystem}
\alias{create_storage_dir}
\alias{create_storage_dir.blob_container}
\alias{create_storage_dir.file_share}
\alias{create_storage_dir.adls_filesystem}
\alias{delete_storage_dir}
\alias{delete_storage_dir.blob_container}
\alias{delete_storage_dir.file_share}
\alias{delete_storage_dir.adls_filesystem}
\alias{delete_storage_file}
\alias{delete_storage_file.blob_container}
\alias{delete_storage_file.file_share}
\alias{delete_storage_file.adls_filesystem}
\alias{storage_file_exists}
\alias{storage_file_exists.blob_container}
\alias{storage_file_exists.file_share}
\alias{storage_file_exists.adls_filesystem}
\alias{storage_dir_exists}
\alias{storage_dir_exists.blob_container}
\alias{storage_dir_exists.file_share}
\alias{storage_dir_exists.adls_filesystem}
\alias{create_storage_snapshot}
\alias{create_storage_snapshot.blob_container}
\alias{list_storage_snapshots}
\alias{list_storage_snapshots.blob_container}
\alias{delete_storage_snapshot}
\alias{delete_storage_snapshot.blob_container}
\alias{list_storage_versions}
\alias{list_storage_versions.blob_container}
\alias{delete_storage_version}
\alias{delete_storage_version.blob_container}
\title{Storage client generics}
\usage{
storage_container(endpoint, ...)

\method{storage_container}{blob_endpoint}(endpoint, name, ...)

\method{storage_container}{file_endpoint}(endpoint, name, ...)

\method{storage_container}{adls_endpoint}(endpoint, name, ...)

\method{storage_container}{character}(endpoint, key = NULL, token = NULL, sas = NULL, ...)

create_storage_container(endpoint, ...)

\method{create_storage_container}{blob_endpoint}(endpoint, name, ...)

\method{create_storage_container}{file_endpoint}(endpoint, name, ...)

\method{create_storage_container}{adls_endpoint}(endpoint, name, ...)

\method{create_storage_container}{storage_container}(endpoint, ...)

\method{create_storage_container}{character}(endpoint, key = NULL, token = NULL, sas = NULL, ...)

delete_storage_container(endpoint, ...)

\method{delete_storage_container}{blob_endpoint}(endpoint, name, ...)

\method{delete_storage_container}{file_endpoint}(endpoint, name, ...)

\method{delete_storage_container}{adls_endpoint}(endpoint, name, ...)

\method{delete_storage_container}{storage_container}(endpoint, ...)

\method{delete_storage_container}{character}(endpoint, key = NULL,
  token = NULL, sas = NULL, confirm = TRUE, ...)

list_storage_containers(endpoint, ...)

\method{list_storage_containers}{blob_endpoint}(endpoint, ...)

\method{list_storage_containers}{file_endpoint}(endpoint, ...)

\method{list_storage_containers}{adls_endpoint}(endpoint, ...)

\method{list_storage_containers}{character}(endpoint, key = NULL, token = NULL, sas = NULL, ...)

list_storage_files(container, ...)

\method{list_storage_files}{blob_container}(container, ...)

\method{list_storage_files}{file_share}(container, ...)

\method{list_storage_files}{adls_filesystem}(container, ...)

create_storage_dir(container, ...)

\method{create_storage_dir}{blob_container}(container, dir, ...)

\method{create_storage_dir}{file_share}(container, dir, ...)

\method{create_storage_dir}{adls_filesystem}(container, dir, ...)

delete_storage_dir(container, ...)

\method{delete_storage_dir}{blob_container}(container, dir, ...)

\method{delete_storage_dir}{file_share}(container, dir, ...)

\method{delete_storage_dir}{adls_filesystem}(container, dir, confirm = TRUE, ...)

delete_storage_file(container, ...)

\method{delete_storage_file}{blob_container}(container, file, ...)

\method{delete_storage_file}{file_share}(container, file, ...)

\method{delete_storage_file}{adls_filesystem}(container, file, confirm = TRUE, ...)

storage_file_exists(container, file, ...)

\method{storage_file_exists}{blob_container}(container, file, ...)

\method{storage_file_exists}{file_share}(container, file, ...)

\method{storage_file_exists}{adls_filesystem}(container, file, ...)

storage_dir_exists(container, dir, ...)

\method{storage_dir_exists}{blob_container}(container, dir, ...)

\method{storage_dir_exists}{file_share}(container, dir, ...)

\method{storage_dir_exists}{adls_filesystem}(container, dir, ...)

create_storage_snapshot(container, file, ...)

\method{create_storage_snapshot}{blob_container}(container, file, ...)

list_storage_snapshots(container, ...)

\method{list_storage_snapshots}{blob_container}(container, ...)

delete_storage_snapshot(container, file, ...)

\method{delete_storage_snapshot}{blob_container}(container, file, ...)

list_storage_versions(container, ...)

\method{list_storage_versions}{blob_container}(container, ...)

delete_storage_version(container, file, ...)

\method{delete_storage_version}{blob_container}(container, file, ...)
}
\arguments{
\item{endpoint}{A storage endpoint object, or for the character methods, a string giving the full URL to the container.}

\item{...}{Further arguments to pass to lower-level functions.}

\item{name}{For the storage container management methods, a container name.}

\item{key, token, sas}{For the character methods, authentication credentials for the container: either an access key, an Azure Active Directory (AAD) token, or a SAS. If multiple arguments are supplied, a key takes priority over a token, which takes priority over a SAS.}

\item{confirm}{For the deletion methods, whether to ask for confirmation first.}

\item{container}{A storage container object.}

\item{file, dir}{For the storage object management methods, a file or directory name.}
}
\description{
Storage client generics
}
\details{
These methods provide a framework for all storage management tasks supported by AzureStor. They dispatch to the appropriate functions for each type of storage.

Storage container management methods:
\itemize{
\item \code{storage_container} dispatches to \code{blob_container}, \code{file_share} or \code{adls_filesystem}
\item \code{create_storage_container} dispatches to \code{create_blob_container}, \code{create_file_share} or \code{create_adls_filesystem}
\item \code{delete_storage_container} dispatches to \code{delete_blob_container}, \code{delete_file_share} or \code{delete_adls_filesystem}
\item \code{list_storage_containers} dispatches to \code{list_blob_containers}, \code{list_file_shares} or \code{list_adls_filesystems}
}

Storage object management methods:
\itemize{
\item \code{list_storage_files} dispatches to \code{list_blobs}, \code{list_azure_files} or \code{list_adls_files}
\item \code{create_storage_dir} dispatches to \code{create_blob_dir}, \code{create_azure_dir} or \code{create_adls_dir}
\item \code{delete_storage_dir} dispatches to \code{delete_blob_dir}, \code{delete_azure_dir} or \code{delete_adls_dir}
\item \code{delete_storage_file} dispatches to \code{delete_blob}, \code{delete_azure_file} or \code{delete_adls_file}
\item \code{storage_file_exists} dispatches to \code{blob_exists}, \code{azure_file_exists} or \code{adls_file_exists}
\item \code{storage_dir_exists} dispatches to \code{blob_dir_exists}, \code{azure_dir_exists} or \code{adls_dir_exists}
\item \code{create_storage_snapshot} dispatches to \code{create_blob_snapshot}
\item \code{list_storage_snapshots} dispatches to \code{list_blob_snapshots}
\item \code{delete_storage_snapshot} dispatches to \code{delete_blob_snapshot}
\item \code{list_storage_versions} dispatches to \code{list_blob_versions}
\item \code{delete_storage_version} dispatches to \code{delete_blob_version}
}
}
\examples{
\dontrun{

# storage endpoints for the one account
bl <- storage_endpoint("https://mystorage.blob.core.windows.net/", key="access_key")
fl <- storage_endpoint("https://mystorage.file.core.windows.net/", key="access_key")

list_storage_containers(bl)
list_storage_containers(fl)

# creating containers
cont <- create_storage_container(bl, "newblobcontainer")
fs <- create_storage_container(fl, "newfileshare")

# creating directories (if possible)
create_storage_dir(cont, "newdir")  # will error out
create_storage_dir(fs, "newdir")

# transfer a file
storage_upload(bl, "~/file.txt", "storage_file.txt")
storage_upload(cont, "~/file.txt", "newdir/storage_file.txt")

}
}
\seealso{
\link{storage_endpoint}, \link{blob_container}, \link{file_share}, \link{adls_filesystem}

\link{list_blobs}, \link{list_azure_files}, \link{list_adls_files}

Similar generics exist for file transfer methods; see the page for \link{storage_download}.
}
