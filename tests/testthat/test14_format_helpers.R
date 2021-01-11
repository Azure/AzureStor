context("File format helpers")

tenant <- Sys.getenv("AZ_TEST_TENANT_ID")
app <- Sys.getenv("AZ_TEST_APP_ID")
password <- Sys.getenv("AZ_TEST_PASSWORD")
subscription <- Sys.getenv("AZ_TEST_SUBSCRIPTION")

if(tenant == "" || app == "" || password == "" || subscription == "")
    skip("File format tests skipped: ARM credentials not set")

rgname <- Sys.getenv("AZ_TEST_STORAGE_RG")
storname <- Sys.getenv("AZ_TEST_STORAGE_NOHNS")

if(rgname == "" || storname == "")
    skip("File format tests skipped: resource names not set")

sub <- AzureRMR::az_rm$new(tenant=tenant, app=app, password=password)$get_subscription(subscription)
stor <- sub$get_resource_group(rgname)$get_storage_account(storname)

bl <- stor$get_blob_endpoint()
cont_name <- make_name()
cont <- create_blob_container(bl, cont_name)

opts <- options(azure_storage_progress_bar=FALSE)


dfs_identical <- function(df1, df2)
{
    identical(dim(df1), dim(df2)) &&
        names(df1) == names(df2) &&
        all(mapply(identical, df1, df2))
}


test_that("read/write RDS works",
{
    obj <- list(c="foo", f=ls, n=42L, x=pi)
    fname <- paste0(make_name(), ".rds")
    expect_silent(storage_save_rds(obj, cont, fname))
    objnew <- storage_load_rds(cont, fname)
    expect_identical(obj, objnew)
})


test_that("read/write RData works",
{
    objorig1 <- obj1 <- list(c="foo", f=ls, n=42L, x=pi)
    objorig2 <- obj2 <- mtcars
    fname <- paste0(make_name(), ".rdata")
    expect_silent(storage_save_rdata(obj1, obj2, container=cont, file=fname))
    rm(obj1, obj2)
    storage_load_rdata(cont, fname)
    expect_true(exists("obj1") &&
                exists("obj2") &&
                identical(obj1, objorig1) &&
                identical(obj2, objorig2))
})


test_that("read/write delim works",
{
    fname0 <- paste0(make_name(), ".tsv")
    expect_silent(storage_write_delim(iris, cont, fname0))
    irisnew0 <- storage_read_delim(cont, fname0)
    irisnew0$Species <- as.factor(irisnew0$Species)
    expect_true(dfs_identical(iris, irisnew0))

    # readr
    fname1 <- paste0(make_name(), ".tsv")
    expect_silent(storage_write_delim_readr(iris, cont, fname1))
    irisnew1 <- storage_read_delim_readr(cont, fname1, col_types="nnnnf")
    expect_true(dfs_identical(iris, irisnew1))

    # base
    fname2 <- paste0(make_name(), ".tsv")
    expect_silent(storage_write_delim_base(iris, cont, fname2))
    irisnew2 <- storage_read_delim_base(cont, fname2, stringsAsFactors=TRUE)
    expect_true(dfs_identical(iris, irisnew2))
})


test_that("read/write CSV works",
{
    # readr
    fname0 <- paste0(make_name(), ".csv")
    expect_silent(storage_write_csv(iris, cont, fname0))
    irisnew0 <- storage_read_csv(cont, fname0)
    irisnew0$Species <- as.factor(irisnew0$Species)
    expect_true(dfs_identical(iris, irisnew0))

    # readr
    fname1 <- paste0(make_name(), ".csv")
    expect_silent(storage_write_csv_readr(iris, cont, fname1))
    irisnew1 <- storage_read_csv_readr(cont, fname1, col_types="nnnnf")
    expect_true(dfs_identical(iris, irisnew1))

    # base
    fname2 <- paste0(make_name(), ".csv")
    expect_silent(storage_write_csv_base(iris, cont, fname2))
    irisnew2 <- storage_read_csv_base(cont, fname2, stringsAsFactors=TRUE)
    expect_true(dfs_identical(iris, irisnew2))
})


test_that("read/write CSV2 works",
{
    # readr
    fname0 <- paste0(make_name(), ".csv2")
    expect_silent(storage_write_csv2(iris, cont, fname0))
    irisnew0 <- storage_read_csv2(cont, fname0)
    irisnew0$Species <- as.factor(irisnew0$Species)
    expect_true(dfs_identical(iris, irisnew0))

    # readr
    fname1 <- paste0(make_name(), ".csv2")
    expect_silent(storage_write_csv2_readr(iris, cont, fname1))
    irisnew1 <- storage_read_csv2_readr(cont, fname1, col_types="nnnnf")
    expect_true(dfs_identical(iris, irisnew1))

    # base
    fname2 <- paste0(make_name(), ".csv2")
    expect_silent(storage_write_csv2_base(iris, cont, fname2))
    irisnew2 <- storage_read_csv2_base(cont, fname2, stringsAsFactors=TRUE)
    expect_true(dfs_identical(iris, irisnew2))
})


teardown(
{
    options(opts)
    conts <- list_blob_containers(bl)
    lapply(conts, delete_blob_container, confirm=FALSE)
})

