//! Catalog lookups and session state.
//!
//! Only the catalog RPCs that released Spark servers implement are exercised
//! here; see the note on `CatalogExt` for the ones that need a newer server.
#![allow(clippy::result_large_err)] // SparkError is the official crate's type
mod common;

use common::{session, unique};

use spark_connect::prelude::*;
use spark_connect::Result;

/// A fresh session starts in the default catalog and database.
#[tokio::test]
async fn reports_the_current_catalog_and_database() -> Result<()> {
    let session = session().await?;
    let catalog = session.catalog();

    assert_eq!(catalog.current_catalog_async().await?, "spark_catalog");
    assert_eq!(catalog.current_database_async().await?, "default");
    assert!(catalog.database_exists_async("default").await?);
    assert!(!catalog.database_exists_async(&unique("nowhere")).await?);
    Ok(())
}

/// A database created through SQL is visible to the catalog, and
/// `set_current_database` moves the session into it.
#[tokio::test]
async fn follows_a_database_through_the_catalog() -> Result<()> {
    let session = session().await?;
    let catalog = session.catalog();
    let database = unique("warehouse");

    session.sql(&format!("CREATE DATABASE {database}"))?.collect_async().await?;
    assert!(catalog.database_exists_async(&database).await?);

    // Listings build a plan rather than reaching the server, so they need no
    // async counterpart; the action on the returned frame is the awaited part.
    let listed = catalog.list_databases_with_pattern(Some(&database))?;
    assert_eq!(listed.count_async().await?, 1);

    catalog.set_current_database_async(&database).await?;
    assert_eq!(catalog.current_database_async().await?, database);

    session.sql(&format!("DROP DATABASE {database}"))?.collect_async().await?;
    assert!(!catalog.database_exists_async(&database).await?);
    Ok(())
}

/// A table is found by name and, with `Option<&str>` arguments, within its
/// database.
#[tokio::test]
async fn finds_a_table_in_its_database() -> Result<()> {
    let session = session().await?;
    let catalog = session.catalog();
    let table = unique("readings");

    session.range(2)?.write().mode("overwrite").save_as_table_async(&table).await?;

    assert!(catalog.table_exists_with_database_async(&table, Some("default")).await?);
    assert!(!catalog.table_exists_with_database_async(&unique("absent"), Some("default")).await?);

    let listed = catalog.list_tables_with_pattern(None, Some(&table))?;
    assert_eq!(listed.count_async().await?, 1);

    let columns = catalog.list_columns_with_database(&table, None)?;
    assert_eq!(columns.count_async().await?, 1);

    session.sql(&format!("DROP TABLE {table}"))?.collect_async().await?;
    Ok(())
}

/// Tables can be cached and uncached through the catalog.
#[tokio::test]
async fn caches_a_table() -> Result<()> {
    let session = session().await?;
    let catalog = session.catalog();
    let table = unique("cached");

    session.range(2)?.write().mode("overwrite").save_as_table_async(&table).await?;
    assert!(!catalog.is_cached_async(&table).await?);

    catalog.cache_table_async(&table).await?;
    assert!(catalog.is_cached_async(&table).await?);

    catalog.uncache_table_async(&table).await?;
    assert!(!catalog.is_cached_async(&table).await?);

    session.sql(&format!("DROP TABLE {table}"))?.collect_async().await?;
    Ok(())
}

/// Built-in functions are found; made-up ones are not.
#[tokio::test]
async fn finds_functions() -> Result<()> {
    let session = session().await?;
    let catalog = session.catalog();

    assert!(catalog.function_exists_async("upper").await?);
    assert!(!catalog.function_exists_async(&unique("nonesuch")).await?);
    Ok(())
}

/// Runtime configuration round-trips, and unsetting restores the default.
#[tokio::test]
async fn reads_and_writes_runtime_configuration() -> Result<()> {
    let session = session().await?;
    let conf = session.conf();

    conf.set_async("spark.sql.shuffle.partitions", "7").await?;
    assert_eq!(conf.get_async("spark.sql.shuffle.partitions").await?.as_deref(), Some("7"));
    assert!(conf.is_modifiable_async("spark.sql.shuffle.partitions").await?);

    conf.unset_async("spark.sql.shuffle.partitions").await?;
    assert_ne!(conf.get_async("spark.sql.shuffle.partitions").await?.as_deref(), Some("7"));
    Ok(())
}
