//! Writing data out and reading it back.
//!
//! These exercise the writers, whose async methods consume the builder rather
//! than borrowing it. Paths are on the server's filesystem, not the client's.
#![allow(clippy::result_large_err)] // SparkError is the official crate's type
mod common;

use common::{session, unique, unique_path};

use spark_connect::prelude::*;
use spark_connect::{Result, SparkSession};

/// Catalog `DROP TABLE` is issued as SQL: the catalog RPC for it exists only in
/// unreleased Spark (see the note on `CatalogExt`).
async fn drop_table(session: &SparkSession, table: &str) -> Result<()> {
    session.sql(&format!("DROP TABLE IF EXISTS {table}"))?.collect_async().await?;
    Ok(())
}

/// A saved table can be read back through the catalog by name.
#[tokio::test]
async fn saves_and_reads_back_a_table() -> Result<()> {
    let session = session().await?;
    let table = unique("people");

    session
        .sql("SELECT * FROM VALUES (1, 'ada'), (2, 'alan') AS t(id, name)")?
        .write()
        .mode("overwrite")
        .save_as_table_async(&table)
        .await?;

    assert!(session.catalog().table_exists_async(&table).await?);
    assert_eq!(session.table(&table)?.count_async().await?, 2);

    drop_table(&session, &table).await?;
    assert!(!session.catalog().table_exists_async(&table).await?);
    Ok(())
}

/// Appending to an existing table adds to it instead of replacing it.
#[tokio::test]
async fn appends_to_a_table() -> Result<()> {
    let session = session().await?;
    let table = unique("events");
    let rows = session.sql("SELECT * FROM VALUES (1), (2) AS t(id)")?;

    rows.write().mode("overwrite").save_as_table_async(&table).await?;
    rows.write().mode("append").insert_into_async(&table).await?;

    assert_eq!(session.table(&table)?.count_async().await?, 4);

    drop_table(&session, &table).await?;
    Ok(())
}

/// Parquet written to a path is readable from that path.
#[tokio::test]
async fn writes_and_reads_parquet() -> Result<()> {
    let session = session().await?;
    let path = unique_path("numbers");

    session.range(3)?.write().mode("overwrite").parquet_async(&path).await?;

    let reloaded = session.read().parquet(&path);
    assert_eq!(reloaded.count_async().await?, 3);
    assert_eq!(reloaded.columns_async().await?, ["id"]);
    Ok(())
}

/// The v2 writer creates a table and appends to it.
#[tokio::test]
async fn creates_a_table_with_the_v2_writer() -> Result<()> {
    let session = session().await?;
    let table = unique("measurements");
    let rows = session.sql("SELECT * FROM VALUES (1), (2) AS t(id)")?;

    rows.write_to(&table).using("delta").create_async().await?;
    assert_eq!(session.table(&table)?.count_async().await?, 2);

    rows.write_to(&table).append_async().await?;
    assert_eq!(session.table(&table)?.count_async().await?, 4);

    drop_table(&session, &table).await?;
    Ok(())
}
