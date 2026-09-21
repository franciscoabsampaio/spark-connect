//! DataFrame actions: inspection, caching and streaming rows.
#![allow(clippy::result_large_err)] // SparkError is the official crate's type
mod common;

use common::session;

use futures_util::StreamExt;
use spark_connect::prelude::*;
use spark_connect::row::Value;
use spark_connect::{Result, StorageLevel, StorageLevelExt};

/// The plan is built with the official API and only the actions are awaited.
#[tokio::test]
async fn inspects_a_dataframe() -> Result<()> {
    let session = session().await?;
    let people = session.sql("SELECT * FROM VALUES (1, 'ada'), (2, 'alan') AS t(id, name)")?;

    assert_eq!(people.columns_async().await?, ["id", "name"]);
    assert_eq!(people.dtypes_async().await?, [
        ("id".to_string(), "int".to_string()),
        ("name".to_string(), "string".to_string()),
    ]);
    assert_eq!(people.count_async().await?, 2);
    assert!(!people.is_empty_async().await?);

    let first = people.first_async().await?.expect("a first row");
    assert_eq!(first.get_by_name("name").and_then(Value::as_str), Some("ada"));
    assert_eq!(people.take_async(1).await?.len(), 1);
    Ok(())
}

/// Transformations compose before any action runs, and the action reports the
/// filtered result.
#[tokio::test]
async fn filters_before_collecting() -> Result<()> {
    let session = session().await?;

    let adults = session
        .sql("SELECT * FROM VALUES (12, 'kid'), (30, 'grown') AS t(age, name)")?
        .filter(col("age").gt(lit(17)))
        .collect_async()
        .await?;

    assert_eq!(adults.len(), 1);
    assert_eq!(adults[0].get_by_name("name").and_then(Value::as_str), Some("grown"));
    Ok(())
}

/// Two plans built the same way are semantically equal; a different one is not.
#[tokio::test]
async fn compares_plans_semantically() -> Result<()> {
    let session = session().await?;
    let numbers = session.range(3)?;

    assert!(numbers.same_semantics_async(&session.range(3)?).await?);
    assert!(!numbers.same_semantics_async(&session.range(4)?).await?);
    Ok(())
}

/// Caching round-trips: the level asked for is the level reported, and
/// unpersisting clears it.
#[tokio::test]
async fn persists_and_unpersists() -> Result<()> {
    let session = session().await?;
    let numbers = session.range(5)?;

    let cached = numbers.persist_async(StorageLevel::memory_only()).await?;
    assert!(cached.is_cached_async().await?);
    assert_eq!(cached.storage_level_async().await?, StorageLevel::memory_only());

    let released = cached.unpersist_async(true).await?;
    assert!(!released.is_cached_async().await?);
    Ok(())
}

/// Rows arrive in order, and the stream ends by itself.
#[tokio::test]
async fn streams_rows_in_order() -> Result<()> {
    let session = session().await?;

    let mut rows = session.range(2_500)?.to_local_iterator_async(false);

    let mut expected = 0;
    while let Some(row) = rows.next().await {
        assert_eq!(row?.get(0).and_then(Value::as_i64), Some(expected));
        expected += 1;
    }

    assert_eq!(expected, 2_500);
    Ok(())
}

/// A caller that stops reading early can drop the stream; nothing hangs and
/// the session stays usable.
#[tokio::test]
async fn abandons_a_stream_midway() -> Result<()> {
    let session = session().await?;

    let mut rows = session.range(100_000)?.to_local_iterator_async(true);
    let first = rows.next().await.expect("at least one row")?;
    assert_eq!(first.get(0).and_then(Value::as_i64), Some(0));
    drop(rows);

    assert_eq!(session.range(3)?.count_async().await?, 3);
    Ok(())
}

/// Failures surface as the stream's first item rather than as a panic.
#[tokio::test]
async fn reports_stream_failures_as_items() -> Result<()> {
    let session = session().await?;

    let mut rows = session.sql("SELECT * FROM no_such_table")?.to_local_iterator_async(false);

    let error = rows.next().await.expect("an item").expect_err("a failure");
    assert_eq!(error.get_condition().as_deref(), Some("TABLE_OR_VIEW_NOT_FOUND"));
    Ok(())
}
