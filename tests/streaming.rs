//! A streaming query, from start to termination.
#![allow(clippy::result_large_err)] // SparkError is the official crate's type
mod common;

use common::{session, unique_path};

use spark_connect::prelude::*;
use spark_connect::{Result, Trigger};

/// A bounded source processed with `AvailableNow`: the query consumes what is
/// already there, terminates on its own, and its output holds every input row.
#[tokio::test]
async fn runs_a_streaming_query_to_completion() -> Result<()> {
    let session = session().await?;
    let source = unique_path("stream_source");
    let destination = unique_path("stream_output");
    let checkpoint = unique_path("stream_checkpoint");

    session.range(5)?.write().mode("overwrite").parquet_async(&source).await?;

    let query = session
        .read_stream()
        .format("parquet")
        .schema("id BIGINT")
        .load(Some(&source))
        .write_stream()
        .format("parquet")
        .option("checkpointLocation", &checkpoint)
        .trigger(Trigger::AvailableNow)
        .start_async(&destination)
        .await?;

    assert!(!query.status_async().await?.status_message.is_empty());

    // Generous, because it bounds a failure rather than the expected duration:
    // `AvailableNow` stops once the existing input is consumed.
    assert_eq!(query.await_termination_async(Some(60.0)).await?, Some(true));
    assert!(!query.is_active_async().await?);
    assert!(query.exception_async().await?.is_none());

    // Every input row reached the sink, and it reads back as a normal DataFrame.
    assert_eq!(session.read().parquet(&destination).count_async().await?, 5);
    Ok(())
}

/// A query over an unbounded source runs until the client stops it.
#[tokio::test]
async fn stops_a_running_streaming_query() -> Result<()> {
    let session = session().await?;
    let destination = unique_path("rate_output");
    let checkpoint = unique_path("rate_checkpoint");

    let query = session
        .read_stream()
        .format("rate")
        .option("rowsPerSecond", "10")
        .load(None)
        .write_stream()
        .format("parquet")
        .option("checkpointLocation", &checkpoint)
        .trigger(Trigger::ProcessingTime("1 second".into()))
        .start_async(&destination)
        .await?;

    // An unbounded source never terminates by itself, so waiting reports that.
    assert_eq!(query.await_termination_async(Some(1.0)).await?, Some(false));

    query.stop_async().await?;
    assert!(!query.is_active_async().await?);
    Ok(())
}
