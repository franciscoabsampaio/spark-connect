//! A streaming query, from start to stop.
#![allow(clippy::result_large_err)] // SparkError is the official crate's type
mod common;

use common::{session, unique_path};

use spark_connect::prelude::*;
use spark_connect::{Result, Trigger};

/// A rate source written to parquet runs until it is stopped, reporting its
/// status while it does.
#[tokio::test]
async fn runs_and_stops_a_streaming_query() -> Result<()> {
    let session = session().await?;
    let destination = unique_path("stream");
    let checkpoint = unique_path("checkpoint");

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

    assert!(query.is_active_async().await?);
    assert!(!query.status_async().await?.status_message.is_empty());

    // Waiting on a query that keeps running reports that it did not terminate.
    assert_eq!(query.await_termination_async(Some(2.0)).await?, Some(false));

    query.stop_async().await?;
    assert!(!query.is_active_async().await?);
    assert!(query.exception_async().await?.is_none());

    // Whatever it wrote before stopping is readable as a normal DataFrame.
    assert!(session.read().parquet(&destination).count_async().await? > 0);
    Ok(())
}
