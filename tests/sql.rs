//! Parameterized SQL, and how failures surface.
#![allow(clippy::result_large_err)] // SparkError is the official crate's type
mod common;

use common::session;

use arrow::array::{BinaryArray, BooleanArray, Float64Array, Int8Array, Int64Array, StringArray};
use spark_connect::prelude::*;
use spark_connect::{LiteralExpression, Result};

/// Every `bind` type reaches the server as the Spark literal it claims to be,
/// and comes back unchanged.
#[tokio::test]
async fn binds_each_supported_rust_type() -> Result<()> {
    let session = session().await?;

    let batches = session
        .query("SELECT ? AS small, ? AS big, ? AS ratio, ? AS flag, ? AS name, ? AS blob")
        .bind(7_i8)
        .bind(9_000_000_000_i64)
        .bind(0.5_f64)
        .bind(true)
        .bind("hello".to_string())
        .bind(vec![1_u8, 2, 3])
        .execute()
        .await?;

    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);

    let column = |i: usize| batch.column(i).as_any();
    assert_eq!(column(0).downcast_ref::<Int8Array>().unwrap().value(0), 7);
    assert_eq!(column(1).downcast_ref::<Int64Array>().unwrap().value(0), 9_000_000_000);
    assert_eq!(column(2).downcast_ref::<Float64Array>().unwrap().value(0), 0.5);
    assert!(column(3).downcast_ref::<BooleanArray>().unwrap().value(0));
    assert_eq!(column(4).downcast_ref::<StringArray>().unwrap().value(0), "hello");
    assert_eq!(column(5).downcast_ref::<BinaryArray>().unwrap().value(0), &[1, 2, 3]);
    Ok(())
}

/// Types with no `ToLiteral` impl of their own are bound by passing the
/// official literal through.
#[tokio::test]
async fn binds_a_literal_expression_directly() -> Result<()> {
    let session = session().await?;

    let batches = session
        .query("SELECT ? AS price")
        .bind(LiteralExpression::Decimal { value: "1.50".into(), precision: 3, scale: 2 })
        .execute()
        .await?;

    let schema = batches[0].schema();
    assert_eq!(schema.field(0).data_type().to_string(), "Decimal128(3, 2)");
    Ok(())
}

/// A bound value is data, never SQL: quotes in it cannot close the literal.
#[tokio::test]
async fn binds_values_without_interpolating_sql() -> Result<()> {
    let session = session().await?;
    let hostile = "'; DROP TABLE users; --";

    let batches = session.query("SELECT ? AS text").bind(hostile).execute().await?;

    let text = batches[0].column(0).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(text.value(0), hostile);
    Ok(())
}

/// Server-side failures arrive as a `SparkError` carrying Spark's own error
/// class, rather than an opaque transport error.
#[tokio::test]
async fn reports_analysis_errors_with_their_condition() -> Result<()> {
    let session = session().await?;

    let error = session
        .sql("SELECT * FROM a_table_that_does_not_exist")?
        .collect_async()
        .await
        .expect_err("querying a missing table should fail");

    assert_eq!(error.get_condition().as_deref(), Some("TABLE_OR_VIEW_NOT_FOUND"));
    assert!(error.message().contains("a_table_that_does_not_exist"), "{}", error.message());
    Ok(())
}

/// A panic inside a blocking call reaches the caller as a panic. It signals a
/// bug, so it is deliberately not turned into an `Err`.
#[tokio::test]
async fn resumes_panics_on_the_calling_task() -> Result<()> {
    let session = session().await?;

    let joined = tokio::spawn(async move { session.run::<_, ()>(|_| panic!("boom")).await }).await;

    let error = joined.expect_err("the panic should have propagated");
    assert!(error.is_panic());
    Ok(())
}

/// With the `chrono` feature, dates and timestamps bind as Spark temporal
/// literals rather than strings.
#[cfg(feature = "chrono")]
#[tokio::test]
async fn binds_chrono_dates_and_timestamps() -> Result<()> {
    use arrow::array::{Date32Array, TimestampMicrosecondArray};
    use chrono::{NaiveDate, NaiveDateTime};

    let session = session().await?;
    let date = NaiveDate::from_ymd_opt(2026, 9, 21).unwrap();
    let timestamp: NaiveDateTime = date.and_hms_opt(12, 30, 0).unwrap();

    let batches = session
        .query("SELECT ? AS day, ? AS moment")
        .bind(date)
        .bind(timestamp)
        .execute()
        .await?;

    let batch = &batches[0];
    let days = batch.column(0).as_any().downcast_ref::<Date32Array>().unwrap();
    assert_eq!(days.value_as_date(0), Some(date));

    let moments = batch.column(1).as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
    assert_eq!(moments.value(0), timestamp.and_utc().timestamp_micros());
    Ok(())
}
