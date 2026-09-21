//! Provides support for building and executing parameterized SQL queries through [`SparkSession::query`].
//!
//! # Overview
//!
//! This module defines the [`SqlQueryBuilder`] type used by [`SparkSession::query`] to
//! support a fluent, type-safe API for parameterized SQL queries.
//!
//! Users are not expected to instantiate [`SqlQueryBuilder`] directly; instead, call
//! [`SparkSession::query`], and then chain `.bind()` calls to attach
//! parameters before executing the query.
//!
//! # Example
//!
//! ```no_run
//! use spark_connect::SparkSession;
//! use arrow::array::RecordBatch;
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), spark_connect::SparkError> {
//! let session = SparkSession::builder()
//!     .remote("sc://localhost:15002")
//!     .get_or_create()
//!     .await?;
//!
//! // Build and execute a parameterized query fluently
//! let results: Vec<RecordBatch> = session
//!     .query("SELECT ? AS id, ? AS name")
//!     .bind(42)
//!     .bind("Alice")
//!     .execute()
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! # How it works
//!
//! - [`SparkSession::query`] creates a [`SqlQueryBuilder`] instance tied to the session
//!   and initializes it with a SQL query string containing `?` placeholders.
//! - `.bind()` attaches parameter values, converting each Rust type into a Spark
//!   [`LiteralExpression`](apache_spark_connect::LiteralExpression) via the [`ToLiteral`] trait.
//! - `.execute()` runs the query asynchronously and collects the resulting Arrow
//!   [`RecordBatch`]es into memory.
//!
//! Parameters require a Spark 4.0+ server. Spark 4.1.x servers fail to bind them
//! with `UNBOUND_SQL_PARAMETER` whenever `spark.sql.extensions` is set (an upstream
//! Spark defect, independent of Spark Connect and of this crate).
//!
//! # See also
//! - [`ToLiteral`] - converts native Rust types into Spark literals.
//!
//! # Errors
//!
//! Returns a [`SparkError`](crate::SparkError) if query preparation or execution fails.

use crate::{Result, SparkSession, ToLiteral};

use apache_spark_connect::Expression;
use arrow::array::RecordBatch;
use std::collections::HashMap;


pub struct SqlQueryBuilder<'a> {
    session: &'a SparkSession,
    query: String,
    params: Vec<Expression>,
}

impl<'a> SqlQueryBuilder<'a> {
    pub(crate) fn new(session: &'a SparkSession, query: &str) -> Self {
        Self {
            session,
            query: query.to_string(),
            params: Vec::new(),
        }
    }

    pub fn bind<T: ToLiteral>(mut self, value: T) -> Self {
        self.params.push(Expression::Literal(value.to_literal()));
        self
    }

    pub async fn execute(self) -> Result<Vec<RecordBatch>> {
        let Self { session, query, params } = self;
        session
            .run(move |spark| spark.sql_with_args(&query, params, HashMap::new())?.collect_record_batches())
            .await
    }
}
