//! High-level user-facing interface for Spark Connect.
//!
//! This module provides [`SparkSession`] - the main entry point for interacting
//! with a Spark Connect server. It wraps the official
//! [`apache_spark_connect::SparkSession`] and dereferences to it, so the whole
//! official session API is available; the methods that reach the server are
//! redefined here as async.
//!
//! # Typical usage
//!
//! ```no_run
//! use spark_connect::SparkSession;
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), spark_connect::SparkError> {
//! let session = SparkSession::builder()
//!     .remote("sc://my-spark-server:15002")
//!     .get_or_create()
//!     .await?;
//!
//! println!("Connected to Spark session: {}", session.session_id());
//! # Ok(())
//! # }
//! ```
//!
//! # Blocking calls
//!
//! Calls into the official crate that reach the server run on tokio's
//! blocking thread pool, so a [`SparkSession`] must be used from within a
//! tokio runtime.
use crate::blocking::blocking;
use crate::query::SqlQueryBuilder;
use crate::Result;

use apache_spark_connect as upstream;
use apache_spark_connect::udf::CommonInlineUserDefinedFunctionExpression;
use apache_spark_connect::{CommonInlineUserDefinedDataSourceExpression, ResourceProfile};
use std::fmt;
use std::ops::Deref;

/// Builder for creating [`SparkSession`] instances.
///
/// Configures a connection to a Spark Connect endpoint
/// following the URL format defined by
/// [Apache Spark's client connection spec](https://github.com/apache/spark/blob/master/sql/connect/docs/client-connection-string.md).
///
/// # Example
///
/// ```no_run
/// use spark_connect::SparkSession;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), spark_connect::SparkError> {
/// let session = SparkSession::builder()
///     .remote("sc://localhost:15002")
///     .get_or_create()
///     .await?;
///
/// println!("Session ID: {}", session.session_id());
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, Default)]
pub struct SparkSessionBuilder {
    remote: Option<String>,
}

impl SparkSessionBuilder {
    /// Sets the connection string.
    ///
    /// The connection string must follow the format:
    /// `sc://<host>:<port>/;key1=value1;key2=value2;...`
    pub fn remote(mut self, url: &str) -> Self {
        self.remote = Some(url.to_string());
        self
    }

    /// Returns a ready-to-use [`SparkSession`].
    ///
    /// The gRPC channel is established lazily, on the first call that reaches the server.
    pub async fn get_or_create(&self) -> Result<SparkSession> {
        let remote = self.remote.clone();
        let inner = blocking(move || {
            let builder = upstream::SparkSession::builder();
            match remote {
                Some(url) => builder.remote(&url),
                None => builder,
            }
            .get_or_create()
        })
        .await?;
        Ok(SparkSession { inner })
    }
}

/// Represents a logical connection to a Spark Connect backend.
///
/// `SparkSession` is the main entry point for executing queries and
/// retrieving results from Spark Connect. Clones share the same session.
///
/// It dereferences to the official [`apache_spark_connect::SparkSession`]:
/// methods that only build a plan - [`sql`](upstream::SparkSession::sql),
/// [`table`](upstream::SparkSession::table), [`read`](upstream::SparkSession::read),
/// [`catalog`](upstream::SparkSession::catalog) - are the official ones, and the
/// actions on what they return are provided by the [`ext`](crate::ext) traits.
#[derive(Clone)]
pub struct SparkSession {
    inner: upstream::SparkSession,
}

impl SparkSession {
    /// Creates a new builder object.
    pub fn builder() -> SparkSessionBuilder {
        SparkSessionBuilder::default()
    }

    /// ["sqlx-like"](https://docs.rs/sqlx/latest/sqlx/) query interface.
    /// Returns a [`SqlQueryBuilder`] to `bind()` parameters and `execute()`.
    pub fn query(&self, query: &str) -> SqlQueryBuilder<'_> {
        SqlQueryBuilder::new(self, query)
    }

    /// Runs `f` against the official [`apache_spark_connect::SparkSession`]
    /// on tokio's blocking thread pool.
    ///
    /// Reaches anything in the official API not covered by this type or the
    /// [`ext`](crate::ext) traits.
    ///
    /// ```no_run
    /// # async fn example(session: spark_connect::SparkSession) -> spark_connect::Result<()> {
    /// let active = session.run(|spark| spark.streams().active()).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn run<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(upstream::SparkSession) -> Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let session = self.inner.clone();
        blocking(move || f(session)).await
    }

    /// Async [`version`](upstream::SparkSession::version).
    pub async fn version(&self) -> Result<String> {
        self.run(|spark| spark.version()).await
    }

    /// Async [`interrupt_all`](upstream::SparkSession::interrupt_all).
    pub async fn interrupt_all(&self) -> Result<Vec<String>> {
        self.run(|spark| spark.interrupt_all()).await
    }

    /// Async [`interrupt_tag`](upstream::SparkSession::interrupt_tag).
    pub async fn interrupt_tag(&self, tag: &str) -> Result<Vec<String>> {
        let tag = tag.to_owned();
        self.run(move |spark| spark.interrupt_tag(&tag)).await
    }

    /// Async [`interrupt_operation`](upstream::SparkSession::interrupt_operation).
    pub async fn interrupt_operation(&self, operation_id: &str) -> Result<Vec<String>> {
        let operation_id = operation_id.to_owned();
        self.run(move |spark| spark.interrupt_operation(&operation_id)).await
    }

    /// Async [`add_artifact`](upstream::SparkSession::add_artifact).
    pub async fn add_artifact(&self, path: &str) -> Result<()> {
        let path = path.to_owned();
        self.run(move |spark| spark.add_artifact(&path)).await
    }

    /// Async [`add_artifacts`](upstream::SparkSession::add_artifacts).
    pub async fn add_artifacts(&self, paths: &[&str]) -> Result<()> {
        let paths: Vec<String> = paths.iter().map(|path| path.to_string()).collect();
        self.run(move |spark| spark.add_artifacts(&paths.iter().map(String::as_str).collect::<Vec<_>>()))
            .await
    }

    /// Async [`copy_from_local_to_fs`](upstream::SparkSession::copy_from_local_to_fs).
    pub async fn copy_from_local_to_fs(&self, local_path: &str, dest_path: &str) -> Result<()> {
        let (local_path, dest_path) = (local_path.to_owned(), dest_path.to_owned());
        self.run(move |spark| spark.copy_from_local_to_fs(&local_path, &dest_path)).await
    }

    /// Async [`register_function`](upstream::SparkSession::register_function).
    pub async fn register_function(&self, udf: CommonInlineUserDefinedFunctionExpression) -> Result<()> {
        self.run(move |spark| spark.register_function(udf)).await
    }

    /// Async [`register_java_function`](upstream::SparkSession::register_java_function).
    pub async fn register_java_function(
        &self,
        name: &str,
        java_class_name: &str,
        return_type_ddl: Option<&str>,
        aggregate: bool,
    ) -> Result<()> {
        let (name, java_class_name) = (name.to_owned(), java_class_name.to_owned());
        let return_type_ddl = return_type_ddl.map(str::to_owned);
        self.run(move |spark| {
            spark.register_java_function(&name, &java_class_name, return_type_ddl.as_deref(), aggregate)
        })
        .await
    }

    /// Async [`register_data_source`](upstream::SparkSession::register_data_source).
    pub async fn register_data_source(
        &self,
        data_source: CommonInlineUserDefinedDataSourceExpression,
    ) -> Result<()> {
        self.run(move |spark| spark.register_data_source(data_source)).await
    }

    /// Async [`build_resource_profile`](upstream::SparkSession::build_resource_profile).
    pub async fn build_resource_profile(&self, profile: &ResourceProfile) -> Result<i32> {
        let profile = profile.clone();
        self.run(move |spark| spark.build_resource_profile(&profile)).await
    }

    /// Async [`stop`](upstream::SparkSession::stop).
    pub async fn stop(&self) -> Result<()> {
        self.run(|spark| spark.stop()).await
    }

    /// A new session on the same connection, with its own server-side state.
    pub fn new_session(&self) -> SparkSession {
        SparkSession { inner: self.inner.new_session() }
    }

    /// A new session on the same connection, with a copy of this session's server-side state.
    pub fn clone_session(&self) -> SparkSession {
        SparkSession { inner: self.inner.clone_session() }
    }
}

impl Deref for SparkSession {
    type Target = upstream::SparkSession;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl fmt::Debug for SparkSession {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SparkSession")
            .field("session_id", &self.session_id())
            .finish()
    }
}
