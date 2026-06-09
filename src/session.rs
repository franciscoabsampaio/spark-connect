//! High-level user-facing interface for Spark Connect.
//!
//! This module provides [`SparkSession`] - the main entry point for interacting
//! with a Spark Connect server. It exposes a familiar API surface inspired by
//! PySpark and Scala's `SparkSession`, while delegating low-level gRPC work to
//! [`SparkConnectClient`](crate::SparkConnectClient).
//!
//! # Typical usage
//!
//! ```no_run
//! use spark_connect::SparkSessionBuilder;
//!
//! # tokio_test::block_on(async {
//! let session = SparkSessionBuilder::new("sc://my-spark-server:15002")
//!     .build()
//!     .await
//!     .expect("failed to connect");
//!
//! println!("Connected to Spark session: {}", session.session_id());
//! # });
//! ```
//! 
//! # Using a Custom Server CA Certificate
//! 
//! Use this when your Spark server uses a self-signed certificate or an 
//! internal company Certificate Authority, but you do not need client authentication.
//!
//! ```no_run
//! # use spark_connect::SparkSessionBuilder;
//! # tokio_test::block_on(async {
//! let ca_cert = std::fs::read("path/to/ca.pem").unwrap();
//!
//! let session = SparkSessionBuilder::new("sc://my-spark-server:15002;use_ssl=true")
//!     .with_ca_certificate(ca_cert)
//!     .build()
//!     .await
//!     .unwrap();
//! # });
//! ```
//! 
//! # Mutual TLS (mTLS) with System Roots
//! 
//! Use this when your Spark server has a publicly trusted certificate (e.g., AWS, Let's Encrypt), 
//! but requires the client to authenticate itself.
//! 
//! ```no_run
//! # use spark_connect::SparkSessionBuilder;
//! # tokio_test::block_on(async {
//! let client_cert = std::fs::read("path/to/client.pem").unwrap();
//! let client_key = std::fs::read("path/to/client.key").unwrap();
//! 
//! let session = SparkSessionBuilder::new("sc://my-spark-server:15002;use_ssl=true")
//!     .with_client_identity(client_cert, client_key)
//!     .build()
//!     .await
//!     .unwrap();
//! # });
//! ```
//!
//! # Mutual TLS (mTLS) with a Custom CA
//! 
//! Use this for fully internal infrastructure where both the server's certificate 
//! needs custom verification AND the client needs to authenticate.
//! 
//! ```no_run
//! # use spark_connect::SparkSessionBuilder;
//! # tokio_test::block_on(async {
//! let ca_cert = std::fs::read("path/to/ca.pem").unwrap();
//! let client_cert = std::fs::read("path/to/client.pem").unwrap();
//! let client_key = std::fs::read("path/to/client.key").unwrap();
//! 
//! let session = SparkSessionBuilder::new("sc://internal-spark-server:15002;use_ssl=true")
//!     .with_ca_certificate(ca_cert)
//!     .with_client_identity(client_cert, client_key)
//!     .build()
//!     .await
//!     .unwrap();
//! # });
//! ```
//!
//! The `SparkSession` provides an ergonomic API for executing SQL, analyzing
//! plans, and inspecting results - without exposing internal client plumbing.
use crate::client::SparkConnectClient;
use crate::conf::{SparkConf, SparkConfKey, ResolvedSparkConf};
use crate::spark;
use crate::spark::expression::Literal;
use crate::query::SqlQueryBuilder;
use crate::{SparkError, error::SparkErrorKind};

use arrow::record_batch::RecordBatch;
use api_parity_rs::{parity, parity_impl};

/// Builder for creating [`SparkSession`] instances.
///
/// Configures a connection to a Spark Connect endpoint
/// following the URL format defined by
/// [Apache Spark's client connection spec](https://github.com/apache/spark/blob/master/connector/connect/docs/client-connection-string.md).
///
/// # Example
///
/// ```
/// use spark_connect::SparkSessionBuilder;
///
/// # tokio_test::block_on(async {
/// let session = SparkSessionBuilder::new("sc://localhost:15002")
///     .build()
///     .await
///     .unwrap();
///
/// println!("Session ID: {}", session.session_id());
/// # });
/// ```
#[derive(Debug)]
pub struct SparkSessionBuilder {
    conf: SparkConf,
}

#[parity_impl(
    path = "pyspark.sql.session.SparkSession.Builder",
    status = Implemented,
)]
impl SparkSessionBuilder {
    /// Creates a new builder with default connection string
    fn new() -> Self {
        Self {
            conf: SparkConf::new()
        }
    }

    /// Sets a configuration option for the [`SparkSession`].
    /// Then validates the configuration.
    #[parity(
        path = ".config",
        status = Implemented,
    )]
    pub fn config(mut self, key: SparkConfKey, value: impl Into<String>) -> Result<Self, SparkError> {
        self.conf.set(key, value.into())?;
        Ok(self)
    }

    /// Sets the channel builder connection string.
    ///
    /// The connection string must follow the format:
    /// `sc://<host>:<port>/;key1=value1;key2=value2;...`
    #[parity(
        path = ".remote",
        status = Implemented,
    )]
    pub fn remote(self, url: &str) -> Result<Self, SparkError> {
        self.config(SparkConfKey::Remote, url)
    }

    /// Sets the Spark master URL to connect to,
    /// such as "local" to run locally, "local[4]"
    /// to run locally with 4 cores, or "spark://master:7077"
    /// to run on a Spark standalone cluster.
    #[parity(
        path = ".master",
        status = Partial,
        comment = "value is stored but classic-mode (non-sc://) resolution is not wired up",
    )]
    pub fn master(self, url: &str) -> Result<Self, SparkError> {
        self.config(SparkConfKey::Master, url)
    }

    /// Sets a name for the application, which will be shown in the Spark web UI.
    /// If no application name is set, a randomly generated name will be used.
    #[parity(
        path = ".appName",
        status = Implemented,
    )]
    pub fn app_name(self, url: &str) -> Result<Self, SparkError> {
        self.config(SparkConfKey::AppName, url)
    }

    /// Enables Hive support, including connectivity to a persistent Hive metastore,
    /// support for Hive SerDes, and Hive user-defined functions.
    #[parity(
        path = ".enableHiveSupport",
        status = Implemented,
    )]
    pub fn enable_hive_support(self) -> Result<Self, SparkError> {
        self.config(SparkConfKey::CatalogImplementation, "hive")
    }

    #[parity(
        path = ".getOrCreate",
        status = Unimplemented,
        comment = "Session reuse is not yet implemented due to underlying complexity."
    )]
    pub fn get_or_create(&self) -> Result<SparkSession, SparkError> {
        Err(SparkError::new(SparkErrorKind::Unimplemented(
            "Session reuse is not yet implemented due to underlying complexity.".into(),
        )))
    }

    /// Returns a ready-to-use [`SparkSession`].
    ///
    /// Starts by resolving the SparkConf,
    /// ensuring no conflicting configurations are present,
    /// and getting values from environment variables if needed.
    #[parity(
        path = ".create",
        status = Partial,
        comment = "Only remote (sc://) mode works; classic master URLs return Unimplemented",
    )]
    pub async fn create(&mut self) -> Result<SparkSession, SparkError> {
        let spark_conf = self.conf.resolve()?;

        match spark_conf {
            // Spark Connect mode
            ResolvedSparkConf::Remote(remote) => {
                // os.environ["SPARK_CONNECT_MODE_ENABLED"] = "1"
                let connect_client = SparkConnectClient::new(&remote).await?;
                Ok(SparkSession::new(
                    Some(connect_client.clone()),
                    connect_client.session_id().to_string()
                ))
            },
            // Classic mode
            ResolvedSparkConf::Master(master) => {
                Err(SparkError::new(SparkErrorKind::Unimplemented(
                    format!("Master is not yet supported: {master:?}"),
                )))
            },
        }
    }
}

/// Represents a logical connection to a Spark Connect backend.
///
/// `SparkSession` is the main entry point for executing commands, analyzing
/// queries, and retrieving results from Spark Connect.
///
/// It wraps an internal [`SparkConnectClient`](crate::SparkConnectClient) and tracks session
/// state (such as the `session_id`).
///
/// # Examples
///
/// ```
/// use spark_connect::SparkSessionBuilder;
///
/// # tokio_test::block_on(async {
/// let session = SparkSessionBuilder::new("sc://localhost:15002")
///     .build()
///     .await
///     .unwrap();
///
/// println!("Session ID: {}", session.session_id());
/// # });
/// ```
#[derive(Clone, Debug)]
pub struct SparkSession {
    connect_client: Option<SparkConnectClient>,
    session_id: String,
}

#[parity_impl(
    path = "pyspark.sql.session.SparkSession",
    status = Implemented,
)]
impl SparkSession {
    /// Creates a new builder object
    #[parity(
        path = ".builder",
        status = Implemented,
    )]
    pub fn builder() -> SparkSessionBuilder {
        SparkSessionBuilder::new()
    }

    /// Creates a new session from a [`SparkConnectClient`].
    ///
    /// Usually invoked internally by [`SparkSessionBuilder::build`].
    pub(crate) fn new(
        connect_client: Option<SparkConnectClient>,
        session_id: String,
    ) -> Self {
        Self { connect_client, session_id }
    }

     /// Returns the unique session identifier for this connection.
    pub fn session_id(&self) -> String {
        self.session_id.to_string()
    }

    /// Returns a mutable reference to the underlying [`SparkConnectClient`].
    ///
    /// While exposed for advanced use cases, typical consumers are advised to rely on
    /// higher-level abstractions in `SparkSession` instead of manipulating the
    /// client directly.
    #[parity(
        path = ".client",
        status = Implemented,
    )]
    pub(crate) fn client(&self) -> Result<SparkConnectClient, SparkError> {
        if let Some(client) = &self.connect_client {
            Ok(client.clone())
        } else {
            Err(SparkError::new(SparkErrorKind::ClientNotFound))
        }
    }

    /// Execute a SQL query and return a lazy [`plan`](crate::spark::Plan).
    #[parity(
        path = ".sql",
        status = Partial,
    )]
    pub async fn sql(
        &self,
        query: &str,
        params: Vec<Literal>
    ) -> Result<spark::Plan, SparkError> {
        let sql_cmd = spark::command::CommandType::SqlCommand(
            spark::SqlCommand {
                sql: query.to_string(),
                args: Default::default(),
                pos_args: params,
            },
        );

        // Execute command
        let mut client = self.client()?;
        let result = client.execute_command(sql_cmd).await?;

        Ok(spark::Plan {
            op_type: Some(spark::plan::OpType::Root(result.relation()?)),
        })
    }

    /// Alternative ["sqlx-like"](https://docs.rs/sqlx/latest/sqlx/) query interface.
    /// Returns a [`SqlQueryBuilder`] to `bind()` parameters and `execute()`.
    pub fn query(
        &self,
        query: &str,
    ) -> SqlQueryBuilder<'_> {
        SqlQueryBuilder::new(&self, query)
    }

    /// Collect the results from a lazy [`plan`](crate::spark::Plan).
    pub async fn collect(&self, plan: spark::Plan) -> Result<Vec<RecordBatch>, SparkError> {
        let mut client = self.client()?;

        Ok(client.to_batches(plan).await?)
    }

    /// Interrupt all running operations.
    #[parity(
        path = ".interruptAll",
        status = Implemented,
    )]
    pub async fn interrupt_all(&self) -> Result<Vec<String>, SparkError> {
        Ok(
            self.client()?.interrupt(
                spark::interrupt_request::InterruptType::All,
                None
            ).await?.interrupted_ids()
        )
    }

    /// Interrupt a specific operation by ID.
    #[parity(
        path = ".interruptOperation",
        status = Implemented,
    )]
    pub async fn interrupt_operation(&self, op_id: &str) -> Result<Vec<String>, SparkError> {
        Ok(
            self.client()?.interrupt(
                spark::interrupt_request::InterruptType::OperationId,
                Some(op_id.to_string()),
            ).await?.interrupted_ids()
        )
    }

    /// Request the version of the Spark Connect server.
    #[parity(
        path = ".version",
        status = Implemented,
    )]
    pub async fn version(&self) -> Result<String, SparkError> {
        let version = spark::analyze_plan_request::Analyze::SparkVersion(
            spark::analyze_plan_request::SparkVersion {},
        );

        let mut client = self.client()?.clone();
        
        Ok(client.analyze(version).await?.version()?)
    }
}

#[cfg(test)]
mod tests {
    use crate::test_utils::test_utils::setup_session;
    use crate::SparkError;
    
    use arrow::array::{Int32Array, StringArray};
    use regex::Regex;

    #[tokio::test]
    async fn test_session_create() {
        let spark = setup_session().await;
        assert!(spark.is_ok());
    }

    /// Verifies that the client can connect, establish a session, and perform
    /// a basic analysis operation (fetching the Spark version).
    /// This tests `SparkConnectClient::new` and `SparkConnectClient::analyze`.
    #[tokio::test]
    async fn test_session_version() -> Result<(), SparkError> {
        // Arrange: Start server and create a session
        let spark = setup_session().await?;
        
        // Act: The version() method on SparkSession will trigger the
        // underlying SparkConnectClient::analyze call.
        let version = spark.version().await?;

        // Assert: Check for a valid version string
        let re = Regex::new(r"^\d+\.\d+\.\d+$").unwrap();
        assert!(re.is_match(&version), "Version {} invalid", version);
        Ok(())
    }

    /// Verifies that the client can execute a SQL query
    /// and correctly retrieve the resulting Arrow RecordBatches.
    /// This tests `SparkConnectClient::execute_command_and_fetch`.
    #[tokio::test]
    async fn test_sql() {
        // Arrange: Start server and create a session
        let session = setup_session().await.expect("Failed to create Spark session");

        // Act: Execute a simple SQL query.
        let lazy_plan = session
            .sql("SELECT 1 AS id, 'hello' AS text", vec![])
            .await
            .expect("SQL query failed");
        let batches = session
            .collect(lazy_plan)
            .await
            .expect("Failed to collect batches");

        // Assert: Validate the structure and content of the returned data
        assert_eq!(batches.len(), 1, "Expected exactly one RecordBatch");
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 1, "Expected one row");
        assert_eq!(batch.num_columns(), 2, "Expected two columns");

        // Verify the data in the first column (id)
        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Column 0 should be an Int32Array");
        assert_eq!(id_col.value(0), 1);
    }
    
    #[tokio::test]
    async fn test_sql_query_builder_bind() -> Result<(), SparkError> {
        let session = setup_session().await?;

        // Use SqlQueryBuilder and bind parameters
        let batches = session
            .query("SELECT ? AS id, ? AS text")
            .bind(42_i32)
            .bind("world")
            .execute()
            .await?;

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 2);

        let id_col = batch.column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id_col.value(0), 42);

        let text_col = batch.column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(text_col.value(0), "world");

        Ok(())
    }
}
