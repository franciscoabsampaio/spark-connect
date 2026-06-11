//! Internal gRPC client abstraction for the Spark Connect protocol.
//!
//! This module defines [`SparkConnectClient`], the low-level asynchronous client that
//! manages communication with a Spark Connect server over gRPC.
//!
//! <div class="warning">
//! 
//! End users are advised <b>not</b> to construct or use `SparkConnectClient` directly - use
//! [`SparkSession`](crate::SparkSession) instead, which provides a high-level API.
//! 
//! </div>
//!
//! # Overview
//!
//! `SparkConnectClient` wraps the generated [`SparkConnectServiceClient`] and provides:
//!
//! - Connection setup (via [`ChannelBuilder`]);
//! - Metadata injection (via [`HeaderInterceptor`]);
//! - Structured request/response handling for:
//!   - [`Analyze` plan](crate::spark::analyze_plan_request::Analyze);
//!   - [Execution `Plan`](crate::spark::Plan);
//!   - [`Interrupt`](crate::spark::interrupt_request::Interrupt);
//!   - Reattach/release semantics.
//!
//! Each call validates the active Spark session and maps server responses into
//! safe Rust types or [`SparkError`] values.
mod channel_builder;
mod error;
mod handlers;
mod middleware;

use api_parity_rs::{parity, parity_impl};
pub use error::ClientError;
use error::ClientErrorKind;
use handlers::{AnalyzeHandler, ExecuteHandler, InterruptHandler};
pub use channel_builder::ChannelBuilder;
use channel_builder::SparkGrpcClient;
use crate::conf::SparkRemoteConf;
use crate::io::IoError;
use crate::spark;
use crate::spark::execute_plan_response::ResponseType;

use arrow::array::RecordBatch;
use arrow::ipc::writer::StreamWriter;
use polars::prelude::{DataFrame, IpcStreamReader, SerReader};
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::codec::Streaming;
use tracing;
use uuid::{self, Uuid};

/// Asynchronous gRPC client for Spark Connect.
///
/// `SparkConnectClient` manages RPC calls, session validation, and response
/// interpretation. It is used internally by [`SparkSession`](crate::SparkSession)
/// to execute plans and perform analysis or interrupt operations.
///
/// <div class="warning">
/// This struct is <b>not</b> intended for direct use; it exposes low-level details
/// of the Spark Connect protocol.
/// </div>
///
/// # Lifecycle
///
/// - Constructed indirectly through [`SparkSessionBuilder`](crate::SparkSessionBuilder);
/// - Maintains session context (e.g. `session_id`, `user_context`);
/// - Automatically attaches metadata headers.
#[derive(Clone, Debug)]
pub struct SparkConnectClient {
    stub: Arc<RwLock<SparkGrpcClient>>,
    builder: ChannelBuilder,
    closed: bool,
    handler_analyze: AnalyzeHandler,
    handler_execute: ExecuteHandler,
    handler_interrupt: InterruptHandler,
    operation_id: Option<String>,
    response_id: Option<String>,
    session_id: Uuid,
    tags: Vec<String>,
    use_reattachable_execute: bool,
    user_agent: Option<String>,
    user_context: Option<spark::UserContext>,
}

#[parity_impl(
    path = "pyspark.sql.connect.client.core.SparkConnectClient",
    status = Implemented,
)]
impl SparkConnectClient {
    /// Create a new client from a gRPC stub and a configured [`ChannelBuilder`].
    ///
    /// Typically called internally by [`SparkSessionBuilder`](crate::SparkSessionBuilder).
    pub(crate) async fn new(
        conf: &SparkRemoteConf
    ) -> Result<Self, ClientError> {
        let builder = ChannelBuilder::default()
            .config(conf)?;
        let grpc_client = builder.to_client().await?;

        Ok(Self {
            stub: Arc::new(RwLock::new(grpc_client)),
            builder: builder.clone(),
            closed: false,
            handler_analyze: AnalyzeHandler::default(),
            handler_execute: ExecuteHandler::default(),
            handler_interrupt: InterruptHandler::default(),
            operation_id: None,
            response_id: None,
            session_id: builder.session_id(),
            tags: vec![],
            use_reattachable_execute: true,
            user_agent: Some(builder.user_agent()?),
            user_context: Some(spark::UserContext {
                user_id: builder.user_id(),
                user_name: builder.user_id(),
                extensions: vec![],
            }),
        })
    }

    #[parity(
        path = ".host",
        status = Implemented,
    )]
    pub fn host(&self) -> String {
        self.builder.host()
    }

    #[parity(
        path = ".token",
        status = Implemented,
    )]
    pub fn token(&self) -> String {
        self.builder.token()
    }

    #[parity(
        path = ".is_closed",
        status = Implemented,
    )]
    pub fn is_closed(&self) -> bool {
        self.closed
    }

    /// Close the client channel and release all execute requests.
    ///
    /// [`tonic`] models connection lifetime by ownership, not an explicit close.
    /// With the channel being a cheap, clonable handle, the underlying sockets
    /// are torn down when the last clone of the channel is dropped.
    /// We could implement a more explicit close mechanism,
    /// but it is much simpler to rely on the `closed` flag as a guard against further use.
    #[parity(
        path = ".close",
        status = Implemented,
    )]
    pub async fn close(&mut self) -> Result<(), ClientError> {
        self.release_all().await?;
        self.closed = true;
        Ok(())
    }

    #[parity(
        path = ".disable_reattachable_execute",
        status = Implemented,
    )]
    pub fn disable_reattachable_execute(&mut self) -> &mut Self {
        self.use_reattachable_execute = false;
        self
    }

    #[parity(
        path = ".enable_reattachable_execute",
        status = Implemented,
    )]
    pub fn enable_reattachable_execute(&mut self) -> &mut Self {
        self.use_reattachable_execute = true;
        self
    }

    #[parity(
        path = ".add_tag",
        status = Implemented,
    )]
    pub fn add_tag(&mut self, tag: String) -> Result<&mut Self, ClientError> {
        self.validate_tag(&tag)?;
        self.tags.push(tag);
        Ok(self)
    }

    #[parity(
        path = ".clear_tags",
        status = Implemented,
    )]
    pub fn clear_tags(&mut self) -> () {
        self.tags.clear();
    }

    #[parity(
        path = ".get_tags",
        status = Implemented,
    )]
    pub fn get_tags(&self) -> Vec<String> {
        self.tags.clone()
    }

    #[parity(
        path = ".remove_tag",
        status = Implemented,
    )]
    pub fn remove_tag(&mut self, tag: &str) -> Result<(), ClientError> {
        self.validate_tag(tag)?;
        self.tags.retain(|t| t != tag);
        Ok(())
    }

    /// Return schema for given plan.
    #[parity(
        path = ".schema",
        status = Implemented,
    )]
    pub async fn schema(&mut self, plan: spark::Plan) -> Result<spark::DataType, ClientError> {
        tracing::debug!("Schema for plan: '{:?}'", plan);

        self.analyze(
            spark::analyze_plan_request::Analyze::Schema(
                spark::analyze_plan_request::Schema { plan: Some(plan) }
            )
        ).await?;

        if self.handler_analyze.schema.is_none() {
            return Err(ClientError::new(ClientErrorKind::AnalyzeResponseNotFound(
                "Schema is empty".to_string()
            )));
        }

        Ok(self.handler_analyze.schema.clone().unwrap())
    }

    /// Return the session ID associated with this client.
    #[parity(
        path = "pyspark.sql.connect.session.SparkSession.session_id",
        status = Implemented,
    )]
    pub fn session_id(&self) -> String {
        self.session_id.to_string()
    }

    /// Return the Spark version obtained from the last analyze request.
    #[parity(
        path = "pyspark.sql.connect.session.SparkSession.version",
        status = Implemented,
    )]
    pub fn version(&self) -> Result<String, ClientError> {
        self.handler_analyze
            .spark_version
            .to_owned()
            .ok_or_else(|| ClientError::new(ClientErrorKind::AnalyzeResponseNotFound(
                "Spark version response is empty".to_string()
            )))
    }

    /// Execute a command and return self.
    #[parity(
        path = ".execute_command",
        status = Partial,
        comment = "Unlike the original, this method is lazy, returning the client instead of a materialized result."
    )]
    pub async fn execute_command(
        &mut self,
        command: spark::command::CommandType
    ) -> Result<&mut Self, ClientError> {
        tracing::debug!("Executing command '{:?}'", command);

        let mut request = self.execute_plan_request_with_metadata();
        
        request.plan = Some(spark::Plan {
            op_type: Some(spark::plan::OpType::Command(spark::Command {
                command_type: Some(command),
            })),
        });

        self.execute_request(request).await
    }

    #[parity(
        path = ".to_table",
        status = Partial,
        comment = "Unlike the original, this method does not return observations, and returns a vector of record batches instead of a table (since it does not exist in Rust)."
    )]
    /// Return given plan as a vector of RecordBatch.
    pub async fn to_batches(
        &mut self, plan: spark::Plan
    ) -> Result<Vec<RecordBatch>, ClientError> {
        tracing::debug!("Executing plan '{:?}'", plan);

        let mut request = self.execute_plan_request_with_metadata();
        
        request.plan = Some(plan);
        
        self.execute_request(request).await?;

        Ok(self.handler_execute.batches.to_owned())
    }

    #[parity(
        path = ".to_pandas",
        status = Partial,
        comment = "Instead of pandas, the most common DataFrame library in Rust is polars."
    )]
    pub async fn to_polars(
        &mut self, plan: spark::Plan
    ) -> Result<DataFrame, ClientError> {
        let batches = self.to_batches(plan).await?;
        if batches.is_empty() {
            return Ok(DataFrame::empty());
        }

        // Polars uses its own Arrow implementation (polars-arrow), not arrow-rs,
        // so bridge the two through the Arrow IPC stream format.
        let mut buf = Vec::new();
        {
            let schema = batches[0].schema();
            let mut writer = StreamWriter::try_new(&mut buf, &schema).map_err(IoError::from)?;
            for batch in &batches {
                writer.write(batch).map_err(IoError::from)?;
            }
            writer.finish().map_err(IoError::from)?;
        }

        Ok(IpcStreamReader::new(std::io::Cursor::new(buf)).finish()?)
    }

    /// Validate that a tag is present in the client's tag list
    /// and that it does not contain any invalid characters.
    fn validate_tag(&self, tag: &str) -> Result<(), ClientError> {
        if tag.contains(",") {
            return Err(ClientError::new(ClientErrorKind::InvalidTag(tag.to_string())));
        }
        if tag.is_empty() {
            return Err(ClientError::new(ClientErrorKind::EmptyTag(tag.to_string())));
        }
        Ok(())
    }

    /// Handle deserialization, streaming, and optional *reattachment* for fault-tolerant execution.
    ///
    /// The resulting record batches can be retrieved with [`batches()`](Self::batches).
    async fn execute_request(
        &mut self,
        request: spark::ExecutePlanRequest
    ) -> Result<&mut Self, ClientError> {
        let mut client = self.stub.write().await;
        let mut stream = client
            .execute_plan(request.clone())
            .await
            .map_err(|status| {
                ClientError::new(ClientErrorKind::ExecutePlanRequest { status, request })
            })?
            .into_inner();
        drop(client);

        self.handler_execute = ExecuteHandler::default();
        self.process_stream(&mut stream).await?;
        
        if self.use_reattachable_execute && self.handler_execute.result_complete {
            self.release_all().await?;
        }
        
        Ok(self)
    }

    /// Return the list of operation IDs that were interrupted.
    pub(crate) fn interrupted_ids(&self) -> Vec<String> {
        self.handler_interrupt.interrupted_ids.to_owned()
    }

    /// Return the last relation received in an [`ExecutePlanResponse`](crate::spark::ExecutePlanResponse).
    pub(crate) fn relation(&self) -> Result<spark::Relation, ClientError> {
        self.handler_execute
            .relation
            .to_owned()
            .ok_or_else(||  ClientError::new(ClientErrorKind::AnalyzeResponseNotFound(
                "relation response is empty".to_string()
            )))
    }

    /// Compare a session_id to the current session's id.
    fn validate_session(&self, session_id: &str) -> Result<(), ClientError> {
        if self.session_id() != session_id {
            return Err(ClientError::new(ClientErrorKind::SessionIDMismatch {
                client_session_id: self.session_id(),
                request_session_id: session_id.to_string()
            }));
        }
        Ok(())
    }

    /// Send an [`AnalyzePlanRequest`](crate::spark::AnalyzePlanRequest)
    /// to the Spark Connect server and updates the internal analysis handler.
    pub(crate) async fn analyze(
        &mut self,
        analyze: spark::analyze_plan_request::Analyze,
    ) -> Result<&mut Self, ClientError> {
        let request = spark::AnalyzePlanRequest {
            session_id: self.session_id(),
            user_context: self.user_context.clone(),
            client_type: self.user_agent.clone(),
            analyze: Some(analyze),
        };
        
        let mut client = self.stub.write().await;
        let resp = client.analyze_plan(request.clone())
            .await
            .map_err(|status| {
                ClientError::new(ClientErrorKind::AnalyzeRequest { status, request })
            })?
            .into_inner();
        drop(client);
        
        self.handle_analyze_response(resp)?;
        
        Ok(self)
    }

    fn handle_analyze_response(
        &mut self,
        resp: spark::AnalyzePlanResponse,
    ) -> Result<(), ClientError> {
        self.validate_session(&resp.session_id)?;

        // clear out any prior responses
        self.handler_analyze = AnalyzeHandler::default();
        
        if let Some(result) = resp.result {
            match result {
                spark::analyze_plan_response::Result::Schema(schema) => {
                    self.handler_analyze.schema = schema.schema
                }
                // spark::analyze_plan_response::Result::Explain(explain) => {
                //     self.handler_analyze.explain = Some(explain.explain_string)
                // }
                // spark::analyze_plan_response::Result::TreeString(tree_string) => {
                //     self.handler_analyze.tree_string = Some(tree_string.tree_string)
                // }
                // spark::analyze_plan_response::Result::IsLocal(is_local) => {
                //     self.handler_analyze.is_local = Some(is_local.is_local)
                // }
                // spark::analyze_plan_response::Result::IsStreaming(is_streaming) => {
                //     self.handler_analyze.is_streaming = Some(is_streaming.is_streaming)
                // }
                // spark::analyze_plan_response::Result::InputFiles(input_files) => {
                //     self.handler_analyze.input_files = Some(input_files.files)
                // }
                spark::analyze_plan_response::Result::SparkVersion(spark_version) => {
                    self.handler_analyze.spark_version = Some(spark_version.version)
                }
                // spark::analyze_plan_response::Result::DdlParse(ddl_parse) => {
                //     self.handler_analyze.ddl_parse = ddl_parse.parsed
                // }
                // spark::analyze_plan_response::Result::SameSemantics(same_semantics) => {
                //     self.handler_analyze.same_semantics = Some(same_semantics.result)
                // }
                // spark::analyze_plan_response::Result::SemanticHash(semantic_hash) => {
                //     self.handler_analyze.semantic_hash = Some(semantic_hash.result)
                // }
                // spark::analyze_plan_response::Result::Persist(_) => {}
                // spark::analyze_plan_response::Result::Unpersist(_) => {}
                // spark::analyze_plan_response::Result::GetStorageLevel(level) => {
                //     self.handler_analyze.get_storage_level = level.storage_level
                // }
                _ => return Err(ClientError::new(ClientErrorKind::Unimplemented(format!(
                    "Handling of analyze response {result:?} not implemented!"
                ))))
            }
        }

        Ok(())
    }

    /// Send an [`InterruptRequest`](crate::spark::InterruptRequest) to Spark.
    ///
    /// Used to stop long-running operations or cancel all running executions.
    pub(crate) async fn interrupt(
        &mut self,
        interrupt_type: spark::interrupt_request::InterruptType,
        id_or_tag: Option<String>,
    ) -> Result<&mut Self, ClientError> {
        let mut request = spark::InterruptRequest {
            session_id: self.session_id(),
            user_context: self.user_context.clone(),
            client_type: self.user_agent.clone(),
            interrupt_type: 0,
            interrupt: None,
        };

        match interrupt_type {
            spark::interrupt_request::InterruptType::All => {
                request.interrupt_type = interrupt_type.into();
            }
            spark::interrupt_request::InterruptType::Tag => {
                return Err(ClientError::new(ClientErrorKind::Unimplemented(
                    "Tag interrupts are not implemented!".to_string()
                )))
            }
            spark::interrupt_request::InterruptType::OperationId => {
                let op_id = id_or_tag.expect("Operation ID can not be empty");
                let interrupt = spark::interrupt_request::Interrupt::OperationId(op_id);
                request.interrupt_type = interrupt_type.into();
                request.interrupt = Some(interrupt);
            }
            spark::interrupt_request::InterruptType::Unspecified => {
                return Err(ClientError::new(ClientErrorKind::UnspecifiedInterruptRequest))
            }
        };

        let mut client = self.stub.write().await;
        let resp = client
            .interrupt(request.clone())
            .await
            .map_err(|status| {
                ClientError::new(ClientErrorKind::InterruptRequest { status, request })
            })?
            .into_inner();
        drop(client);
        
        self.handler_interrupt = InterruptHandler::default();
        self.handler_interrupt.interrupted_ids = resp.interrupted_ids;
        
        Ok(self)
    }

    fn execute_plan_request_with_metadata(&mut self) -> spark::ExecutePlanRequest {
        let operation_id = uuid::Uuid::new_v4().to_string();

        spark::ExecutePlanRequest {
            session_id: self.session_id(),
            user_context: self.user_context.clone(),
            operation_id: Some(operation_id),
            plan: None,
            client_type: self.user_agent.clone(),
            request_options: vec![spark::execute_plan_request::RequestOption {
                request_option: Some(
                    spark::execute_plan_request::request_option::RequestOption::ReattachOptions(
                        spark::ReattachOptions { reattachable: self.use_reattachable_execute },
                    ),
                ),
            }],
            tags: self.get_tags(),
        }
    }
    
    fn handle_execute_response(
        &mut self,
        resp: spark::ExecutePlanResponse
    ) -> Result<(), ClientError> {
        self.validate_session(&resp.session_id)?;

        self.operation_id = Some(resp.operation_id);
        self.response_id = Some(resp.response_id);

        if let Some(data) = resp.response_type {
            match data {
                ResponseType::ArrowBatch(res) => {
                    let (batches, total_count) = crate::io::deserialize(
                        res.data.as_slice(), res.row_count
                    )?;

                    self.handler_execute.batches.extend(batches);
                    self.handler_execute.total_count += total_count;
                }
                ResponseType::SqlCommandResult(sql_cmd) => {
                    self.handler_execute.relation = sql_cmd.clone().relation
                }
                // ResponseType::WriteStreamOperationStartResult(write_stream_op) => {
                //     self.handler.write_stream_operation_start_result = Some(write_stream_op)
                // }
                // ResponseType::StreamingQueryCommandResult(stream_qry_cmd) => {
                //     self.handler.streaming_query_command_result = Some(stream_qry_cmd)
                // }
                // ResponseType::GetResourcesCommandResult(resource_cmd) => {
                //     self.handler.get_resources_command_result = Some(resource_cmd)
                // }
                // ResponseType::StreamingQueryManagerCommandResult(stream_qry_mngr_cmd) => {
                //     self.handler.streaming_query_manager_command_result = Some(stream_qry_mngr_cmd)
                // }
                ResponseType::ResultComplete(_) => self.handler_execute.result_complete = true,
                _ => return Err(ClientError::new(ClientErrorKind::Unimplemented(
                    format!("Handling of plan response {data:?} not implemented!")
                )))
            }
        }
        Ok(())
    }

    /// Execute an [execution reattachment request](crate::spark::ReattachExecuteRequest).
    async fn reattach(&mut self) -> Result<(), ClientError> {
        let request = spark::ReattachExecuteRequest {
            session_id: self.session_id(),
            user_context: self.user_context.clone(),
            operation_id: self.operation_id.clone().unwrap(),
            client_type: self.user_agent.clone(),
            last_response_id: self.response_id.clone(),
        };

        let mut client = self.stub.write().await;
        let mut stream = client
            .reattach_execute(request.clone())
            .await
            .map_err(|status| {
                ClientError::new(ClientErrorKind::ReattachExecuteRequest { status, request })
            })?
            .into_inner();
        drop(client);

        self.process_stream(&mut stream).await?;
        
        if self.use_reattachable_execute && self.handler_execute.result_complete {
            self.release_all().await?;
        }

        Ok(())
    }
    
    async fn process_stream(
        &mut self, stream: &mut Streaming<spark::ExecutePlanResponse>
    ) -> Result<(), ClientError> {
        while let Some(_resp) = match stream.message().await {
            Ok(Some(msg)) => {
                self.handle_execute_response(msg.clone())?;
                Some(msg)
            }
            Ok(None) => {
                if self.use_reattachable_execute && !self.handler_execute.result_complete {
                    Box::pin(self.reattach()).await?;
                }
                None
            }
            Err(status) => {
                if self.use_reattachable_execute && self.response_id.is_some() {
                    self.release_until().await?;
                }
                return Err(ClientError::new(ClientErrorKind::Stream(status)));
            }
        } {}

        Ok(())
    }

    async fn release_until(&mut self) -> Result<(), ClientError> {
        let release_until = spark::release_execute_request::ReleaseUntil {
            response_id: self.response_id.clone().unwrap(),
        };

        self.release_execute(spark::release_execute_request::Release::ReleaseUntil(
            release_until,
        )).await
    }

    async fn release_all(&mut self) -> Result<(), ClientError> {
        let release_all = spark::release_execute_request::ReleaseAll {};

        self.release_execute(spark::release_execute_request::Release::ReleaseAll(
            release_all,
        )).await
    }

    async fn release_execute(
        &mut self,
        release: spark::release_execute_request::Release,
    ) -> Result<(), ClientError> {
        let mut client = self.stub.write().await;

        let request = spark::ReleaseExecuteRequest {
            session_id: self.session_id(),
            user_context: self.user_context.clone(),
            operation_id: self.operation_id.clone().unwrap(),
            client_type: self.user_agent.clone(),
            release: Some(release),
        };

        let _resp = client
            .release_execute(request.clone())
            .await
            .map_err(|status| {
                ClientError::new(ClientErrorKind::ReleaseExecuteRequest { status, request })
            })?
            .into_inner();

        Ok(())
    }
}


#[cfg(test)]
mod tests {
    use crate::test_utils::test_utils::setup_session;
    use crate::spark;

    /// Verify that the client correctly handles and reports errors, such as
    /// a session validation failure.
    #[tokio::test]
    async fn test_validate_session_error() {
        // Arrange: Start server and create a session
        let session = setup_session().await.expect("Failed to create Spark session");

        // Create a clone of the client and manually corrupt its session ID
        let mut client_with_bad_session = session.client()?.clone();
        client_with_bad_session.session_id = "invalid-session-id".to_string();

        // Act: Attempt to use the corrupted client. This will cause the real server
        // to return an error that Spark Connect may not map directly to a session
        // ID mismatch, but it will be an error nonetheless.
        let result = client_with_bad_session
            .analyze(spark::analyze_plan_request::Analyze::SparkVersion(
                spark::analyze_plan_request::SparkVersion {},
            ))
            .await;

        // Assert: The operation should fail.
        assert!(
            result.is_err(),
            "Expected an error due to invalid session ID"
        );
    }
    
    /// Verify that the client can send an interrupt request without errors.
    /// This tests the `SparkConnectClient::interrupt_request` method.
    #[tokio::test]
    async fn test_interrupt_all_request() {
        // Arrange: Start server and create a session
        let session = setup_session().await.expect("Failed to create Spark session");
        
        // Act: Send an "interrupt all" request. The server should accept this
        // command gracefully even if nothing is running.
        let mut client = session.client()?;
        let result = client
            .interrupt(spark::interrupt_request::InterruptType::All, None)
            .await
            .unwrap();
            
        // Assert: The request should succeed. The response may be empty.
        assert_eq!(result.session_id(), session.session_id());
    }
}