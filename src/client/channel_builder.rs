#![allow(rustdoc::invalid_html_tags)]

use std::collections::HashMap;
use std::env::consts;
use std::str::FromStr;
use api_parity_rs::{parity, parity_impl};
use tonic::transport::{Channel, ClientTlsConfig}; 
use uuid::Uuid;

use crate::SPARK_VERSION;
use crate::conf::SparkRemoteConf;
use crate::spark::spark_connect_service_client::SparkConnectServiceClient;
use super::error::{ClientError, ClientErrorKind};
use super::middleware::HeaderInterceptor;

/// Utility type alias for a gRPC channel with an attached interceptor.
type InterceptedChannel = tonic::service::interceptor::InterceptedService<Channel, HeaderInterceptor>;
pub(crate) type SparkGrpcClient = SparkConnectServiceClient<InterceptedChannel>;

/// Parses and validates Spark Connect connection strings.
///
/// ChannelBuilder is used internally by SparkSessionBuilder
/// to configure connections according to the
/// [Spark Connect client connection specification](https://github.com/apache/spark/blob/master/connector/connect/docs/client-connection-string.md).
///
/// It extracts host, port, and optional parameters from URLs of the form:
///
/// `sc://<host>:<port>/;key1=value1;key2=value2;...`
///
/// Supported keys include:
/// - token - authentication token (converted to Bearer header);
/// - user_id - custom user identifier (defaults to $USER);
/// - user_agent - overrides the default Rust client identifier;
/// - session_id - UUID for reusing a session;
///
/// End users should prefer [`SparkSessionBuilder`](crate::SparkSessionBuilder) instead.
#[derive(Clone, Debug)]
pub struct ChannelBuilder {
    host: String,
    port: u16,
    session_id: Option<Uuid>,
    token: Option<String>,
    use_ssl: bool,
    user_id: String,
    user_agent: Option<String>,
    metadata: HashMap<String, String>,
}

/// By default, attempts to get the connection string from the SPARK_REMOTE
/// environment variable. If not set, defaults to a connection string that
/// connects to port 15002 on localhost.
impl Default for ChannelBuilder {
    fn default() -> ChannelBuilder {
        ChannelBuilder {
            host: "localhost".into(),
            port: ChannelBuilder::default_port(),
            session_id: None,
            token: None,
            use_ssl: false,
            user_id: String::new(),
            user_agent: None,
            metadata: HashMap::new(),
        }
    }
}
#[parity_impl(
    path = "pyspark.sql.connect.client.core.ChannelBuilder",
    status = Implemented,
)]
impl ChannelBuilder {
    #[parity(
        path = ".default_port",
        status = Implemented,
    )]
    pub fn default_port() -> u16 {
        15002
    }

    pub(crate) fn config(mut self, conf: &SparkRemoteConf) -> Result<Self, ClientError> {
        self.host = conf.uri.host.clone();
        self.port = conf.uri.port;
        if let Some(user_id) = conf.user_id.clone() {
            self.user_id = user_id;
        }
        self.user_agent = conf.user_agent.clone();

        // Extract known headers and remove them from the map.
        if let Some(mut headers) = conf.uri.headers.clone() {
            self.user_id = headers.remove("user_id").unwrap_or_default();
            self.user_agent = headers.remove("user_agent");
            self.token = headers.remove("token");

            if let Some(session_id) = headers.remove("session_id") {
                self.session_id = Some(Uuid::from_str(&session_id)
                    .map_err(|source|
                        ClientError::new(ClientErrorKind::InvalidSessionID {
                            source, session_id
                        })
                    )?)
            }

            if let Some(use_ssl) = headers.remove("use_ssl") {
                self.use_ssl = use_ssl.to_lowercase() == "true";
            }

            // Token can only be provided if 'use_ssl' is set to 'true'
            if self.token.is_some() && !self.use_ssl {
                return Err(ClientError::new(ClientErrorKind::TokenRequiresSSL));
            }

            // Add any remaining custom headers.
            if !headers.is_empty() {
                self.metadata = headers;
            }
        }

        // Set missing metadata fields
        self.metadata.insert("authorization".into(), self.token());
        self.metadata.insert("user_agent".into(), self.user_agent()?);
        self.metadata.insert("x-spark-connect-user-id".into(), self.user_id());

        Ok(self)
    }

    #[parity(
        path = ".get",
        status = Implemented,
    )]
    pub fn get(&self, key: &str) -> Option<String> {
        self.metadata.get(key).cloned()
    }

    #[parity(
        path = ".endpoint",
        status = Implemented,
    )]
    pub fn endpoint(&self) -> String {
        let scheme = if self.secure() { "https" } else { "http" };
        format!("{}://{}:{}", scheme, self.host, self.port)
    }

    pub(crate) fn host(&self) -> String {
        self.host.clone()
    }

    pub(crate) fn token(&self) -> String {
        let token = self.token.clone().unwrap_or_default();
        format!("Bearer {}", token)
    }

    #[parity(
        path = ".metadata",
        status = Partial,
        comment = "This implementation returns all metadata, whereas the original implementation does not return parameters that are explicitly used by the channel."
    )]
    pub fn metadata(&self) -> HashMap<String, String> {
        self.metadata.clone()
    }

    #[parity(
        path = ".secure",
        status = Implemented,
    )]
    pub fn secure(&self) -> bool {
        self.use_ssl
    }

    #[parity(
        path = ".session_id",
        status = Implemented,
    )]
    pub fn session_id(&self) -> Uuid {
        if let Some(session_id) = self.session_id {
            session_id
        } else {
            Uuid::new_v4()
        }
    }

    #[parity(
        path = ".userAgent",
        status = Implemented,
    )]
    pub fn user_agent(&self) -> Result<String, ClientError> {
        // The leading underscore distinguishes internal/default user agents
        // from user-defined ones.
        let user_agent = self.user_agent.clone().unwrap_or("_SPARK_CONNECT_RUST".into());
        let pkg_version = env!("CARGO_PKG_VERSION");
        let os = consts::OS.to_lowercase();

        if user_agent.len() > 2048 {
            return Err(ClientError::new(ClientErrorKind::UserAgentTooLong {
                user_agent: user_agent.clone()
            }));
        }

        Ok(format!(
            "{} spark/{} os/{} pkg/{}",
            user_agent, SPARK_VERSION, os, pkg_version
        ))
    }

    #[parity(
        path = ".userId",
        status = Implemented,
    )]
    pub fn user_id(&self) -> String {
        self.user_id.clone()
    }

    /// Create gRPC channel.
    /// 
    /// Applies the parameters of the connection string and creates a new
    /// gRPC channel according to the configuration.
    /// Passes optional channel options to construct the channel.
    #[parity(
        path = ".toChannel",
        status = Partial,
        comment = "Creates a gRPC client from the channel builder, instead of a channel."
    )]
    pub async fn to_client(&self) -> Result<SparkGrpcClient, ClientError> {
        let mut endpoint = Channel::from_shared(self.endpoint())
            .map_err(|source| ClientError::new(
                ClientErrorKind::InvalidConnectionString {
                    source,
                    conn_string: self.endpoint(),
                    msg: "Invalid connection string".into()
                }
            ))?;

        // 2. Configure TLS
        if self.secure() {
            let mut tls_config = ClientTlsConfig::new().domain_name(&self.host);
            
            // Optional: Configure CA certificate if provided
            // (Assuming you add `ca_cert_pem` to your ChannelBuilder or Conf)
            /*
            if let Some(ca_pem) = &self.ca_cert_pem {
                let cert = Certificate::from_pem(ca_pem);
                tls_config = tls_config.ca_certificate(cert);
            } else {
                tls_config = tls_config.with_native_roots();
            }
            */
            tls_config = tls_config.with_native_roots();

            // Optional: Configure mTLS if provided
            /*
            if let Some((cert_pem, key_pem)) = &self.client_identity_pem {
                let identity = Identity::from_pem(cert_pem, key_pem);
                tls_config = tls_config.identity(identity);
            }
            */

            endpoint = endpoint.tls_config(tls_config).map_err(|source| {
                ClientError::new(ClientErrorKind::Transport(source))
            })?;
        }
        // If !self.secure(), `endpoint` remains insecure

        // 3. Connect to the endpoint
        let channel = endpoint.connect().await.map_err(|source| {
            ClientError::new(ClientErrorKind::Transport(source))
        })?;
        
        // 4. Wrap the channel with the interceptor
        let interceptor = HeaderInterceptor::new(self.metadata());
        let grpc_client = SparkConnectServiceClient::with_interceptor(channel, interceptor);

        // 5. Return the gRPC client
        Ok(grpc_client)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_channel_builder_default() {
        let expected_url = "http://localhost:15002".to_string();

        let cb = ChannelBuilder::default();

        assert_eq!(expected_url, cb.endpoint())
    }

    #[test]
    fn test_invalid_scheme_error() {
        let connection = "http://127.0.0.1:15002";
        let err = ChannelBuilder::new(connection).unwrap_err();
        match err.kind {
            ClientErrorKind::InvalidConnectionString { msg, conn_string, source } => {
                assert!(msg.contains("must start with 'sc://'"));
                assert_eq!(conn_string, connection);
                assert!(source.is_none());
            }
            other => panic!("unexpected error kind: {other:?}"),
        }
    }

    #[test]
    fn test_missing_host_error() {
        let connection = "sc://:15002";
        let err = ChannelBuilder::new(connection).unwrap_err();
        match err.kind {
            ClientErrorKind::InvalidConnectionString { msg, conn_string, source } => {
                assert!(msg.contains("failed to parse"));
                assert_eq!(conn_string, connection);
                assert!(source.is_some());
            }
            other => panic!("unexpected error kind: {other:?}"),
        }
    }

    #[test]
    fn test_missing_port_error() {
        let connection = "sc://127.0.0.1";
        let err = ChannelBuilder::new(connection).unwrap_err();
        match err.kind {
            ClientErrorKind::InvalidConnectionString { msg, conn_string, source } => {
                assert!(msg.contains("port must not be empty"));
                assert_eq!(conn_string, connection);
                assert!(source.is_none());
            }
            other => panic!("unexpected error kind: {other:?}"),
        }
    }

    #[test]
    fn test_valid_connection_builds() {
        let connection = "sc://myhost.com:443/;user_agent=some_agent;user_id=user123";
        let builder = ChannelBuilder::new(connection).unwrap();

        assert_eq!(builder.endpoint(), "http://myhost.com:443");
        assert_eq!(builder.user_id.unwrap(), "user123");
        assert!(builder.user_agent.unwrap().contains("some_agent"));
    }

    #[test]
    fn test_invalid_token_usage_error() {
        let connection = "sc://myhost.com:443/;token=1234;use_ssl=false";
        let err = ChannelBuilder::new(connection).unwrap_err();
        match err.kind {
            ClientErrorKind::TokenRequiresSSL => {}
            other => panic!("unexpected error kind: {other:?}"),
        }
    }
}
