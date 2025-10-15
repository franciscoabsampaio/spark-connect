#![allow(rustdoc::invalid_html_tags)]

use crate::client::error::{ClientError, ClientErrorKind};

use std::collections::HashMap;
use std::env;
use std::str::FromStr;
use url::Url;
use uuid::Uuid;

pub(crate) type Host = String;
pub(crate) type Port = u16;
pub(crate) type UrlParse = (Host, Port, Option<HashMap<String, String>>);

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
/// - token — authentication token (converted to Bearer header);
/// - user_id — custom user identifier (defaults to $USER);
/// - user_agent — overrides the default Rust client identifier;
/// - session_id — UUID for reusing a session;
/// - use_ssl — enables TLS (requires `tls` feature).
///
/// End users should prefer [`SparkSessionBuilder`](crate::SparkSessionBuilder) instead.
#[derive(Clone, Debug)]
pub struct ChannelBuilder {
    pub(crate) host: Host,
    pub(crate) port: Port,
    pub(crate) session_id: Uuid,
    pub(crate) token: Option<String>,
    pub(crate) user_id: Option<String>,
    pub(crate) user_agent: Option<String>,
    pub(crate) use_ssl: bool,
    pub(crate) headers: Option<HashMap<String, String>>,
}

/// By default, connects to port 15002 on localhost.
impl Default for ChannelBuilder {
    fn default() -> Self {
        let connection = match env::var("SPARK_REMOTE") {
            Ok(conn) => conn.to_string(),
            Err(_) => "sc://localhost:15002".to_string(),
        };

        ChannelBuilder::new(&connection).unwrap()
    }
}

impl ChannelBuilder {
    /// Create builder and validate a connection string.
    #[allow(unreachable_code)]
    pub(crate) fn new(connection: &str) -> Result<ChannelBuilder, ClientError> {
        let (host, port, headers) = ChannelBuilder::parse_connection_string(connection)?;

        let mut channel_builder = ChannelBuilder {
            host,
            port,
            session_id: Uuid::new_v4(),
            token: None,
            user_id: ChannelBuilder::create_user_id(None),
            user_agent: ChannelBuilder::create_user_agent(None),
            use_ssl: false,
            headers: None,
        };

        if let Some(mut headers) = headers {
            channel_builder.user_id = headers
                .remove("user_id")
                .map(|user_id| ChannelBuilder::create_user_id(Some(&user_id)))
                .unwrap_or_else(|| ChannelBuilder::create_user_id(None));

            channel_builder.user_agent = headers
                .remove("user_agent")
                .map(|user_agent| ChannelBuilder::create_user_agent(Some(&user_agent)))
                .unwrap_or_else(|| ChannelBuilder::create_user_agent(None));

            if let Some(token) = headers.remove("token") {
                let token = format!("Bearer {token}");
                channel_builder.token = Some(token.clone());
                headers.insert("authorization".to_string(), token);
            }

            if let Some(session_id) = headers.remove("session_id") {
                channel_builder.session_id = Uuid::from_str(&session_id)
                    .map_err(|source|
                        ClientError::new(ClientErrorKind::InvalidSessionID {
                            source, session_id
                        })
                    )?
            }

            if let Some(use_ssl) = headers.remove("use_ssl") {
                if use_ssl.to_lowercase() == "true" {
                    #[cfg(not(feature = "tls"))]
                    {
                        panic!("The 'use_ssl' option requires the 'tls' feature, but it's not enabled!");
                    };
                    channel_builder.use_ssl = true
                }
            };

            if !headers.is_empty() {
                channel_builder.headers = Some(headers);
            }
        }

        Ok(channel_builder)
    }

    pub(crate) fn endpoint(&self) -> String {
        let scheme = if cfg!(feature = "tls") {
            "https"
        } else {
            "http"
        };

        format!("{}://{}:{}", scheme, self.host, self.port)
    }

    pub(crate) fn headers(&self) -> Option<HashMap<String, String>> {
        self.headers.to_owned()
    }

    pub(crate) fn create_user_agent(user_agent: Option<&str>) -> Option<String> {
        let user_agent = user_agent.unwrap_or("_SPARK_CONNECT_RUST");
        let pkg_version = env!("CARGO_PKG_VERSION");
        let os = env::consts::OS.to_lowercase();

        Some(format!(
            "{} os/{} spark_connect/{}",
            user_agent, os, pkg_version
        ))
    }

    pub(crate) fn create_user_id(user_id: Option<&str>) -> Option<String> {
        match user_id {
            Some(user_id) => Some(user_id.to_string()),
            None => env::var("USER").ok(),
        }
    }

    pub(crate) fn parse_connection_string(connection: &str) -> Result<UrlParse, ClientError> {
        let url = Url::parse(connection)
            .map_err(|source| {
                ClientError::new(ClientErrorKind::InvalidConnectionString {
                    source: Some(source), conn_string: connection.to_string(), msg: "failed to parse connection string".to_string()
                })
            }
        )?;

        if url.scheme() != "sc" {
            return Err(ClientError::new(ClientErrorKind::InvalidConnectionString {
                source: None, conn_string: connection.to_string(), msg: "the connection string must start with 'sc://'".to_string()
            }))
        };

        let host = url
            .host_str()
            .ok_or_else(|| {
                ClientError::new(ClientErrorKind::InvalidConnectionString  {
                    source: None,
                    conn_string: connection.to_string(),
                    msg: "the hostname must not be empty"
                        .to_string(),
                })
            })?
            .to_string();

        let port = url
            .port()
            .ok_or_else(|| {
                ClientError::new(ClientErrorKind::InvalidConnectionString  {
                    source: None,
                    conn_string: connection.to_string(),
                    msg: "the port must not be empty."
                        .to_string(),
                })
        })?;

        let headers = ChannelBuilder::parse_headers(url);

        Ok((host, port, headers))
    }

    pub(crate) fn parse_headers(url: Url) -> Option<HashMap<String, String>> {
        let path: Vec<&str> = url
            .path()
            .split(';')
            .filter(|&pair| (pair != "/") & (!pair.is_empty()))
            .collect();

        if path.is_empty() || (path.len() == 1 && (path[0].is_empty() || path[0] == "/")) {
            return None;
        }

        let headers: HashMap<String, String> = path
            .iter()
            .copied()
            .map(|pair| {
                let mut parts = pair.splitn(2, '=');
                (
                    parts.next().unwrap_or("").to_string(),
                    parts.next().unwrap_or("").to_string(),
                )
            })
            .collect();

        if headers.is_empty() {
            return None;
        }

        Some(headers)
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
        let connection = "sc://myhost.com:443/;token=ABCDEFG;user_agent=some_agent;user_id=user123";
        let builder = ChannelBuilder::new(connection).unwrap();

        assert_eq!(builder.endpoint(), "http://myhost.com:443");
        assert_eq!(builder.token.unwrap(), "Bearer ABCDEFG");
        assert_eq!(builder.user_id.unwrap(), "user123");
        assert!(builder.user_agent.unwrap().contains("some_agent"));
    }

    #[test]
    #[should_panic(
        expected = "The 'use_ssl' option requires the 'tls' feature, but it's not enabled!"
    )]
    fn test_panic_ssl() {
        let connection = "sc://127.0.0.1:443/;use_ssl=true";

        ChannelBuilder::new(connection).unwrap();
    }
}
