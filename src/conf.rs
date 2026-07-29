use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::fmt;
use url::Url;

/// A validated sc:// URI, guaranteed to have host, port, and optional headers.
#[derive(Debug, Clone)]
pub(crate) struct SparkRemoteUri {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) headers: Option<HashMap<String, String>>,
}

impl SparkRemoteUri {
    pub(crate) fn parse(value: &str) -> Result<Self, SparkConfError> {
        let url = Url::parse(value)
            .map_err(|source| SparkConfError::unparseable_uri(value, source))?;

        if url.scheme() != "sc" {
            return Err(SparkConfError::invalid_uri(
                value,
                "Remote URI must use the sc:// scheme",
            ));
        }

        let host = url.host_str()
            .filter(|h| !h.is_empty())
            .ok_or_else(|| SparkConfError::invalid_uri(value, "The hostname must not be empty"))?
            .to_string();

        let port = url.port()
            .ok_or_else(|| SparkConfError::invalid_uri(value, "The port must not be empty"))?;

        let headers = Self::parse_headers(&url);

        Ok(SparkRemoteUri { host, port, headers })
    }

    fn parse_headers(url: &Url) -> Option<HashMap<String, String>> {
        let headers: HashMap<String, String> = url
            .path()
            .split(';')
            .filter(|&s| s != "/" && !s.is_empty())
            .filter_map(|pair| {
                let (k, v) = pair.split_once('=')?;
                Some((k.to_lowercase(), v.to_string()))
            })
            .collect();

        (!headers.is_empty()).then_some(headers)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SparkRemoteConf {
    pub(crate) uri: SparkRemoteUri,
    pub(crate) user_id: Option<String>,
    pub(crate) user_agent: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) enum ResolvedSparkConf {
    Remote(SparkRemoteConf),
    Master(String),
}

#[derive(Debug, Clone)]
pub enum SparkMaster {
    Remote(String),
    Master(String),
}

#[derive(Debug)]
pub enum SparkConfKey {
    AppName,
    CatalogImplementation,
    GrpcSslEnabled,
    GrpcSslTrustCertCollectionFile,
    Remote,
    Master,
    User,
    UserAgent
}

#[derive(Clone, Debug, Default)]
pub struct SparkConf {
    pub master: Option<SparkMaster>,
    pub app_name: Option<String>,
    pub catalog_implementation: Option<String>,
    pub grpc_ssl_enabled: Option<bool>,
    pub grpc_ssl_trust_cert_collection_file: Option<String>,
    pub user_id: Option<String>,
    pub user_agent: Option<String>,
}

impl SparkConf {
    pub fn new() -> Self {
        Self::default()
    }

    fn set_master(&mut self, incoming: SparkMaster) -> Result<(), SparkConfError> {
        match (&incoming, &self.master) {
            (SparkMaster::Remote(r), Some(SparkMaster::Master(m))) => {
                Err(SparkConfError::ConflictingMaster {
                    master: m.clone(),
                    remote: r.clone(),
                })
            }
            (SparkMaster::Master(m), Some(SparkMaster::Remote(r))) => {
                Err(SparkConfError::ConflictingMaster {
                    master: m.clone(),
                    remote: r.clone(),
                })
            }
            _ => {
                self.master = Some(incoming);
                Ok(())
            }
        }
    }

    pub(crate) fn set(&mut self, key: SparkConfKey, value: String) -> Result<(), SparkConfError> {
        match key {
            SparkConfKey::AppName => self.app_name = Some(value),
            SparkConfKey::CatalogImplementation => self.catalog_implementation = Some(value),
            SparkConfKey::GrpcSslEnabled => {
                self.grpc_ssl_enabled =
                    Some(value.parse::<bool>().map_err(|_| SparkConfError::InvalidBooleanValue {
                        key,
                        value,
                    })?)
            }
            SparkConfKey::GrpcSslTrustCertCollectionFile => {
                self.grpc_ssl_trust_cert_collection_file = Some(value)
            }
            SparkConfKey::Remote => self.set_master(SparkMaster::Remote(value))?,
            SparkConfKey::Master => self.set_master(SparkMaster::Master(value))?,
            SparkConfKey::User => self.user_id = Some(value),
            SparkConfKey::UserAgent => self.user_agent = Some(value),
        }
        Ok(())
    }

    /// Consumes the configuration to resolve it.
    /// Resolve the configuration by checking for conflicts and setting defaults.
    /// If a configuration value is not set, it will attempt to read from environment variables.
    pub(crate) fn resolve(&mut self) -> Result<ResolvedSparkConf, SparkConfError> {
        if self.master.is_none() {
            if let Ok(value) = env::var("SPARK_REMOTE") {
                self.set(SparkConfKey::Remote, value)?;
            }
            if let Ok(value) = env::var("MASTER") {
                self.set(SparkConfKey::Master, value)?;
            }
        }
        if self.user_id.is_none() {
            if let Ok(value) = env::var("USER") {
                self.set(SparkConfKey::User, value)?;
            }
        }
        if self.user_agent.is_none() {
            if let Ok(value) = env::var("SPARK_CONNECT_USER_AGENT") {
                self.set(SparkConfKey::UserAgent, value)?;
            }
        }

        match &self.master {
            Some(SparkMaster::Remote(remote)) => {
                Ok(ResolvedSparkConf::Remote(SparkRemoteConf {
                    uri: SparkRemoteUri::parse(remote)?,
                    user_id: self.user_id.clone(),
                    user_agent: self.user_agent.clone(),
                }))
            }
            Some(SparkMaster::Master(master)) => {
                let uri = Url::parse(master)
                    .map_err(|source| SparkConfError::unparseable_uri(master, source))?;
                if uri.scheme() == "sc" {
                    return Err(SparkConfError::invalid_uri(
                        master,
                        "Spark Connect mode is not supported for master URIs",
                    ));
                }
                Ok(ResolvedSparkConf::Master(master.clone()))
            }
            None => {
                Err(SparkConfError::MissingConnectionUri)
            }
        }
    }
}

#[derive(Debug)]
pub enum SparkConfError {
    ConflictingMaster { master: String, remote: String },
    InvalidBooleanValue { key: SparkConfKey, value: String },
    /// `source` is set only when the URI failed to parse at all; the other
    /// rejections (wrong scheme, missing port, ...) have no underlying error.
    InvalidConnectionUri { uri: String, message: String, source: Option<url::ParseError> },
    MissingConnectionUri,
}

impl SparkConfError {
    /// A URI rejected on its own terms, with no underlying error to report.
    fn invalid_uri(uri: &str, message: impl Into<String>) -> Self {
        Self::InvalidConnectionUri {
            uri: uri.to_string(),
            message: message.into(),
            source: None,
        }
    }

    /// A URI the parser could not read, preserving the parser's own error.
    fn unparseable_uri(uri: &str, source: url::ParseError) -> Self {
        Self::InvalidConnectionUri {
            uri: uri.to_string(),
            message: "Failed to parse URI".into(),
            source: Some(source),
        }
    }
}

impl fmt::Display for SparkConfError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SparkConfError::ConflictingMaster { master, remote } => {
                write!(f, "Conflicting master configurations: '{remote}' (remote) vs '{master}' (local)")
            },
            SparkConfError::InvalidBooleanValue { key, value } => {
                write!(f, "Invalid boolean value for '{key:?}': '{value}'")
            },
            Self::InvalidConnectionUri { uri, message, .. } => {
                write!(f, "Connection URI is invalid: '{uri}': {message}")
            },
            Self::MissingConnectionUri => write!(
                f, "No connection URI is set. Please set either SPARK_REMOTE, MASTER, or use .remote(), .master()"
            ),
        }
    }
}

impl Error for SparkConfError {
    /// Only a parse failure has an underlying cause; every other variant
    /// describes itself entirely through its own fields and terminates the
    /// chain with `None`.
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidConnectionUri { source, .. } => {
                source.as_ref().map(|e| e as &(dyn Error + 'static))
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Asserts `value` is rejected with an [`SparkConfError::InvalidConnectionUri`]
    /// whose message contains `expected`, and returns the underlying parse
    /// error, if any.
    fn assert_rejected(value: &str, expected: &str) -> Option<url::ParseError> {
        match SparkRemoteUri::parse(value).unwrap_err() {
            SparkConfError::InvalidConnectionUri { uri, message, source } => {
                assert_eq!(uri, value);
                assert!(
                    message.contains(expected),
                    "expected message containing {expected:?}, got {message:?}"
                );
                source
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn test_invalid_scheme_error() {
        assert!(assert_rejected("http://127.0.0.1:15002", "sc:// scheme").is_none());
    }

    /// An unparseable URI keeps the parser's own error as its source, so the
    /// specific reason survives past our own message.
    #[test]
    fn test_missing_host_error() {
        let source = assert_rejected("sc://:15002", "Failed to parse URI");

        assert_eq!(source, Some(url::ParseError::EmptyHost));
    }

    #[test]
    fn test_missing_port_error() {
        assert!(assert_rejected("sc://127.0.0.1", "The port must not be empty").is_none());
    }

    /// A `source` returning `self` makes any consumer walking the chain spin
    /// forever. Walking it here must terminate.
    #[test]
    fn test_error_source_chain_terminates() {
        let errors = [
            SparkConfError::ConflictingMaster {
                master: "local[4]".into(),
                remote: "sc://localhost:15002".into(),
            },
            SparkConfError::InvalidBooleanValue {
                key: SparkConfKey::GrpcSslEnabled,
                value: "yes".into(),
            },
            SparkConfError::invalid_uri("sc://127.0.0.1", "The port must not be empty"),
            SparkConfError::unparseable_uri("sc://:15002", url::ParseError::EmptyHost),
            SparkConfError::MissingConnectionUri,
        ];

        for error in &errors {
            let mut depth = 0;
            let mut current: Option<&dyn Error> = Some(error);
            while let Some(e) = current {
                depth += 1;
                assert!(depth <= 8, "{error:?} has a cyclic source chain");
                current = e.source();
            }
        }
    }

    #[test]
    fn test_valid_uri_parses_host_and_port() {
        let uri = SparkRemoteUri::parse("sc://myhost.com:443").unwrap();

        assert_eq!(uri.host, "myhost.com");
        assert_eq!(uri.port, 443);
        assert!(uri.headers.is_none());
    }

    #[test]
    fn test_headers_are_parsed_and_lowercased() {
        let uri = SparkRemoteUri::parse(
            "sc://myhost.com:443/;User_Id=user123;token=1234",
        )
        .unwrap();

        let headers = uri.headers.expect("expected headers");
        assert_eq!(headers.get("user_id").map(String::as_str), Some("user123"));
        assert_eq!(headers.get("token").map(String::as_str), Some("1234"));
    }
}