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
        let url = Url::parse(value).map_err(|_| SparkConfError::InvalidConnectionUri {
            uri: value.to_string(),
            message: "Failed to parse URI".into(),
        })?;

        if url.scheme() != "sc" {
            return Err(SparkConfError::InvalidConnectionUri {
                uri: value.to_string(),
                message: "Remote URI must use the sc:// scheme".into(),
            });
        }

        let host = url.host_str()
            .filter(|h| !h.is_empty())
            .ok_or_else(|| SparkConfError::InvalidConnectionUri {
                uri: value.to_string(),
                message: "The hostname must not be empty".into(),
            })?
            .to_string();

        let port = url.port().ok_or_else(|| SparkConfError::InvalidConnectionUri {
            uri: value.to_string(),
            message: "The port must not be empty".into(),
        })?;

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
                let uri = Url::parse(master).map_err(|e| SparkConfError::InvalidConnectionUri {
                    uri: master.to_string(),
                    message: e.to_string(),
                })?;
                if uri.scheme() == "sc" {
                    return Err(SparkConfError::InvalidConnectionUri {
                        uri: master.to_string(),
                        message: "Spark Connect mode is not supported for master URIs".into(),
                    });
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
    InvalidConnectionUri { uri: String, message: String },
    MissingConnectionUri,
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
            Self::InvalidConnectionUri { uri, message } => {
                write!(f, "Connection URI is invalid: '{uri}': {message}")
            },
            Self::MissingConnectionUri => write!(
                f, "No connection URI is set. Please set either SPARK_REMOTE, MASTER, or use .remote(), .master()"
            ),
        }
    }
}

impl Error for SparkConfError {
	fn source(&self) -> Option<&(dyn Error + 'static)> {
		Some(self)
	}
}