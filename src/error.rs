use crate::{client::ClientError, conf::SparkConfError};

use core::fmt;
use std::error::Error;

/// Wraps application errors into a common SparkError enum.
#[derive(Debug)]
pub struct SparkError {
    pub(crate) kind: SparkErrorKind,
}

impl SparkError {
    pub(crate) fn new(kind: SparkErrorKind) -> Self {
        SparkError { kind }
    }
}

impl fmt::Display for SparkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SparkError: {}", self.kind)
    }
}

impl Error for SparkError {
	fn source(&self) -> Option<&(dyn Error + 'static)> {
		Some(&self.kind)
	}
}

impl From<ClientError> for SparkError {
    fn from(error: ClientError) -> Self {
        SparkError::new(SparkErrorKind::Client(error))
    }
}

impl From<SparkConfError> for SparkError {
    fn from(error: SparkConfError) -> Self {
        SparkError::new(SparkErrorKind::Config(error))
    }
}

#[derive(Debug)]
pub(crate) enum SparkErrorKind {
    Client(ClientError),
    ClientNotFound,
    Config(SparkConfError),
    Unimplemented(String)
}

impl fmt::Display for SparkErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Client(e) => write!(f, "Client error: {}", e),
            Self::ClientNotFound => write!(f, "Client not found. Please configure a remote Spark session."),
            Self::Config(e) => write!(f, "Spark configuration is invalid: {}", e),
            Self::Unimplemented(msg) => write!(f, "Unimplemented: {}", msg),
        }
    }
}

impl Error for SparkErrorKind {
	fn source(&self) -> Option<&(dyn Error + 'static)> {
		match self {
			Self::Client(source) => Some(source),
			Self::Config(source) => Some(source),
            _ => None
		}
	}
}