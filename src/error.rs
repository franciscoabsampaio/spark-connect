use crate::client::ClientError;

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

#[derive(Debug)]
pub(crate) enum SparkErrorKind {
    Client(ClientError),
    InvalidConnectionUri { source: http::uri::InvalidUri, uri: String },
    Transport(tonic::transport::Error)
}

impl fmt::Display for SparkErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Client(_) => write!(f, "Client error"),
            Self::InvalidConnectionUri { uri, .. } => write!(f, "Connection URI is invalid: '{uri}'"),
            Self::Transport(_) => write!(f, "Tonic transport error")
        }
    }
}

impl Error for SparkErrorKind {
	fn source(&self) -> Option<&(dyn Error + 'static)> {
		match self {
			Self::Client(source) => Some(source),
			Self::InvalidConnectionUri { source, .. } => Some(source),
			Self::Transport(source) => Some(source),
		}
	}
}