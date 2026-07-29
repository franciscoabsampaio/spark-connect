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
    ColumnIndexOutOfBounds { index: usize, num_columns: usize },
    ColumnNotFound(String),
    ColumnTypeMismatch { index: usize, expected: &'static str, found: arrow::datatypes::DataType },
    EmptyResult,
    InvalidField(String),
    InvalidObservationName,
    NullValue { index: usize },
    ObservationNotReady,
    RowIndexOutOfBounds { index: usize, num_rows: usize },
    Unimplemented(String)
}

impl fmt::Display for SparkErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Client(e) => write!(f, "Client error: {}", e),
            Self::ClientNotFound => write!(f, "Client not found. Please configure a remote Spark session."),
            Self::Config(e) => write!(f, "Spark configuration is invalid: {}", e),
            Self::ColumnIndexOutOfBounds { index, num_columns } => write!(f, "Column index {} out of bounds: batch has {} columns", index, num_columns),
            Self::ColumnNotFound(name) => write!(f, "Column not found: {}", name),
            Self::ColumnTypeMismatch { index, expected, found } => write!(f, "Column {} type mismatch: expected {}, found {:?}", index, expected, found),
            Self::EmptyResult => write!(f, "Result is empty: no record batches returned"),
            Self::InvalidField(field) => write!(f, "Invalid field: {}", field),
            Self::InvalidObservationName => write!(f, "Invalid observation name: must not be empty"),
            Self::NullValue { index } => write!(f, "Column {} is null at the requested row", index),
            Self::ObservationNotReady => write!(f, "Cannot retrieve observation result before it is complete"),
            Self::RowIndexOutOfBounds { index, num_rows } => write!(f, "Row index {} out of bounds: column has {} rows", index, num_rows),
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