use crate::io::IoError;
use crate::spark;

use std::error::Error;
use std::fmt;
use url;


#[derive(Debug)]
#[non_exhaustive]
pub(crate) struct ClientError {
    pub(crate) kind: ClientErrorKind
}

impl ClientError {
    pub(crate) fn new(kind: ClientErrorKind) -> Self {
        ClientError { kind }
    }
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ClientError: {}", self.kind)
    }
}

impl Error for ClientError {
	fn source(&self) -> Option<&(dyn Error + 'static)> {
		Some(&self.kind)
	}
}

#[derive(Debug)]
pub(crate) enum ClientErrorKind {
    AnalyzeRequest { status: tonic::Status, request: spark::AnalyzePlanRequest },
    AnalyzeResponseNotFound(String),
    ExecutePlanRequest { status: tonic::Status, request: spark::ExecutePlanRequest },
    InterruptRequest { status: tonic::Status, request: spark::InterruptRequest },
    InvalidSessionID { source: uuid::Error, session_id: String },
    InvalidConnectionString { source: Option<url::ParseError>, conn_string: String,  msg: String },
    Io(IoError),
    ReattachExecuteRequest { status: tonic::Status, request: spark::ReattachExecuteRequest },
    ReleaseExecuteRequest { status: tonic::Status, request: spark::ReleaseExecuteRequest },
    SessionIDMismatch { client_session_id: String, request_session_id: String },
    Stream(tonic::Status),
    Unimplemented(String),
    UnspecifiedInterruptRequest
}

impl fmt::Display for ClientErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AnalyzeRequest { status, request } => write!(
                f, "AnalyzeRequest failed with status '{status}': {request:?}"
            ),
            Self::AnalyzeResponseNotFound(msg) => write!(f, "No analyze response found: {msg}."),
            Self::ExecutePlanRequest { status, request } => write!(
                f, "ExecutePlanRequest failed with status '{status}': {request:?}"
            ),
            Self::InterruptRequest { status, request } => write!(
                f, "InterruptRequest failed with status '{status}': {request:?}"
            ),
            Self::InvalidSessionID { session_id, .. } => write!(f, "Failed to parse session ID: '{session_id}'"),
            Self::InvalidConnectionString { conn_string, msg, .. } => write!(
                f, "Failed to parse the connection URL '{conn_string}': {msg}. Please update the URL to follow the correct format, e.g., 'sc://hostname:port'."
            ),
            Self::Io(_) => write!(f, "Failed to deserialize Arrow RecordBatch."),
            Self::ReattachExecuteRequest { status, request } => write!(
                f, "ReattachExecuteRequest failed with status '{status}': {request:?}"
            ),
            Self::ReleaseExecuteRequest { status, request } => write!(
                f, "ReleaseExecuteRequest failed with status '{status}': {request:?}"
            ),
            Self::SessionIDMismatch { client_session_id, request_session_id } => write!(
                f, "Request session ID does not match the client: {client_session_id} != {request_session_id}"
            ),
            Self::Stream(status) => write!(f, "Failed to process stream: status {status}"),
            Self::Unimplemented(msg) => write!(f, "{msg}"),
            Self::UnspecifiedInterruptRequest => write!(f, "Interrupt Type was not specified."),
        }
    }
}

impl Error for ClientErrorKind {
	fn source(&self) -> Option<&(dyn Error + 'static)> {
		match self {
			Self::InvalidSessionID { source, .. } => Some(source),
			Self::InvalidConnectionString { source, .. } => match source {
                Some(src) => Some(src),
                None => None
            },
			Self::Io(source) => Some(source),
			_ => None,
		}
	}
}

impl From<IoError> for ClientError {
    fn from(error: IoError) -> Self {
        ClientError::new(ClientErrorKind::Io(error))
    }
}