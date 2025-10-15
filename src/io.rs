use arrow::array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use std::error::Error;
use std::fmt;


#[derive(Debug)]
pub(crate) enum IoError {
    Arrow(arrow::error::ArrowError),
    RowCount { expected: i64, got: i64 }
}

impl fmt::Display for IoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Arrow(_) => write!(f, "IoError: ArrowError"),
            Self::RowCount { expected, got } => write!(f, "IoError: Expected {expected} rows in arrow batch but got {got}.")
        }
    }
}

impl Error for IoError {
	fn source(&self) -> Option<&(dyn Error + 'static)> {
		match self {
			Self::Arrow(source) => Some(source),
			_ => None,
		}
	}
}

impl From<arrow::error::ArrowError> for IoError {
    fn from(error: arrow::error::ArrowError) -> Self {
        IoError::Arrow(error)
    }
}

pub(crate) fn deserialize(stream: &[u8], row_count: i64) -> Result<(Vec<RecordBatch>, isize), IoError> {
    let reader = StreamReader::try_new(stream, None)?;
    
    let mut batches: Vec<RecordBatch> = vec![];
    let mut total_count: isize = 0;

    for batch in reader {
        let record = batch?;
        if record.num_rows() != row_count as usize {
            return Err(IoError::RowCount { expected: row_count, got: record.num_rows() as i64 });
        };
        batches.push(record);
        total_count += row_count as isize;
    }

    Ok((batches, total_count))
}