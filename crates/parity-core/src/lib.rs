//! Runtime types for the parity tracker.
//!
//! Annotate items with [`parity_impl`] / [`parity`] (re-exported from `parity-macros`)
//! to register a [`ParityEntry`] in the global [`inventory`] registry. A binary
//! (e.g. `parity-dump`) can then iterate all entries and emit a report.

pub use inventory;
pub use parity_macros::{parity, parity_impl};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Implemented,
    Partial,
    Unimplemented,
}

impl Status {
    pub fn as_str(&self) -> &'static str {
        match self {
            Status::Implemented => "implemented",
            Status::Partial => "partial",
            Status::Unimplemented => "unimplemented",
        }
    }
}

#[derive(Debug)]
pub struct ParityEntry {
    pub pyspark: &'static str,
    pub rust: &'static str,
    pub status: Status,
    pub since: Option<&'static str>,
    pub comment: Option<&'static str>,
    pub issue: Option<u32>,
    pub spark_versions: &'static [&'static str],
}

inventory::collect!(ParityEntry);