//! Iterate every `ParityEntry` registered across the crate and emit them as JSON,
//! stamped with the crate version.
//!
//! Run with:
//! ```bash
//! cargo run --features api-parity --bin api_parity_dump > parity/spark-connect.json
//! ```

use api_parity_core::{inventory, ParityEntry};
use serde::Serialize;

// Touch the lib crate so its api_parity annotations are linked in.
#[allow(unused_imports)]
use spark_connect as _;

#[derive(Serialize)]
struct EntryDto<'a> {
    reference: &'a str,
    implementation: &'a str,
    status: &'a str,
    since: Option<&'a str>,
    comment: Option<&'a str>,
    issue: Option<u32>,
}

#[derive(Serialize)]
struct Output<'a> {
    version: &'a str,
    entries: Vec<EntryDto<'a>>,
}

impl<'a> From<&'a ParityEntry> for EntryDto<'a> {
    fn from(e: &'a ParityEntry) -> Self {
        Self {
            reference: e.reference,
            implementation: e.implementation,
            status: e.status.as_str(),
            since: e.since,
            comment: e.comment,
            issue: e.issue,
        }
    }
}

fn main() {
    let mut entries: Vec<&ParityEntry> = inventory::iter::<ParityEntry>.into_iter().collect();
    entries.sort_by_key(|e| e.reference);

    let out = Output {
        version: env!("CARGO_PKG_VERSION"),
        entries: entries.iter().copied().map(EntryDto::from).collect(),
    };

    println!("{}", serde_json::to_string_pretty(&out).expect("serialize"));
}
