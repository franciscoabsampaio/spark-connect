//! Iterates every `ParityEntry` registered across the crate and emits them as JSON,
//! stamped with the Spark version this binary was compiled against.
//!
//! Run with:
//! ```bash
//! cargo run --bin api_parity-dump > target/api_parity.json
//! ```
//!
//! For multi-version coverage, build once per Spark feature and merge the outputs
//! downstream. The JSON envelope looks like:
//!
//! ```json
//! {
//!   "spark_version": "3.5.7",
//!   "entries": [ ... ]
//! }
//! ```
//!
//! The resulting JSON is consumed by a separate tool (future work) that joins it
//! against a canonical PySpark API inventory and produces a human-readable matrix.

use api_parity_core::{inventory, ParityEntry};

// Touch the lib crate so its api_parity annotations are linked in.
#[allow(unused_imports)]
use spark_connect as _;

fn json_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn opt_str(v: Option<&str>) -> String {
    match v {
        Some(s) => json_str(s),
        None => "null".into(),
    }
}

fn opt_u32(v: Option<u32>) -> String {
    match v {
        Some(n) => n.to_string(),
        None => "null".into(),
    }
}

fn main() {
    let mut entries: Vec<&ParityEntry> = inventory::iter::<ParityEntry>.into_iter().collect();
    entries.sort_by_key(|e| e.reference);

    println!("{{");
    println!("  \"spark_version\": {},", json_str(spark_connect::SPARK_VERSION));
    println!("  \"entries\": [");
    for (i, e) in entries.iter().enumerate() {
        print!(
            "    {{\"reference\":{},\"implementation\":{},\"status\":{},\"since\":{},\"comment\":{},\"issue\":{}}}",
            json_str(e.reference),
            json_str(e.implementation),
            json_str(e.status.as_str()),
            opt_str(e.since),
            opt_str(e.comment),
            opt_u32(e.issue),
        );
        if i + 1 < entries.len() {
            println!(",");
        } else {
            println!();
        }
    }
    println!("  ]");
    println!("}}");
}
