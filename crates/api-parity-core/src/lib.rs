//! Runtime types for the api_parity tracker.
//!
//! # How it works
//!
//! 1. Source code is annotated with `#[api_parity_impl]` (on `impl` blocks) or
//!    `#[api_parity(...)]` (on free functions). Those macros live in `api_parity-macros`
//!    and are re-exported below.
//! 2. Each annotation expands to an `inventory::submit! { ParityEntry { ... } }`
//!    call. The `inventory` crate uses link-time registration: each `submit!`
//!    drops a static into a special section, and `inventory::iter::<T>()`
//!    walks them at runtime. No central registry, no init order.
//! 3. A binary (e.g. `api_parity-dump`) iterates `inventory::iter::<ParityEntry>`
//!    and serializes the entries to JSON. That JSON is the "numerator" half
//!    of the api_parity report; the "denominator" comes from `pyspark_inventory.py`.
//!
//! The crate is intentionally domain-agnostic: `ParityEntry::reference` is
//! just an opaque string. It can name a package API, a REST endpoint, etc.

// Re-exported so the macros can refer to `::api_parity_core::inventory::submit!`
// without users having to add `inventory` as a direct dependency.
pub use inventory;
pub use api_parity_macros::{api_parity, api_parity_impl};

/// Implementation state of a tracked API.
///
/// `Unimplemented` is special-cased by the macros: it requires a `comment`
/// explaining *why* the stub exists, so reviewers get context at the call site.
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

/// One row in the api_parity table. All fields are `&'static str` so the struct
/// can be built in `inventory::submit!` (which requires a `const`-constructible
/// value).
#[derive(Debug)]
pub struct ParityEntry {
    /// Canonical name of the API being mirrored (e.g. `"SparkSession.sql"`).
    /// Free-form: the join script matches on this string.
    pub reference: &'static str,
    /// Path of the local implementation (e.g. `"SparkSession::sql"`).
    /// Built by the macros from `Self` + fn name (impl block) or
    /// `module_path!() ++ "::" ++ fn` (free fn).
    pub implementation: &'static str,
    pub status: Status,
    /// Opaque version string set by the user (e.g. `"3.5"`). The crate does
    /// not interpret it.
    pub since: Option<&'static str>,
    /// Free-form note. Required when `status == Unimplemented`.
    pub comment: Option<&'static str>,
    /// Tracker issue number (e.g. GitHub issue #42).
    pub issue: Option<u32>,
}

// Tells `inventory` that `ParityEntry` is a collected type; this is what
// makes `inventory::iter::<ParityEntry>()` work in downstream binaries.
inventory::collect!(ParityEntry);