//! Fixtures shared by the acceptance tests.
//!
//! Every test connects to the Spark Connect server that `make test` starts on
//! `localhost:15002`, and names whatever it creates uniquely, so the tests can
//! run in parallel against one server.
#![allow(dead_code)]

use spark_connect::{Result, SparkSession};

use uuid::Uuid;

pub const REMOTE: &str = "sc://localhost:15002";

/// A session against the test server.
pub async fn session() -> Result<SparkSession> {
    SparkSession::builder().remote(REMOTE).get_or_create().await
}

/// A name no concurrent test will pick, such as `people_3f2a1b9c`.
pub fn unique(prefix: &str) -> String {
    let id = Uuid::new_v4().simple().to_string();
    format!("{prefix}_{}", &id[..8])
}

/// A unique path on the *server's* filesystem, for the reader and writer tests.
pub fn unique_path(prefix: &str) -> String {
    format!("/tmp/{}", unique(prefix))
}
