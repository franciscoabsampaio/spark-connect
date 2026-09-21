//! Session lifecycle, isolation, and reaching the rest of the official API.
#![allow(clippy::result_large_err)] // SparkError is the official crate's type
mod common;

use common::{session, unique};

use spark_connect::prelude::*;
use spark_connect::Result;

/// The builder connects and reports the server it reached.
#[tokio::test]
async fn connects_and_reports_the_server_version() -> Result<()> {
    let session = session().await?;

    let version = session.version().await?;

    assert!(version.starts_with('4'), "expected a Spark 4 server, got {version}");
    assert!(!session.session_id().is_empty());
    Ok(())
}

/// Temporary views belong to the session that created them: a new session
/// cannot see them, and each session has its own id.
#[tokio::test]
async fn isolates_temporary_views_between_sessions() -> Result<()> {
    let session = session().await?;
    let view = unique("scratch");
    session.range(2)?.create_or_replace_temp_view_async(&view).await?;

    let separate = session.new_session();

    assert_ne!(separate.session_id(), session.session_id());
    assert!(session.catalog().table_exists_async(&view).await?);
    assert!(!separate.catalog().table_exists_async(&view).await?);
    Ok(())
}

/// Clones share the session, and so see the same temporary views.
#[tokio::test]
async fn shares_state_between_clones() -> Result<()> {
    let session = session().await?;
    let view = unique("shared");
    session.range(2)?.create_or_replace_temp_view_async(&view).await?;

    let clone = session.clone();

    assert_eq!(clone.session_id(), session.session_id());
    assert!(clone.catalog().table_exists_async(&view).await?);
    Ok(())
}

/// `run` reaches parts of the official API that have no wrapper here, such as
/// the streaming query manager.
#[tokio::test]
async fn reaches_the_official_api_through_run() -> Result<()> {
    let session = session().await?;

    let active = session.run(|spark| spark.streams().active()).await?;

    assert!(active.is_empty());
    Ok(())
}

/// Actions own what they need, so they can be spawned and awaited elsewhere.
#[tokio::test]
async fn spawns_actions_as_independent_tasks() -> Result<()> {
    let session = session().await?;

    let mut tasks = Vec::new();
    for n in 1..=3 {
        tasks.push(tokio::spawn(session.range(n)?.count_async()));
    }

    let mut counts = Vec::new();
    for task in tasks {
        counts.push(task.await.expect("every task should finish")?);
    }
    assert_eq!(counts, [1, 2, 3]);
    Ok(())
}

/// Interrupting a session with nothing running reports no cancelled operations.
#[tokio::test]
async fn interrupts_an_idle_session() -> Result<()> {
    let session = session().await?;

    assert!(session.interrupt_all().await?.is_empty());
    assert!(session.interrupt_tag("nothing-tagged-this").await?.is_empty());
    Ok(())
}
