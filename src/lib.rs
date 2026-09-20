/*!
# spark-connect

![spark-connect](../docs/banner.jpg)

<b>An idiomatic, SQL-first Rust client for Apache Spark Connect.</b>

This crate is an async layer over the official
[`apache-spark-connect`](https://crates.io/crates/apache-spark-connect) client.

It allows you to build and execute SQL queries, bind parameters safely,
and collect Arrow `RecordBatch` results - just like any other SQL toolkit -
all in native Rust.

## ✨ Features

- ⚙️ **Spark-compatible connection builder** (`sc://host:port` format);
- 🪶 **Async execution** on `tokio`;
- 🧩 **Parameterized queries**;
- 🧾 **Arrow-native results** returned as `Vec<RecordBatch>`;

## Getting Started

```no_run
use spark_connect::SparkSession;

# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
// 1️⃣ Connect to a Spark Connect endpoint
let session = SparkSession::builder()
    .remote("sc://localhost:15002")
    .get_or_create()
    .await?;

// 2️⃣ Execute a simple SQL query and receive a Vec<RecordBatches>
let batches = session
    .query("SELECT ? AS rule, ? AS text")
    .bind(42)
    .bind("world")
    .execute()
    .await?;

# Ok(())
# }
```

It's that simple!

## 🧩 Parameterized Queries

Behind the scenes, the [`SparkSession::query`] method
uses the [`ToLiteral`] trait to safely bind parameters
before execution.

## 🧰 DataFrames

The official client's DataFrame API is used as it is: transformations only
build a plan, and each action has an `_async` counterpart in the
[`prelude`], run off the async executor:

```no_run
use spark_connect::prelude::*;

# async fn example(spark: SparkSession) -> spark_connect::Result<()> {
let df = spark.sql("SELECT * FROM people")?.filter(col("age").gt(lit(17)));

let adults = df.count_async().await?;
let rows = df.collect_async().await?;
# Ok(())
# }
```

This crate re-exports the official crate's items - [`DataFrame`], [`Column`],
[`functions`], [`col`], [`lit`], ... - alongside its own. Anything else in the
official API is reachable through [`SparkSession::run`].

## ⚖️ Trade-offs

The official client blocks on a runtime of its own, so every call that
reaches the server runs on tokio's blocking thread pool. In practice:

- **Dropping a future does not cancel the query.** A timed-out or
  abandoned action runs to completion on the server and its result is
  discarded. Use [`SparkSession::interrupt_all`] or
  [`SparkSession::interrupt_tag`] to stop work on the server.
- **Each in-flight action holds a blocking thread.** Tokio's pool allows 512
  by default, far above typical Spark concurrency.
- **The official synchronous actions remain callable** - `df.collect()` next to
  `df.collect_async()` - and panic when called from an async task.
- **Large results can be streamed** row by row with
  [`to_local_iterator_async`](ext::DataFrameStreamExt::to_local_iterator_async).

## 🧠 Concepts

- <b>[`SparkSession`]</b> - the main entry point for executing
  SQL queries and managing a session.
- <b>[`SqlQueryBuilder`](crate::query::SqlQueryBuilder)</b> - helper for binding parameters
  and executing queries.
- <b>[`ext`]</b> - async counterparts of the official types' actions.

## ⚙️ Requirements

- A running **Spark Connect server** (Spark 4.0+);
- Network access to the configured `sc://` endpoint;
- `tokio` runtime;
- `protoc` on the `PATH` (or `PROTOC` set) at build time, required by `apache-spark-connect-proto`.

## 🔒 Example Connection Strings

```text
sc://localhost:15002
sc://spark-cluster:15002/;user_id=francisco
sc://10.0.0.5:15002/;session_id=abc123;user_agent=my-app
```

## 📘 Learn More

- [Apache Spark Connect documentation](https://spark.apache.org/docs/latest/spark-connect.html);
- [Apache Arrow RecordBatch specification](https://arrow.apache.org/docs/format/Columnar.html).

## 🙏 Acknowledgements

This project takes heavy inspiration from the [spark-connect-rs](https://github.com/sjrusso8/spark-connect-rs) project, and would've been much harder without it!

---
© 2025 Francisco A. B. Sampaio. Licensed under the MIT License.

This project is not affiliated with, endorsed by, or sponsored by the Apache Software Foundation.
“Apache”, “Apache Spark”, and “Spark Connect” are trademarks of the Apache Software Foundation.
*/

#![allow(clippy::result_large_err)] // SparkError is the official crate's type

mod blocking;
mod error;
pub mod ext;
mod literal;
pub mod query;
mod spark_session;

pub use apache_spark_connect;
pub use apache_spark_connect::*;
pub use error::{Result, SparkError};
pub use literal::ToLiteral;
pub use spark_session::{SparkSession, SparkSessionBuilder};

/// The session, the [`ToLiteral`] trait, every [`ext`] trait, and the
/// official [`col`] and [`lit`].
pub mod prelude {
    pub use crate::ext::{
        CatalogExt, DataFrameExt, DataFrameStreamExt, DataFrameWriterExt, DataFrameWriterV2Ext,
        DataStreamWriterExt, GroupedDataExt, MergeIntoWriterExt, RuntimeConfExt, StreamingQueryExt,
    };
    pub use crate::{col, lit, SparkSession, ToLiteral};
}

#[cfg(test)]
mod test_utils;
