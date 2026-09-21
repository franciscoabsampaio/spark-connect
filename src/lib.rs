/*!
# spark-connect

![spark-connect](https://raw.githubusercontent.com/franciscoabsampaio/spark-connect/main/src/docs/banner_0_3_0.png)

<b>An idiomatic, SQL-first Rust client for Apache Spark Connect.</b>

This crate is an async layer over the official
[`apache-spark-connect`](https://crates.io/crates/apache-spark-connect) client,
whose API is synchronous. It brings the whole official API into `async` code and
adds a [sqlx](https://docs.rs/sqlx/latest/sqlx/)-style interface for binding
parameters safely.

## ✨ Features

- ⚙️ **Spark-compatible connection builder** (`sc://host:port` format);
- 🪶 **Async execution** on `tokio`, for the whole official API;
- 🧩 **Parameterized queries**, bound through the [`ToLiteral`] trait;
- 🧾 **Results as Arrow `RecordBatch`es, typed `Row`s, or a row stream**;

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

// 2️⃣ Execute a simple SQL query and receive a Vec<RecordBatch>
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

The [`SparkSession::query`] method uses the [`ToLiteral`] trait to bind each
parameter as a Spark literal, so values never reach the server as SQL text:

```no_run
# async fn example(session: spark_connect::SparkSession) -> spark_connect::Result<()> {
// This
let batches = session
    .query("SELECT ? AS id, ? AS text")
    .bind(42)
    .bind("world")
    .execute()
    .await?;

// is the same as this
use spark_connect::{prelude::*, Expression};

let batches = session
    .run(|spark| {
        spark
            .sql_with_args(
                "SELECT ? AS id, ? AS text",
                vec![
                    Expression::Literal(42.to_literal()),
                    Expression::Literal("world".to_literal()),
                ],
                Default::default(),
            )?
            .collect_record_batches()
    })
    .await?;
# Ok(())
# }
```

`bind` accepts Rust primitives, `String`/`&str`, `Vec<u8>` and, with the
`chrono` feature, `NaiveDate` and `NaiveDateTime`. Pass a
[`LiteralExpression`] directly for
decimals, arrays, maps, structs and typed nulls.

Named parameters and the rest of the official SQL API are reached through
[`sql_with_args`](apache_spark_connect::SparkSession::sql_with_args), as above.

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
- `protoc` at build time.

### Installing protoc

`apache-spark-connect-proto` compiles the Connect `.proto` files while it
builds, using the Protocol Buffers compiler:

```bash
apt-get install -y protobuf-compiler  # Debian, Ubuntu
brew install protobuf                 # macOS
winget install protobuf               # Windows
```

If it is not on the `PATH`, point `PROTOC` at it:

```bash
PROTOC=/path/to/protoc cargo build
```

In GitHub Actions:

```yaml
- uses: arduino/setup-protoc@v3
  with:
    repo-token: ${{ secrets.GITHUB_TOKEN }}
```

## 🔒 Example Connection Strings

```text
sc://localhost:15002
sc://spark-cluster:15002/;user_id=francisco
sc://10.0.0.5:15002/;session_id=abc123;user_agent=my-app
```

## 📘 Learn More

- [`apache-spark-connect` API reference](https://docs.rs/apache-spark-connect) -
  the DataFrame, column and function APIs this crate builds on;
- [Apache Spark Connect documentation](https://spark.apache.org/docs/latest/spark-connect.html);
- [Spark Connect client connection string](https://github.com/apache/spark/blob/master/sql/connect/docs/client-connection-string.md).

---
© 2025 Francisco A. B. Sampaio. Licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).

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
