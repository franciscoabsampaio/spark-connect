[![Crates.io](https://img.shields.io/crates/v/spark-connect.svg)](https://crates.io/crates/spark-connect)
[![Docs.rs](https://docs.rs/spark-connect/badge.svg)](https://docs.rs/spark-connect)

# spark-connect

![spark-connect](./src/docs/banner.jpg)

<b>An idiomatic, SQL-first Rust client for Apache Spark Connect.</b>

This crate provides a fully asynchronous, strongly typed API for interacting
with a remote Spark Connect server over gRPC.

It allows you to build and execute SQL queries, bind parameters safely,
and collect Arrow `RecordBatch` results - just like any other SQL toolkit -
all in native Rust.

## [Changelog](https://github.com/franciscoabsampaio/spark-connect/blob/main/Changelog.md)

## ✨ Features

- ⚙️ **Spark-compatible connection builder** (`sc://host:port` format);
- 🪶 **Async execution** using `tokio` and `tonic`;
- 🧩 **Parameterized queries**;
- 🧾 **Arrow-native results** returned as `Vec<RecordBatch>`;

## Getting Started

```rs
use spark_connect::SparkSessionBuilder;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  // 1️⃣ Connect to a Spark Connect endpoint
  let session = SparkSessionBuilder::new("sc://localhost:15002")
      .build()
      .await?;

  // 2️⃣ Execute a simple SQL query and receive a Vec<RecordBatches>
  let batches = session
      .query("SELECT ? AS rule, ? AS text")
      .bind(42)
      .bind("world")
      .execute()
      .await?;

  Ok(())
}
```

It's that simple!

## 🧩 Parameterized Queries

Behind the scenes, the [`SparkSession::query`] method
uses the [`ToLiteral`] trait to safely bind parameters
before execution:

```rs
use spark_connect::ToLiteral;
 
// This is
 
let batches = session
    .query("SELECT ? AS id, ? AS text")
    .bind(42)
    .bind("world")
    .await?;

// the same as this

let lazy_plan = session.sql(
    "SELECT ? AS id, ? AS text",
    vec![42.to_literal(), "world".to_literal()]
).await?;
let batches = session.collect(lazy_plan);
```

## 😴 Lazy Execution

The biggest advantage to using the [`sql()`](SparkSession::sql) method
instead of [`query()`](SparkSession::query) is lazy execution -
queries can be lazily evaluated and collected afterwards.
If you're coming from PySpark or Scala, this should be the familiar interface.

## 🧠 Concepts

- <b>[`SparkSession`](crate::SparkSession)</b> — the main entry point for executing
  SQL queries and managing a session.
- <b>[`SparkClient`](crate::SparkClient)</b> — low-level gRPC client (used internally).
- <b>[`SqlQueryBuilder`](crate::query::SqlQueryBuilder)</b> — helper for binding parameters
  and executing queries.

## ⚙️ Requirements

- A running **Spark Connect server** (Spark 3.4+);
- Network access to the configured `sc://` endpoint;
- `tokio` runtime.

## 🔒 Example Connection Strings

```text
sc://localhost:15002
sc://spark-cluster:15002/?user_id=francisco
sc://10.0.0.5:15002;session_id=abc123;user_agent=my-app
```

## 🏗️ Building With Different Versions of Spark Connect

Currently, this crate is built against Spark 3.5.x. If you need to build against a different version of Spark Connect, you can:

1. Clone this repository.
2. Go to the [official Apache Spark repository](https://github.com/apache/spark/) and find the protobuf definitions for the desired version. Refer to the table below for the exact path.
3. Download the `protobuf` directory and replace the `protobuf/` directory of this repository with the desired version.
4. After replacing the files, run `cargo build` to regenerate the gRPC client code.
5. Use the crate as usual.

| Version | Path to the protobuf directory |
|--------:|------------------|
| 4.x     | [`branch-4.x / sql/connect/common/src/main/protobuf`](https://github.com/apache/spark/tree/branch-4.1/sql/connect/common/src/main/protobuf) |
| 3.4-3.5 | [`branch-3.x / connector/connect/common/src/main/protobuf`](https://github.com/apache/spark/tree/branch-3.5/connector/connect/common/src/main/protobuf) |

⚠️ Note that compatibility is not guaranteed, and you may encounter issues if there are significant changes between versions.

## 📘 Learn More

- [Apache Spark Connect documentation](https://spark.apache.org/docs/latest/spark-connect.html);
- [Apache Arrow RecordBatch specification](https://arrow.apache.org/docs/format/Columnar.html).

## 🙏 Acknowledgements

This project takes heavy inspiration from the [spark-connect-rs](https://github.com/sjrusso8/spark-connect-rs) project, and would've been much harder without it!

---
© 2025 Francisco A. B. Sampaio. Licensed under the MIT License.

This project is not affiliated with, endorsed by, or sponsored by the Apache Software Foundation.
“Apache”, “Apache Spark”, and “Spark Connect” are trademarks of the Apache Software Foundation.
