# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0](https://github.com/franciscoabsampaio/spark-connect/releases/tag/v0.3.0) - 2026-09-20

This release rebuilds the crate on Apache's official
[`apache-spark-connect`](https://crates.io/crates/apache-spark-connect) client.
The whole DataFrame API comes from there; this crate is the async layer over it,
plus the sqlx-style query interface.

### Added

- `CHANGELOG.md`!
- `_async` counterparts of every blocking action on the official types, through
  the extension traits in `ext` (`DataFrameExt`, `CatalogExt`, `RuntimeConfExt`,
  the writers, streaming). Their futures are `Send + 'static`, so they can be
  spawned.
- `prelude`, re-exporting the session, `ToLiteral`, the extension traits, `col`
  and `lit`.
- `SparkSession::run`, to reach anything in the official API that has no
  wrapper here.
- `DataFrameStreamExt::to_local_iterator_async`, streaming a DataFrame's rows
  with backpressure as the server produces them.
- Re-export of the official crate, so `DataFrame`, `Column`, `functions`, `col`
  and `lit` are reachable without adding a second dependency.

### Changed

- `SparkSession` wraps the official session and dereferences to it: its
  planning API (`sql`, `table`, `read`, `catalog`, `conf`, `range`) is used as
  it is, while the methods that reach the server - `version`, `interrupt_all`,
  `interrupt_operation`, `stop`, the `register_*` functions, artifacts - are
  async under the same names.
- `ToLiteral` now produces the official `LiteralExpression`, and covers `i8`.
- Errors are the official `SparkError`, which carries the error class, SQL
  state and query context.
- Switched from SLSA attestation to GitHub's default attestation action.
- A Spark 4.0+ server is required, and `protoc` must be available at build
  time (`apache-spark-connect-proto` compiles the protos).

### Deprecated

- `SparkSessionBuilder::new(connection)` and `build()`, in favour of
  `SparkSession::builder().remote(url).get_or_create()`.

### Removed

- The hand-rolled gRPC client, the vendored protobuf definitions and the build
  script that compiled them, along with the `spark-3-4` and `spark-3-5`
  features: the official crate owns the protocol.
- `SparkSession::sql(query, params)` and `SparkSession::collect(plan)`. Use the
  official `sql`/`sql_with_args`, which return a `DataFrame`, then
  `collect_async()` - or `query().bind().execute()`, which is unchanged.
- The in-tree DataFrame, Catalog, plan, types and storage-level implementations,
  and the PySpark api-parity tooling.
- The `tls`, `tokio-macros` and `parity` features. TLS is always compiled in by
  the official client, which trusts the system roots and supports `token`;
  custom CA certificates and mTLS are not available.

## [0.2.2](https://github.com/franciscoabsampaio/spark-connect/releases/tag/v0.2.2) - 2025-02-12

### Changed

- Build script now uses vendored protobuf-compiler, no longer requiring users to manually install it.

## [0.2.1](https://github.com/franciscoabsampaio/spark-connect/releases/tag/v0.2.1) - 2025-01-22

### Added

- Databricks example.

### Changed

- Fix TLS.
- Enforce SSL if `token` header is used.
- Enforce lower case in HTTP headings.

## [0.2.0](https://github.com/franciscoabsampaio/spark-connect/releases/tag/v0.2.0) - 2025-01-10

### Changed

- Standardize protobuf directory to support different versions of Spark.

## [0.1.1](https://github.com/franciscoabsampaio/spark-connect/releases/tag/v0.1.1) - 2025-10-25

### Changed

- Remove explicit dependency on arrow_ipc (use feature instead).

## [0.1.0](https://github.com/franciscoabsampaio/spark-connect/releases/tag/v0.1.0) - 2025-10-17

- Initial release.
