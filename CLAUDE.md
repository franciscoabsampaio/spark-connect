# spark-connect

Rust client for Apache Spark Connect (gRPC). Async-first, Arrow-native results, SQL-first API modeled after PySpark.

## Commands

```bash
cargo build                   # build (default: spark-3-5 + tls features)
cargo test                    # unit tests only (no Docker required)
make docker                   # start Spark Connect server in Docker (port 15002)
make test                     # start Docker + run full test suite + stop Docker
make stop                     # stop and remove the spark-delta container
make parity                   # regenerate the API report and the test checklist
make deps                     # install Python tooling + the pinned pyspark
make protos                   # fetch Connect .proto files
make vendor                   # fetch upstream PySpark test files
```

`SPARK_VERSION` (default 3.5.7) is the single knob: it picks the release fetched
by `protos` and `vendor`, and the pyspark `deps` installs. Move the whole repo
with `make protos vendor deps parity SPARK_VERSION=4.0.0`.

`make parity` splits into `parity-api` (report.md) and `parity-tests`
(tests.md); both abort unless the venv's pyspark matches `SPARK_VERSION`.

## Architecture

```
src/
  lib.rs            # crate root; re-exports public API
  session.rs        # SparkSession + SparkSessionBuilder (main entry point)
  query.rs          # SqlQueryBuilder - parameter binding and execution
  client/           # low-level gRPC client (SparkConnectClient)
    mod.rs
    channel_builder.rs  # parses sc:// URLs, builds tonic Channel (TLS logic here)
    handlers.rs
    middleware.rs
  conf.rs           # SparkConf / SparkConfKey
  literal.rs        # ToLiteral trait - Rust types → Spark literals
  error.rs          # SparkError
  io.rs             # Arrow IPC deserialization
  version.rs        # Version - parses/orders Spark releases, backs require_since
  test_utils.rs     # shared test helpers (cfg(test) only)
protobuf/
  spark-3.5/        # vendored .proto files, keyed by release line
vendor/
  spark-3.5/        # upstream PySpark test suites, mirroring their paths in the Spark repo
scripts/
  parity_tests_md.py  # renders api-parity/ref-tests.json as a checklist
build.rs            # compiles protos, sets SPARK_VERSION env var at compile time
```

## Gotchas

- **Tests need Docker**: `cargo test` alone skips integration tests. Use `make test` to run the full suite - it starts/stops the `spark-delta` container automatically.
- **`make docker` is not idempotent**: the container is named `spark-delta`; running it twice will fail. Run `make stop` first.
- **Spark version is a compile-time feature flag**: default is `spark-3-5`, which selects `protobuf/spark-3.5/`. `build.rs` reads the enabled feature from `CARGO_FEATURE_SPARK_*`, so adding a release means dropping in the protos and adding one `[features]` line - never editing `build.rs`. It panics if zero or more than one is enabled; since Cargo unions features, a second one usually arrives via a dependency.
- **TLS is on by default**: to disable, pass `--no-default-features --features spark-3-5` (omit `tls`).
- **Vendored dirs are keyed by release line, not patch**: `spark-3.5/`, not `spark-3.5.7/`. A patch upgrade overwrites in place, so `git diff` after `make protos` / `make vendor` is the upstream drift report. Never hand-edit files under `protobuf/` or `vendor/` - they are byte-identical to upstream.
- **Two ways to gate version-exclusive APIs**: `#[cfg(feature = "spark-x-y")]` when the code references a proto message the selected set lacks; `session.require_since(V)` when it compiles everywhere but the *server* must be new enough (see `Observation::SINCE`). The server version is resolved once during `create()`, so the check is a local comparison.
- **Doctests are stale**: the `SparkSessionBuilder::new(url).build()` examples in `lib.rs`, `query.rs`, and `session.rs` predate the `SparkSession::builder()` / `.create()` API and do not compile. `cargo test --lib` passes; `cargo test --doc` does not.
- **Test parity is a checklist, not a diff**: Rust `#[cfg(test)]` functions reach neither api-parity producer, so `api-parity/tests.md` is hand-ticked and `make parity-tests` preserves the ticks.
