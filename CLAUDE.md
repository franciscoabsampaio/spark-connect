# spark-connect

Async, SQL-first Rust layer over the official `apache-spark-connect` crate. Adds sqlx-style parameter binding and Arrow results; a psql-like CLI is planned.

## Commands

```bash
cargo build                   # build (needs protoc - see Gotchas)
cargo test                    # unit tests + doctests (integration tests fail without a server)
make docker                   # start Spark Connect server in Docker (port 15002)
make test                     # start Docker + run full test suite + stop Docker
make stop                     # stop and remove the spark-delta container
```

## Architecture

```text
src/
  lib.rs            # crate root; glob re-exports the official crate, then our items shadow it; prelude
  spark_session.rs  # SparkSession (Deref to official) + builder; async versions of its blocking methods; `run`
  ext.rs            # `async_ext!` table: `_async` extension traits per official type; RowStream
  blocking.rs       # spawn_blocking helper + Arg/Lend traits that carry borrowed args onto the blocking thread
  query.rs          # SqlQueryBuilder - positional parameter binding and execution
  literal.rs        # ToLiteral trait - Rust types → official LiteralExpression
  error.rs          # re-exports the official SparkError / Result
  test_utils.rs     # shared test helpers (cfg(test) only)
```

## Gotchas

- **Official crate shares our lib name**: `apache-spark-connect`'s lib is also `spark_connect`, so it is renamed to `apache_spark_connect` in `Cargo.toml`. Keep that rename.
- **Official API blocks, and panics in async code**: its actions call `block_on` on a private runtime, which panics inside a tokio task. Transformations are pure plan-building and safe. Every action goes through `blocking::blocking` (tokio `spawn_blocking`): session methods are redefined on our `SparkSession` under their official names; other types get `_async` methods via `async_ext!` in `ext.rs` (inherent methods can't be shadowed). A new blocking official method = one line in the matching table; types that aren't `Clone` (e.g. `StreamingQueryManager`) go through `run`.
- **Our session module is `spark_session`, not `session`**: `lib.rs` glob re-exports the official crate, and a private `mod session` would hide the official `session` module.
- **Panics in blocking calls are resumed, not converted to errors** - deliberate; see `blocking.rs`.
- **Spark 4.0+ server required**: the official client binds SQL parameters only via the 4.0 proto fields (`pos_arguments`); a 3.5 server ignores them and fails with `UNBOUND_SQL_PARAMETER`.
- **protoc required at build time**: `apache-spark-connect-proto` compiles protos with the system `protoc`. Install it or point `PROTOC` at a binary.
- **arrow version is pinned by the official crate**: `RecordBatch` crosses the boundary, so our `arrow` major must match theirs.
- **TLS is native roots only**: the official channel supports `use_ssl` + `token`, but no custom CA or client identity (mTLS).
- **The test server is pinned to a Spark 4.0.4 image**: 4.1.x drops SQL parameters whenever `spark.sql.extensions` is set (positional and named, Connect and classic, Delta and Iceberg alike), so the bind tests fail there. Fix submitted upstream as SPARK-59672; unpin once it ships in a 4.1.x release.
- **Tests need Docker**: integration tests expect a server on `localhost:15002`. Use `make test`.
- **`make docker` is not idempotent**: the container is named `spark-delta`; running it twice will fail. Run `make stop` first.
