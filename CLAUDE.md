# spark-connect

Notes for contributors and coding agents working on this repository. Usage
documentation lives in `README.md` and on [docs.rs](https://docs.rs/spark-connect).

Async, SQL-first Rust layer over the official `apache-spark-connect` crate: it brings
that crate's synchronous API into async code, and adds sqlx-style parameter binding.
A psql-like CLI is planned.

## Commands

```bash
make test                     # the full suite, self-contained: provisions protoc, starts the
                              # server, runs `cargo test --all-features`, stops the server
make docker                   # start Spark Connect server in Docker (port 15002), wait for readiness
make stop                     # stop and remove the spark-delta container
make protoc                   # fetch protoc into .tools/ unless one is on PATH
cargo build                   # needs protoc: on PATH, or PROTOC=$(make -s protoc-path)
cargo test --all-features     # same tests, against a server you started yourself
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
tests/              # acceptance tests, run against the Docker server as an external consumer
  common/mod.rs     # session fixture; unique names so tests can share one server
```

## Gotchas

- **Official API blocks, and panics in async code**: its actions call `block_on` on a private runtime, which panics inside a tokio task; its transformations are pure plan-building and safe. Every action goes through `blocking::blocking` (tokio `spawn_blocking`). Session methods are redefined on our `SparkSession` under their official names; other types get `_async` methods from the `async_ext!` tables in `ext.rs`, because inherent methods cannot be shadowed. Wrapping a newly blocking official method is one line in the matching table; types that aren't `Clone` (e.g. `StreamingQueryManager`) go through `run` instead.
- **Panics in blocking calls are resumed, not converted to errors** - deliberate; see `blocking.rs`.
- **The official crate shares our lib name**, so it is renamed to `apache_spark_connect` in `Cargo.toml`, and `lib.rs` glob re-exports it. Our own modules must not collide with its module names - hence `spark_session`, not `session`.
- **Version constraints come from the official crate**: `arrow` must stay on its major, since `RecordBatch` crosses the boundary, and `protoc` is needed at build time because `apache-spark-connect-proto` compiles the protos.
- **Spark 4.0+ server required**: parameters are bound through 4.0 proto fields. The test server is pinned to a 4.0.4 image because 4.1.x drops SQL parameters whenever `spark.sql.extensions` is set - fixed upstream as SPARK-59672, so unpin once that ships.
- **TLS is native roots only**: the official channel supports `use_ssl` + `token`, but no custom CA or client identity (mTLS).
- **Some official APIs are ahead of every released server**: its protos come from Spark's development branch, so ten `CatalogExt` methods (drop/create database, drop table/view, truncate, analyze, list views/partitions, table properties, create-table string) fail on 4.0 and 4.1 with `CATTYPE_NOT_SET not supported`. Listed on `CatalogExt`; the tests use SQL DDL instead.
- **Tests need Docker**: integration tests expect a server on `localhost:15002`. Use `make test`, which starts one, waits for it to accept connections, and removes it afterwards.
