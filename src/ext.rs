//! Async counterparts of the official types' blocking actions.
//!
//! The official crate's transformations - `select`, `filter`, `join`, the
//! column functions, the reader and writer builders - only build a plan and
//! are safe to call from async code as they are. Its actions reach the server
//! and block, so each is paired here with an `_async` method that runs it on
//! tokio's blocking thread pool.
//!
//! The returned futures are `Send + 'static`: they own everything they need
//! and can be spawned. Dropping one does not cancel the operation on the
//! server; the action runs to completion and its result is discarded.
//!
//! ```no_run
//! use spark_connect::prelude::*;
//!
//! # async fn example(spark: SparkSession) -> spark_connect::Result<()> {
//! let adults = spark
//!     .sql("SELECT * FROM people")?
//!     .filter(col("age").gt(lit(17)))
//!     .count_async()
//!     .await?;
//! # Ok(())
//! # }
//! ```
use crate::blocking::{Arg, Lend};
use crate::Result;

use apache_spark_connect::catalog::{Catalog, CatalogMetadata, Database, Function, Table, TablePartition};
use apache_spark_connect::conf::RuntimeConf;
use apache_spark_connect::group::GroupedData;
use apache_spark_connect::merge::MergeIntoWriter;
use apache_spark_connect::readwriter::{DataFrameWriter, DataFrameWriterV2};
use apache_spark_connect::row::{Row, Value};
use apache_spark_connect::streaming::{DataStreamWriter, StreamingQuery, StreamingQueryException, StreamingQueryStatus};
use apache_spark_connect::udf::CommonInlineUserDefinedFunctionExpression;
use apache_spark_connect::{Column, DataFrame, DataType, StorageLevel};
use arrow::record_batch::RecordBatch;
use futures_core::Stream;
use std::collections::HashMap;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// Declares an extension trait pairing each official action with an async one.
///
/// Types borrowed by the official method (`by ref`) are cloned into the
/// blocking task; types it consumes (`by value`) are moved.
macro_rules! async_ext {
    (
        $(#[$attr:meta])*
        pub trait $Trait:ident for $Type:ident by ref {
            $($async_fn:ident => $fn:ident($($arg:ident: $ArgTy:ty),*) -> $Ret:ty;)*
        }
    ) => {
        async_ext!(@emit $(#[$attr])* $Trait $Type [&Self] [clone] {
            $($async_fn => $fn($($arg: $ArgTy),*) -> $Ret;)*
        });
    };
    (
        $(#[$attr:meta])*
        pub trait $Trait:ident for $Type:ident by value {
            $($async_fn:ident => $fn:ident($($arg:ident: $ArgTy:ty),*) -> $Ret:ty;)*
        }
    ) => {
        async_ext!(@emit $(#[$attr])* $Trait $Type [Self] [move] {
            $($async_fn => $fn($($arg: $ArgTy),*) -> $Ret;)*
        });
    };
    (@take clone $this:tt) => { $this.clone() };
    (@take move $this:tt) => { $this };
    (
        @emit $(#[$attr:meta])* $Trait:ident $Type:ident [$Recv:ty] [$take:ident] {
            $($async_fn:ident => $fn:ident($($arg:ident: $ArgTy:ty),*) -> $Ret:ty;)*
        }
    ) => {
        $(#[$attr])*
        pub trait $Trait {
            $(
                #[doc = concat!("Async [`", stringify!($Type), "::", stringify!($fn), "`].")]
                fn $async_fn(self: $Recv $(, $arg: $ArgTy)*) -> impl Future<Output = Result<$Ret>> + Send + 'static;
            )*
        }

        impl $Trait for $Type {
            $(
                fn $async_fn(self: $Recv $(, $arg: $ArgTy)*) -> impl Future<Output = Result<$Ret>> + Send + 'static {
                    let this = async_ext!(@take $take self);
                    $(let $arg = Arg::own($arg);)*
                    crate::blocking::blocking(move || this.$fn($(Lend::lend(&$arg)),*))
                }
            )*
        }
    };
}

async_ext! {
    /// Async actions on a [`DataFrame`].
    pub trait DataFrameExt for DataFrame by ref {
        collect_async => collect() -> Vec<Row>;
        collect_record_batches_async => collect_record_batches() -> Vec<RecordBatch>;
        count_async => count() -> i64;
        show_async => show(n: usize) -> ();
        first_async => first() -> Option<Row>;
        head_async => head() -> Option<Row>;
        take_async => take(n: usize) -> Vec<Row>;
        is_empty_async => is_empty() -> bool;
        exists_async => exists() -> bool;
        scalar_async => scalar() -> Option<Value>;
        schema_async => schema() -> DataType;
        columns_async => columns() -> Vec<String>;
        dtypes_async => dtypes() -> Vec<(String, String)>;
        print_schema_async => print_schema() -> ();
        explain_async => explain() -> ();
        explain_mode_async => explain_mode(mode: &str) -> ();
        input_files_async => input_files() -> Vec<String>;
        is_cached_async => is_cached() -> bool;
        cache_async => cache() -> DataFrame;
        persist_async => persist(storage_level: StorageLevel) -> DataFrame;
        unpersist_async => unpersist(blocking: bool) -> DataFrame;
        storage_level_async => storage_level() -> StorageLevel;
        checkpoint_async => checkpoint() -> DataFrame;
        local_checkpoint_async => local_checkpoint() -> DataFrame;
        semantic_hash_async => semantic_hash() -> i32;
        same_semantics_async => same_semantics(other: &DataFrame) -> bool;
        create_temp_view_async => create_temp_view(name: &str) -> ();
        create_or_replace_temp_view_async => create_or_replace_temp_view(name: &str) -> ();
        create_global_temp_view_async => create_global_temp_view(name: &str) -> ();
        create_or_replace_global_temp_view_async => create_or_replace_global_temp_view(name: &str) -> ();
        register_temp_table_async => register_temp_table(name: &str) -> ();
        to_arrow_async => to_arrow() -> Vec<u8>;
        to_json_async => to_json() -> Vec<String>;
        foreach_async => foreach(func: CommonInlineUserDefinedFunctionExpression) -> ();
        foreach_partition_async => foreach_partition(func: CommonInlineUserDefinedFunctionExpression) -> ();
    }
}

async_ext! {
    /// Async actions on a [`GroupedData`].
    pub trait GroupedDataExt for GroupedData by ref {
        input_columns_async => input_columns() -> Vec<String>;
    }
}

async_ext! {
    /// Async actions on a [`Catalog`].
    pub trait CatalogExt for Catalog by ref {
        analyze_table_async => analyze_table(table_name: &str, no_scan: bool) -> ();
        cache_table_async => cache_table(table_name: &str) -> ();
        cache_table_with_storage_level_async => cache_table_with_storage_level(table_name: &str, storage_level: Option<StorageLevel>) -> ();
        clear_cache_async => clear_cache() -> ();
        create_database_async => create_database(db_name: &str, if_not_exists: bool, properties: HashMap<String, String>) -> ();
        current_catalog_async => current_catalog() -> String;
        current_database_async => current_database() -> String;
        database_exists_async => database_exists(db_name: &str) -> bool;
        drop_database_async => drop_database(db_name: &str, if_exists: bool, cascade: bool) -> ();
        drop_global_temp_view_async => drop_global_temp_view(view_name: &str) -> bool;
        drop_table_async => drop_table(table_name: &str, if_exists: bool, purge: bool) -> ();
        drop_temp_view_async => drop_temp_view(view_name: &str) -> bool;
        drop_view_async => drop_view(view_name: &str, if_exists: bool) -> ();
        function_exists_async => function_exists(function_name: &str) -> bool;
        function_exists_with_database_async => function_exists_with_database(function_name: &str, db_name: Option<&str>) -> bool;
        get_create_table_string_async => get_create_table_string(table_name: &str, as_serde: bool) -> String;
        get_database_typed_async => get_database_typed(db_name: &str) -> Database;
        get_function_typed_async => get_function_typed(function_name: &str) -> Function;
        get_table_properties_async => get_table_properties(table_name: &str) -> Vec<(String, String)>;
        get_table_typed_async => get_table_typed(table_name: &str) -> Table;
        is_cached_async => is_cached(table_name: &str) -> bool;
        list_catalogs_typed_async => list_catalogs_typed(pattern: Option<&str>) -> Vec<CatalogMetadata>;
        list_databases_typed_async => list_databases_typed(pattern: Option<&str>) -> Vec<Database>;
        list_partitions_typed_async => list_partitions_typed(table_name: &str) -> Vec<TablePartition>;
        list_tables_typed_async => list_tables_typed(db_name: Option<&str>, pattern: Option<&str>) -> Vec<Table>;
        list_views_async => list_views(db_name: Option<&str>, pattern: Option<&str>) -> DataFrame;
        list_views_typed_async => list_views_typed(db_name: Option<&str>, pattern: Option<&str>) -> Vec<Table>;
        recover_partitions_async => recover_partitions(table_name: &str) -> ();
        refresh_by_path_async => refresh_by_path(path: &str) -> ();
        refresh_table_async => refresh_table(table_name: &str) -> ();
        set_current_catalog_async => set_current_catalog(catalog_name: &str) -> ();
        set_current_database_async => set_current_database(db_name: &str) -> ();
        table_exists_async => table_exists(table_name: &str) -> bool;
        table_exists_with_database_async => table_exists_with_database(table_name: &str, db_name: Option<&str>) -> bool;
        truncate_table_async => truncate_table(table_name: &str) -> ();
        uncache_table_async => uncache_table(table_name: &str) -> ();
    }
}

async_ext! {
    /// Async actions on a [`RuntimeConf`].
    pub trait RuntimeConfExt for RuntimeConf by ref {
        get_async => get(key: &str) -> Option<String>;
        get_all_async => get_all() -> HashMap<String, String>;
        get_with_default_async => get_with_default(key: &str, default: Option<&str>) -> Option<String>;
        is_modifiable_async => is_modifiable(key: &str) -> bool;
        set_async => set(key: &str, value: &str) -> ();
        unset_async => unset(key: &str) -> ();
    }
}

async_ext! {
    /// Async actions on a [`DataFrameWriter`].
    pub trait DataFrameWriterExt for DataFrameWriter by value {
        save_async => save(path: Option<&str>) -> ();
        save_as_table_async => save_as_table(table_name: &str) -> ();
        insert_into_async => insert_into(table_name: &str) -> ();
        csv_async => csv(path: &str) -> ();
        json_async => json(path: &str) -> ();
        orc_async => orc(path: &str) -> ();
        parquet_async => parquet(path: &str) -> ();
        text_async => text(path: &str) -> ();
        xml_async => xml(path: &str) -> ();
    }
}

async_ext! {
    /// Async actions on a [`DataFrameWriterV2`].
    pub trait DataFrameWriterV2Ext for DataFrameWriterV2 by value {
        append_async => append() -> ();
        create_async => create() -> ();
        create_or_replace_async => create_or_replace() -> ();
        overwrite_async => overwrite(condition: Column) -> ();
        overwrite_partitions_async => overwrite_partitions() -> ();
        replace_async => replace() -> ();
    }
}

async_ext! {
    /// Async actions on a [`MergeIntoWriter`].
    pub trait MergeIntoWriterExt for MergeIntoWriter by value {
        merge_async => merge() -> ();
    }
}

async_ext! {
    /// Async actions on a [`DataStreamWriter`].
    pub trait DataStreamWriterExt for DataStreamWriter by value {
        start_async => start(path: &str) -> StreamingQuery;
        to_table_async => to_table(table_name: &str) -> StreamingQuery;
    }
}

async_ext! {
    /// Async actions on a [`StreamingQuery`].
    pub trait StreamingQueryExt for StreamingQuery by ref {
        is_active_async => is_active() -> bool;
        status_async => status() -> StreamingQueryStatus;
        stop_async => stop() -> ();
        await_termination_async => await_termination(timeout_sec: Option<f64>) -> Option<bool>;
        exception_async => exception() -> Option<StreamingQueryException>;
        last_progress_async => last_progress() -> Option<String>;
        recent_progress_async => recent_progress() -> Vec<String>;
        process_all_available_async => process_all_available() -> ();
        explain_async => explain(extended: bool) -> String;
    }
}

/// Rows buffered ahead of a [`RowStream`]'s consumer before the producing thread waits.
const ROW_BUFFER: usize = 1024;

/// Streams a [`DataFrame`]'s rows as the server produces them.
pub trait DataFrameStreamExt {
    /// Async [`DataFrame::to_local_iterator`].
    ///
    /// Rows are fetched on tokio's blocking thread pool and yielded as they
    /// arrive, holding at most one server batch plus a bounded row buffer in
    /// memory. Dropping the stream stops fetching.
    fn to_local_iterator_async(&self, prefetch_partitions: bool) -> RowStream;
}

impl DataFrameStreamExt for DataFrame {
    fn to_local_iterator_async(&self, prefetch_partitions: bool) -> RowStream {
        let df = self.clone();
        let (sender, rows) = mpsc::channel(ROW_BUFFER);

        let task = tokio::task::spawn_blocking(move || {
            let iterator = match df.to_local_iterator(prefetch_partitions) {
                Ok(iterator) => iterator,
                Err(error) => return drop(sender.blocking_send(Err(error))),
            };
            for row in iterator {
                if sender.blocking_send(row).is_err() {
                    break;
                }
            }
        });

        RowStream { rows, task: Some(task) }
    }
}

/// The rows of a [`DataFrame`], returned by
/// [`to_local_iterator_async`](DataFrameStreamExt::to_local_iterator_async).
///
/// A panic while fetching is resumed on the consuming task once the rows
/// fetched before it are exhausted.
pub struct RowStream {
    rows: mpsc::Receiver<Result<Row>>,
    task: Option<JoinHandle<()>>,
}

impl Stream for RowStream {
    type Item = Result<Row>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(row) = ready!(self.rows.poll_recv(cx)) {
            return Poll::Ready(Some(row));
        }
        if let Some(task) = self.task.as_mut() {
            let finished = ready!(Pin::new(task).poll(cx));
            self.task = None;
            if let Err(error) = finished
                && error.is_panic()
            {
                std::panic::resume_unwind(error.into_panic());
            }
        }
        Poll::Ready(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::test_utils::setup_session;
    use crate::{col, lit};

    use arrow::array::Int64Array;

    #[tokio::test]
    async fn test_dataframe_actions() -> Result<()> {
        let spark = setup_session().await?;
        let df = spark.range(10)?.filter(col("id").lt(lit(4)));

        assert_eq!(df.count_async().await?, 4);
        assert_eq!(df.collect_async().await?.len(), 4);
        assert_eq!(df.columns_async().await?, vec!["id"]);
        Ok(())
    }

    #[tokio::test]
    async fn test_collect_record_batches() -> Result<()> {
        let spark = setup_session().await?;

        let batches = spark.range(3)?.collect_record_batches_async().await?;

        let ids: Vec<i64> = batches
            .iter()
            .flat_map(|batch| batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap().values().to_vec())
            .collect();
        assert_eq!(ids, vec![0, 1, 2]);
        Ok(())
    }

    /// Verifies that the futures are `'static`: they outlive the value they
    /// were created from and can be spawned.
    #[tokio::test]
    async fn test_action_future_is_spawnable() -> Result<()> {
        let spark = setup_session().await?;

        let count = tokio::spawn(spark.range(5)?.count_async()).await.unwrap()?;

        assert_eq!(count, 5);
        Ok(())
    }

    #[tokio::test]
    async fn test_temp_view_and_catalog() -> Result<()> {
        let spark = setup_session().await?;

        spark.range(3)?.create_or_replace_temp_view_async("ext_numbers").await?;

        assert!(spark.catalog().table_exists_async("ext_numbers").await?);
        assert!(spark.catalog().drop_temp_view_async("ext_numbers").await?);
        Ok(())
    }

    #[tokio::test]
    async fn test_runtime_conf() -> Result<()> {
        let spark = setup_session().await?;
        let conf = spark.conf();

        conf.set_async("spark.sql.shuffle.partitions", "7").await?;

        assert_eq!(conf.get_async("spark.sql.shuffle.partitions").await?.as_deref(), Some("7"));
        Ok(())
    }

    #[tokio::test]
    async fn test_row_stream() -> Result<()> {
        use std::future::poll_fn;

        let spark = setup_session().await?;
        let mut rows = spark.range(2500)?.to_local_iterator_async(false);

        let mut count = 0;
        while let Some(row) = poll_fn(|cx| Pin::new(&mut rows).poll_next(cx)).await {
            row?;
            count += 1;
        }

        assert_eq!(count, 2500);
        Ok(())
    }

    #[tokio::test]
    async fn test_row_stream_surfaces_errors() -> Result<()> {
        use std::future::poll_fn;

        let spark = setup_session().await?;
        let mut rows = spark.sql("SELECT * FROM table_that_does_not_exist")?.to_local_iterator_async(false);

        let first = poll_fn(|cx| Pin::new(&mut rows).poll_next(cx)).await;

        assert!(matches!(first, Some(Err(_))));
        Ok(())
    }
}
