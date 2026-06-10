# API parity report

- Reference: python `pyspark.sql.connect` (version `3.5.4`)
- Port:      rust `spark-connect` (version `0.2.3`)

## Summary

- Reference paths: **1551**
- Covered: **19** (1.2%)
  - implemented: 14
  - partial: 5
  - unimplemented: 0
- Stale port paths (no match in reference): **21**

## Per-class coverage

| Class | Class status | Members | Covered | % |
|---|---|---:|---:|---:|
| `pyspark.sql.connect.client.core.ChannelBuilder` | implemented | 9 | 9 | 100% |
| `pyspark.sql.connect.client.core.SparkConnectClient` | implemented | 31 | 4 | 13% |
| `pyspark.sql.connect.session.SparkSession` | — | 29 | 2 | 7% |
| `pyspark.sql.connect.dataframe.DataFrame` | implemented | 102 | 1 | 1% |
| `pyspark.sql.connect._typing.UserDefinedFunctionCallable` | — | 0 | 0 | 0% |
| `pyspark.sql.connect._typing.UserDefinedFunctionLike` | — | 2 | 0 | 0% |
| `pyspark.sql.connect.catalog.Catalog` | — | 27 | 0 | 0% |
| `pyspark.sql.connect.client.artifact.Artifact` | — | 0 | 0 | 0% |
| `pyspark.sql.connect.client.artifact.ArtifactManager` | — | 3 | 0 | 0% |
| `pyspark.sql.connect.client.artifact.InMemory` | — | 0 | 0 | 0% |
| `pyspark.sql.connect.client.artifact.LocalData` | — | 0 | 0 | 0% |
| `pyspark.sql.connect.client.artifact.LocalFile` | — | 0 | 0 | 0% |
| `pyspark.sql.connect.client.core.AnalyzeResult` | — | 1 | 0 | 0% |
| `pyspark.sql.connect.client.core.AttemptManager` | — | 1 | 0 | 0% |
| `pyspark.sql.connect.client.core.ConfigResult` | — | 1 | 0 | 0% |
| `pyspark.sql.connect.client.core.MetricValue` | — | 3 | 0 | 0% |
| `pyspark.sql.connect.client.core.PlanMetrics` | — | 4 | 0 | 0% |
| `pyspark.sql.connect.client.core.PlanObservedMetrics` | — | 2 | 0 | 0% |
| `pyspark.sql.connect.client.core.RetryState` | — | 5 | 0 | 0% |
| `pyspark.sql.connect.client.core.Retrying` | — | 0 | 0 | 0% |
| `pyspark.sql.connect.client.reattach.ExecutePlanResponseReattachableIterator` | — | 4 | 0 | 0% |
| `pyspark.sql.connect.column.Column` | — | 33 | 0 | 0% |
| `pyspark.sql.connect.conf.RuntimeConf` | — | 4 | 0 | 0% |
| `pyspark.sql.connect.conversion.ArrowTableToRowsConversion` | — | 1 | 0 | 0% |
| `pyspark.sql.connect.conversion.LocalDataToArrowConversion` | — | 1 | 0 | 0% |
| `pyspark.sql.connect.dataframe.DataFrameNaFunctions` | — | 3 | 0 | 0% |
| `pyspark.sql.connect.dataframe.DataFrameStatFunctions` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.expressions.CallFunction` | — | 3 | 0 | 0% |
| `pyspark.sql.connect.expressions.CaseWhen` | — | 3 | 0 | 0% |
| `pyspark.sql.connect.expressions.CastExpression` | — | 3 | 0 | 0% |
| `pyspark.sql.connect.expressions.ColumnAlias` | — | 3 | 0 | 0% |
| `pyspark.sql.connect.expressions.ColumnReference` | — | 3 | 0 | 0% |
| `pyspark.sql.connect.expressions.CommonInlineUserDefinedFunction` | — | 5 | 0 | 0% |
| `pyspark.sql.connect.expressions.DistributedSequenceID` | — | 3 | 0 | 0% |
| `pyspark.sql.connect.expressions.DropField` | — | 3 | 0 | 0% |
| `pyspark.sql.connect.expressions.Expression` | — | 3 | 0 | 0% |
| `pyspark.sql.connect.expressions.JavaUDF` | — | 1 | 0 | 0% |
| `pyspark.sql.connect.expressions.LambdaFunction` | — | 3 | 0 | 0% |
| `pyspark.sql.connect.expressions.LiteralExpression` | — | 3 | 0 | 0% |
| `pyspark.sql.connect.expressions.PythonUDF` | — | 1 | 0 | 0% |
| `pyspark.sql.connect.expressions.SQLExpression` | — | 3 | 0 | 0% |
| `pyspark.sql.connect.expressions.SortOrder` | — | 3 | 0 | 0% |
| `pyspark.sql.connect.expressions.UnresolvedExtractValue` | — | 3 | 0 | 0% |
| `pyspark.sql.connect.expressions.UnresolvedFunction` | — | 3 | 0 | 0% |
| `pyspark.sql.connect.expressions.UnresolvedNamedLambdaVariable` | — | 4 | 0 | 0% |
| `pyspark.sql.connect.expressions.UnresolvedRegex` | — | 3 | 0 | 0% |
| `pyspark.sql.connect.expressions.UnresolvedStar` | — | 3 | 0 | 0% |
| `pyspark.sql.connect.expressions.WindowExpression` | — | 3 | 0 | 0% |
| `pyspark.sql.connect.expressions.WithField` | — | 3 | 0 | 0% |
| `pyspark.sql.connect.group.GroupedData` | — | 12 | 0 | 0% |
| `pyspark.sql.connect.group.PandasCogroupedOps` | — | 1 | 0 | 0% |
| `pyspark.sql.connect.plan.Aggregate` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.ApplyInPandasWithState` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.CacheTable` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.CachedLocalRelation` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.CachedRelation` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.CachedRemoteRelation` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.ClearCache` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.CoGroupMap` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.CollectMetrics` | — | 7 | 0 | 0% |
| `pyspark.sql.connect.plan.CommonInlineUserDefinedTableFunction` | — | 7 | 0 | 0% |
| `pyspark.sql.connect.plan.CreateExternalTable` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.CreateTable` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.CreateView` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.CurrentCatalog` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.CurrentDatabase` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.DataSource` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.DatabaseExists` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.Deduplicate` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.Drop` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.DropGlobalTempView` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.DropTempView` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.Filter` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.FunctionExists` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.GetDatabase` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.GetFunction` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.GetTable` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.GroupMap` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.Hint` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.HtmlString` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.IsCached` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.Join` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.Limit` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.ListCatalogs` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.ListColumns` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.ListDatabases` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.ListFunctions` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.ListTables` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.LocalRelation` | — | 7 | 0 | 0% |
| `pyspark.sql.connect.plan.LogicalPlan` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.MapPartitions` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.NADrop` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.NAFill` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.NAReplace` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.Offset` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.Project` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.PythonUDTF` | — | 1 | 0 | 0% |
| `pyspark.sql.connect.plan.Range` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.Read` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.RecoverPartitions` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.RefreshByPath` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.RefreshTable` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.Repartition` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.RepartitionByExpression` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.SQL` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.Sample` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.SetCurrentCatalog` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.SetCurrentDatabase` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.SetOperation` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.ShowString` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.Sort` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.StatApproxQuantile` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.StatCorr` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.StatCov` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.StatCrosstab` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.StatDescribe` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.StatFreqItems` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.StatSampleBy` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.StatSummary` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.SubqueryAlias` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.TableExists` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.Tail` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.ToDF` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.ToSchema` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.UncacheTable` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.Unpivot` | — | 7 | 0 | 0% |
| `pyspark.sql.connect.plan.WithColumns` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.WithColumnsRenamed` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.WithWatermark` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.WriteOperation` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.plan.WriteOperationV2` | — | 7 | 0 | 0% |
| `pyspark.sql.connect.plan.WriteStreamOperation` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.proto.base_pb2_grpc.SparkConnectService` | — | 8 | 0 | 0% |
| `pyspark.sql.connect.proto.base_pb2_grpc.SparkConnectServiceServicer` | — | 8 | 0 | 0% |
| `pyspark.sql.connect.proto.base_pb2_grpc.SparkConnectServiceStub` | — | 0 | 0 | 0% |
| `pyspark.sql.connect.readwriter.DataFrameReader` | — | 12 | 0 | 0% |
| `pyspark.sql.connect.readwriter.DataFrameWriter` | — | 16 | 0 | 0% |
| `pyspark.sql.connect.readwriter.DataFrameWriterV2` | — | 11 | 0 | 0% |
| `pyspark.sql.connect.readwriter.OptionUtils` | — | 0 | 0 | 0% |
| `pyspark.sql.connect.session.SparkSession.Builder` | — | 8 | 0 | 0% |
| `pyspark.sql.connect.streaming.query.StreamingQuery` | — | 12 | 0 | 0% |
| `pyspark.sql.connect.streaming.query.StreamingQueryManager` | — | 6 | 0 | 0% |
| `pyspark.sql.connect.streaming.readwriter.DataStreamReader` | — | 11 | 0 | 0% |
| `pyspark.sql.connect.streaming.readwriter.DataStreamWriter` | — | 11 | 0 | 0% |
| `pyspark.sql.connect.types.UnparsedDataType` | — | 7 | 0 | 0% |
| `pyspark.sql.connect.udf.UDFRegistration` | — | 3 | 0 | 0% |
| `pyspark.sql.connect.udf.UserDefinedFunction` | — | 1 | 0 | 0% |
| `pyspark.sql.connect.udtf.UDTFRegistration` | — | 1 | 0 | 0% |
| `pyspark.sql.connect.udtf.UserDefinedTableFunction` | — | 1 | 0 | 0% |
| `pyspark.sql.connect.window.Window` | — | 4 | 0 | 0% |
| `pyspark.sql.connect.window.WindowFrame` | — | 0 | 0 | 0% |
| `pyspark.sql.connect.window.WindowSpec` | — | 4 | 0 | 0% |

## Detail

### `pyspark.sql.connect.client.core.ChannelBuilder`

- Class status: **implemented** (impl `ChannelBuilder`)

| Member | Kind | Status | Implementation | Comment |
|---|---|---|---|---|
| `default_port` | method | implemented | `ChannelBuilder::default_port` |  |
| `endpoint` | property | implemented | `ChannelBuilder::endpoint` |  |
| `get` | method | implemented | `ChannelBuilder::get` |  |
| `metadata` | method | partial | `ChannelBuilder::metadata` | This implementation returns all metadata, whereas the original implementation does not return parameters that are explicitly used by the channel. |
| `secure` | property | implemented | `ChannelBuilder::secure` |  |
| `session_id` | property | implemented | `ChannelBuilder::session_id` |  |
| `toChannel` | method | partial | `ChannelBuilder::to_client` | Creates a gRPC client from the channel builder, instead of a channel. |
| `userAgent` | property | implemented | `ChannelBuilder::user_agent` |  |
| `userId` | property | implemented | `ChannelBuilder::user_id` |  |

### `pyspark.sql.connect.client.core.SparkConnectClient`

- Class status: **implemented** (impl `SparkConnectClient`)

| Member | Kind | Status | Implementation | Comment |
|---|---|---|---|---|
| `add_artifacts` | method | — | — |  |
| `add_tag` | method | — | — |  |
| `cache_artifact` | method | — | — |  |
| `clear_tags` | method | — | — |  |
| `close` | method | — | — |  |
| `config` | method | — | — |  |
| `copy_from_local_to_fs` | method | — | — |  |
| `disable_reattachable_execute` | method | — | — |  |
| `enable_reattachable_execute` | method | — | — |  |
| `execute_command` | method | partial | `SparkConnectClient::execute_command` | Unlike the original, this method is lazy, returning the client instead of a materialized result. |
| `explain_string` | method | — | — |  |
| `get_config_with_defaults` | method | — | — |  |
| `get_configs` | method | — | — |  |
| `get_tags` | method | implemented | `SparkConnectClient::get_tags` |  |
| `host` | property | — | — |  |
| `interrupt_all` | method | — | — |  |
| `interrupt_operation` | method | — | — |  |
| `interrupt_tag` | method | — | — |  |
| `is_closed` | property | — | — |  |
| `register_java` | method | — | — |  |
| `register_udf` | method | — | — |  |
| `register_udtf` | method | — | — |  |
| `remove_tag` | method | — | — |  |
| `retry_exception` | method | — | — |  |
| `same_semantics` | method | — | — |  |
| `schema` | method | — | — |  |
| `semantic_hash` | method | — | — |  |
| `to_pandas` | method | partial | `SparkConnectClient::to_polars` | Instead of pandas, the most common DataFrame library in Rust is polars. |
| `to_table` | method | partial | `SparkConnectClient::to_batches` | Unlike the original, this method does not return observations, and returns a vector of record batches instead of a table (since it does not exist in Rust). |
| `to_table_as_iterator` | method | — | — |  |
| `token` | property | — | — |  |

### `pyspark.sql.connect.session.SparkSession`

| Member | Kind | Status | Implementation | Comment |
|---|---|---|---|---|
| `active` | method | — | — |  |
| `addArtifact` | method | — | — |  |
| `addArtifacts` | method | — | — |  |
| `addTag` | method | — | — |  |
| `builder` | property | — | — |  |
| `catalog` | property | — | — |  |
| `clearTags` | method | — | — |  |
| `client` | property | — | — |  |
| `conf` | property | — | — |  |
| `copyFromLocalToFs` | method | — | — |  |
| `createDataFrame` | method | — | — |  |
| `getActiveSession` | method | — | — |  |
| `getTags` | method | — | — |  |
| `interruptAll` | method | — | — |  |
| `interruptOperation` | method | — | — |  |
| `interruptTag` | method | — | — |  |
| `is_stopped` | property | — | — |  |
| `range` | method | — | — |  |
| `read` | property | — | — |  |
| `readStream` | property | — | — |  |
| `removeTag` | method | — | — |  |
| `session_id` | property | implemented | `SparkConnectClient::session_id` |  |
| `sql` | method | — | — |  |
| `stop` | method | — | — |  |
| `streams` | property | — | — |  |
| `table` | method | — | — |  |
| `udf` | property | — | — |  |
| `udtf` | property | — | — |  |
| `version` | property | implemented | `SparkConnectClient::version` |  |

### `pyspark.sql.connect.dataframe.DataFrame`

- Class status: **implemented** (impl `DataFrame`)

| Member | Kind | Status | Implementation | Comment |
|---|---|---|---|---|
| `agg` | method | — | — |  |
| `alias` | method | — | — |  |
| `approxQuantile` | method | — | — |  |
| `cache` | method | — | — |  |
| `checkSameSparkSession` | method | — | — |  |
| `coalesce` | method | — | — |  |
| `colRegex` | method | — | — |  |
| `collect` | method | implemented | `DataFrame::collect` |  |
| `columns` | property | — | — |  |
| `corr` | method | — | — |  |
| `count` | method | — | — |  |
| `cov` | method | — | — |  |
| `createGlobalTempView` | method | — | — |  |
| `createOrReplaceGlobalTempView` | method | — | — |  |
| `createOrReplaceTempView` | method | — | — |  |
| `createTempView` | method | — | — |  |
| `crossJoin` | method | — | — |  |
| `crosstab` | method | — | — |  |
| `cube` | method | — | — |  |
| `describe` | method | — | — |  |
| `distinct` | method | — | — |  |
| `drop` | method | — | — |  |
| `dropDuplicates` | method | — | — |  |
| `dropDuplicatesWithinWatermark` | method | — | — |  |
| `drop_duplicates` | method | — | — |  |
| `drop_duplicates_within_watermark` | method | — | — |  |
| `dropna` | method | — | — |  |
| `dtypes` | property | — | — |  |
| `exceptAll` | method | — | — |  |
| `explain` | method | — | — |  |
| `fillna` | method | — | — |  |
| `filter` | method | — | — |  |
| `first` | method | — | — |  |
| `freqItems` | method | — | — |  |
| `groupBy` | method | — | — |  |
| `groupby` | method | — | — |  |
| `head` | method | — | — |  |
| `hint` | method | — | — |  |
| `inputFiles` | method | — | — |  |
| `intersect` | method | — | — |  |
| `intersectAll` | method | — | — |  |
| `isEmpty` | method | — | — |  |
| `isLocal` | method | — | — |  |
| `isStreaming` | property | — | — |  |
| `is_cached` | property | — | — |  |
| `join` | method | — | — |  |
| `limit` | method | — | — |  |
| `mapInArrow` | method | — | — |  |
| `mapInPandas` | method | — | — |  |
| `melt` | method | — | — |  |
| `na` | property | — | — |  |
| `observe` | method | — | — |  |
| `offset` | method | — | — |  |
| `orderBy` | method | — | — |  |
| `pandas_api` | method | — | — |  |
| `persist` | method | — | — |  |
| `printSchema` | method | — | — |  |
| `randomSplit` | method | — | — |  |
| `registerTempTable` | method | — | — |  |
| `repartition` | method | — | — |  |
| `repartitionByRange` | method | — | — |  |
| `replace` | method | — | — |  |
| `rollup` | method | — | — |  |
| `sameSemantics` | method | — | — |  |
| `sample` | method | — | — |  |
| `sampleBy` | method | — | — |  |
| `schema` | property | — | — |  |
| `select` | method | — | — |  |
| `selectExpr` | method | — | — |  |
| `semanticHash` | method | — | — |  |
| `show` | method | — | — |  |
| `sort` | method | — | — |  |
| `sortWithinPartitions` | method | — | — |  |
| `sparkSession` | property | — | — |  |
| `stat` | property | — | — |  |
| `storageLevel` | property | — | — |  |
| `subtract` | method | — | — |  |
| `summary` | method | — | — |  |
| `tail` | method | — | — |  |
| `take` | method | — | — |  |
| `to` | method | — | — |  |
| `toDF` | method | — | — |  |
| `toLocalIterator` | method | — | — |  |
| `toPandas` | method | — | — |  |
| `to_pandas_on_spark` | method | — | — |  |
| `transform` | method | — | — |  |
| `union` | method | — | — |  |
| `unionAll` | method | — | — |  |
| `unionByName` | method | — | — |  |
| `unpersist` | method | — | — |  |
| `unpivot` | method | — | — |  |
| `where` | method | — | — |  |
| `withColumn` | method | — | — |  |
| `withColumnRenamed` | method | — | — |  |
| `withColumns` | method | — | — |  |
| `withColumnsRenamed` | method | — | — |  |
| `withMetadata` | method | — | — |  |
| `withPlan` | method | — | — |  |
| `withWatermark` | method | — | — |  |
| `write` | property | — | — |  |
| `writeStream` | property | — | — |  |
| `writeTo` | method | — | — |  |

## Stale port references

Port entries whose `path` did not resolve in the reference.
Likely a typo, a removed reference API, or a path-convention drift.

| Path | Implementation | Comment |
|---|---|---|
| `pyspark.sql.session.SparkSession` | `SparkSession` |  |
| `pyspark.sql.session.SparkSession.Builder` | `SparkSessionBuilder` |  |
| `pyspark.sql.session.SparkSession.Builder.appName` | `SparkSessionBuilder::app_name` |  |
| `pyspark.sql.session.SparkSession.Builder.config` | `SparkSessionBuilder::config` |  |
| `pyspark.sql.session.SparkSession.Builder.create` | `SparkSessionBuilder::create` | Only remote (sc://) mode works; classic master URLs return Unimplemented |
| `pyspark.sql.session.SparkSession.Builder.enableHiveSupport` | `SparkSessionBuilder::enable_hive_support` |  |
| `pyspark.sql.session.SparkSession.Builder.getOrCreate` | `SparkSessionBuilder::get_or_create` | Session reuse is not yet implemented due to underlying complexity. |
| `pyspark.sql.session.SparkSession.Builder.master` | `SparkSessionBuilder::master` | value is stored but classic-mode (non-sc://) resolution is not wired up |
| `pyspark.sql.session.SparkSession.Builder.remote` | `SparkSessionBuilder::remote` |  |
| `pyspark.sql.session.SparkSession.builder` | `SparkSession::builder` |  |
| `pyspark.sql.session.SparkSession.client` | `SparkSession::client` |  |
| `pyspark.sql.session.SparkSession.interruptAll` | `SparkSession::interrupt_all` |  |
| `pyspark.sql.session.SparkSession.interruptOperation` | `SparkSession::interrupt_operation` |  |
| `pyspark.sql.session.SparkSession.sql` | `SparkSession::sql` |  |
| `pyspark.sql.session.SparkSession.version` | `SparkSession::version` |  |
| `pyspark.sql.types.DataType` | `spark_connect::types::DataType` |  |
| `pyspark.sql.types.StructField` | `StructField` |  |
| `pyspark.sql.types.StructField.fromJson` | `StructField::from_json` |  |
| `pyspark.sql.types.StructField.jsonValue` | `StructField::json_value` |  |
| `pyspark.sql.types.StructField.simpleString` | `StructField::simple_string` |  |
| `pyspark.sql.types.StructType` | `StructType` |  |

