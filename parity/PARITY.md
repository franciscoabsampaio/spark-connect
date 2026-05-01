# API parity report

- Python: `parity/pyspark-3.5.json` (version `3.5.4`)
- Rust:   `parity/spark-connect.json` (version `3.5.7`)

## Summary

- Classes in Python inventory: **246**
- Classes declared by Rust: **4** (1.6%)
- Members in Python inventory: **1703**
- Members covered by Rust: **23** (1.4%)
  - implemented: 17
  - partial: 5
  - unimplemented: 1
- Stale Rust references (no match in Python): **0**

## Per-class coverage

| Class | Class status | Python members | Covered | % |
|---|---|---:|---:|---:|
| `pyspark.sql.connect.client.core.ChannelBuilder` | implemented | 9 | 9 | 100% |
| `pyspark.sql.session.SparkSession.Builder` | implemented | 7 | 7 | 100% |
| `pyspark.sql.session.SparkSession` | implemented | 29 | 6 | 21% |
| `pyspark.sql.connect.session.SparkSession` | — | 29 | 1 | 3% |
| `pyspark.sql.catalog.Catalog` | — | 27 | 0 | 0% |
| `pyspark.sql.column.Column` | — | 32 | 0 | 0% |
| `pyspark.sql.conf.RuntimeConfig` | — | 4 | 0 | 0% |
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
| `pyspark.sql.connect.client.core.SparkConnectClient` | implemented | 31 | 0 | 0% |
| `pyspark.sql.connect.client.reattach.ExecutePlanResponseReattachableIterator` | — | 4 | 0 | 0% |
| `pyspark.sql.connect.column.Column` | — | 33 | 0 | 0% |
| `pyspark.sql.connect.conf.RuntimeConf` | — | 4 | 0 | 0% |
| `pyspark.sql.connect.conversion.ArrowTableToRowsConversion` | — | 1 | 0 | 0% |
| `pyspark.sql.connect.conversion.LocalDataToArrowConversion` | — | 1 | 0 | 0% |
| `pyspark.sql.connect.dataframe.DataFrame` | — | 102 | 0 | 0% |
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
| `pyspark.sql.context.HiveContext` | — | 24 | 0 | 0% |
| `pyspark.sql.context.SQLContext` | — | 23 | 0 | 0% |
| `pyspark.sql.dataframe.DataFrame` | — | 106 | 0 | 0% |
| `pyspark.sql.dataframe.DataFrameNaFunctions` | — | 3 | 0 | 0% |
| `pyspark.sql.dataframe.DataFrameStatFunctions` | — | 6 | 0 | 0% |
| `pyspark.sql.group.GroupedData` | — | 12 | 0 | 0% |
| `pyspark.sql.observation.Observation` | — | 1 | 0 | 0% |
| `pyspark.sql.pandas.conversion.PandasConversionMixin` | — | 1 | 0 | 0% |
| `pyspark.sql.pandas.conversion.SparkConversionMixin` | — | 1 | 0 | 0% |
| `pyspark.sql.pandas.functions.PandasUDFType` | — | 0 | 0 | 0% |
| `pyspark.sql.pandas.group_ops.PandasCogroupedOps` | — | 1 | 0 | 0% |
| `pyspark.sql.pandas.group_ops.PandasGroupedOpsMixin` | — | 4 | 0 | 0% |
| `pyspark.sql.pandas.map_ops.PandasMapOpsMixin` | — | 2 | 0 | 0% |
| `pyspark.sql.pandas.serializers.ApplyInPandasWithStateSerializer` | — | 4 | 0 | 0% |
| `pyspark.sql.pandas.serializers.ArrowCollectSerializer` | — | 3 | 0 | 0% |
| `pyspark.sql.pandas.serializers.ArrowStreamPandasSerializer` | — | 4 | 0 | 0% |
| `pyspark.sql.pandas.serializers.ArrowStreamPandasUDFSerializer` | — | 4 | 0 | 0% |
| `pyspark.sql.pandas.serializers.ArrowStreamPandasUDTFSerializer` | — | 4 | 0 | 0% |
| `pyspark.sql.pandas.serializers.ArrowStreamSerializer` | — | 3 | 0 | 0% |
| `pyspark.sql.pandas.serializers.ArrowStreamUDFSerializer` | — | 3 | 0 | 0% |
| `pyspark.sql.pandas.serializers.CogroupUDFSerializer` | — | 4 | 0 | 0% |
| `pyspark.sql.pandas.serializers.SpecialLengths` | — | 0 | 0 | 0% |
| `pyspark.sql.readwriter.DataFrameReader` | — | 12 | 0 | 0% |
| `pyspark.sql.readwriter.DataFrameWriter` | — | 16 | 0 | 0% |
| `pyspark.sql.readwriter.DataFrameWriterV2` | — | 11 | 0 | 0% |
| `pyspark.sql.readwriter.OptionUtils` | — | 0 | 0 | 0% |
| `pyspark.sql.session.classproperty` | — | 3 | 0 | 0% |
| `pyspark.sql.sql_formatter.SQLStringFormatter` | — | 9 | 0 | 0% |
| `pyspark.sql.streaming.listener.JStreamingQueryListener` | — | 4 | 0 | 0% |
| `pyspark.sql.streaming.listener.JStreamingQueryListener.Java` | — | 0 | 0 | 0% |
| `pyspark.sql.streaming.listener.QueryIdleEvent` | — | 5 | 0 | 0% |
| `pyspark.sql.streaming.listener.QueryProgressEvent` | — | 3 | 0 | 0% |
| `pyspark.sql.streaming.listener.QueryStartedEvent` | — | 6 | 0 | 0% |
| `pyspark.sql.streaming.listener.QueryTerminatedEvent` | — | 6 | 0 | 0% |
| `pyspark.sql.streaming.listener.SinkProgress` | — | 7 | 0 | 0% |
| `pyspark.sql.streaming.listener.SourceProgress` | — | 12 | 0 | 0% |
| `pyspark.sql.streaming.listener.StateOperatorProgress` | — | 16 | 0 | 0% |
| `pyspark.sql.streaming.listener.StreamingQueryListener` | — | 5 | 0 | 0% |
| `pyspark.sql.streaming.listener.StreamingQueryProgress` | — | 19 | 0 | 0% |
| `pyspark.sql.streaming.query.StreamingQuery` | — | 12 | 0 | 0% |
| `pyspark.sql.streaming.query.StreamingQueryManager` | — | 6 | 0 | 0% |
| `pyspark.sql.streaming.readwriter.DataStreamReader` | — | 11 | 0 | 0% |
| `pyspark.sql.streaming.readwriter.DataStreamWriter` | — | 11 | 0 | 0% |
| `pyspark.sql.streaming.state.GroupState` | — | 12 | 0 | 0% |
| `pyspark.sql.streaming.state.GroupStateTimeout` | — | 0 | 0 | 0% |
| `pyspark.sql.types.AnsiIntervalType` | — | 7 | 0 | 0% |
| `pyspark.sql.types.ArrayType` | — | 8 | 0 | 0% |
| `pyspark.sql.types.AtomicType` | — | 7 | 0 | 0% |
| `pyspark.sql.types.BinaryType` | — | 7 | 0 | 0% |
| `pyspark.sql.types.BooleanType` | — | 7 | 0 | 0% |
| `pyspark.sql.types.ByteType` | — | 7 | 0 | 0% |
| `pyspark.sql.types.CharType` | — | 7 | 0 | 0% |
| `pyspark.sql.types.DataType` | — | 7 | 0 | 0% |
| `pyspark.sql.types.DataTypeSingleton` | — | 1 | 0 | 0% |
| `pyspark.sql.types.DateConverter` | — | 2 | 0 | 0% |
| `pyspark.sql.types.DateType` | — | 7 | 0 | 0% |
| `pyspark.sql.types.DatetimeConverter` | — | 2 | 0 | 0% |
| `pyspark.sql.types.DatetimeNTZConverter` | — | 2 | 0 | 0% |
| `pyspark.sql.types.DayTimeIntervalType` | — | 7 | 0 | 0% |
| `pyspark.sql.types.DayTimeIntervalTypeConverter` | — | 2 | 0 | 0% |
| `pyspark.sql.types.DecimalType` | — | 7 | 0 | 0% |
| `pyspark.sql.types.DoubleType` | — | 7 | 0 | 0% |
| `pyspark.sql.types.FloatType` | — | 7 | 0 | 0% |
| `pyspark.sql.types.FractionalType` | — | 7 | 0 | 0% |
| `pyspark.sql.types.IntegerType` | — | 7 | 0 | 0% |
| `pyspark.sql.types.IntegralType` | — | 7 | 0 | 0% |
| `pyspark.sql.types.LongType` | — | 7 | 0 | 0% |
| `pyspark.sql.types.MapType` | — | 8 | 0 | 0% |
| `pyspark.sql.types.NullType` | — | 7 | 0 | 0% |
| `pyspark.sql.types.NumericType` | — | 7 | 0 | 0% |
| `pyspark.sql.types.NumpyArrayConverter` | — | 2 | 0 | 0% |
| `pyspark.sql.types.NumpyScalarConverter` | — | 2 | 0 | 0% |
| `pyspark.sql.types.ShortType` | — | 7 | 0 | 0% |
| `pyspark.sql.types.StringType` | — | 7 | 0 | 0% |
| `pyspark.sql.types.StructField` | — | 8 | 0 | 0% |
| `pyspark.sql.types.StructType` | — | 10 | 0 | 0% |
| `pyspark.sql.types.TimestampNTZType` | — | 7 | 0 | 0% |
| `pyspark.sql.types.TimestampType` | — | 7 | 0 | 0% |
| `pyspark.sql.types.UserDefinedType` | — | 13 | 0 | 0% |
| `pyspark.sql.types.VarcharType` | — | 7 | 0 | 0% |
| `pyspark.sql.types.YearMonthIntervalType` | — | 7 | 0 | 0% |
| `pyspark.sql.udf.UDFRegistration` | — | 3 | 0 | 0% |
| `pyspark.sql.udf.UserDefinedFunction` | — | 2 | 0 | 0% |
| `pyspark.sql.udtf.UDTFRegistration` | — | 1 | 0 | 0% |
| `pyspark.sql.udtf.UserDefinedTableFunction` | — | 2 | 0 | 0% |
| `pyspark.sql.utils.ForeachBatchFunction` | — | 1 | 0 | 0% |
| `pyspark.sql.utils.ForeachBatchFunction.Java` | — | 0 | 0 | 0% |
| `pyspark.sql.window.Window` | — | 4 | 0 | 0% |
| `pyspark.sql.window.WindowSpec` | — | 4 | 0 | 0% |

## Detail

### `pyspark.sql.connect.client.core.ChannelBuilder`

- Class status: **implemented** (impl `ChannelBuilder`)

| Member | Status | Implementation | Comment |
|---|---|---|---|
| `default_port` | implemented | `ChannelBuilder::default_port` |  |
| `endpoint` | implemented | `ChannelBuilder::endpoint` |  |
| `get` | implemented | `ChannelBuilder::get` |  |
| `metadata` | partial | `ChannelBuilder::metadata` | This implementation returns all metadata, whereas the original implementation does not return parameters that are explicitly used by the channel. |
| `secure` | implemented | `ChannelBuilder::secure` |  |
| `session_id` | implemented | `ChannelBuilder::session_id` |  |
| `toChannel` | partial | `ChannelBuilder::to_client` | Creates a gRPC client from the channel builder, instead of a channel. |
| `userAgent` | implemented | `ChannelBuilder::user_agent` |  |
| `userId` | implemented | `ChannelBuilder::user_id` |  |

### `pyspark.sql.session.SparkSession.Builder`

- Class status: **implemented** (impl `SparkSessionBuilder`)

| Member | Status | Implementation | Comment |
|---|---|---|---|
| `appName` | implemented | `SparkSessionBuilder::app_name` |  |
| `config` | implemented | `SparkSessionBuilder::config` |  |
| `create` | partial | `SparkSessionBuilder::create` | Only remote (sc://) mode works; classic master URLs return Unimplemented |
| `enableHiveSupport` | implemented | `SparkSessionBuilder::enable_hive_support` |  |
| `getOrCreate` | unimplemented | `SparkSessionBuilder::get_or_create` | Session reuse is not yet implemented due to underlying complexity. |
| `master` | partial | `SparkSessionBuilder::master` | value is stored but classic-mode (non-sc://) resolution is not wired up |
| `remote` | implemented | `SparkSessionBuilder::remote` |  |

### `pyspark.sql.session.SparkSession`

- Class status: **implemented** (impl `SparkSession`)

| Member | Status | Implementation | Comment |
|---|---|---|---|
| `active` | — | — |  |
| `addArtifact` | — | — |  |
| `addArtifacts` | — | — |  |
| `addTag` | — | — |  |
| `builder` | implemented | `SparkSession::builder` |  |
| `catalog` | — | — |  |
| `clearTags` | — | — |  |
| `client` | implemented | `SparkSession::client` |  |
| `conf` | — | — |  |
| `copyFromLocalToFs` | — | — |  |
| `createDataFrame` | — | — |  |
| `getActiveSession` | — | — |  |
| `getTags` | — | — |  |
| `interruptAll` | implemented | `SparkSession::interrupt_all` |  |
| `interruptOperation` | implemented | `SparkSession::interrupt_operation` |  |
| `interruptTag` | — | — |  |
| `newSession` | — | — |  |
| `range` | — | — |  |
| `read` | — | — |  |
| `readStream` | — | — |  |
| `removeTag` | — | — |  |
| `sparkContext` | — | — |  |
| `sql` | partial | `SparkSession::sql` |  |
| `stop` | — | — |  |
| `streams` | — | — |  |
| `table` | — | — |  |
| `udf` | — | — |  |
| `udtf` | — | — |  |
| `version` | implemented | `SparkSession::version` |  |

### `pyspark.sql.connect.session.SparkSession`

| Member | Status | Implementation | Comment |
|---|---|---|---|
| `active` | — | — |  |
| `addArtifact` | — | — |  |
| `addArtifacts` | — | — |  |
| `addTag` | — | — |  |
| `builder` | — | — |  |
| `catalog` | — | — |  |
| `clearTags` | — | — |  |
| `client` | — | — |  |
| `conf` | — | — |  |
| `copyFromLocalToFs` | — | — |  |
| `createDataFrame` | — | — |  |
| `getActiveSession` | — | — |  |
| `getTags` | — | — |  |
| `interruptAll` | — | — |  |
| `interruptOperation` | — | — |  |
| `interruptTag` | — | — |  |
| `is_stopped` | — | — |  |
| `range` | — | — |  |
| `read` | — | — |  |
| `readStream` | — | — |  |
| `removeTag` | — | — |  |
| `session_id` | implemented | `SparkConnectClient::session_id` |  |
| `sql` | — | — |  |
| `stop` | — | — |  |
| `streams` | — | — |  |
| `table` | — | — |  |
| `udf` | — | — |  |
| `udtf` | — | — |  |
| `version` | — | — |  |

### `pyspark.sql.connect.client.core.SparkConnectClient`

- Class status: **implemented** (impl `SparkConnectClient`)

| Member | Status | Implementation | Comment |
|---|---|---|---|
| `add_artifacts` | — | — |  |
| `add_tag` | — | — |  |
| `cache_artifact` | — | — |  |
| `clear_tags` | — | — |  |
| `close` | — | — |  |
| `config` | — | — |  |
| `copy_from_local_to_fs` | — | — |  |
| `disable_reattachable_execute` | — | — |  |
| `enable_reattachable_execute` | — | — |  |
| `execute_command` | — | — |  |
| `explain_string` | — | — |  |
| `get_config_with_defaults` | — | — |  |
| `get_configs` | — | — |  |
| `get_tags` | — | — |  |
| `host` | — | — |  |
| `interrupt_all` | — | — |  |
| `interrupt_operation` | — | — |  |
| `interrupt_tag` | — | — |  |
| `is_closed` | — | — |  |
| `register_java` | — | — |  |
| `register_udf` | — | — |  |
| `register_udtf` | — | — |  |
| `remove_tag` | — | — |  |
| `retry_exception` | — | — |  |
| `same_semantics` | — | — |  |
| `schema` | — | — |  |
| `semantic_hash` | — | — |  |
| `to_pandas` | — | — |  |
| `to_table` | — | — |  |
| `to_table_as_iterator` | — | — |  |
| `token` | — | — |  |

