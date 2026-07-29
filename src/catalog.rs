use crate::batch::RecordBatchesExt;
use crate::DataFrame;
use crate::error::SparkErrorKind;
use crate::storage_level::StorageLevel;
use crate::types::StructType;
use crate::{SparkError, SparkSession};
use crate::spark;
use crate::plan::LogicalPlan;

use std::collections::HashMap;

use arrow::array::RecordBatch;
use api_parity_rs::{parity, parity_impl};

/// Spark SQL catalog interface.
///
/// Use [`Catalog`] on an active session.
pub struct Catalog {
    spark_session: SparkSession,
}

/// Name and description of a catalog, as returned by [`Catalog::list_catalogs`].
#[parity(
    path = "pyspark.sql.catalog.CatalogMetadata",
    status = Implemented,
)]
pub struct CatalogMetadata {
    pub name: String,
    pub description: Option<String>,
}

pub struct Database {
    pub name: String,
    pub catalog: Option<String>,
    pub description: Option<String>,
    pub location_uri: String,
}

#[parity_impl(
    path = "pyspark.sql.catalog.Database",
    status = Implemented,
)]
impl Database {
    /// Reshapes a Vec<RecordBatch> into `Database` rows.
    fn from(batches: &[RecordBatch]) -> Result<Vec<Database>, SparkError> {
        let names = batches.str_column_required(0)?;
        let catalogs = batches.str_column(1)?;
        let descriptions = batches.str_column(2)?;
        let location_uris = batches.str_column_required(3)?;

        Ok((0..batches.total_rows())
            .map(|row| Database {
                name: names[row].to_string(),
                catalog: catalogs[row].map(str::to_string),
                description: descriptions[row].map(str::to_string),
                location_uri: location_uris[row].to_string(),
            })
            .collect())
    }
}

pub struct Table {
    pub name: String,
    pub catalog: Option<String>,
    pub namespace: Option<Vec<String>>,
    pub description: Option<String>,
    pub table_type: String,
    pub is_temporary: bool,
}

#[parity_impl(
    path = "pyspark.sql.catalog.Table",
    status = Implemented,
)]
impl Table {
    /// Reshapes a Vec<RecordBatch> result into `Table` rows.
    fn from(batches: &[RecordBatch]) -> Result<Vec<Table>, SparkError> {
        let names = batches.str_column_required(0)?;
        let catalogs = batches.str_column(1)?;
        let mut namespaces = batches.str_list_column(2)?;
        let descriptions = batches.str_column(3)?;
        let table_types = batches.str_column_required(4)?;
        let is_temporary = batches.bool_column_required(5)?;

        Ok((0..batches.total_rows())
            .map(|row| Table {
                name: names[row].to_string(),
                catalog: catalogs[row].map(str::to_string),
                namespace: namespaces[row].take(),
                description: descriptions[row].map(str::to_string),
                table_type: table_types[row].to_string(),
                is_temporary: is_temporary[row],
            })
            .collect())
    }

    /// The single namespace component, when the table sits directly in a
    /// database rather than a deeper namespace.
    #[parity(
        path = ".database",
        status = Implemented,
    )]
    pub fn database(&self) -> Option<&str> {
        match self.namespace.as_deref() {
            Some([database]) => Some(database),
            _ => None,
        }
    }
}

pub struct Function {
    pub name: String,
    pub catalog: Option<String>,
    pub namespace: Option<Vec<String>>,
    pub description: Option<String>,
    pub class_name: String,
    pub is_temporary: bool,
}


#[parity_impl(
    path = "pyspark.sql.catalog.Function",
    status = Implemented,
)]
impl Function {
    /// Reshapes a Vec<RecordBatch> result into `Function` rows.
    fn from(batches: &[RecordBatch]) -> Result<Vec<Function>, SparkError> {
        let names = batches.str_column_required(0)?;
        let catalogs = batches.str_column(1)?;
        let mut namespaces = batches.str_list_column(2)?;
        let descriptions = batches.str_column(3)?;
        let class_names = batches.str_column_required(4)?;
        let is_temporary = batches.bool_column_required(5)?;

        Ok((0..batches.total_rows())
            .map(|row| Function {
                name: names[row].to_string(),
                catalog: catalogs[row].map(str::to_string),
                namespace: namespaces[row].take(),
                description: descriptions[row].map(str::to_string),
                class_name: class_names[row].to_string(),
                is_temporary: is_temporary[row],
            })
            .collect())
    }
}

/// A column of a table or view.
///
/// PySpark 4 adds an `isCluster` field, but Spark 3.5 returns six columns and
/// no clustering flag, so it is absent here.
pub struct Column {
    pub name: String,
    pub description: Option<String>,
    pub data_type: String,
    pub nullable: bool,
    pub is_partition: bool,
    pub is_bucket: bool,
}

#[parity_impl(
    path = "pyspark.sql.catalog.Column",
    status = Implemented,
)]
impl Column {
    /// Reshapes a Vec<RecordBatch> result into `Column` rows.
    fn from(batches: &[RecordBatch]) -> Result<Vec<Column>, SparkError> {
        let names = batches.str_column_required(0)?;
        let descriptions = batches.str_column(1)?;
        let data_types = batches.str_column_required(2)?;
        let nullable = batches.bool_column_required(3)?;
        let is_partition = batches.bool_column_required(4)?;
        let is_bucket = batches.bool_column_required(5)?;

        Ok((0..batches.total_rows())
            .map(|row| Column {
                name: names[row].to_string(),
                description: descriptions[row].map(str::to_string),
                data_type: data_types[row].to_string(),
                nullable: nullable[row],
                is_partition: is_partition[row],
                is_bucket: is_bucket[row],
            })
            .collect())
    }
}

/// Takes the single row a `get*` call is expected to return.
fn single_row<T>(rows: Vec<T>) -> Result<T, SparkError> {
    rows.into_iter()
        .next()
        .ok_or_else(|| SparkError::new(SparkErrorKind::EmptyResult))
}

#[parity_impl(
    path = "pyspark.sql.connect.catalog.Catalog",
    status = Implemented,
)]
impl Catalog {
    /// Create a new Catalog that wraps the underlying Spark session.
    pub fn new(spark_session: SparkSession) -> Self {
        Catalog { spark_session }
    }

    async fn execute_and_fetch(&mut self, catalog_type: spark::catalog::CatType) -> Result<Vec<RecordBatch>, SparkError> {
        let relation_type = spark::relation::RelType::Catalog(spark::Catalog {
            cat_type: Some(catalog_type)
        });
        let relation = LogicalPlan::new(None, None)
            .create_proto_relation(relation_type);

        let plan = spark::Plan {
            op_type: Some(spark::plan::OpType::Root(relation)),
        };

        Ok(DataFrame::new(self.spark_session.clone(), plan).collect().await?)
    }

    /// Returns the current catalog in this session.
    #[parity(
        path = ".currentCatalog",
        status = Implemented,
    )]
    pub async fn current_catalog(&mut self) -> Result<String, SparkError> {
        let catalog_type = spark::catalog::CatType::CurrentCatalog(
            spark::CurrentCatalog {},
        );

        Ok(self.execute_and_fetch(catalog_type).await?.first_str(0)?.to_string())
    }

    /// Sets the current catalog in this session.
    #[parity(
        path = ".setCurrentCatalog",
        status = Implemented,
    )]
    pub async fn set_current_catalog(&mut self, catalog_name: &str) -> Result<(), SparkError> {
        let catalog_type = spark::catalog::CatType::SetCurrentCatalog(
            spark::SetCurrentCatalog { catalog_name: catalog_name.into() },
        );

        self.execute_and_fetch(catalog_type).await?;

        Ok(())
    }

    /// Returns a list of catalogs available in this session.
    /// 
    /// With `pattern`, returns only catalogs whose name matches that pattern.
    #[parity(
        path = ".listCatalogs",
        status = Implemented,
    )]
    pub async fn list_catalogs(&mut self, pattern: Option<String>) -> Result<Vec<CatalogMetadata>, SparkError> {
        let catalog_type = spark::catalog::CatType::ListCatalogs(
            spark::ListCatalogs { pattern },
        );

        let batches = self.execute_and_fetch(catalog_type).await?;

        let names = batches.str_column_required(0)?;
        let descriptions = batches.str_column(1)?;

        Ok((0..batches.total_rows())
            .map(|row| CatalogMetadata {
                name: names[row].to_string(),
                description: descriptions[row].map(str::to_string),
            })
            .collect()
        )
    }

    /// Returns the current database (namespace) in this session.
    #[parity(
        path = ".currentDatabase",
        status = Implemented,
    )]
    pub async fn current_database(&mut self) -> Result<String, SparkError> {
        let catalog_type = spark::catalog::CatType::CurrentDatabase(
            spark::CurrentDatabase {},
        );

        Ok(self.execute_and_fetch(catalog_type).await?.first_str(0)?.to_string())
    }

    /// Sets the current database (namespace) in this session.
    #[parity(
        path = ".setCurrentDatabase",
        status = Implemented,
    )]
    pub async fn set_current_database(&mut self, db_name: &str) -> Result<(), SparkError> {
        let catalog_type = spark::catalog::CatType::SetCurrentDatabase(
            spark::SetCurrentDatabase { db_name: db_name.into() },
        );

        self.execute_and_fetch(catalog_type).await?;

        Ok(())
    }

    /// Returns a list of databases (namespaces) available within the current catalog.
    ///
    /// With `pattern`, returns only databases whose name matches that pattern.
    #[parity(
        path = ".listDatabases",
        status = Implemented,
    )]
    pub async fn list_databases(&mut self, pattern: Option<String>) -> Result<Vec<Database>, SparkError> {
        let catalog_type = spark::catalog::CatType::ListDatabases(
            spark::ListDatabases { pattern },
        );

        Database::from(&self.execute_and_fetch(catalog_type).await?)
    }

    /// Returns the database (namespace) with the given name.
    #[parity(
        path = ".getDatabase",
        status = Implemented,
    )]
    pub async fn get_database(&mut self, db_name: &str) -> Result<Database, SparkError> {
        let catalog_type = spark::catalog::CatType::GetDatabase(
            spark::GetDatabase { db_name: db_name.into() },
        );

        single_row(Database::from(&self.execute_and_fetch(catalog_type).await?)?)
    }

    /// Returns whether the database (namespace) with the given name exists.
    #[parity(
        path = ".databaseExists",
        status = Implemented,
    )]
    pub async fn database_exists(&mut self, db_name: &str) -> Result<bool, SparkError> {
        let catalog_type = spark::catalog::CatType::DatabaseExists(
            spark::DatabaseExists { db_name: db_name.into() },
        );

        self.execute_and_fetch(catalog_type).await?.first_bool(0)
    }

    /// Returns a list of tables and views in the given database.
    ///
    /// Without `db_name`, lists tables in the current database. With
    /// `pattern`, returns only tables whose name matches that pattern.
    #[parity(
        path = ".listTables",
        status = Implemented,
    )]
    pub async fn list_tables(
        &mut self,
        db_name: Option<String>,
        pattern: Option<String>,
    ) -> Result<Vec<Table>, SparkError> {
        let catalog_type = spark::catalog::CatType::ListTables(
            spark::ListTables { db_name, pattern },
        );

        Table::from(&self.execute_and_fetch(catalog_type).await?)
    }

    /// Returns the table or view with the given name.
    ///
    /// `table_name` may be qualified (`db_name.table_name`).
    #[parity(
        path = ".getTable",
        status = Implemented,
    )]
    pub async fn get_table(&mut self, table_name: &str) -> Result<Table, SparkError> {
        let catalog_type = spark::catalog::CatType::GetTable(
            spark::GetTable { table_name: table_name.into(), db_name: None },
        );

        single_row(Table::from(&self.execute_and_fetch(catalog_type).await?)?)
    }

    /// Returns whether the table or view with the given name exists.
    ///
    /// `table_name` may be qualified (`db_name.table_name`).
    #[parity(
        path = ".tableExists",
        status = Partial,
        comment = "The deprecated `dbName` parameter is not offered; qualify `table_name` instead.",
    )]
    pub async fn table_exists(&mut self, table_name: &str) -> Result<bool, SparkError> {
        let catalog_type = spark::catalog::CatType::TableExists(
            spark::TableExists { table_name: table_name.into(), db_name: None },
        );

        self.execute_and_fetch(catalog_type).await?.first_bool(0)
    }

    /// Returns the columns of the given table or view.
    ///
    /// `table_name` may be qualified (`db_name.table_name`).
    #[parity(
        path = ".listColumns",
        status = Partial,
        comment = "The deprecated `dbName` parameter is not offered; qualify `table_name` instead.",
    )]
    pub async fn list_columns(&mut self, table_name: &str) -> Result<Vec<Column>, SparkError> {
        let catalog_type = spark::catalog::CatType::ListColumns(
            spark::ListColumns { table_name: table_name.into(), db_name: None },
        );

        Column::from(&self.execute_and_fetch(catalog_type).await?)
    }

    /// Returns a list of functions registered in the given database.
    ///
    /// Without `db_name`, lists functions in the current database. With
    /// `pattern`, returns only functions whose name matches that pattern.
    #[parity(
        path = ".listFunctions",
        status = Implemented,
    )]
    pub async fn list_functions(
        &mut self,
        db_name: Option<String>,
        pattern: Option<String>,
    ) -> Result<Vec<Function>, SparkError> {
        let catalog_type = spark::catalog::CatType::ListFunctions(
            spark::ListFunctions { db_name, pattern },
        );

        Function::from(&self.execute_and_fetch(catalog_type).await?)
    }

    /// Returns the function with the given name.
    ///
    /// `function_name` may be qualified (`db_name.function_name`).
    #[parity(
        path = ".getFunction",
        status = Implemented,
    )]
    pub async fn get_function(&mut self, function_name: &str) -> Result<Function, SparkError> {
        let catalog_type = spark::catalog::CatType::GetFunction(
            spark::GetFunction { function_name: function_name.into(), db_name: None },
        );

        single_row(Function::from(&self.execute_and_fetch(catalog_type).await?)?)
    }

    /// Returns whether the function with the given name exists.
    ///
    /// `function_name` may be qualified (`db_name.function_name`).
    #[parity(
        path = ".functionExists",
        status = Partial,
        comment = "The deprecated `dbName` parameter is not offered; qualify `function_name` instead.",
    )]
    pub async fn function_exists(&mut self, function_name: &str) -> Result<bool, SparkError> {
        let catalog_type = spark::catalog::CatType::FunctionExists(
            spark::FunctionExists { function_name: function_name.into(), db_name: None },
        );

        self.execute_and_fetch(catalog_type).await?.first_bool(0)
    }

    /// Creates a table based on the dataset in a data source.
    #[parity(
        path = ".createTable",
        status = Implemented,
    )]
    pub async fn create_table(
        &mut self,
        table_name: &str,
        path: Option<&str>,
        source: Option<&str>,
        schema: Option<&StructType>,
        description: Option<&str>,
        options: HashMap<String, String>,
    ) -> Result<Vec<RecordBatch>, SparkError> {
        // PySpark's NOT_EXPECTED_TYPE check has no counterpart here: passing a
        // non-`StructType` schema does not typecheck in the first place.
        let catalog_type = spark::catalog::CatType::CreateTable(
            spark::CreateTable {
                table_name: table_name.into(),
                path: path.map(Into::into),
                source: source.map(Into::into),
                schema: schema.map(StructType::to_proto).transpose()?,
                description: description.map(Into::into),
                options,
            },
        );

        Ok(self.execute_and_fetch(catalog_type).await?)
    }

    /// Drops the local temporary view with the given name, returning whether
    /// it had been registered.
    #[parity(
        path = ".dropTempView",
        status = Implemented,
    )]
    pub async fn drop_temp_view(&mut self, view_name: &str) -> Result<bool, SparkError> {
        let catalog_type = spark::catalog::CatType::DropTempView(
            spark::DropTempView { view_name: view_name.into() },
        );

        self.execute_and_fetch(catalog_type).await?.first_bool(0)
    }

    /// Drops the global temporary view with the given name, returning whether
    /// it had been registered.
    #[parity(
        path = ".dropGlobalTempView",
        status = Implemented,
    )]
    pub async fn drop_global_temp_view(&mut self, view_name: &str) -> Result<bool, SparkError> {
        let catalog_type = spark::catalog::CatType::DropGlobalTempView(
            spark::DropGlobalTempView { view_name: view_name.into() },
        );

        self.execute_and_fetch(catalog_type).await?.first_bool(0)
    }

    /// Returns whether the given table or view is cached.
    #[parity(
        path = ".isCached",
        status = Implemented,
    )]
    pub async fn is_cached(&mut self, table_name: &str) -> Result<bool, SparkError> {
        let catalog_type = spark::catalog::CatType::IsCached(
            spark::IsCached { table_name: table_name.into() },
        );

        self.execute_and_fetch(catalog_type).await?.first_bool(0)
    }

    /// Caches the given table or view in memory.
    ///
    /// Without `storage_level`, Spark applies its own default.
    #[parity(
        path = ".cacheTable",
        status = Implemented,
    )]
    pub async fn cache_table(
        &mut self,
        table_name: &str,
        storage_level: Option<StorageLevel>,
    ) -> Result<(), SparkError> {
        let catalog_type = spark::catalog::CatType::CacheTable(
            spark::CacheTable {
                table_name: table_name.into(),
                storage_level: storage_level.map(Into::into),
            },
        );

        self.execute_and_fetch(catalog_type).await?;

        Ok(())
    }

    /// Removes the given table or view from the in-memory cache.
    #[parity(
        path = ".uncacheTable",
        status = Implemented,
    )]
    pub async fn uncache_table(&mut self, table_name: &str) -> Result<(), SparkError> {
        let catalog_type = spark::catalog::CatType::UncacheTable(
            spark::UncacheTable { table_name: table_name.into() },
        );

        self.execute_and_fetch(catalog_type).await?;

        Ok(())
    }

    /// Removes every cached table and view from the in-memory cache.
    #[parity(
        path = ".clearCache",
        status = Implemented,
    )]
    pub async fn clear_cache(&mut self) -> Result<(), SparkError> {
        let catalog_type = spark::catalog::CatType::ClearCache(spark::ClearCache {});

        self.execute_and_fetch(catalog_type).await?;

        Ok(())
    }

    /// Invalidates and refreshes the cached metadata of the given table, and
    /// of any cached data that depends on it.
    #[parity(
        path = ".refreshTable",
        status = Implemented,
    )]
    pub async fn refresh_table(&mut self, table_name: &str) -> Result<(), SparkError> {
        let catalog_type = spark::catalog::CatType::RefreshTable(
            spark::RefreshTable { table_name: table_name.into() },
        );

        self.execute_and_fetch(catalog_type).await?;

        Ok(())
    }

    /// Invalidates and refreshes any cached data (and metadata) for anything
    /// containing the given path.
    #[parity(
        path = ".refreshByPath",
        status = Implemented,
    )]
    pub async fn refresh_by_path(&mut self, path: &str) -> Result<(), SparkError> {
        let catalog_type = spark::catalog::CatType::RefreshByPath(
            spark::RefreshByPath { path: path.into() },
        );

        self.execute_and_fetch(catalog_type).await?;

        Ok(())
    }

    /// Recovers all the partitions of the given table, updating the catalog.
    #[parity(
        path = ".recoverPartitions",
        status = Implemented,
    )]
    pub async fn recover_partitions(&mut self, table_name: &str) -> Result<(), SparkError> {
        let catalog_type = spark::catalog::CatType::RecoverPartitions(
            spark::RecoverPartitions { table_name: table_name.into() },
        );

        self.execute_and_fetch(catalog_type).await?;

        Ok(())
    }

    /// Creates a table from a data source.
    #[parity(
        path = ".createExternalTable",
        status = Unimplemented,
        comment = "Deprecated since Spark 4.0; use [`create_table`](Self::create_table)."
    )]
    pub async fn create_external_table(&mut self) -> SparkError {
        return SparkError::new(SparkErrorKind::Unimplemented("createExternalTable is deprecated.".into()))
    }

    /// An alias for `spark.udf.register`.
    #[parity(
        path = ".registerFunction",
        status = Unimplemented,
        comment = "Requires spark.udf.register."
    )]
    pub async fn register_function(&self) -> SparkError {
        return SparkError::new(SparkErrorKind::Unimplemented("registerFunction requires spark.udf.register".into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::test_utils::setup_session;
    use crate::types::{DataType, StructField};

    /// Pins the column order and nullability `list_databases` relies on:
    /// `(name, catalog, description, locationUri)`, with `name` and
    /// `locationUri` never null.
    #[tokio::test]
    async fn test_list_databases_schema() -> Result<(), SparkError> {
        let session = setup_session().await?;
        let mut catalog = session.catalog();

        let databases = catalog.list_databases(None).await?;

        let default = databases
            .iter()
            .find(|db| db.name == "default")
            .expect("expected a `default` database");
        assert_eq!(default.catalog.as_deref(), Some("spark_catalog"));
        assert!(
            default.location_uri.contains("warehouse"),
            "unexpected location_uri: {}",
            default.location_uri
        );
        Ok(())
    }

    /// Creates a table to inspect, dropping any leftover of the same name
    /// first so reruns are independent.
    async fn setup_table(session: &SparkSession, name: &str) -> Result<(), SparkError> {
        for statement in [
            format!("DROP TABLE IF EXISTS {name}"),
            format!("CREATE TABLE {name} (id INT, label STRING) USING parquet"),
        ] {
            session.sql(&statement, vec![]).await?.collect().await?;
        }
        Ok(())
    }

    /// Creates a table from an explicit `StructType`, then reads the schema
    /// back to confirm the server parsed the DDL we rendered.
    #[tokio::test]
    async fn test_create_table_with_schema() -> Result<(), SparkError> {
        let session = setup_session().await?;
        session
            .sql("DROP TABLE IF EXISTS parity_created", vec![])
            .await?
            .collect()
            .await?;
        let mut catalog = session.catalog();

        let schema = StructType::new(vec![
            StructField::new("id".into(), DataType::Int32, true, None),
            StructField::new("label".into(), DataType::Utf8, true, None),
        ]);
        catalog
            .create_table(
                "parity_created",
                None,
                Some("parquet"),
                Some(&schema),
                Some("created by the parity suite"),
                HashMap::new(),
            )
            .await?;

        assert!(catalog.table_exists("parity_created").await?);

        let columns = catalog.list_columns("parity_created").await?;
        let rendered: Vec<(&str, &str)> = columns
            .iter()
            .map(|c| (c.name.as_str(), c.data_type.as_str()))
            .collect();
        assert_eq!(rendered, [("id", "int"), ("label", "string")]);
        Ok(())
    }

    /// Without a schema, the source infers one from the data at `path`.
    #[tokio::test]
    async fn test_create_table_without_schema() -> Result<(), SparkError> {
        let session = setup_session().await?;
        let path = "/tmp/parity_inferred";
        for statement in [
            "DROP TABLE IF EXISTS parity_inferred".to_string(),
            format!(
                "INSERT OVERWRITE DIRECTORY '{path}' USING parquet \
                 SELECT 1 AS id, 'a' AS label"
            ),
        ] {
            session.sql(&statement, vec![]).await?.collect().await?;
        }
        let mut catalog = session.catalog();

        catalog
            .create_table("parity_inferred", Some(path), Some("parquet"), None, None, HashMap::new())
            .await?;

        let columns = catalog.list_columns("parity_inferred").await?;
        let rendered: Vec<(&str, &str)> = columns
            .iter()
            .map(|c| (c.name.as_str(), c.data_type.as_str()))
            .collect();
        assert_eq!(rendered, [("id", "int"), ("label", "string")]);
        Ok(())
    }

    #[tokio::test]
    async fn test_get_database() -> Result<(), SparkError> {
        let session = setup_session().await?;
        let mut catalog = session.catalog();

        let database = catalog.get_database("default").await?;

        assert_eq!(database.name, "default");
        assert_eq!(database.catalog.as_deref(), Some("spark_catalog"));
        Ok(())
    }

    #[tokio::test]
    async fn test_database_exists() -> Result<(), SparkError> {
        let session = setup_session().await?;
        let mut catalog = session.catalog();

        assert!(catalog.database_exists("default").await?);
        assert!(!catalog.database_exists("no_such_database").await?);
        Ok(())
    }

    /// Pins the `listTables` column order: `(name, catalog, namespace,
    /// description, tableType, isTemporary)`.
    #[tokio::test]
    async fn test_list_and_get_table() -> Result<(), SparkError> {
        let session = setup_session().await?;
        setup_table(&session, "parity_tables").await?;
        let mut catalog = session.catalog();

        let tables = catalog.list_tables(None, Some("parity_tables".into())).await?;
        let listed = tables
            .iter()
            .find(|t| t.name == "parity_tables")
            .expect("expected the table just created");
        assert_eq!(listed.catalog.as_deref(), Some("spark_catalog"));
        assert_eq!(listed.database(), Some("default"));
        assert!(!listed.is_temporary);

        let fetched = catalog.get_table("parity_tables").await?;
        assert_eq!(fetched.name, listed.name);
        assert_eq!(fetched.table_type, listed.table_type);
        Ok(())
    }

    #[tokio::test]
    async fn test_table_exists() -> Result<(), SparkError> {
        let session = setup_session().await?;
        setup_table(&session, "parity_exists").await?;
        let mut catalog = session.catalog();

        assert!(catalog.table_exists("parity_exists").await?);
        assert!(catalog.table_exists("default.parity_exists").await?);
        assert!(!catalog.table_exists("no_such_table").await?);
        Ok(())
    }

    /// Pins the `listColumns` column order: `(name, description, dataType,
    /// nullable, isPartition, isBucket)`. Spark 3.5 returns no `isCluster`.
    #[tokio::test]
    async fn test_list_columns() -> Result<(), SparkError> {
        let session = setup_session().await?;
        setup_table(&session, "parity_columns").await?;
        let mut catalog = session.catalog();

        let columns = catalog.list_columns("parity_columns").await?;

        let names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["id", "label"]);
        assert_eq!(columns[0].data_type, "int");
        assert!(columns[0].nullable);
        assert!(!columns[0].is_partition);
        assert!(!columns[0].is_bucket);
        Ok(())
    }

    /// Pins the `listFunctions` column order: `(name, catalog, namespace,
    /// description, className, isTemporary)`.
    #[tokio::test]
    async fn test_list_and_get_function() -> Result<(), SparkError> {
        let session = setup_session().await?;
        let mut catalog = session.catalog();

        let functions = catalog.list_functions(None, Some("abs".into())).await?;
        let listed = functions
            .iter()
            .find(|f| f.name == "abs")
            .expect("expected the built-in `abs` function");
        assert!(listed.class_name.contains("Abs"), "unexpected class: {}", listed.class_name);

        let fetched = catalog.get_function("abs").await?;
        assert_eq!(fetched.class_name, listed.class_name);
        Ok(())
    }

    #[tokio::test]
    async fn test_function_exists() -> Result<(), SparkError> {
        let session = setup_session().await?;
        let mut catalog = session.catalog();

        assert!(catalog.function_exists("abs").await?);
        assert!(!catalog.function_exists("no_such_function").await?);
        Ok(())
    }

    #[tokio::test]
    async fn test_temp_view_lifecycle() -> Result<(), SparkError> {
        let session = setup_session().await?;
        session
            .sql("CREATE OR REPLACE TEMPORARY VIEW parity_view AS SELECT 1 AS id", vec![])
            .await?
            .collect()
            .await?;
        let mut catalog = session.catalog();

        assert!(catalog.table_exists("parity_view").await?);
        assert!(catalog.drop_temp_view("parity_view").await?);
        // Already dropped, so the second call reports nothing to drop.
        assert!(!catalog.drop_temp_view("parity_view").await?);
        Ok(())
    }

    #[tokio::test]
    async fn test_cache_lifecycle() -> Result<(), SparkError> {
        let session = setup_session().await?;
        setup_table(&session, "parity_cache").await?;
        let mut catalog = session.catalog();

        assert!(!catalog.is_cached("parity_cache").await?);

        catalog.cache_table("parity_cache", Some(StorageLevel::MEMORY_AND_DISK)).await?;
        assert!(catalog.is_cached("parity_cache").await?);

        catalog.uncache_table("parity_cache").await?;
        assert!(!catalog.is_cached("parity_cache").await?);

        // `clear_cache` is a no-op here, but must still round-trip cleanly.
        catalog.clear_cache().await?;
        Ok(())
    }

    /// The maintenance calls return no rows; this pins that they are accepted
    /// rather than erroring.
    #[tokio::test]
    async fn test_refresh_calls() -> Result<(), SparkError> {
        let session = setup_session().await?;
        setup_table(&session, "parity_refresh").await?;
        let mut catalog = session.catalog();

        catalog.refresh_table("parity_refresh").await?;
        catalog.refresh_by_path("/tmp").await?;
        Ok(())
    }

    /// `pattern` should filter by database name.
    #[tokio::test]
    async fn test_list_databases_pattern() -> Result<(), SparkError> {
        let session = setup_session().await?;
        let mut catalog = session.catalog();

        let databases = catalog.list_databases(Some("def*".to_string())).await?;

        assert!(databases.iter().any(|db| db.name == "default"));
        Ok(())
    }
}