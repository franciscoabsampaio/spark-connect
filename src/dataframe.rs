use crate::{SparkError, SparkSession};
use crate::spark::Plan;

use api_parity_rs::{parity, parity_impl};
use arrow::array::RecordBatch;

pub struct DataFrame {
    session: SparkSession,
    plan: Plan,
}

#[parity_impl(
    path = "pyspark.sql.connect.dataframe.DataFrame",
    status = Implemented,
)]
impl DataFrame {
    pub fn new(session: SparkSession, plan: Plan) -> Self {
        Self { session, plan }
    }

    #[parity(
        path = ".collect",
        status = Implemented,
    )]
    /// Returns all the records in the DataFrame as a vector of `[RecordBatch](arrow::array::RecordBatch)`.
    /// Notes
    /// -----
    /// This method should only be used if the resulting list is expected to be small,
    /// as all the data is loaded into the driver's memory.
    ///
    /// Examples
    /// --------
    /// Example: Collecting all rows of a DataFrame
    ///
    /// ```rs
    /// df = spark.sql(<query>)
    /// df.collect()
    /// ```
    pub async fn collect(&self) -> Result<Vec<RecordBatch>, SparkError> {
        let mut client = self.session.client()?;

        client.to_batches(self.plan.clone())
            .await
            .map_err(|e| SparkError::from(e))
    }
}