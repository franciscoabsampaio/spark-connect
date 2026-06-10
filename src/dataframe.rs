use crate::{SparkError, SparkSession};
use crate::spark::Plan;

use arrow::array::RecordBatch;

pub struct DataFrame {
    session: SparkSession,
    plan: Plan,
}

impl DataFrame {
    pub fn new(session: SparkSession, plan: Plan) -> Self {
        Self { session, plan }
    }

    /// Collect the results from a lazy [`plan`](crate::spark::Plan).
    pub async fn collect(&self) -> Result<Vec<RecordBatch>, SparkError> {
        let mut client = self.session.client()?;

        client.to_batches(self.plan.clone())
            .await
            .map_err(|e| SparkError::from(e))
    }
}