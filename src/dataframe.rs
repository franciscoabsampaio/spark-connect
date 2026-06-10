use spark::Plan;

pub struct DataFrame {
    client: SparkConnectClient,
    plan: Plan,
}

impl DataFrame {
    pub fn new(client: SparkConnectClient, plan: Plan) -> Self {
        Self { client, plan }
    }

    /// Collect the results from a lazy [`plan`](crate::spark::Plan).
    pub async fn collect(&self) -> Result<Vec<RecordBatch>, SparkError> {
        let mut client = self.client.clone();

        client.to_batches(self.plan.clone()).await
    }
}