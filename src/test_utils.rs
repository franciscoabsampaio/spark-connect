#[cfg(test)]
pub mod test_utils {
    use crate::{SparkSession, error::SparkError};

    /// Test fixture to create a SparkSession.
    /// Requires a Spark Connect server instance available at localhost:15002.
    pub async fn setup_session() -> Result<SparkSession, SparkError> {
        let connection = format!("sc://localhost:15002");

        SparkSession::builder()
            .remote(&connection)
            .build().await
    }
}
