use spark_connect::SparkSessionBuilder;

#[tokio::main]
async fn main() {
    // Retrieve Databricks connection parameters from environment variables.
    let host = std::env::var("DATABRICKS_HOST")
        .expect("DATABRICKS_HOST env var not set");
    let cluster_id = std::env::var("DATABRICKS_CLUSTER_ID")
        .expect("DATABRICKS_CLUSTER_ID env var not set");
    let token = std::env::var("DATABRICKS_TOKEN")
        .expect("DATABRICKS_TOKEN env var not set");

    // Build the Spark session.
    let session = SparkSessionBuilder::new(&format!(
        "sc://{host}:443/;\
        use_ssl=true;\
        x-databricks-cluster-id={cluster_id};\
        token={token};"
    ))
        .build()
        .await
        .expect("Failed to create Spark session");

    // Execute a simple query to verify the connection.
    let batches = session
        .query("SELECT COUNT(*) FROM raw.raw_balance")
        .execute()
        .await
        .unwrap();
    
    println!("Retrieved: {:?}", batches);
}
