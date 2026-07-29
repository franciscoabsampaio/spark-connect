use crate::{SparkError, error::SparkErrorKind};
use crate::spark::execute_plan_response::ObservedMetrics;

use api_parity_rs::parity_impl;
use uuid;


/// Struct to observe (named) metrics on a [`crate::DataFrame`].
///
/// Metrics are aggregation expressions, which are applied to the DataFrame while it is being
/// processed by an action.
///
/// The metrics have the following guarantees:
///
/// - It will compute the defined aggregates (metrics) on all the data that is flowing through
///   the Dataset during the action.
/// - It will report the value of the defined aggregate columns as soon as we reach the end of
///   the action.
///
/// The metrics columns must either contain a literal (e.g. lit(42)), or should contain one or
/// more aggregate functions (e.g. sum(a) or sum(a + b) + avg(c) - lit(1)). Expressions that
/// contain references to the input Dataset's columns must always be wrapped in an aggregate
/// function.
///
/// An Observation instance collects the metrics while the first action is executed. Subsequent
/// actions do not modify the metrics returned by [`Observation.get`]. Retrieval of the metric via
/// [`Observation.get`] is non-blocking.
pub struct Observation {
    name: Option<uuid::Uuid>,
    result: Option<ObservedMetrics>,
}

#[parity_impl(
    path = "pyspark.sql.connect.observation.Observation",
    status = Partial,
    comment = "Attachment to DataFrame is not yet implemented.",
    since = "4.0.0"
)]
impl Observation {
    /// Constructs a named or unnamed Observation instance.
    pub fn new(name: Option<uuid::Uuid>) -> Self {
        Observation {
            name,
            result: None,
        }
    }

    #[parity(
        path = ".get",
        status = Implemented
    )]
    /// Get the [`ObservedMetrics`].
    ///
    /// Waits until the observed dataset finishes its first action. Only the result of the
    /// first action is available. Subsequent actions do not modify the result.
    pub fn get(&self) -> Result<ObservedMetrics, SparkError> {
        if let Some(result) = self.result.clone() {
            Ok(result)
        } else {
            Err(SparkError::new(SparkErrorKind::ObservationNotReady))
        }
    }
}