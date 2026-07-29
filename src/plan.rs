use crate::observation::Observation;
use crate::spark;

use api_parity_rs::{parity, parity_impl};
use std::collections::HashMap;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering::SeqCst;


static NEXT_PLAN_ID: AtomicI64 = AtomicI64::new(1);



#[derive(Clone)]
pub struct LogicalPlan {
    /// The child logical plan.
    child: Option<Box<LogicalPlan>>,
    /// The list of logical plans that are referenced as subqueries in this logical plan.
    references: Option<Vec<LogicalPlan>>,
    root_plan_id: i64,
    plan_id_with_references: Option<i64>,
}

#[parity_impl(
    path = "pyspark.sql.connect.plan.LogicalPlan",
    status = Implemented
)]
impl LogicalPlan {
    fn fresh_plan_id() -> i64 {
        NEXT_PLAN_ID.fetch_add(1, SeqCst)
    }

    pub fn new(child: Option<LogicalPlan>, references: Option<Vec<LogicalPlan>>) -> Self {
        LogicalPlan {
            child: child.map(Box::new),
            references: references.clone(),
            root_plan_id: Self::fresh_plan_id(),
            plan_id_with_references: if references.unwrap_or_default().len() > 0 {
                Some(Self::fresh_plan_id())
            } else {
                None
            },
        }
    }

    pub(crate) fn create_proto_relation(&self, relation_type: spark::relation::RelType) -> spark::Relation {
        return spark::Relation {
            common: Some(spark::RelationCommon {
                source_info: "".to_string(),
                plan_id: Some(self.plan_id()),
            }),
            rel_type: Some(relation_type),
        };
    }

    pub(crate) fn plan_id(&self) -> i64 {
        self.plan_id_with_references.unwrap_or(self.root_plan_id)
    }

    #[parity(
        path = ".observations",
        status = Implemented
    )]
    pub fn observations(&self) -> HashMap<String, Observation> {
        if let Some(child) = self.child.as_ref() {
            child.observations()
        } else {
            HashMap::new()
        }
    }
}