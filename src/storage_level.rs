//! Storage levels for cached tables and DataFrames.
use crate::spark;

use api_parity_rs::parity;

/// How Spark should keep a cached dataset: in memory, on disk, off-heap,
/// serialized or not, and how many replicas to hold.
///
/// The associated constants mirror PySpark's presets; build the struct
/// directly for anything they do not cover.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[parity(
    path = "pyspark.storagelevel.StorageLevel",
    status = Implemented,
)]
pub struct StorageLevel {
    pub use_disk: bool,
    pub use_memory: bool,
    pub use_off_heap: bool,
    pub deserialized: bool,
    pub replication: i32,
}

/// The presets mirror `pyspark.storagelevel.StorageLevel`.
impl StorageLevel {
    pub const NONE: Self = Self::new(false, false, false, false, 1);
    pub const DISK_ONLY: Self = Self::new(true, false, false, false, 1);
    pub const DISK_ONLY_2: Self = Self::new(true, false, false, false, 2);
    pub const DISK_ONLY_3: Self = Self::new(true, false, false, false, 3);
    pub const MEMORY_ONLY: Self = Self::new(false, true, false, true, 1);
    pub const MEMORY_ONLY_2: Self = Self::new(false, true, false, true, 2);
    pub const MEMORY_AND_DISK: Self = Self::new(true, true, false, true, 1);
    pub const MEMORY_AND_DISK_2: Self = Self::new(true, true, false, true, 2);
    pub const OFF_HEAP: Self = Self::new(true, true, true, false, 1);
    pub const MEMORY_AND_DISK_DESER: Self = Self::new(true, true, false, true, 1);

    pub const fn new(
        use_disk: bool,
        use_memory: bool,
        use_off_heap: bool,
        deserialized: bool,
        replication: i32,
    ) -> Self {
        StorageLevel { use_disk, use_memory, use_off_heap, deserialized, replication }
    }
}

impl From<StorageLevel> for spark::StorageLevel {
    fn from(level: StorageLevel) -> Self {
        spark::StorageLevel {
            use_disk: level.use_disk,
            use_memory: level.use_memory,
            use_off_heap: level.use_off_heap,
            deserialized: level.deserialized,
            replication: level.replication,
        }
    }
}
