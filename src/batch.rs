//! Accessors for Arrow query results.
//!
//! Spark Connect returns results as a `Vec<RecordBatch>`.
//! Arrow arrays are dynamically typed, so reading a value
//! means downcasting a column to its concrete array type
//! and bounds-checking the row.
//! The helpers here wrap that boilerplate
//! and surface every failure as a [`SparkError`] instead of panicking.
use crate::error::SparkErrorKind;
use crate::SparkError;

use arrow::array::{Array, ArrayAccessor, BooleanArray, ListArray, StringArray};
use arrow::record_batch::RecordBatch;

/// Downcasts a column of a single batch to a concrete Arrow array type.
///
/// Fails with [`SparkErrorKind::ColumnIndexOutOfBounds`] if `index` is past the last column,
/// or [`SparkErrorKind::ColumnTypeMismatch`] if the column is not of type `T`.
pub fn get_column<T: Array + 'static>(
    batch: &RecordBatch,
    index: usize,
) -> Result<&T, SparkError> {
    let array = batch.columns().get(index).ok_or_else(|| {
        SparkError::new(SparkErrorKind::ColumnIndexOutOfBounds {
            index,
            num_columns: batch.num_columns(),
        })
    })?;

    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        SparkError::new(SparkErrorKind::ColumnTypeMismatch {
            index,
            expected: std::any::type_name::<T>(),
            found: batch.schema().field(index).data_type().clone(),
        })
    })
}

/// Downcasts a column of a single batch to a concrete Arrow array type by name.
///
/// Fails with [`SparkErrorKind::ColumnNotFound`] if no column has that name.
pub fn get_column_by_name<'a, T: Array + 'static>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a T, SparkError> {
    let index = batch
        .schema()
        .index_of(name)
        .map_err(|_| SparkError::new(SparkErrorKind::ColumnNotFound(name.to_string())))?;

    get_column::<T>(batch, index)
}

/// Reads `row` from an array, returning `None` for SQL nulls.
fn value_at<A: ArrayAccessor>(array: A, row: usize) -> Result<Option<A::Item>, SparkError> {
    if row >= array.len() {
        return Err(SparkError::new(SparkErrorKind::RowIndexOutOfBounds {
            index: row,
            num_rows: array.len(),
        }));
    }
    Ok((!array.is_null(row)).then(|| array.value(row)))
}

/// Flattens column `col` across every batch, downcasting each to `T`.
fn collect_column<'a, T: Array + 'static, V>(
    batches: &'a [RecordBatch],
    col: usize,
    read: impl Fn(&'a T, usize) -> Result<Option<V>, SparkError>,
) -> Result<Vec<Option<V>>, SparkError> {
    let mut out = Vec::with_capacity(batches.total_rows());
    for batch in batches {
        let array = get_column::<T>(batch, col)?;
        for row in 0..array.len() {
            out.push(read(array, row)?);
        }
    }
    Ok(out)
}

/// Rejects nulls in a column that the caller expects the server to always fill.
fn require<V>(values: Vec<Option<V>>, col: usize) -> Result<Vec<V>, SparkError> {
    values
        .into_iter()
        .map(|value| value.ok_or_else(|| SparkError::new(SparkErrorKind::NullValue { index: col })))
        .collect()
}

/// Convenience accessors over a slice of [`RecordBatch`]es.
///
/// Implemented for `[RecordBatch]`, so it is available on `Vec<RecordBatch>`,
/// slices, and references alike. `first_*` methods read a scalar out of the
/// leading batch (the common shape of catalog/metadata queries); `*_column`
/// methods flatten a whole column across every batch.
pub trait RecordBatchesExt {
    /// Total number of rows across all batches.
    fn total_rows(&self) -> usize;

    /// The first batch, or [`SparkErrorKind::EmptyResult`] if there are none.
    fn first_batch(&self) -> Result<&RecordBatch, SparkError>;

    /// The value at `(col, row 0)` of the first batch as a string.
    /// Errors on an empty result or a null cell.
    fn first_str(&self, col: usize) -> Result<&str, SparkError>;

    /// The value at `(col, row 0)` of the first batch as a boolean.
    /// Errors on an empty result or a null cell.
    fn first_bool(&self, col: usize) -> Result<bool, SparkError>;

    /// Every value of `col`, flattened across all batches, as strings.
    fn str_column(&self, col: usize) -> Result<Vec<Option<&str>>, SparkError>;

    /// Like [`str_column`](Self::str_column), but for columns the server never
    /// leaves null. Fails with [`SparkErrorKind::NullValue`] if one is.
    fn str_column_required(&self, col: usize) -> Result<Vec<&str>, SparkError>;

    /// Every value of `col`, flattened across all batches, as booleans.
    fn bool_column(&self, col: usize) -> Result<Vec<Option<bool>>, SparkError>;

    /// Like [`bool_column`](Self::bool_column), but for columns the server
    /// never leaves null. Fails with [`SparkErrorKind::NullValue`] if one is.
    fn bool_column_required(&self, col: usize) -> Result<Vec<bool>, SparkError>;

    /// Every value of `col`, flattened across all batches, as lists of strings
    /// (Spark's `array<string>`, used for table and function namespaces).
    ///
    /// Nulls *within* a list are dropped; a null list itself stays `None`.
    fn str_list_column(&self, col: usize) -> Result<Vec<Option<Vec<String>>>, SparkError>;
}

impl RecordBatchesExt for [RecordBatch] {
    fn total_rows(&self) -> usize {
        self.iter().map(RecordBatch::num_rows).sum()
    }

    fn first_batch(&self) -> Result<&RecordBatch, SparkError> {
        self.first()
            .ok_or_else(|| SparkError::new(SparkErrorKind::EmptyResult))
    }

    fn first_str(&self, col: usize) -> Result<&str, SparkError> {
        let array = get_column::<StringArray>(self.first_batch()?, col)?;
        value_at(array, 0)?
            .ok_or_else(|| SparkError::new(SparkErrorKind::NullValue { index: col }))
    }

    fn first_bool(&self, col: usize) -> Result<bool, SparkError> {
        let array = get_column::<BooleanArray>(self.first_batch()?, col)?;
        value_at(array, 0)?
            .ok_or_else(|| SparkError::new(SparkErrorKind::NullValue { index: col }))
    }

    fn str_column(&self, col: usize) -> Result<Vec<Option<&str>>, SparkError> {
        collect_column::<StringArray, _>(self, col, value_at)
    }

    fn str_column_required(&self, col: usize) -> Result<Vec<&str>, SparkError> {
        require(self.str_column(col)?, col)
    }

    fn bool_column(&self, col: usize) -> Result<Vec<Option<bool>>, SparkError> {
        collect_column::<BooleanArray, _>(self, col, value_at)
    }

    fn bool_column_required(&self, col: usize) -> Result<Vec<bool>, SparkError> {
        require(self.bool_column(col)?, col)
    }

    fn str_list_column(&self, col: usize) -> Result<Vec<Option<Vec<String>>>, SparkError> {
        collect_column::<ListArray, _>(self, col, |array, row| {
            let Some(values) = value_at(array, row)? else {
                return Ok(None);
            };

            let strings = values.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                SparkError::new(SparkErrorKind::ColumnTypeMismatch {
                    index: col,
                    expected: std::any::type_name::<StringArray>(),
                    found: values.data_type().clone(),
                })
            })?;

            Ok(Some(
                strings
                    .iter()
                    .flatten()
                    .map(str::to_string)
                    .collect(),
            ))
        })
    }
}
