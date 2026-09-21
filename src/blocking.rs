//! Moves calls into the official crate off the async executor.
//!
//! Every official method that reaches the server blocks on a runtime of its
//! own, which panics when invoked from an async task. [`blocking`] runs such a
//! call on tokio's blocking thread pool instead.
//!
//! [`Arg`] and [`Lend`] carry a method's arguments onto that thread: [`Arg`]
//! turns each argument into an owned, `'static` value, and [`Lend`] hands it
//! back in the shape the official method expects.
use crate::{Result, SparkError};

use apache_spark_connect::udf::CommonInlineUserDefinedFunctionExpression;
use apache_spark_connect::{Column, DataFrame, StorageLevel};
use std::collections::HashMap;

/// Runs `f` on tokio's blocking thread pool.
///
/// A panic in `f` is resumed on the calling task rather than converted into
/// an error: it signals a bug, not a condition callers can recover from.
pub(crate) async fn blocking<F, T>(f: F) -> Result<T>
where
    F: FnOnce() -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    match tokio::task::spawn_blocking(f).await {
        Ok(result) => result,
        Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
        Err(error) => Err(SparkError::connect_msg(error.to_string())),
    }
}

/// An argument that can be moved onto the blocking thread.
pub(crate) trait Arg {
    type Owned: Lend + Send + 'static;

    fn own(self) -> Self::Owned;
}

/// An owned argument, lent back to the official method.
pub(crate) trait Lend {
    type Ref<'a>
    where
        Self: 'a;

    fn lend(&self) -> Self::Ref<'_>;
}

impl Arg for &str {
    type Owned = String;

    fn own(self) -> String {
        self.to_owned()
    }
}

impl Lend for String {
    type Ref<'a> = &'a str;

    fn lend(&self) -> &str {
        self
    }
}

impl Arg for Option<&str> {
    type Owned = Option<String>;

    fn own(self) -> Option<String> {
        self.map(str::to_owned)
    }
}

impl Lend for Option<String> {
    type Ref<'a> = Option<&'a str>;

    fn lend(&self) -> Option<&str> {
        self.as_deref()
    }
}

impl Arg for &DataFrame {
    type Owned = DataFrame;

    fn own(self) -> DataFrame {
        self.clone()
    }
}

impl Lend for DataFrame {
    type Ref<'a> = &'a DataFrame;

    fn lend(&self) -> &DataFrame {
        self
    }
}

/// Implements [`Arg`] and [`Lend`] for arguments the official methods take by value.
macro_rules! by_value {
    ($($ty:ty),* $(,)?) => {$(
        impl Arg for $ty {
            type Owned = Self;

            fn own(self) -> Self {
                self
            }
        }

        impl Lend for $ty {
            type Ref<'a> = Self;

            fn lend(&self) -> Self {
                self.clone()
            }
        }
    )*};
}

by_value!(
    bool,
    usize,
    Option<f64>,
    HashMap<String, String>,
    StorageLevel,
    Option<StorageLevel>,
    Column,
    CommonInlineUserDefinedFunctionExpression,
);
