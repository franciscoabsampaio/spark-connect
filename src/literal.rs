use apache_spark_connect::LiteralExpression;

#[cfg(feature = "chrono")]
use chrono::{NaiveDate, NaiveDateTime};

/// A trait that allows automatic conversion of Rust primitives and complex types into Spark data types.
///
/// # Overview
///
/// The `ToLiteral` trait provides a unified interface for converting Rust values into Spark SQL
/// [`LiteralExpression`]s. Implementations of this trait allow seamless and type-safe
/// construction of Spark literals from native Rust values, supporting primitive types (such as
/// integers, floats, booleans, and strings). Complex types (decimals, arrays, maps, structs,
/// typed nulls) are bound by passing a [`LiteralExpression`] directly.
///
/// Special cases are handled for date/time types when the `chrono` feature is enabled.
///
/// # Examples
///
/// ```rust
/// use spark_connect::ToLiteral;
///
/// let lit = 42i32.to_literal(); // LiteralExpression::Integer(42)
/// let lit = "hello".to_literal(); // LiteralExpression::String("hello")
/// ```
///
/// This trait is intended for use with [`SparkSession::query()`](crate::SparkSession::query)
/// to facilitate construction of parameterized queries.
pub trait ToLiteral {
    fn to_literal(self) -> LiteralExpression;
}

/// Macro to implement ToLiteral for a type mapping to a LiteralExpression variant.
macro_rules! impl_to_literal {
    ($ty:ty => $variant:ident) => {
        impl ToLiteral for $ty {
            fn to_literal(self) -> LiteralExpression {
                LiteralExpression::$variant(self.into())
            }
        }
    };
}

// Primitives
impl_to_literal!(i8 => Byte);
impl_to_literal!(i16 => Short);
impl_to_literal!(i32 => Integer);
impl_to_literal!(i64 => Long);
impl_to_literal!(f32 => Float);
impl_to_literal!(f64 => Double);
impl_to_literal!(bool => Boolean);
impl_to_literal!(String => String);
impl_to_literal!(&str => String);
impl_to_literal!(Vec<u8> => Binary);

impl ToLiteral for LiteralExpression {
    fn to_literal(self) -> LiteralExpression {
        self
    }
}

#[cfg(feature = "chrono")]
impl ToLiteral for NaiveDate {
    fn to_literal(self) -> LiteralExpression {
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        let days_since_epoch = self.signed_duration_since(epoch).num_days() as i32;
        LiteralExpression::Date(days_since_epoch)
    }
}

#[cfg(feature = "chrono")]
impl ToLiteral for NaiveDateTime {
    fn to_literal(self) -> LiteralExpression {
        LiteralExpression::Timestamp(self.and_utc().timestamp_micros())
    }
}
