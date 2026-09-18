use crate::{SparkError, error::SparkErrorKind};

use core::fmt;


/// A Spark release, ordered by `major`, then `minor`, then `patch`.
///
/// Used to compare the version reported by the connected server against the
/// version an API was introduced in - see
/// [`SparkSession::require_since`](crate::SparkSession::require_since).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Version {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
}

impl Version {
    pub const fn new(major: u32, minor: u32, patch: u32) -> Self {
        Self { major, minor, patch }
    }

    /// Parses `major.minor[.patch]`, discarding any pre-release or build
    /// suffix - servers report versions such as `4.0.0-preview2`.
    ///
    /// A missing patch reads as `0`.
    pub fn parse(version: &str) -> Result<Self, SparkError> {
        let invalid = || SparkError::new(SparkErrorKind::InvalidVersion(version.to_string()));

        let core = version.split(['-', '+']).next().ok_or_else(invalid)?;
        let mut fields = core.split('.');
        let mut field = || fields.next().and_then(|f| f.parse().ok()).ok_or_else(invalid);

        let major = field()?;
        let minor = field()?;
        let patch = field().unwrap_or(0);

        Ok(Self::new(major, minor, patch))
    }
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_full_version() {
        assert_eq!(Version::parse("3.5.4").unwrap(), Version::new(3, 5, 4));
    }

    #[test]
    fn test_parse_defaults_missing_patch() {
        assert_eq!(Version::parse("4.0").unwrap(), Version::new(4, 0, 0));
    }

    #[test]
    fn test_parse_discards_suffix() {
        assert_eq!(Version::parse("4.0.0-preview2").unwrap(), Version::new(4, 0, 0));
    }

    #[test]
    fn test_parse_rejects_garbage() {
        assert!(Version::parse("").is_err());
        assert!(Version::parse("3").is_err());
        assert!(Version::parse("three.five.four").is_err());
    }

    #[test]
    fn test_ordering_is_by_field_not_lexicographic() {
        assert!(Version::new(3, 10, 0) > Version::new(3, 9, 0));
        assert!(Version::new(4, 0, 0) > Version::new(3, 5, 7));
        assert!(Version::new(3, 5, 7) >= Version::new(3, 5, 7));
    }

    #[test]
    fn test_display_round_trips() {
        let version = Version::new(4, 0, 0);
        assert_eq!(Version::parse(&version.to_string()).unwrap(), version);
    }
}
