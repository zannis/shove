//! Redis stream entry-ID (`ms-seq`) parsing.
//!
//! Stream IDs order numerically per component, so comparing the raw strings
//! is wrong (`"9-1"` sorts after `"10-0"` lexicographically). Parse into a
//! `(u64, u64)` tuple and compare those.

/// Parse a `ms-seq` stream ID into its numeric components.
///
/// Returns `None` for anything that is not two `u64`s joined by a dash —
/// callers treat an unparseable ID as "no safe answer" rather than guessing.
pub(super) fn parse(id: &str) -> Option<(u64, u64)> {
    let (ms, seq) = id.split_once('-')?;
    Some((ms.parse().ok()?, seq.parse().ok()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_valid_ids() {
        assert_eq!(parse("0-0"), Some((0, 0)));
        assert_eq!(parse("1526919030474-55"), Some((1526919030474, 55)));
    }

    #[test]
    fn tuple_ordering_is_numeric_not_lexicographic() {
        // "9-1" > "10-0" as strings, but must compare numerically smaller.
        assert!(parse("9-1").unwrap() < parse("10-0").unwrap());
        assert!(parse("10-2").unwrap() > parse("10-1").unwrap());
        assert_eq!(parse("5-5").unwrap(), parse("5-5").unwrap());
    }

    #[test]
    fn rejects_malformed_ids() {
        assert_eq!(parse(""), None);
        assert_eq!(parse("5"), None);
        assert_eq!(parse("a-b"), None);
        assert_eq!(parse("5-"), None);
        assert_eq!(parse("-5"), None);
    }
}
