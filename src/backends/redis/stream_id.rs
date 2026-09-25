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

/// The millisecond component of a stream id, as `MessageMetadata::timestamp_ms`.
///
/// Redis fills it with the instance clock when it generates the id, so for an
/// auto-generated id this is when the entry was appended. It is not a publish
/// time in general: a publisher may supply an explicit id, and after a clock
/// rollback Redis reuses the top entry's time and increments the sequence
/// part instead. `None` for an id that does not parse or a time past
/// `i64::MAX`, never a guess.
pub(super) fn time_component_ms(id: &str) -> Option<i64> {
    let (ms, _) = parse(id)?;
    i64::try_from(ms).ok()
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

    /// The time component is the id's first field, checked into an `i64`:
    /// a malformed id or a time past `i64::MAX` reads as unknown.
    #[test]
    fn time_component_is_the_ids_first_field_checked() {
        assert_eq!(
            time_component_ms("1526919030474-55"),
            Some(1_526_919_030_474)
        );
        assert_eq!(time_component_ms("0-0"), Some(0));
        assert_eq!(time_component_ms("9223372036854775807-0"), Some(i64::MAX));
        assert_eq!(time_component_ms("9223372036854775808-0"), None);
        assert_eq!(time_component_ms("18446744073709551615-1"), None);
        assert_eq!(time_component_ms("a-b"), None);
        assert_eq!(time_component_ms(""), None);
    }
}
