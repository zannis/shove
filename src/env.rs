//! Optional environment-variable configuration for shove's tuning knobs.
//!
//! Every service that runs shove in production ends up hand-rolling the same
//! small parser: read `SOMETHING_MAX_CONSUMERS`, trim it, treat an empty string
//! as unset, fall back to a default, range-check the result, and produce an
//! error that actually names the variable when an operator typos it. This
//! module owns that shape so downstream repos don't have to.
//!
//! It is deliberately **opt-in** (feature `env-config`) and deliberately
//! **prefix-scoped**: shove never claims a bare variable name like
//! `MAX_CONSUMERS`, because one process routinely runs several consumer groups
//! that need to be tuned independently.
//!
//! # Layers
//!
//! - [`EnvVars`] — the primitive: a prefix-scoped, typed, bounded reader.
//!   Use it directly for knobs shove doesn't model.
//! - [`ConsumerTuning`] — consumer range + prefetch, the knobs every
//!   coordinated-group backend's consumer-group config takes.
//! - [`KafkaTopicTuning`] — replication factor and partition floor.
//! - [`AutoscalerConfig::from_env`](crate::autoscaler::AutoscalerConfig::from_env)
//!   and [`NatsStreamConfig::from_env`](crate::topology::NatsStreamConfig::from_env)
//!   live next to their own types.
//!
//! # Example
//!
//! ```
//! use shove::env::ConsumerTuning;
//!
//! // ORDERS_MIN_CONSUMERS / ORDERS_MAX_CONSUMERS / ORDERS_PREFETCH_COUNT
//! let tuning = ConsumerTuning::from_env("ORDERS")?;
//! # let _ = tuning.range();
//! # Ok::<_, shove::ShoveError>(())
//! ```
//!
//! # Unset means "keep the default"
//!
//! A variable that is missing, empty, or whitespace-only is treated as unset
//! and the documented default applies. A variable that is *set to something
//! unparseable or out of range* is an error, never a silent fallback — a typo
//! in a deployment manifest should fail the process at startup, not quietly
//! run at a default the operator didn't ask for.

use std::collections::BTreeMap;
use std::env;
use std::fmt::Display;
use std::ops::RangeInclusive;
use std::str::FromStr;
use std::time::Duration;

use crate::error::{Result, ShoveError};

/// A prefix-scoped, typed reader over environment variables.
///
/// Every lookup reads `{PREFIX}_{KEY}`. Values are trimmed; a missing, empty,
/// or whitespace-only value counts as unset.
///
/// Construct with [`with_prefix`](Self::with_prefix) to read the process
/// environment, or [`from_pairs`](Self::from_pairs) to read an explicit map —
/// the latter lets you unit-test your own config wiring without mutating global
/// process state (`std::env::set_var` is `unsafe` in edition 2024 and racy
/// across test threads).
///
/// ```
/// use shove::env::EnvVars;
///
/// let vars = EnvVars::from_pairs("ORDERS", [("ORDERS_MAX_CONSUMERS", "32")]);
/// assert_eq!(vars.parse_in("MAX_CONSUMERS", 1u16, 1..=64)?, 32);
/// assert_eq!(vars.parse_in("MIN_CONSUMERS", 1u16, 1..=64)?, 1);
/// # Ok::<_, shove::ShoveError>(())
/// ```
#[derive(Debug, Clone)]
pub struct EnvVars {
    prefix: String,
    source: Source,
}

#[derive(Debug, Clone)]
enum Source {
    Process,
    Explicit(BTreeMap<String, String>),
}

impl EnvVars {
    /// Read the process environment, scoped to `prefix`.
    ///
    /// An empty prefix is allowed and means the keys are used verbatim; prefer
    /// a real prefix so two consumer groups in one process can be tuned
    /// independently.
    pub fn with_prefix(prefix: impl Into<String>) -> Self {
        Self {
            prefix: prefix.into(),
            source: Source::Process,
        }
    }

    /// Read an explicit set of `(name, value)` pairs instead of the process
    /// environment. Names are the **full** variable names, prefix included.
    pub fn from_pairs<K, V>(
        prefix: impl Into<String>,
        pairs: impl IntoIterator<Item = (K, V)>,
    ) -> Self
    where
        K: Into<String>,
        V: Into<String>,
    {
        Self {
            prefix: prefix.into(),
            source: Source::Explicit(
                pairs
                    .into_iter()
                    .map(|(k, v)| (k.into(), v.into()))
                    .collect(),
            ),
        }
    }

    /// The full variable name a `key` resolves to, e.g. `ORDERS_MAX_CONSUMERS`.
    /// Useful when building your own error messages.
    pub fn var_name(&self, key: &str) -> String {
        if self.prefix.is_empty() {
            key.to_string()
        } else {
            format!("{}_{}", self.prefix, key)
        }
    }

    /// The trimmed value of `{PREFIX}_{KEY}`, or `None` when unset, empty, or
    /// whitespace-only.
    ///
    /// A value that is set but not valid Unicode is an **error**, not an unset:
    /// `std::env::var` reports it as `VarError::NotUnicode`, and treating that
    /// as absent would silently run at a default the operator did not ask for —
    /// exactly the failure this module exists to prevent.
    pub fn get(&self, key: &str) -> Result<Option<String>> {
        let name = self.var_name(key);
        let raw = match &self.source {
            Source::Process => match env::var(&name) {
                Ok(v) => Some(v),
                Err(env::VarError::NotPresent) => None,
                Err(env::VarError::NotUnicode(raw)) => {
                    return Err(ShoveError::Validation(format!(
                        "{name}: value is not valid Unicode ({})",
                        raw.to_string_lossy()
                    )));
                }
            },
            Source::Explicit(map) => map.get(&name).cloned(),
        };
        let Some(raw) = raw else {
            return Ok(None);
        };
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            Ok(None)
        } else {
            Ok(Some(trimmed.to_string()))
        }
    }

    /// Parse the value, falling back to `default` when unset.
    pub fn parse<T>(&self, key: &str, default: T) -> Result<T>
    where
        T: FromStr,
        T::Err: Display,
    {
        Ok(self.opt_parse(key)?.unwrap_or(default))
    }

    /// Parse the value if set, returning `None` when unset.
    pub fn opt_parse<T>(&self, key: &str) -> Result<Option<T>>
    where
        T: FromStr,
        T::Err: Display,
    {
        let Some(raw) = self.get(key)? else {
            return Ok(None);
        };
        raw.parse::<T>()
            .map(Some)
            .map_err(|e| self.invalid(key, &raw, &format!("expected {}: {e}", type_hint::<T>())))
    }

    /// Parse the value and require it to fall inside `range`, falling back to
    /// `default` when unset. The `default` is *not* range-checked — it is the
    /// caller's own constant, not operator input.
    pub fn parse_in<T>(&self, key: &str, default: T, range: RangeInclusive<T>) -> Result<T>
    where
        T: FromStr + PartialOrd + Display,
        T::Err: Display,
    {
        Ok(self.opt_parse_in(key, range)?.unwrap_or(default))
    }

    /// Parse the value if set and require it to fall inside `range`.
    pub fn opt_parse_in<T>(&self, key: &str, range: RangeInclusive<T>) -> Result<Option<T>>
    where
        T: FromStr + PartialOrd + Display,
        T::Err: Display,
    {
        let Some(value) = self.opt_parse::<T>(key)? else {
            return Ok(None);
        };
        if !range.contains(&value) {
            let raw = self.get(key)?.unwrap_or_default();
            return Err(self.invalid(
                key,
                &raw,
                &format!("must be in {}..={}", range.start(), range.end()),
            ));
        }
        Ok(Some(value))
    }

    /// Parse a boolean. Accepts `true`/`false`, `1`/`0`, `yes`/`no`, `on`/`off`,
    /// case-insensitively.
    pub fn flag(&self, key: &str, default: bool) -> Result<bool> {
        let Some(raw) = self.get(key)? else {
            return Ok(default);
        };
        match raw.to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" | "on" => Ok(true),
            "false" | "0" | "no" | "off" => Ok(false),
            _ => Err(self.invalid(
                key,
                &raw,
                "expected a boolean (true/false, 1/0, yes/no, on/off)",
            )),
        }
    }

    /// Parse a whole number of seconds into a [`Duration`], falling back to
    /// `default` when unset.
    pub fn secs(&self, key: &str, default: Duration) -> Result<Duration> {
        Ok(self.opt_secs(key)?.unwrap_or(default))
    }

    /// Parse a whole number of seconds into a [`Duration`] if set.
    pub fn opt_secs(&self, key: &str) -> Result<Option<Duration>> {
        Ok(self.opt_parse::<u64>(key)?.map(Duration::from_secs))
    }

    /// Parse a value out of a fixed set of names, falling back to `default`
    /// when unset. Matching is case-insensitive and treats `-` and `_` as
    /// equivalent, so `work_queue`, `work-queue`, and `WorkQueue` all match a
    /// `"work_queue"` choice.
    ///
    /// The error lists every accepted name, so an operator typo is
    /// self-correcting.
    pub fn choice<T: Copy>(&self, key: &str, default: T, choices: &[(&str, T)]) -> Result<T> {
        let Some(raw) = self.get(key)? else {
            return Ok(default);
        };
        let normalized = normalize_choice(&raw);
        for (name, value) in choices {
            if normalize_choice(name) == normalized {
                return Ok(*value);
            }
        }
        let accepted = choices
            .iter()
            .map(|(name, _)| *name)
            .collect::<Vec<_>>()
            .join(", ");
        Err(self.invalid(key, &raw, &format!("expected one of: {accepted}")))
    }

    /// A [`ShoveError::Validation`] naming the variable and the value that was
    /// rejected. Use this for cross-field rules the typed readers can't express.
    pub fn invalid(&self, key: &str, value: &str, expectation: &str) -> ShoveError {
        ShoveError::Validation(format!(
            "{}: invalid value {value:?} ({expectation})",
            self.var_name(key)
        ))
    }
}

fn normalize_choice(s: &str) -> String {
    s.chars()
        .filter(|c| *c != '-' && *c != '_')
        .flat_map(char::to_lowercase)
        .collect()
}

fn type_hint<T>() -> &'static str {
    // `std::any::type_name` is good enough here: the value lands in an error
    // message next to the offending string, so `u16` / `f64` reads fine.
    let name = std::any::type_name::<T>();
    name.rsplit("::").next().unwrap_or(name)
}

// ---------------------------------------------------------------------------
// ConsumerTuning
// ---------------------------------------------------------------------------

/// Consumer-group sizing read from the environment: the autoscaler's consumer
/// range plus the per-consumer prefetch count.
///
/// These are the three knobs every coordinated-group backend's consumer-group
/// config takes, so `ConsumerTuning` is backend-agnostic and feeds them all.
/// SQS has no consumer group — there [`prefetch_count_or`](Self::prefetch_count_or)
/// feeds `ConsumerOptions` and the range does not apply.
///
/// | Variable | Type | Default |
/// |---|---|---|
/// | `{PREFIX}_MIN_CONSUMERS` | `u16`, `>= 1` | `1` |
/// | `{PREFIX}_MAX_CONSUMERS` | `u16`, `>= 1` | same as min (fixed-size group) |
/// | `{PREFIX}_PREFETCH_COUNT` | `u16`, `>= 1` | unset — keep the backend default |
///
/// Setting only `MAX_CONSUMERS` gives an autoscaling group from `1`; setting
/// only `MIN_CONSUMERS` gives a fixed-size group. `MIN > MAX` is an error that
/// names both variables.
///
/// ```
/// use shove::env::ConsumerTuning;
///
/// let tuning = ConsumerTuning::from_pairs(
///     "ORDERS",
///     [("ORDERS_MIN_CONSUMERS", "2"), ("ORDERS_MAX_CONSUMERS", "16")],
/// )?;
/// assert_eq!(tuning.range(), 2..=16);
/// assert_eq!(tuning.prefetch_count_or(10), 10); // unset -> caller's default
/// # Ok::<_, shove::ShoveError>(())
/// ```
///
/// Wiring it into a group config (RabbitMQ shown; every coordinated-group
/// backend takes the same `new(range)` shape):
///
/// ```
/// # #[cfg(feature = "rabbitmq")] {
/// use shove::RabbitMq;
/// use shove::consumer_group::ConsumerGroupConfig;
/// use shove::env::ConsumerTuning;
/// use shove::rabbitmq::RabbitMqConsumerGroupConfig;
///
/// let tuning = ConsumerTuning::from_pairs("ORDERS", [("ORDERS_MAX_CONSUMERS", "16")])?;
/// let config: ConsumerGroupConfig<RabbitMq> = ConsumerGroupConfig::new(
///     RabbitMqConsumerGroupConfig::new(tuning.range())
///         .with_prefetch_count(tuning.prefetch_count_or(10)),
/// );
/// # let _ = config;
/// # }
/// # Ok::<_, shove::ShoveError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerTuning {
    min_consumers: u16,
    max_consumers: u16,
    prefetch_count: Option<u16>,
}

impl ConsumerTuning {
    /// Read from the process environment under `prefix`.
    pub fn from_env(prefix: impl Into<String>) -> Result<Self> {
        Self::from_vars(&EnvVars::with_prefix(prefix))
    }

    /// Read from an explicit set of `(name, value)` pairs. See
    /// [`EnvVars::from_pairs`].
    pub fn from_pairs<K, V>(
        prefix: impl Into<String>,
        pairs: impl IntoIterator<Item = (K, V)>,
    ) -> Result<Self>
    where
        K: Into<String>,
        V: Into<String>,
    {
        Self::from_vars(&EnvVars::from_pairs(prefix, pairs))
    }

    /// Read from an existing [`EnvVars`], so one reader can populate several
    /// config structs.
    pub fn from_vars(vars: &EnvVars) -> Result<Self> {
        let min_consumers = vars.parse_in("MIN_CONSUMERS", 1, 1..=u16::MAX)?;
        let max_consumers = vars.parse_in("MAX_CONSUMERS", min_consumers, 1..=u16::MAX)?;
        if min_consumers > max_consumers {
            return Err(ShoveError::Validation(format!(
                "{} ({min_consumers}) must be <= {} ({max_consumers})",
                vars.var_name("MIN_CONSUMERS"),
                vars.var_name("MAX_CONSUMERS"),
            )));
        }
        Ok(Self {
            min_consumers,
            max_consumers,
            prefetch_count: vars.opt_parse_in("PREFETCH_COUNT", 1..=u16::MAX)?,
        })
    }

    /// The consumer range, ready to hand to a backend's
    /// `ConsumerGroupConfig::new(range)`.
    pub fn range(&self) -> RangeInclusive<u16> {
        self.min_consumers..=self.max_consumers
    }

    pub fn min_consumers(&self) -> u16 {
        self.min_consumers
    }

    pub fn max_consumers(&self) -> u16 {
        self.max_consumers
    }

    /// The configured prefetch count, or `None` when the variable was unset.
    pub fn prefetch_count(&self) -> Option<u16> {
        self.prefetch_count
    }

    /// The configured prefetch count, or `default` when unset — so a call site
    /// stays a single unconditional `.with_prefetch_count(...)`.
    pub fn prefetch_count_or(&self, default: u16) -> u16 {
        self.prefetch_count.unwrap_or(default)
    }
}

// ---------------------------------------------------------------------------
// KafkaTopicTuning
// ---------------------------------------------------------------------------

/// Kafka topic durability knobs read from the environment.
///
/// | Variable | Type | Default |
/// |---|---|---|
/// | `{PREFIX}_REPLICATION_FACTOR` | `i32`, `1..=32767` | unset |
/// | `{PREFIX}_MIN_PARTITIONS` | `i32`, `>= 1` | unset |
///
/// Both are `Option` because "unset" has to stay distinguishable from "1":
/// a single-broker dev cluster and a production R3 cluster run the same binary,
/// and shove's own defaults should apply when the operator says nothing.
///
/// ```ignore
/// let tuning = KafkaTopicTuning::from_env("INGEST")?;
/// let mut declarer = broker.topology();
/// if let Some(rf) = tuning.replication_factor() {
///     declarer = declarer.with_replication_factor(rf);
/// }
/// ```
#[cfg(feature = "kafka")]
#[cfg_attr(docsrs, doc(cfg(feature = "kafka")))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct KafkaTopicTuning {
    replication_factor: Option<i32>,
    min_partitions: Option<i32>,
}

#[cfg(feature = "kafka")]
impl KafkaTopicTuning {
    /// Read from the process environment under `prefix`.
    pub fn from_env(prefix: impl Into<String>) -> Result<Self> {
        Self::from_vars(&EnvVars::with_prefix(prefix))
    }

    /// Read from an explicit set of `(name, value)` pairs. See
    /// [`EnvVars::from_pairs`].
    pub fn from_pairs<K, V>(
        prefix: impl Into<String>,
        pairs: impl IntoIterator<Item = (K, V)>,
    ) -> Result<Self>
    where
        K: Into<String>,
        V: Into<String>,
    {
        Self::from_vars(&EnvVars::from_pairs(prefix, pairs))
    }

    /// Read from an existing [`EnvVars`].
    pub fn from_vars(vars: &EnvVars) -> Result<Self> {
        Ok(Self {
            // Kafka stores the replication factor as a signed 16-bit value.
            replication_factor: vars.opt_parse_in("REPLICATION_FACTOR", 1..=i32::from(i16::MAX))?,
            min_partitions: vars.opt_parse_in("MIN_PARTITIONS", 1..=i32::MAX)?,
        })
    }

    /// Feeds [`TopologyDeclarer::with_replication_factor`](crate::topology_declarer::TopologyDeclarer::with_replication_factor)
    /// and [`ConsumerGroup::with_default_replication_factor`](crate::consumer_group::ConsumerGroup::with_default_replication_factor).
    pub fn replication_factor(&self) -> Option<i32> {
        self.replication_factor
    }

    /// Feeds [`TopologyDeclarer::with_min_partitions`](crate::topology_declarer::TopologyDeclarer::with_min_partitions).
    pub fn min_partitions(&self) -> Option<i32> {
        self.min_partitions
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vars(pairs: &[(&str, &str)]) -> EnvVars {
        EnvVars::from_pairs("SVC", pairs.to_vec())
    }

    /// A value set to bytes that are not valid UTF-8 must fail, not fall back.
    /// `std::env::var` reports it as `VarError::NotUnicode`, and the historical
    /// `.ok()` turned that into "unset" — a typo'd manifest would then run at a
    /// default nobody asked for, which is precisely what this module promises
    /// never to do.
    ///
    /// Process-scoped, so it has to read the real environment; nextest runs
    /// every test in its own process, so the mutation cannot reach another test.
    #[cfg(unix)]
    #[test]
    fn non_unicode_process_value_is_an_error_not_a_default() {
        use std::ffi::OsStr;
        use std::os::unix::ffi::OsStrExt;

        let name = "SHOVE_ENV_TEST_NOT_UNICODE_MAX_CONSUMERS";
        // SAFETY: nextest gives this test its own process, so no other thread
        // in it is reading the environment concurrently.
        unsafe { env::set_var(name, OsStr::from_bytes(&[0x66, 0xff, 0x6f])) };

        let vars = EnvVars::with_prefix("SHOVE_ENV_TEST_NOT_UNICODE");
        let err = vars
            .parse("MAX_CONSUMERS", 4u16)
            .expect_err("a non-Unicode value must not resolve to the default");
        assert!(
            matches!(&err, ShoveError::Validation(m) if m.contains(name)),
            "error must name the variable, got: {err}"
        );

        // SAFETY: same as above.
        unsafe { env::remove_var(name) };
    }

    #[test]
    fn unset_falls_back_to_default() {
        assert_eq!(vars(&[]).parse("MAX_CONSUMERS", 4u16).unwrap(), 4);
    }

    #[test]
    fn empty_and_whitespace_count_as_unset() {
        assert_eq!(
            vars(&[("SVC_MAX_CONSUMERS", "")])
                .parse("MAX_CONSUMERS", 4u16)
                .unwrap(),
            4
        );
        assert_eq!(
            vars(&[("SVC_MAX_CONSUMERS", "   ")])
                .parse("MAX_CONSUMERS", 4u16)
                .unwrap(),
            4
        );
    }

    #[test]
    fn value_is_trimmed_before_parsing() {
        assert_eq!(
            vars(&[("SVC_MAX_CONSUMERS", "  12\n")])
                .parse("MAX_CONSUMERS", 4u16)
                .unwrap(),
            12
        );
    }

    #[test]
    fn unparseable_value_errors_and_names_the_variable() {
        let err = vars(&[("SVC_MAX_CONSUMERS", "lots")])
            .parse("MAX_CONSUMERS", 4u16)
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("SVC_MAX_CONSUMERS"), "got: {msg}");
        assert!(msg.contains("\"lots\""), "got: {msg}");
    }

    #[test]
    fn out_of_range_value_errors_and_shows_the_bounds() {
        let err = vars(&[("SVC_MAX_CONSUMERS", "0")])
            .parse_in("MAX_CONSUMERS", 4u16, 1..=64)
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("SVC_MAX_CONSUMERS"), "got: {msg}");
        assert!(msg.contains("1..=64"), "got: {msg}");
    }

    #[test]
    fn default_is_not_range_checked() {
        // The default is the caller's own constant, not operator input.
        assert_eq!(vars(&[]).parse_in("N", 0u16, 1..=64).unwrap(), 0);
    }

    #[test]
    fn empty_prefix_uses_the_key_verbatim() {
        let vars = EnvVars::from_pairs("", [("MAX_CONSUMERS", "7")]);
        assert_eq!(vars.var_name("MAX_CONSUMERS"), "MAX_CONSUMERS");
        assert_eq!(vars.parse("MAX_CONSUMERS", 1u16).unwrap(), 7);
    }

    #[test]
    fn flag_accepts_the_documented_spellings() {
        for truthy in ["true", "TRUE", "1", "yes", "On"] {
            assert!(
                vars(&[("SVC_ENABLED", truthy)])
                    .flag("ENABLED", false)
                    .unwrap()
            );
        }
        for falsy in ["false", "0", "NO", "off"] {
            assert!(
                !vars(&[("SVC_ENABLED", falsy)])
                    .flag("ENABLED", true)
                    .unwrap()
            );
        }
        assert!(
            vars(&[("SVC_ENABLED", "maybe")])
                .flag("ENABLED", true)
                .is_err()
        );
    }

    #[test]
    fn secs_parses_whole_seconds() {
        assert_eq!(
            vars(&[("SVC_TIMEOUT_SECS", "45")])
                .secs("TIMEOUT_SECS", Duration::from_secs(30))
                .unwrap(),
            Duration::from_secs(45)
        );
        assert_eq!(vars(&[]).opt_secs("TIMEOUT_SECS").unwrap(), None);
    }

    #[test]
    fn choice_is_case_and_separator_insensitive() {
        let choices = [("work_queue", 1u8), ("limits", 2)];
        for spelling in ["work_queue", "work-queue", "WorkQueue", "WORK_QUEUE"] {
            assert_eq!(
                vars(&[("SVC_RETENTION", spelling)])
                    .choice("RETENTION", 0, &choices)
                    .unwrap(),
                1
            );
        }
        let err = vars(&[("SVC_RETENTION", "forever")])
            .choice("RETENTION", 0, &choices)
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("work_queue, limits"), "got: {msg}");
    }

    #[test]
    fn consumer_tuning_defaults_to_a_single_consumer() {
        let tuning = ConsumerTuning::from_vars(&vars(&[])).unwrap();
        assert_eq!(tuning.range(), 1..=1);
        assert_eq!(tuning.prefetch_count(), None);
        assert_eq!(tuning.prefetch_count_or(10), 10);
    }

    #[test]
    fn consumer_tuning_max_alone_autoscales_from_one() {
        let tuning = ConsumerTuning::from_vars(&vars(&[("SVC_MAX_CONSUMERS", "16")])).unwrap();
        assert_eq!(tuning.range(), 1..=16);
    }

    #[test]
    fn consumer_tuning_min_alone_pins_a_fixed_size_group() {
        let tuning = ConsumerTuning::from_vars(&vars(&[("SVC_MIN_CONSUMERS", "4")])).unwrap();
        assert_eq!(tuning.range(), 4..=4);
    }

    #[test]
    fn consumer_tuning_rejects_inverted_range_naming_both_variables() {
        let err = ConsumerTuning::from_vars(&vars(&[
            ("SVC_MIN_CONSUMERS", "8"),
            ("SVC_MAX_CONSUMERS", "2"),
        ]))
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("SVC_MIN_CONSUMERS"), "got: {msg}");
        assert!(msg.contains("SVC_MAX_CONSUMERS"), "got: {msg}");
    }

    #[test]
    fn consumer_tuning_rejects_zero_consumers() {
        assert!(ConsumerTuning::from_vars(&vars(&[("SVC_MIN_CONSUMERS", "0")])).is_err());
        assert!(ConsumerTuning::from_vars(&vars(&[("SVC_PREFETCH_COUNT", "0")])).is_err());
    }

    #[test]
    fn consumer_tuning_reads_prefetch() {
        let tuning = ConsumerTuning::from_vars(&vars(&[("SVC_PREFETCH_COUNT", "50")])).unwrap();
        assert_eq!(tuning.prefetch_count(), Some(50));
        assert_eq!(tuning.prefetch_count_or(10), 50);
    }

    #[cfg(feature = "kafka")]
    #[test]
    fn kafka_topic_tuning_reads_replication_and_partitions() {
        let tuning = KafkaTopicTuning::from_vars(&vars(&[
            ("SVC_REPLICATION_FACTOR", "3"),
            ("SVC_MIN_PARTITIONS", "12"),
        ]))
        .unwrap();
        assert_eq!(tuning.replication_factor(), Some(3));
        assert_eq!(tuning.min_partitions(), Some(12));

        let unset = KafkaTopicTuning::from_vars(&vars(&[])).unwrap();
        assert_eq!(unset.replication_factor(), None);
        assert_eq!(unset.min_partitions(), None);
    }

    #[cfg(feature = "kafka")]
    #[test]
    fn kafka_topic_tuning_rejects_zero_replication() {
        assert!(KafkaTopicTuning::from_vars(&vars(&[("SVC_REPLICATION_FACTOR", "0")])).is_err());
    }
}
