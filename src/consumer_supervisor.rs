//! Consumer supervisor harness and its outcome type. See DESIGN_V2.md §6.5.

/// Result of draining a supervisor or consumer group.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SupervisorOutcome {
    pub errors: usize,
    pub panics: usize,
    pub timed_out: bool,
}

impl SupervisorOutcome {
    /// Canonical process exit code: `0` clean, `1` any error, `2` any panic,
    /// `3` drain timeout. Highest non-zero condition wins.
    pub fn exit_code(&self) -> i32 {
        if self.timed_out {
            3
        } else if self.panics > 0 {
            2
        } else if self.errors > 0 {
            1
        } else {
            0
        }
    }

    pub fn is_clean(&self) -> bool {
        self.exit_code() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clean_outcome_has_exit_code_zero() {
        assert_eq!(SupervisorOutcome::default().exit_code(), 0);
        assert!(SupervisorOutcome::default().is_clean());
    }

    #[test]
    fn errors_produce_exit_code_one() {
        let o = SupervisorOutcome {
            errors: 3,
            panics: 0,
            timed_out: false,
        };
        assert_eq!(o.exit_code(), 1);
    }

    #[test]
    fn panics_outrank_errors() {
        let o = SupervisorOutcome {
            errors: 3,
            panics: 1,
            timed_out: false,
        };
        assert_eq!(o.exit_code(), 2);
    }

    #[test]
    fn timeout_outranks_everything() {
        let o = SupervisorOutcome {
            errors: 3,
            panics: 1,
            timed_out: true,
        };
        assert_eq!(o.exit_code(), 3);
    }
}
