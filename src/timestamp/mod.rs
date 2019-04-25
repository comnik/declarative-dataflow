//! Various timestamp implementations.

use std::time::Duration;

pub mod altneu;
pub mod pair;

/// Possible timestamp types.
///
/// This enum captures the currently supported timestamp types, and is
/// the least common denominator for the types of times moved around.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub enum Time {
    /// Logical transaction time or sequence numbers.
    TxId(u64),
    /// Real time.
    Real(Duration),
    /// Bitemporal.
    Bi(Duration, u64),
}

impl std::convert::From<Time> for u64 {
    fn from(t: Time) -> u64 {
        if let Time::TxId(time) = t {
            time
        } else {
            panic!("Time {:?} can't be converted to u64", t);
        }
    }
}

impl std::convert::From<Time> for Duration {
    fn from(t: Time) -> Duration {
        if let Time::Real(time) = t {
            time
        } else {
            panic!("Time {:?} can't be converted to Duration", t);
        }
    }
}

impl std::convert::From<Time> for pair::Pair<Duration, u64> {
    fn from(t: Time) -> Self {
        if let Time::Bi(sys, event) = t {
            Self::new(sys, event)
        } else {
            panic!("Time {:?} can't be converted to Pair", t);
        }
    }
}

/// Extension trait for timestamp types that can be safely re-wound to
/// an earlier time. This is required for automatically advancing
/// traces according to their configured slack.
pub trait Rewind {
    /// Returns a new timestamp corresponding to self rewound by the
    /// specified amount of slack. Calling rewind is always safe, in
    /// that no invalid times will be returned.
    ///
    /// e.g. 0.rewind(TxId(10)) -> 0
    /// and Duration(0).rewind(Real(Duration(1))) -> Duration(0)
    fn rewind(&self, slack: Time) -> Self;
}

impl Rewind for u64 {
    fn rewind(&self, slack: Time) -> Self {
        match self.checked_sub(slack.into()) {
            None => *self,
            Some(rewound) => rewound,
        }
    }
}

impl Rewind for Duration {
    fn rewind(&self, slack: Time) -> Self {
        match self.checked_sub(slack.into()) {
            None => *self,
            Some(rewound) => rewound,
        }
    }
}

impl Rewind for pair::Pair<Duration, u64> {
    fn rewind(&self, slack: Time) -> Self {
        let slack_pair: Self = slack.into();

        let first_rewound = self.first.rewind(Time::Real(slack_pair.first));
        let second_rewound = self.second.rewind(Time::TxId(slack_pair.second));

        Self::new(first_rewound, second_rewound)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{Rewind, Time};
    use Time::{Real, TxId};

    #[test]
    fn test_rewind() {
        assert_eq!((0 as u64).rewind(TxId(1)), 0);
        assert_eq!((10 as u64).rewind(TxId(5)), 5);

        assert_eq!(
            Duration::from_secs(0).rewind(Real(Duration::from_secs(10))),
            Duration::from_secs(0)
        );
        assert_eq!(
            Duration::from_millis(12345).rewind(Real(Duration::from_millis(45))),
            Duration::from_millis(12300)
        );
    }
}
