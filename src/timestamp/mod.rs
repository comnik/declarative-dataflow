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
pub trait Rewind: std::convert::From<Time> {
    /// Returns a new timestamp corresponding to self rewound by the
    /// specified amount of slack. Calling rewind is always safe, in
    /// that no invalid times will be returned.
    ///
    /// e.g. 0.rewind(10) -> 0
    /// and Duration(0).rewind(Duration(1)) -> Duration(0)
    fn rewind(&self, slack: Self) -> Self;
}

impl Rewind for u64 {
    fn rewind(&self, slack: Self) -> Self {
        match self.checked_sub(slack) {
            None => *self,
            Some(rewound) => rewound,
        }
    }
}

impl Rewind for Duration {
    fn rewind(&self, slack: Self) -> Self {
        match self.checked_sub(slack) {
            None => *self,
            Some(rewound) => rewound,
        }
    }
}

impl Rewind for pair::Pair<Duration, u64> {
    fn rewind(&self, slack: Self) -> Self {
        let first_rewound = self.first.rewind(slack.first);
        let second_rewound = self.second.rewind(slack.second);

        Self::new(first_rewound, second_rewound)
    }
}

/// Extension trait for timestamp types that can be rounded up to
/// interval bounds, thus coarsening the granularity of timestamps and
/// delaying results.
pub trait Coarsen {
    /// Returns a new timestamp delayed up to the next multiple of the
    /// specified window size.
    fn coarsen(&self, window_size: &Self) -> Self;
}

impl Coarsen for u64 {
    fn coarsen(&self, window_size: &Self) -> Self {
        (self / window_size + 1) * window_size
    }
}

impl Coarsen for Duration {
    fn coarsen(&self, window_size: &Self) -> Self {
        let w_secs = window_size.as_secs();
        let w_nanos = window_size.subsec_nanos();

        let secs_coarsened = if w_secs == 0 {
            self.as_secs()
        } else {
            (self.as_secs() / w_secs + 1) * w_secs
        };

        let nanos_coarsened = if w_nanos == 0 {
            0
        } else {
            (self.subsec_nanos() / w_nanos + 1) * w_nanos
        };

        Duration::new(secs_coarsened, nanos_coarsened)
    }
}

impl Coarsen for pair::Pair<Duration, u64> {
    fn coarsen(&self, window_size: &Self) -> Self {
        let first_coarsened = self.first.coarsen(&window_size.first);
        let second_coarsened = self.second.coarsen(&window_size.second);

        Self::new(first_coarsened, second_coarsened)
    }
}

#[cfg(test)]
mod tests {
    use super::{Coarsen, Rewind};
    use std::time::Duration;

    #[test]
    fn test_rewind() {
        assert_eq!((0 as u64).rewind(1), 0);
        assert_eq!((10 as u64).rewind(5), 5);

        assert_eq!(
            Duration::from_secs(0).rewind(Duration::from_secs(10)),
            Duration::from_secs(0)
        );
        assert_eq!(
            Duration::from_millis(12345).rewind(Duration::from_millis(45)),
            Duration::from_millis(12300)
        );
    }

    #[test]
    fn test_coarsen() {
        assert_eq!((0 as u64).coarsen(&10), 10);
        assert_eq!((6 as u64).coarsen(&10), 10);
        assert_eq!((11 as u64).coarsen(&10), 20);

        assert_eq!(
            Duration::from_secs(0).coarsen(&Duration::from_secs(10)),
            Duration::from_secs(10)
        );
        assert_eq!(
            Duration::new(10, 500).coarsen(&Duration::from_secs(10)),
            Duration::from_secs(20),
        );
    }
}
