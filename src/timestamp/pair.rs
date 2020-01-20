//! Code by Frank McSherry, customized here. Original source:
//! github.com/TimelyDataflow/differential-dataflow/blob/master/experiments/src/bin/multitemporal.rs
//!
//! This module contains a definition of a new timestamp time, a
//! "pair" or product.
//!
//! This is a minimal self-contained implementation, in that it
//! doesn't borrow anything from the rest of the library other than
//! the traits it needs to implement. With this type and its
//! implementations, you can use it as a timestamp type.

/// A pair of timestamps, partially ordered by the product order.
#[derive(Hash, Default, Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct Pair<S, T> {
    /// First timestamp coordinate, e.g. system time.
    pub first: S,
    /// Second timestamp coordinate, e.g. event time.
    pub second: T,
}

impl<S, T> Pair<S, T> {
    /// Create a new pair.
    pub fn new(first: S, second: T) -> Self {
        Pair { first, second }
    }
}

// Implement timely dataflow's `PartialOrder` trait.
use timely::order::PartialOrder;
impl<S: PartialOrder, T: PartialOrder> PartialOrder for Pair<S, T> {
    fn less_equal(&self, other: &Self) -> bool {
        self.first.less_equal(&other.first) && self.second.less_equal(&other.second)
    }
}

use timely::progress::timestamp::Refines;
impl<S: Timestamp, T: Timestamp> Refines<()> for Pair<S, T> {
    fn to_inner(_outer: ()) -> Self {
        Self::minimum()
    }
    fn to_outer(self) {}
    fn summarize(_summary: <Self>::Summary) {}
}

// Implement timely dataflow's `PathSummary` trait.
// This is preparation for the `Timestamp` implementation below.
use timely::progress::PathSummary;

impl<S: Timestamp, T: Timestamp> PathSummary<Pair<S, T>> for () {
    fn results_in(&self, timestamp: &Pair<S, T>) -> Option<Pair<S, T>> {
        Some(timestamp.clone())
    }
    fn followed_by(&self, other: &Self) -> Option<Self> {
        Some(other.clone())
    }
}

// Implement timely dataflow's `Timestamp` trait.
use timely::progress::Timestamp;
impl<S: Timestamp, T: Timestamp> Timestamp for Pair<S, T> {
    type Summary = ();
    fn minimum() -> Self {
        Pair {
            first: S::minimum(),
            second: T::minimum(),
        }
    }
}

// Implement differential dataflow's `Lattice` trait.
// This extends the `PartialOrder` implementation with additional structure.
use differential_dataflow::lattice::Lattice;
impl<S: Lattice, T: Lattice> Lattice for Pair<S, T> {
    fn join(&self, other: &Self) -> Self {
        Pair {
            first: self.first.join(&other.first),
            second: self.second.join(&other.second),
        }
    }
    fn meet(&self, other: &Self) -> Self {
        Pair {
            first: self.first.meet(&other.first),
            second: self.second.meet(&other.second),
        }
    }
}

use std::fmt::{Debug, Error, Formatter};

/// Debug implementation to avoid seeing fully qualified path names.
impl<TOuter: Debug, TInner: Debug> Debug for Pair<TOuter, TInner> {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        f.write_str(&format!("({:?}, {:?})", self.first, self.second))
    }
}

impl<TOuter, TInner> std::ops::Sub for Pair<TOuter, TInner>
where
    TOuter: std::ops::Sub<Output = TOuter>,
    TInner: std::ops::Sub<Output = TInner>,
{
    type Output = Self;

    fn sub(self, other: Self) -> Self {
        Pair {
            first: self.first.sub(other.first),
            second: self.second.sub(other.second),
        }
    }
}
