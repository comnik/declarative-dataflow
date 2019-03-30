//! Types and operators to work with external data sources.

use std::cell::RefCell;
use std::rc::Weak;
use std::time::Instant;

use timely::dataflow::{Scope, Stream};
use timely::order::TotalOrder;
use timely::progress::Timestamp;

use differential_dataflow::lattice::Lattice;

use crate::server::scheduler::Scheduler;
use crate::{Aid, Value};

#[cfg(feature = "csv-source")]
pub mod csv_file;
pub mod differential_logging;
// pub mod json_file;
pub mod timely_logging;

#[cfg(feature = "csv-source")]
pub use self::csv_file::CsvFile;
// pub use self::json_file::JsonFile;

/// An external data source that can provide Datoms.
pub trait Sourceable<T>
where
    T: Timestamp + Lattice + TotalOrder,
{
    /// Conjures from thin air (or from wherever the source lives) one
    /// or more timely streams feeding directly into attributes.
    fn source<S: Scope<Timestamp = T>>(
        &self,
        scope: &mut S,
        t0: Instant,
        scheduler: Weak<RefCell<Scheduler>>,
    ) -> Vec<(Aid, Stream<S, ((Value, Value), T, isize)>)>;
}

/// Supported external data sources.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub enum Source {
    /// Timely logging streams
    TimelyLogging(timely_logging::TimelyLogging),
    /// Differential logging streams
    DifferentialLogging(differential_logging::DifferentialLogging),
    /// CSV files
    #[cfg(feature = "csv-source")]
    CsvFile(CsvFile),
    // /// Files containing json objects
    // JsonFile(JsonFile),
}

#[cfg(feature = "real-time")]
impl Sourceable<std::time::Duration> for Source {
    fn source<S: Scope<Timestamp = std::time::Duration>>(
        &self,
        scope: &mut S,
        t0: Instant,
        scheduler: Weak<RefCell<Scheduler>>,
    ) -> Vec<(Aid, Stream<S, ((Value, Value), std::time::Duration, isize)>)> {
        match *self {
            Source::TimelyLogging(ref source) => source.source(scope, t0, scheduler),
            Source::DifferentialLogging(ref source) => source.source(scope, t0, scheduler),
            #[cfg(feature = "csv-source")]
            Source::CsvFile(ref source) => source.source(scope, t0, scheduler),
            _ => unimplemented!(),
        }
    }
}

impl Sourceable<u64> for Source {
    fn source<S: Scope<Timestamp = u64>>(
        &self,
        _scope: &mut S,
        _t0: Instant,
        _scheduler: Weak<RefCell<Scheduler>>,
    ) -> Vec<(Aid, Stream<S, ((Value, Value), u64, isize)>)> {
        match *self {
            // Source::TimelyLogging(ref source) => source.source(scope, t0),
            // Source::DifferentialLogging(ref source) => source.source(scope, t0),
            // #[cfg(feature = "csv-source")]
            // Source::CsvFile(ref source) => source.source(scope, t0),
            // Source::JsonFile(ref source) => source.source(scope, t0),
            _ => unimplemented!(),
        }
    }
}
