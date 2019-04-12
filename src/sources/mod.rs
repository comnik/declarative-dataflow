//! Types and operators to work with external data sources.

use std::cell::RefCell;
use std::rc::Weak;
use std::time::Instant;

use timely::dataflow::{Scope, Stream};
use timely::order::TotalOrder;
use timely::progress::Timestamp;

use differential_dataflow::lattice::Lattice;

use crate::server::scheduler::Scheduler;
use crate::AttributeConfig;
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
pub trait Sourceable<S>
where
    S: Scope,
    S::Timestamp: Timestamp + Lattice + TotalOrder,
{
    /// Conjures from thin air (or from wherever the source lives) one
    /// or more timely streams feeding directly into attributes.
    fn source(
        &self,
        scope: &mut S,
        t0: Instant,
        scheduler: Weak<RefCell<Scheduler>>,
    ) -> Vec<(
        Aid,
        AttributeConfig,
        Stream<S, ((Value, Value), S::Timestamp, isize)>,
    )>;
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
impl<S: Scope<Timestamp = std::time::Duration>> Sourceable<S> for Source {
    fn source(
        &self,
        scope: &mut S,
        t0: Instant,
        scheduler: Weak<RefCell<Scheduler>>,
    ) -> Vec<(
        Aid,
        AttributeConfig,
        Stream<S, ((Value, Value), std::time::Duration, isize)>,
    )> {
        match *self {
            Source::TimelyLogging(ref source) => source.source(scope, t0, scheduler),
            Source::DifferentialLogging(ref source) => source.source(scope, t0, scheduler),
            #[cfg(feature = "csv-source")]
            Source::CsvFile(ref source) => source.source(scope, t0, scheduler),
            _ => unimplemented!(),
        }
    }
}

#[cfg(not(feature = "real-time"))]
impl<S: Scope<Timestamp = u64>> Sourceable<S> for Source {
    fn source(
        &self,
        _scope: &mut S,
        _t0: Instant,
        _scheduler: Weak<RefCell<Scheduler>>,
    ) -> Vec<(
        Aid,
        AttributeConfig,
        Stream<S, ((Value, Value), u64, isize)>,
    )> {
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
