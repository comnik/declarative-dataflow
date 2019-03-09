//! Types and operators to work with external data sources.

use std::time::Duration;

use timely::dataflow::{Scope, Stream};
use timely::order::TotalOrder;
use timely::progress::Timestamp;

use differential_dataflow::lattice::Lattice;

use crate::Value;

#[cfg(feature = "csv-source")]
pub mod csv_file;
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
    /// Creates a timely operator reading from the source and
    /// producing inputs.
    fn source<S: Scope<Timestamp = T>>(
        &self,
        scope: &mut S,
        names: Vec<String>,
    ) -> Stream<S, (usize, ((Value, Value), T, isize))>;
}

/// Supported external data sources.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub enum Source {
    /// Timely logging streams
    TimelyLogging(timely_logging::TimelyLogging),
    /// CSV files
    #[cfg(feature = "csv-source")]
    CsvFile(CsvFile),
    // /// Files containing json objects
    // JsonFile(JsonFile),
}

impl Sourceable<Duration> for Source {
    fn source<S: Scope<Timestamp = Duration>>(
        &self,
        scope: &mut S,
        names: Vec<String>,
    ) -> Stream<S, (usize, ((Value, Value), Duration, isize))> {
        match *self {
            Source::TimelyLogging(ref source) => source.source(scope, names),
            _ => unimplemented!(),
        }
    }
}

impl Sourceable<u64> for Source {
    fn source<S: Scope<Timestamp = u64>>(
        &self,
        scope: &mut S,
        names: Vec<String>,
    ) -> Stream<S, (usize, ((Value, Value), u64, isize))> {
        match *self {
            #[cfg(feature = "csv-source")]
            Source::CsvFile(ref source) => source.source(scope, names),
            // Source::JsonFile(ref source) => source.source(scope, names),
            _ => unimplemented!(),
        }
    }
}
