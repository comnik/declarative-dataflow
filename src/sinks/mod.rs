//! Types and operators to feed outputs into external systems.

use timely::dataflow::{Scope, Stream};
use timely::order::TotalOrder;
use timely::progress::Timestamp;

use differential_dataflow::lattice::Lattice;

use crate::{Error, ResultDiff};

#[cfg(feature = "csv-source")]
pub mod csv_file;

#[cfg(feature = "csv-source")]
pub use self::csv_file::CsvFile;

/// An external system that wants to receive result diffs.
pub trait Sinkable {
    /// The timestamp type accepted by this system.
    type Timestamp: Timestamp + Lattice + TotalOrder;

    /// Creates a timely operator reading from the source and
    /// producing inputs.
    fn sink<S: Scope<Timestamp = Self::Timestamp>>(
        &self,
        stream: &Stream<S, ResultDiff<S::Timestamp>>,
    ) -> Result<(), Error>;
}

/// Supported external systems.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub enum Sink {
    /// CSV files
    #[cfg(feature = "csv-source")]
    CsvFile(CsvFile),
}

impl Sinkable for Sink {
    type Timestamp = u64;

    fn sink<S: Scope<Timestamp = u64>>(
        &self,
        stream: &Stream<S, ResultDiff<S::Timestamp>>,
    ) -> Result<(), Error> {
        match *self {
            #[cfg(feature = "csv-source")]
            Sink::CsvFile(ref sink) => sink.sink(stream),
        }
    }
}
