//! Types and operators to work with external data sources.

use timely::dataflow::{Scope, Stream};
use timely::order::TotalOrder;
use timely::progress::Timestamp;

use differential_dataflow::lattice::Lattice;

use crate::Value;

#[cfg(feature = "csv-source")]
pub mod csv_file;
pub mod json_file;

#[cfg(feature = "csv-source")]
pub use self::csv_file::CsvFile;
pub use self::json_file::JsonFile;

/// An external data source that can provide Datoms.
pub trait Sourceable {
    /// Creates a timely operator reading from the source and
    /// producing inputs.
    fn source<T, S>(
        &self,
        scope: &S,
        names: Vec<String>,
    ) -> Stream<S, (usize, ((Value, Value), T, isize))>
    where
        T: Timestamp + Lattice + TotalOrder + Default,
        S: Scope<Timestamp = T>;
}

/// Supported external data sources.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub enum Source {
    /// CSV files
    #[cfg(feature = "csv-source")]
    CsvFile(CsvFile),
    /// Files containing json objects
    JsonFile(JsonFile),
}

impl Sourceable for Source {
    fn source<T, S>(
        &self,
        scope: &S,
        names: Vec<String>,
    ) -> Stream<S, (usize, ((Value, Value), T, isize))>
    where
        T: Timestamp + Lattice + TotalOrder + Default,
        S: Scope<Timestamp = T>,
    {
        match *self {
            #[cfg(feature = "csv-source")]
            Source::CsvFile(ref source) => source.source(scope, names),
            Source::JsonFile(ref source) => source.source(scope, names),
        }
    }
}
