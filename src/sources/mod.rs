//! Types and operators to work with external data sources.

extern crate differential_dataflow;
extern crate timely;

use timely::dataflow::{Scope, Stream};

use crate::Value;

pub mod csv_file;
pub use self::csv_file::CsvFile;
pub mod json_file;
pub use self::json_file::JsonFile;

/// An external data source that can provide Datoms.
pub trait Sourceable {
    /// Creates a timely operator reading from the source and
    /// producing inputs.
    fn source<G: Scope<Timestamp = u64>>(
        &self,
        scope: &G,
        names: Vec<String>,
    ) -> Stream<G, (usize, ((Value, Value), u64, isize))>;
}

/// Supported external data sources.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub enum Source {
    /// CSV files
    CsvFile(CsvFile),
    /// Files containing json objects
    JsonFile(JsonFile),
}

impl Sourceable for Source {
    fn source<G: Scope<Timestamp = u64>>(
        &self,
        scope: &G,
        names: Vec<String>,
    ) -> Stream<G, (usize, ((Value, Value), u64, isize))> {
        match self {
            &Source::CsvFile(ref source) => source.source(scope, names),
            &Source::JsonFile(ref source) => source.source(scope, names),
        }
    }
}
