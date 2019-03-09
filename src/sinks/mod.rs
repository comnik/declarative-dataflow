//! Types and operators to feed outputs into external systems.

use std::time::Duration;

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Operator;
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
pub trait Sinkable<T>
where
    T: Timestamp + Lattice + TotalOrder,
{
    /// Creates a timely operator reading from the source and
    /// producing inputs.
    fn sink<S: Scope<Timestamp = T>>(&self, stream: &Stream<S, ResultDiff<T>>)
        -> Result<(), Error>;
}

/// Supported external systems.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub enum Sink {
    /// /dev/null, used for benchmarking
    TheVoid,
    /// CSV files
    #[cfg(feature = "csv-source")]
    CsvFile(CsvFile),
}

impl Sinkable<u64> for Sink {
    fn sink<S: Scope<Timestamp = u64>>(
        &self,
        stream: &Stream<S, ResultDiff<u64>>,
    ) -> Result<(), Error> {
        match *self {
            Sink::TheVoid => {
                stream.sink(Pipeline, "TheVoid", move |input| {
                    if input.frontier.is_empty() {
                        println!("Inputs to void sink have ceased.");
                    }
                });

                Ok(())
            }
            #[cfg(feature = "csv-source")]
            Sink::CsvFile(ref sink) => sink.sink(stream),
        }
    }
}

impl Sinkable<Duration> for Sink {
    fn sink<S: Scope<Timestamp = Duration>>(
        &self,
        stream: &Stream<S, ResultDiff<Duration>>,
    ) -> Result<(), Error> {
        match *self {
            Sink::TheVoid => {
                stream.sink(Pipeline, "TheVoid", move |input| {
                    if input.frontier.is_empty() {
                        println!("Inputs to void sink have ceased.");
                    }
                });

                Ok(())
            }
            _ => unimplemented!(),
        }
    }
}
