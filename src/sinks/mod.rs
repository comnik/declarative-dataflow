//! Types and operators to feed outputs into external systems.

use std::fs::File;
use std::io::{LineWriter, Write};
use std::time::{Duration, Instant};

use timely::dataflow::channels::pact::ParallelizationContract;
use timely::dataflow::operators::generic::Operator;
use timely::dataflow::operators::generic::OutputHandle;
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
    /// Creates a timely operator reading from the source andn
    /// producing inputs.
    fn sink<S, P>(
        &self,
        stream: &Stream<S, ResultDiff<T>>,
        pact: P,
    ) -> Result<Stream<S, ResultDiff<T>>, Error>
    where
        S: Scope<Timestamp = T>,
        P: ParallelizationContract<S::Timestamp, ResultDiff<T>>;
}

/// Supported external systems.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub enum Sink {
    /// /dev/null, used for benchmarking
    TheVoid(Option<String>),
    /// CSV files
    #[cfg(feature = "csv-source")]
    CsvFile(CsvFile),
}

impl Sinkable<u64> for Sink {
    fn sink<S, P>(
        &self,
        stream: &Stream<S, ResultDiff<u64>>,
        pact: P,
    ) -> Result<Stream<S, ResultDiff<u64>>, Error>
    where
        S: Scope<Timestamp = u64>,
        P: ParallelizationContract<S::Timestamp, ResultDiff<u64>>,
    {
        match *self {
            #[cfg(feature = "csv-source")]
            Sink::CsvFile(ref sink) => sink.sink(stream, pact),
            _ => unimplemented!(),
        }
    }
}

impl Sinkable<Duration> for Sink {
    fn sink<S, P>(
        &self,
        stream: &Stream<S, ResultDiff<Duration>>,
        pact: P,
    ) -> Result<Stream<S, ResultDiff<Duration>>, Error>
    where
        S: Scope<Timestamp = Duration>,
        P: ParallelizationContract<S::Timestamp, ResultDiff<Duration>>,
    {
        match *self {
            Sink::TheVoid(ref filename) => {
                let mut writer = match *filename {
                    None => None,
                    Some(ref filename) => {
                        let file = File::create(filename.to_owned()).unwrap();
                        Some(LineWriter::new(file))
                    }
                };

                let mut t0 = Instant::now();
                let mut last = Duration::from_millis(0);
                let mut buffer = Vec::new();

                let sunk = stream.unary_frontier(pact, "TheVoid", move |_cap, _info| {
                    move |input, _output: &mut OutputHandle<_, ResultDiff<Duration>, _>| {
                        let mut received_input = false;
                        input.for_each(|time, data| {
                            data.swap(&mut buffer);
                            received_input = !buffer.is_empty();
                            buffer.clear();
                        });

                        if input.frontier.is_empty() {
                            println!("[{:?}] inputs to void sink ceased", t0.elapsed());

                            if let Some(ref mut writer) = &mut writer {
                                writeln!(writer, "{},{:?}", t0.elapsed().as_millis(), last)
                                    .unwrap();
                            }
                        } else if received_input && !input.frontier.frontier().less_equal(&last) {
                            if let Some(ref mut writer) = &mut writer {
                                writeln!(writer, "{},{:?}", t0.elapsed().as_millis(), last)
                                    .unwrap();
                            }

                            last = input.frontier.frontier()[0].clone();
                            t0 = Instant::now();
                        }
                    }
                });

                Ok(sunk)
            }
            _ => unimplemented!(),
        }
    }
}
