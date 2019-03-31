//! Types and operators to feed outputs into external systems.

use std::fs::File;
use std::io::{LineWriter, Write};
use std::time::{Duration, Instant};

use timely::dataflow::channels::pact::ParallelizationContract;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::generic::FrontieredInputHandle;
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
    TheVoid(String),
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
            Sink::TheVoid(ref name) => {
                let file = File::create(name).unwrap();
                let mut writer = LineWriter::new(file);

                let mut builder = OperatorBuilder::new(name.to_owned(), stream.scope());
                let mut input = builder.new_input(stream, pact);
                let (_output, sunk) = builder.new_output();

                builder.build(|_capabilities| {
                    let mut t0 = Instant::now();
                    let mut last = Duration::from_millis(0);

                    move |frontiers| {
                        let input_handle = FrontieredInputHandle::new(&mut input, &frontiers[0]);

                        if input_handle.frontier.is_empty() {
                            println!("[{:?}] inputs to void sink ceased", t0.elapsed());
                        } else if !input_handle.frontier.frontier().less_equal(&last) {
                            write!(writer, "{},{:?}\n", t0.elapsed().as_millis(), last).unwrap();

                            last = input_handle.frontier.frontier()[0].clone();
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
