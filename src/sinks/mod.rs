//! Types and operators to feed outputs into external systems.

use std::fs::File;
use std::io::{LineWriter, Write};
use std::time::Instant;

use timely::dataflow::channels::pact::ParallelizationContract;
use timely::dataflow::operators::generic::{Operator, OutputHandle};
use timely::dataflow::operators::probe::Probe;
use timely::dataflow::{ProbeHandle, Scope, Stream};
use timely::progress::Timestamp;

use differential_dataflow::lattice::Lattice;

use crate::{Error, Output, ResultDiff, Time};

// #[cfg(feature = "csv-source")]
// pub mod csv_file;
// #[cfg(feature = "csv-source")]
// pub use self::csv_file::CsvFile;

#[cfg(feature = "serde_json")]
pub mod assoc_in;
#[cfg(feature = "serde_json")]
pub use self::assoc_in::AssocIn;

/// A struct encapsulating any state required to create sinks.
pub struct SinkingContext {
    /// The name of the dataflow feeding this sink.
    pub name: String,
    /// Granularity at which to send results. None indicates no delay.
    pub granularity: Option<Time>,
}

/// An external system that wants to receive result diffs.
pub trait Sinkable<T>
where
    T: Timestamp + Lattice,
{
    /// Creates a timely operator feeding dataflow outputs to a
    /// specialized data sink.
    fn sink<S, P>(
        &self,
        stream: &Stream<S, ResultDiff<T>>,
        pact: P,
        probe: &mut ProbeHandle<T>,
        context: SinkingContext,
    ) -> Result<Option<Stream<S, Output<S::Timestamp>>>, Error>
    where
        S: Scope<Timestamp = T>,
        P: ParallelizationContract<S::Timestamp, ResultDiff<T>>;
}

/// Supported external systems.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub enum Sink {
    /// /dev/null, used for benchmarking
    TheVoid(Option<String>),
    // /// CSV files
    // #[cfg(feature = "csv-source")]
    // CsvFile(CsvFile),
    /// Nested Hash-Maps
    #[cfg(feature = "serde_json")]
    AssocIn(AssocIn),
}

impl<T: Timestamp + Lattice + Default> Sinkable<T> for Sink {
    fn sink<S, P>(
        &self,
        stream: &Stream<S, ResultDiff<T>>,
        pact: P,
        probe: &mut ProbeHandle<T>,
        context: SinkingContext,
    ) -> Result<Option<Stream<S, Output<S::Timestamp>>>, Error>
    where
        S: Scope<Timestamp = T>,
        P: ParallelizationContract<S::Timestamp, ResultDiff<T>>,
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
                let mut last: T = Default::default();
                let mut buffer = Vec::new();

                stream
                    .unary_frontier(pact, "TheVoid", move |_cap, _info| {
                        move |input, _output: &mut OutputHandle<_, ResultDiff<T>, _>| {
                            let mut received_input = false;
                            input.for_each(|_time, data| {
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
                            } else if received_input && !input.frontier.frontier().less_equal(&last)
                            {
                                if let Some(ref mut writer) = &mut writer {
                                    writeln!(writer, "{},{:?}", t0.elapsed().as_millis(), last)
                                        .unwrap();
                                }

                                last = input.frontier.frontier()[0].clone();
                                t0 = Instant::now();
                            }
                        }
                    })
                    .probe_with(probe);

                Ok(None)
            }
            #[cfg(feature = "serde_json")]
            Sink::AssocIn(ref sink) => sink.sink(stream, pact, probe, context),
            _ => unimplemented!(),
        }
    }
}
