//! Operator and utilities to write output diffs into csv files.

use timely::dataflow::channels::pact::ParallelizationContract;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::generic::FrontieredInputHandle;
use timely::dataflow::{Scope, Stream};

use super::Sinkable;
use crate::{Error, ResultDiff};

/// A local filesystem data sink.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub struct CsvFile {
    /// Path to a file on each workers local filesystem.
    pub path: String,
    /// Does the file include a header?
    pub has_headers: bool,
    /// Column delimiter to use.
    pub delimiter: u8,
    /// Allow flexible length records?
    pub flexible: bool,
}

impl Sinkable<u64> for CsvFile {
    fn sink<S, P>(
        &self,
        stream: &Stream<S, ResultDiff<u64>>,
        pact: P,
    ) -> Result<Stream<S, ResultDiff<u64>>, Error>
    where
        S: Scope<Timestamp = u64>,
        P: ParallelizationContract<S::Timestamp, ResultDiff<u64>>,
    {
        let writer_result = csv::WriterBuilder::new()
            .has_headers(self.has_headers)
            .delimiter(self.delimiter)
            .from_path(&self.path);

        match writer_result {
            Err(error) => Err(Error::fault(format!("Failed to create writer: {}", error))),
            Ok(mut writer) => {
                let mut recvd = Vec::new();
                let mut vector = Vec::new();

                let name = format!("CsvFile({})", &self.path);

                let mut builder = OperatorBuilder::new(name, stream.scope());
                let mut input = builder.new_input(stream, pact);
                let (_output, sunk) = builder.new_output();

                builder.build(|_capabilities| {
                    move |frontiers| {
                        let mut input_handle =
                            FrontieredInputHandle::new(&mut input, &frontiers[0]);

                        input_handle.for_each(|_cap, data| {
                            data.swap(&mut vector);
                            // @TODO what to do with diff here?
                            for (tuple, time, _diff) in vector.drain(..) {
                                recvd.push((time, tuple));
                            }
                        });

                        recvd.sort_by(|x, y| x.0.cmp(&y.0));

                        // determine how many (which) elements to read from `recvd`.
                        let count = recvd
                            .iter()
                            .filter(|&(ref time, _)| !input_handle.frontier().less_equal(time))
                            .count();

                        for (_, tuple) in recvd.drain(..count) {
                            writer.serialize(tuple).expect("failed to write record");
                        }

                        if input_handle.frontier.is_empty() {
                            println!("Inputs to csv sink have ceased.");
                        }
                    }
                });

                Ok(sunk)
            }
        }
    }
}
