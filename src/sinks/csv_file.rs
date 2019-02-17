//! Operator and utilities to write output diffs into csv files.

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Operator;
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

impl Sinkable for CsvFile {
    type Timestamp = u64;

    fn sink<S: Scope<Timestamp = Self::Timestamp>>(
        &self,
        stream: &Stream<S, ResultDiff<S::Timestamp>>,
    ) -> Result<(), Error> {
        let writer_result = csv::WriterBuilder::new()
            .has_headers(self.has_headers)
            .delimiter(self.delimiter)
            .from_path(&self.path);

        match writer_result {
            Err(error) => Err(Error {
                category: "df.error.category/fault",
                message: format!("Failed to create writer: {}", error),
            }),
            Ok(mut writer) => {
                let mut recvd = Vec::new();
                let mut vector = Vec::new();

                stream.sink(
                    Pipeline,
                    &format!("CsvFile({})", &self.path),
                    move |input| {
                        input.for_each(|_cap, data| {
                            data.swap(&mut vector);
                            for (tuple, time, diff) in vector.drain(..) {
                                recvd.push((time, tuple));
                            }
                        });

                        recvd.sort_by(|x, y| x.0.cmp(&y.0));

                        // determine how many (which) elements to read from `recvd`.
                        let count = recvd
                            .iter()
                            .filter(|&(ref time, _)| !input.frontier().less_equal(time))
                            .count();

                        for (_, tuple) in recvd.drain(..count) {
                            writer.serialize(tuple).expect("failed to write record");
                        }
                    },
                );

                Ok(())
            }
        }
    }
}
