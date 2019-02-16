//! Operator and utilities to source data from csv files.

use timely::dataflow::operators::generic;
use timely::dataflow::{Scope, Stream};

use chrono::DateTime;

use crate::sources::Sourceable;
use crate::{Eid, Value};

/// A local filesystem data source.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub struct CsvFile {
    /// Path to a file on each workers local filesystem.
    pub path: String,
    /// Does the file include a header?
    pub has_headers: bool,
    /// Column delimiter to use.
    pub delimiter: u8,
    /// Comment symbol to use.
    pub comment: Option<u8>,
    /// Allow flexible length records?
    pub flexible: bool,
    /// Special column offset for the entity id.
    pub eid_offset: usize,
    /// Special column offset for the timestamp.
    pub timestamp_offset: Option<usize>,
    /// Specifies the column offsets and their value types, that
    /// should be introduced.
    pub schema: Vec<(usize, Value)>,
}

impl Sourceable for CsvFile {
    type Timestamp = u64;

    fn source<S: Scope<Timestamp = Self::Timestamp>>(
        &self,
        scope: &S,
        names: Vec<String>,
    ) -> Stream<S, (usize, ((Value, Value), Self::Timestamp, isize))> {
        let filename = self.path.clone();

        generic::operator::source(
            scope,
            &format!("CsvFile({})", filename),
            |capability, info| {
                let activator = scope.activator_for(&info.address[..]);

                let mut cap = Some(capability);

                let worker_index = scope.index();
                let num_workers = scope.peers();

                let reader = csv::ReaderBuilder::new()
                    .has_headers(self.has_headers)
                    .delimiter(self.delimiter)
                    .comment(self.comment)
                    .from_path(&filename)
                    .expect("failed to create reader");

                let mut iterator = reader.into_records();

                let mut num_datums_read = 0;
                let mut datum_index = 0;

                let schema = self.schema.clone();
                let eid_offset = self.eid_offset;
                let timestamp_offset = self.timestamp_offset;

                move |output| {
                    if iterator.reader().is_done() {
                        info!(
                            "[WORKER {}] read {} out of {} datums",
                            worker_index, num_datums_read, datum_index
                        );
                        cap = None;
                    } else {
                        // let mut fuel = 256;
                        let mut session = output.session(cap.as_ref().unwrap());

                        while let Some(result) = iterator.next() {
                            let record = result.expect("read error");

                            if datum_index % num_workers == worker_index {
                                let eid = Value::Eid(
                                    record[eid_offset].parse::<Eid>().expect("not a eid"),
                                );
                                let time: u64 = match timestamp_offset {
                                    None => Default::default(),
                                    Some(timestamp_offset) => {
                                        let epoch =
                                            DateTime::parse_from_rfc3339(&record[timestamp_offset])
                                                .expect("not a valid rfc3339 datetime")
                                                .timestamp();

                                        if epoch >= 0 {
                                            epoch as u64
                                        } else {
                                            panic!("invalid epoch");
                                        }
                                    }
                                };

                                for (name_idx, (offset, type_hint)) in schema.iter().enumerate() {
                                    let v = match type_hint {
                                        Value::String(_) => {
                                            Value::String(record[*offset].to_string())
                                        }
                                        Value::Number(_) => Value::Number(
                                            record[*offset].parse::<i64>().expect("not a number"),
                                        ),
                                        Value::Eid(_) => Value::Eid(
                                            record[*offset].parse::<Eid>().expect("not a eid"),
                                        ),
                                        _ => panic!(
                                        "Only String, Number, and Eid are supported at the moment."
                                    ),
                                    };

                                    session.give((name_idx, ((eid.clone(), v), time, 1)));
                                }

                                num_datums_read += 1;
                            }

                            datum_index += 1;

                            // fuel -= 1;
                            // if fuel <= 0 {
                            //     break;
                            // }
                        }

                        if iterator.reader().is_done() {
                            info!(
                                "[WORKER {}] read {} out of {} datums",
                                worker_index, num_datums_read, datum_index
                            );
                            cap = None;
                        } else {
                            // cap.downgrade(..);
                            activator.activate();
                        }
                    }
                }
            },
        )
    }
}
