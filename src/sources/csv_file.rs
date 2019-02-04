//! Operator and utilities to source data from csv files.

extern crate timely;

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use timely::dataflow::operators::generic;
use timely::dataflow::{Scope, Stream};

use crate::sources::Sourceable;
use crate::{Eid, Value};

/// A local filesystem data source.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub struct CsvFile {
    /// Path to a file on each workers local filesystem.
    pub path: String,
    /// Separator to use.
    pub separator: char,
    /// Specifies the column offsets and their value types, that
    /// should be introduced.
    pub schema: Vec<(usize, Value)>,
}

impl Sourceable for CsvFile {
    fn source<G: Scope<Timestamp = u64>>(
        &self,
        scope: &G,
        _names: Vec<String>,
    ) -> Stream<G, (usize, ((Value, Value), u64, isize))> {
        let filename = self.path.clone();

        generic::operator::source(scope, &format!("File({})", filename), |capability, info| {
            let activator = scope.activator_for(&info.address[..]);

            let mut cap = Some(capability);

            let worker_index = scope.index();
            let num_workers = scope.peers();

            let path = Path::new(&filename);
            let file = File::open(&path).unwrap();
            let reader = BufReader::new(file);
            let mut iterator = reader.lines().peekable();

            let mut num_datums_read = 0;
            let mut datum_index = 0;

            // @TODO this is annoying
            let separator = self.separator;
            let schema = self.schema.clone();

            move |output| {
                if iterator.peek().is_some() {
                    let mut session = output.session(cap.as_ref().unwrap());

                    for readline in iterator.by_ref().take(255) {
                        let line: String = readline.expect("read error");

                        if (datum_index % num_workers == worker_index) && !line.is_empty() {
                            let columns: Vec<&str> = line.split(separator).collect();

                            let eid = Value::Eid(
                                columns[0]
                                    .trim()
                                    .trim_matches('"')
                                    .parse::<Eid>()
                                    .expect("not a eid"),
                            );

                            for (name_idx, (offset, type_hint)) in schema.iter().enumerate() {
                                let v = match type_hint {
                                    Value::String(_) => Value::String(
                                        columns[*offset].trim().trim_matches('"').to_string(),
                                    ),
                                    Value::Number(_) => Value::Number(
                                        columns[*offset]
                                            .trim()
                                            .trim_matches('"')
                                            .parse::<i64>()
                                            .expect("not a number"),
                                    ),
                                    Value::Eid(_) => Value::Eid(
                                        columns[*offset]
                                            .trim()
                                            .trim_matches('"')
                                            .parse::<Eid>()
                                            .expect("not a eid"),
                                    ),
                                    _ => panic!(
                                        "Only String, Number, and Eid are supported at the moment."
                                    ),
                                };

                                session.give((name_idx, ((eid.clone(), v), 0, 1)));
                            }

                            num_datums_read += 1;
                        }

                        datum_index += 1;
                    }

                    activator.activate();
                } else {
                    println!(
                        "[WORKER {}] read {} out of {} datums",
                        worker_index, num_datums_read, datum_index
                    );
                    cap = None;
                }
            }
        })
    }
}
