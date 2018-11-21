//! Operator and utilities to source data from csv files.

extern crate timely;

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use timely::dataflow::operators::generic;
use timely::dataflow::{Scope, Stream};

use {Value, Entity};

use sources::Sourceable;

/// A local filesystem data source.
#[derive(Deserialize, Clone, Debug)]
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
    fn source<G: Scope<Timestamp = u64>>(&self, scope: &G, _names: Vec<String>) -> Stream<G, ((usize, Vec<Value>), u64, isize)> {
        let filename = self.path.clone();

        generic::operator::source(scope, &format!("File({})", filename), |capability| {

            let mut capability = Some(capability);

            let worker_index = scope.index();
            let num_workers = scope.peers();

            let path = Path::new(&filename);
            let file = File::open(&path).unwrap();
            let reader = BufReader::new(file);
            let mut iterator = reader.lines().peekable();

            let mut num_datums_read = 0;
            let mut datum_index = 0;

            // @TODO this is annoying
            let separator = self.separator.clone();
            let schema = self.schema.clone();
            
            move |output| {
                if iterator.peek().is_some() {

                    let mut session = output.session(capability.as_ref().unwrap());
                    
                    for readline in iterator.by_ref().take(255) {
                        
                        let line: String = readline.ok().expect("read error");

                        if (datum_index % num_workers == worker_index) && line.len() > 0 {

                            let columns: Vec<&str> = line.split(separator).collect();

                            for (name_idx, (offset, type_hint)) in schema.iter().enumerate() {
                                let eid = Value::Eid(datum_index as Entity);
                                let v = match type_hint {
                                    Value::String(_) => Value::String(columns[*offset].trim().trim_matches('"').to_string()),
                                    Value::Number(_) => Value::Number(columns[*offset].trim().trim_matches('"').parse::<i64>().expect("not a number")),
                                    _ => panic!("Only String and Number are supported at the moment."),
                                };

                                session.give(((name_idx, vec![eid, v]), 0, 1));
                            }
                            
                            num_datums_read += 1;
                        }

                        datum_index += 1;
                    }
                } else {
                    println!("[WORKER {}] read {} out of {} datums", worker_index, num_datums_read, datum_index);
                    capability = None;
                }
            }
        })
    }
}
