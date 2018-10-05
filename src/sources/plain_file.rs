//! Operator and utilities to source data from plain files.

extern crate timely;

use std::fs::{File};
use std::io::{BufRead, BufReader};
use std::path::{Path};

use timely::dataflow::{Scope, Stream};
use timely::dataflow::operators::generic;
use timely::progress::timestamp::{RootTimestamp};
use timely::progress::nested::product::{Product};

use {Value};

use sources::{Sourceable};

/// A local filesystem data source.
#[derive(Deserialize, Clone, Debug)]
pub struct PlainFile {
    /// Path to a file on each workers local filesystem.
    path: String,
}

impl Sourceable for PlainFile {

    fn source<G: Scope>(&self, scope: &G) -> Stream<G, (Vec<Value>, Product<RootTimestamp, usize>, isize)> {

        let filename = self.path.clone();
        
        generic::operator::source(
            scope,
            &format!("File({})", filename),
            |capability| {
            
                let mut cap = Some(capability);

                let worker_index = scope.index();
                let num_workers = scope.peers();

                move |output| {
                    if let Some(cap) = cap.as_mut() {
                        
                        let path = Path::new(&filename);
                        let file = File::open(&path).unwrap();
                        let reader = BufReader::new(file);

                        let mut num_datums_read = 0;
                        let mut datum_index = 0;

                        for readline in reader.lines() {

                            let line = readline.ok().expect("read error");
                            
                            if (datum_index % num_workers == worker_index) && line.len() > 0 {

                                let mut elts = line[..].split_whitespace();
                                let e: i64 = elts.next().unwrap().parse().ok().expect("malformed key");
                                let v: i64 = elts.next().unwrap().parse().ok().expect("malformed value");

                                output.session(&cap).give((vec![Value::Number(e), Value::Number(v)], RootTimestamp::new(0), 1));
                                num_datums_read += 1;
                            }
                            
                            datum_index += 1;
                        }

                        println!("[WORKER {}] read {} out of {} datums", worker_index, num_datums_read, datum_index);
                    }

                    cap = None;
                }
            }
        )
    }
}
