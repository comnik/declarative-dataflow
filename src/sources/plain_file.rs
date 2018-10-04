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

                move |output| {
                    if let Some(cap) = cap.as_mut() {
                        
                        let path = Path::new(&filename);
                        let file = File::open(&path).unwrap();
                        let reader = BufReader::new(file);

                        // let mut datum_idx = 0;

                        for readline in reader.lines() {

                            let line = readline.ok().expect("read error");
                            let mut elts = line[..].split_whitespace();
                            let e: i64 = elts.next().unwrap().parse().ok().expect("malformed key");
                            let v: i64 = elts.next().unwrap().parse().ok().expect("malformed value");

                            // if (datum_idx % num_workers == worker_index) && line.len() > 0 {
                            output.session(&cap).give((vec![Value::Number(e), Value::Number(v)], RootTimestamp::new(0), 1));
                            // }
                            
                            // datum_idx += 1;
                        }
                    }

                    cap = None;
                }
            }
        )
    }
}
