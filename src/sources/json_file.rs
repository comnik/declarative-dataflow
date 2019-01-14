//! Operator and utilities to source data from plain files containing
//! arbitrary json structures.

extern crate serde_json;
extern crate timely;

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use timely::dataflow::operators::generic;
use timely::dataflow::{Scope, Stream};

// use sources::json_file::flate2::read::GzDecoder;

use {Value, Eid};

use sources::Sourceable;

/// A local filesystem data source containing JSON objects.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct JsonFile {
    /// Path to a file on each workers local filesystem.
    pub path: String,
}

impl Sourceable for JsonFile {
    fn source<G: Scope>(&self, scope: &G, names: Vec<String>) -> Stream<G, ((usize, Vec<Value>), u64, isize)> {
        let filename = self.path.clone();

        generic::operator::source(scope, &format!("File({})", filename), move |capability, info| {

            let activator = scope.activator_for(&info.address[..]);
            
            let mut cap = Some(capability);

            let worker_index = scope.index();
            let num_workers = scope.peers();

            let path = Path::new(&filename);
            let file = File::open(&path).unwrap();
            // let reader = BufReader::new(GzDecoder::new(file));
            let reader = BufReader::new(file);
            let mut iterator = reader.lines().peekable();

            let mut num_objects_read = 0;
            let mut object_index = 0;

            move |output| {
                if iterator.peek().is_some() {
                    
                    let mut session = output.session(cap.as_ref().unwrap());

                    for readline in iterator.by_ref().take(256 - 1) {
                        
                        let line = readline.ok().expect("read error");

                        if (object_index % num_workers == worker_index) && line.len() > 0 {

                            // @TODO parse only the names we are interested in
                            // @TODO run with Value = serde_json::Value
                            
                            let mut obj: serde_json::Value = serde_json::from_str(&line).unwrap();
                            let obj_map = obj.as_object().unwrap();

                            // In the common case we assume that all objects share
                            // roughly the same number of attributes, a (potentially small)
                            // subset of which is actually requested downstream.
                            //
                            // otherwise:
                            // for (k, v) in obj.as_object().unwrap() {

                            for (name_idx, k) in names.iter().enumerate() {
                                match obj_map.get(k) {
                                    None => {},
                                    Some(json_value) => {
                                        let v = match *json_value {
                                            serde_json::Value::String(ref s) => Value::String(s.to_string()),
                                            serde_json::Value::Number(ref num) => {
                                                match num.as_i64() {
                                                    None => panic!("only i64 supported at the moment"),
                                                    Some(num) => Value::Number(num),
                                                }
                                            },
                                            serde_json::Value::Bool(ref b) => Value::Bool(*b),
                                            _ => panic!("only strings, booleans, and i64 types supported at the moment"),
                                        };

                                        session.give(((name_idx, vec![Value::Eid(object_index as Eid), v]), 0, 1));
                                    }
                                }
                            }
                            
                            num_objects_read += 1;
                        }

                        object_index += 1;
                    }

                    // println!("[WORKER {}] read {} out of {} objects", worker_index, num_objects_read, object_index);

                    activator.activate();
                    
                } else {
                    cap = None;
                }
            }
        })
    }
}
