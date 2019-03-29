//! Operator and utilities to source data from plain files containing
//! arbitrary json structures.

use std::cell::RefCell;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::rc::Weak;
use std::time::{Duration, Instant};

use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::{Scope, Stream};

// use sources::json_file::flate2::read::GzDecoder;

use crate::server::scheduler::Scheduler;
use crate::sources::Sourceable;
use crate::{Aid, Eid, Value};
use Value::{Bool, Number};

/// A local filesystem data source containing JSON objects.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub struct JsonFile {
    /// Path to a file on each workers local filesystem.
    pub path: String,
    /// Attributes to ingest.
    pub attributes: Vec<Aid>,
}

impl Sourceable<Duration> for JsonFile {
    fn source<S: Scope<Timestamp = Duration>>(
        &self,
        scope: &mut S,
        t0: Instant,
        _scheduler: Weak<RefCell<Scheduler>>,
    ) -> Vec<(Aid, Stream<S, ((Value, Value), Duration, isize)>)> {
        let filename = self.path.clone();

        // The following is mostly the innards of
        // `generic::source`. We use a builder directly, because we
        // need multiple outputs (one for each attribute the user has
        // epxressed interest in).
        let mut demux = OperatorBuilder::new(format!("JsonFile({})", filename), scope.clone());
        let operator_info = demux.operator_info();
        demux.set_notify(false);

        let mut wrappers = Vec::with_capacity(self.attributes.len());
        let mut streams = Vec::with_capacity(self.attributes.len());

        for _ in self.attributes.iter() {
            let (wrapper, stream) = demux.new_output();
            wrappers.push(wrapper);
            streams.push(stream);
        }

        let scope_handle = scope.clone();
        let attributes = self.attributes.clone();

        demux.build(move |mut capabilities| {

            let scope = scope_handle;
            let activator = scope.activator_for(&operator_info.address[..]);

            let worker_index = scope.index();
            let num_workers = scope.peers();

            let path = Path::new(&filename);
            let file = File::open(&path).unwrap();
            // let reader = BufReader::new(GzDecoder::new(file));
            let reader = BufReader::new(file);
            let mut iterator = reader.lines().peekable();

            let mut num_objects_read = 0;
            let mut object_index = 0;

            move |_frontiers| {
                let mut handles = Vec::with_capacity(attributes.len());
                for wrapper in wrappers.iter_mut() {
                    handles.push(wrapper.activate());
                }

                if iterator.peek().is_some() {
                    let mut sessions = Vec::with_capacity(attributes.len());
                    for (idx, handle) in handles.iter_mut().enumerate() {
                        sessions.push(handle.session(capabilities.get(idx).unwrap()));
                    }

                    let time = Instant::now().duration_since(t0);

                    for readline in iterator.by_ref().take(256 - 1) {
                        let line = readline.expect("read error");

                        if (object_index % num_workers == worker_index) && !line.is_empty() {
                            // @TODO parse only the names we are interested in
                            // @TODO run with Value = serde_json::Value

                            let obj: serde_json::Value = serde_json::from_str(&line).unwrap();
                            let obj_map = obj.as_object().unwrap();

                            // In the common case we assume that all objects share
                            // roughly the same number of attributes, a (potentially small)
                            // subset of which is actually requested downstream.
                            //
                            // otherwise:
                            // for (k, v) in obj.as_object().unwrap() {

                            for (idx, aid) in attributes.iter().enumerate() {
                                match obj_map.get(aid) {
                                    None => {}
                                    Some(json_value) => {
                                        let v = match *json_value {
                                            serde_json::Value::String(ref s) => Value::String(s.to_string()),
                                            serde_json::Value::Number(ref num) => {
                                                match num.as_i64() {
                                                    None => panic!("only i64 supported at the moment"),
                                                    Some(num) => Number(num),
                                                }
                                            },
                                            serde_json::Value::Bool(ref b) => Bool(*b),
                                            _ => panic!("only strings, booleans, and i64 types supported at the moment"),
                                        };

                                        let tuple = (Value::Eid(object_index as Eid), v);

                                        sessions.get_mut(idx)
                                            .unwrap()
                                            .give((tuple, time, 1));
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
                    capabilities.drain(..);
                }
            }
        });

        streams
    }
}
