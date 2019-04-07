//! Operator and utilities to source data from csv files.

use std::cell::RefCell;
use std::rc::Rc;
use std::rc::Weak;
use std::time::{Duration, Instant};

use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::{Scope, Stream};

use crate::server::scheduler::Scheduler;
use crate::sources::Sourceable;
use crate::{Aid, Eid, Value};
use chrono::NaiveDateTime;

/// A local filesystem data source.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub struct CsvFile {
    /// Path to a file on each workers local filesystem.
    pub path: String,
    /// Does the file include a header?
    pub has_headers: bool,
    /// Column delimiter to use.
    pub delimiter: u8,
    /// Comment variable to use.
    pub comment: Option<u8>,
    /// Allow flexible length records?
    pub flexible: bool,
    /// Special column offset for the entity id.
    pub eid_offset: usize,
    /// Special column offset for the timestamp.
    pub timestamp_offset: Option<usize>,
    /// Specifies the column offsets and their value types, that
    /// should be introduced.
    pub schema: Vec<(Aid, (usize, Value))>,
}

impl Sourceable<Duration> for CsvFile {
    fn source<S: Scope<Timestamp = Duration>>(
        &self,
        scope: &mut S,
        t0: Instant,
        scheduler: Weak<RefCell<Scheduler>>,
    ) -> Vec<(Aid, Stream<S, ((Value, Value), Duration, isize)>)> {
        // let filename = self.path.clone();
        let directory = self.path.clone();

        // The following is mostly the innards of
        // `generic::source`. We use a builder directly, because we
        // need multiple outputs (one for each attribute the user has
        // epxressed interest in).
        let mut demux = OperatorBuilder::new(format!("CsvFile({})", directory), scope.clone());
        let operator_info = demux.operator_info();
        demux.set_notify(false);

        // Order is very important here, because otherwise the
        // capabilities won't match up with the output streams later
        // on (when creating sessions). We stick to the order dictated
        // by the schema.
        let mut wrappers = Vec::with_capacity(self.schema.len());
        let mut streams = Vec::with_capacity(self.schema.len());

        for _ in self.schema.iter() {
            let (wrapper, stream) = demux.new_output();
            wrappers.push(wrapper);
            streams.push(stream);
        }

        demux.build(move |mut capabilities| {
            let activator = Rc::new(scope.activator_for(&operator_info.address[..]));

            let worker_index = scope.index();
            let num_workers = scope.peers();

            let mut datum_index = 0;
            let mut num_datums_read = 0;

            let schema = self.schema.clone();
            let has_headers = self.has_headers.clone();
            let delimiter = self.delimiter.clone();
            let comment = self.comment.clone();

            // Our source does not provide any eids
            let mut eid = worker_index;

            let mut files = std::fs::read_dir(&directory)
                .expect("Could not get an iterator over the path")
                .peekable();

            let filepath = files
                .next()
                .unwrap()
                .expect("File path reading error")
                .path();

            let reader = csv::ReaderBuilder::new()
                .has_headers(has_headers)
                .delimiter(delimiter)
                .comment(comment)
                .from_path(&filepath)
                .expect("failed to create reader");

            let mut iterator = reader.into_records();

            move |_frontiers| {
                let mut handles = Vec::with_capacity(schema.len());
                for wrapper in wrappers.iter_mut() {
                    handles.push(wrapper.activate());
                }

                let mut sessions = Vec::with_capacity(schema.len());

                for (idx, handle) in handles.iter_mut().enumerate() {
                    sessions.push(handle.session(&capabilities[idx]));
                }

                let time_secs = Instant::now().duration_since(t0).as_secs();
                let time = Duration::new(time_secs, 0);

                // The amount of work we want to do before letting timely schedule somebody else
                let mut fuel = 100000;

                while let Some(result) = iterator.next() {
                    let record = result.expect("read error");

                    // @TODO Hardcoded the number of processes to 3
                    if datum_index % (num_workers / 3) == (worker_index % (num_workers / 3)) {

                        for (idx, (aid, (offset, type_hint))) in schema.iter().enumerate() {
                            let v = match type_hint {
                                Value::String(_) =>  Ok(Value::String(record[*offset].to_string())),
                                Value::Number(_) => match record[*offset].parse::<i64>() {
                                    Ok(record) => Ok(Value::Number(record)),
                                    Err(err) => Err(format!("[Number]: {}", err.to_string())),
                                },
                                Value::Eid(_) => match record[*offset].parse::<Eid>() {
                                    Ok(record) => Ok(Value::Eid(record)),
                                    Err(err) => Err(format!("[Eid]: {}", err.to_string())),
                                },
                                Value::Rational32(_) => match record[*offset].parse::<f64>() {
                                    Ok(record) => Ok(Value::Number(record as i64)),
                                    Err(err) => Err(format!("[Float]: {}", err.to_string())),
                                },
                                Value::Instant(_) => match NaiveDateTime::parse_from_str(&record[*offset].to_string(), "%F %H:%M:%S") {
                                    Ok(record) => Ok(Value::Instant(record.timestamp() as u64)),
                                    Err(err) => Err(format!("[Instant]: {}", err.to_string())),
                                },
                                _ => panic!(
                                    "Only String, Number, Eid, Timestamp and Floats are supported at the moment."
                                ),
                            };

                            match v {
                                Ok(v) => {
                                    let tuple = (Value::Eid(eid as u64), v);
                                    sessions.get_mut(idx).unwrap().give((tuple, time, 1));
                                },
                                Err(err) => {warn!("Error while parsing: {}", err);}
                            }
                        }

                        eid += num_workers;
                        num_datums_read += 1;
                    }

                    datum_index += 1;

                    fuel -= 1;
                    if fuel <= 0 {
                        break;
                    }
                }

                // Are there still things to read?
                if iterator.reader().is_done() && files.peek().is_none() {
                    info!(
                        "[WORKER {}] Done reading {} out of {} datums",
                        worker_index, num_datums_read, datum_index,
                    );
                    capabilities.drain(..);
                } else if iterator.reader().is_done() {
                    // Get next file iterator
                    let filepath = files
                        .next()
                        .unwrap()
                        .expect("File path reading error")
                        .path();

                    let reader = csv::ReaderBuilder::new()
                        .has_headers(has_headers)
                        .delimiter(delimiter)
                        .comment(comment)
                        .from_path(&filepath)
                        .expect("failed to create reader");

                    iterator = reader.into_records();

                    for cap in capabilities.iter_mut() {
                        cap.downgrade(&time);
                    }
                    // We want to drop the borrow directly
                    {scheduler.upgrade().unwrap().borrow_mut().schedule(Rc::downgrade(&activator))}
                } else {
                    for cap in capabilities.iter_mut() {
                        cap.downgrade(&time);
                    }
                    {scheduler.upgrade().unwrap().borrow_mut().schedule(Rc::downgrade(&activator))}
                }
            }
        });

        let mut out = Vec::with_capacity(streams.len());
        for (idx, stream) in streams.drain(..).enumerate() {
            let aid = self.schema[idx].0.clone();
            out.push((aid.to_string(), stream));
        }

        out
    }
}
