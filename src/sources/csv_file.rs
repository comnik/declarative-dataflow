//! Operator and utilities to source data from csv files.

use std::rc::Rc;
use std::time::{Duration, Instant};

use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::{Scope, Stream};

// use chrono::DateTime;

use crate::sources::{Sourceable, SourcingContext};
use crate::{Aid, Eid, Value};
use crate::{AttributeConfig, InputSemantics};

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
    /// Batch size.
    pub fuel: Option<usize>,
    /// Scheduling interval.
    pub interval: Option<Duration>,
}

impl<S: Scope<Timestamp = Duration>> Sourceable<S> for CsvFile {
    fn source(
        &self,
        scope: &mut S,
        context: SourcingContext<S::Timestamp>,
    ) -> Vec<(
        Aid,
        AttributeConfig,
        Stream<S, ((Value, Value), Duration, isize)>,
    )> {
        let filename = self.path.clone();

        // The following is mostly the innards of
        // `generic::source`. We use a builder directly, because we
        // need multiple outputs (one for each attribute the user has
        // epxressed interest in).
        let mut demux = OperatorBuilder::new(format!("CsvFile({})", filename), scope.clone());
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
            let total_fuel: i64 = self.fuel.unwrap_or(256) as i64;

            // Grab scheduler handle for deferred re-activation.
            let scheduler = context.scheduler;
            let t0 = context.t0;
            let interval = self.interval.unwrap_or(Duration::from_secs(1));

            move |_frontiers| {
                if iterator.reader().is_done() {
                    info!(
                        "[WORKER {}] read {} out of {} datums",
                        worker_index, num_datums_read, datum_index
                    );
                    capabilities.drain(..);
                } else {
                    let mut fuel = total_fuel;
                    
                    let mut handles = Vec::with_capacity(schema.len());
                    for wrapper in wrappers.iter_mut() {
                        handles.push(wrapper.activate());
                    }

                    let mut sessions = Vec::with_capacity(schema.len());
                    for (idx, handle) in handles.iter_mut().enumerate() {
                        sessions.push(handle.session(capabilities.get(idx).unwrap()));
                    }

                    let time = Instant::now().duration_since(t0);

                    info!("Ingesting at {:?}", time);

                    while let Some(result) = iterator.next() {
                        let record = result.expect("read error");

                        // if datum_index % num_workers == worker_index {
                        let eid = Value::Eid(record[eid_offset].parse::<Eid>().expect("not a eid"));
                        // let time = match timestamp_offset {
                        //     None => Default::default(),
                        //     Some(timestamp_offset) => {
                        //         let epoch =
                        //             DateTime::parse_from_rfc3339(&record[timestamp_offset])
                        //                 .expect("not a valid rfc3339 datetime")
                        //                 .timestamp();

                        //         if epoch >= 0 {
                        //             epoch as u64
                        //         } else {
                        //             panic!("invalid epoch");
                        //         }
                        //     }
                        // };

                        for (idx, (_aid, (offset, type_hint))) in schema.iter().enumerate() {
                            let v = match type_hint {
                                Value::String(_) => Value::String(record[*offset].to_string()),
                                Value::Number(_) => Value::Number(
                                    record[*offset].parse::<i64>().expect("not a number"),
                                ),
                                Value::Eid(_) => {
                                    Value::Eid(record[*offset].parse::<Eid>().expect("not a eid"))
                                }
                                _ => panic!(
                                    "Only String, Number, and Eid are supported at the moment."
                                ),
                            };

                            let tuple = (eid.clone(), v);
                            sessions.get_mut(idx).unwrap().give((tuple, time, 1));
                        }

                        num_datums_read += 1;
                        // }

                        datum_index += 1;

                        fuel -= 1;
                        if fuel <= 0 {
                            break;
                        }
                    }

                    if iterator.reader().is_done() {
                        info!(
                            "[WORKER {}] read {} out of {} datums",
                            worker_index, num_datums_read, datum_index
                        );
                        capabilities.drain(..);
                    } else {
                        // Incorporate processing time in downgrade
                        let time = Instant::now().duration_since(t0);

                        for cap in capabilities.iter_mut() {
                            cap.downgrade(&time);
                        }

                        // Notify the server that we want to be scheduled again soon
                        {
                            scheduler
                                .upgrade()
                                .unwrap()
                                .borrow_mut()
                                .schedule_after(interval, Rc::downgrade(&activator))
                        }
                    }
                }
            }
        });

        let mut out = Vec::with_capacity(streams.len());
        for (idx, stream) in streams.drain(..).enumerate() {
            let aid = self.schema[idx].0.clone();
            out.push((
                aid.to_string(),
                AttributeConfig::real_time(InputSemantics::CardinalityMany),
                stream,
            ));
        }

        out
    }
}
