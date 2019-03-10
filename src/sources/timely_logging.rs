//! Operator and utilities to source data from the underlying Timely
//! logging streams.

use std::collections::HashMap;
use std::rc::Rc;
use std::time::Duration;

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::capture::event::link::EventLink;
use timely::dataflow::operators::capture::Replay;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::{Scope, Stream};
use timely::logging::{BatchLogger, TimelyEvent};

use crate::sources::Sourceable;
use crate::{Aid, Value};
use Value::{Address, Bool, Eid};

/// One or more taps into Timely logging.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub struct TimelyLogging {
    /// The log attributes that should be materialized.
    pub attributes: Vec<Aid>,
}

impl Sourceable<Duration> for TimelyLogging {
    fn source<S: Scope<Timestamp = Duration>>(
        &self,
        scope: &mut S,
    ) -> HashMap<Aid, Stream<S, ((Value, Value), Duration, isize)>> {
        let events = Rc::new(EventLink::new());
        let mut logger = BatchLogger::new(events.clone());

        scope
            .log_register()
            .insert::<TimelyEvent, _>("timely", move |time, data| logger.publish_batch(time, data));

        let input = Some(events).replay_into(scope);

        let mut demux = OperatorBuilder::new("Timely Logging Demux".to_string(), scope.clone());

        let mut input = demux.new_input(&input, Pipeline);

        let mut wrappers = HashMap::with_capacity(self.attributes.len());
        let mut streams = HashMap::with_capacity(self.attributes.len());

        for aid in self.attributes.iter() {
            let (wrapper, stream) = demux.new_output();
            wrappers.insert(aid.to_string(), wrapper);
            streams.insert(aid.to_string(), stream);
        }

        let mut demux_buffer = Vec::new();
        let num_interests = self.attributes.len();

        demux.build(move |_capability| {
            move |_frontiers| {
                let mut handles = HashMap::with_capacity(num_interests);
                for (aid, wrapper) in wrappers.iter_mut() {
                    handles.insert(aid.to_string(), wrapper.activate());
                }

                input.for_each(|time, data| {
                    data.swap(&mut demux_buffer);

                    let mut sessions = HashMap::with_capacity(num_interests);
                    for (aid, handle) in handles.iter_mut() {
                        sessions.insert(aid.to_string(), handle.session(&time));
                    }

                    for (time, _worker, datum) in demux_buffer.drain(..) {
                        match datum {
                            TimelyEvent::Operates(x) => {
                                let eid = Eid(x.id as u64);
                                let name = Value::String(x.name);
                                let address = Address(x.addr);

                                sessions
                                    .get_mut("operates/address")
                                    .map(|s| s.give(((eid.clone(), address), time, 1)));
                                sessions
                                    .get_mut("operates/name")
                                    .map(|s| s.give(((eid, name), time, 1)));
                            }
                            TimelyEvent::Channels(x) => {
                                let eid = Eid(x.id as u64);
                                let address = Address(x.scope_addr);
                                let src_index = Eid(x.source.0 as u64);
                                let src_port = Eid(x.source.1 as u64);
                                let target_index = Eid(x.target.0 as u64);
                                let target_port = Eid(x.target.1 as u64);

                                sessions
                                    .get_mut("channels/address")
                                    .map(|s| s.give(((eid.clone(), address), time, 1)));
                                sessions
                                    .get_mut("channels/src-index")
                                    .map(|s| s.give(((eid.clone(), src_index), time, 1)));
                                sessions
                                    .get_mut("channels/src-port")
                                    .map(|s| s.give(((eid.clone(), src_port), time, 1)));
                                sessions
                                    .get_mut("channels/target-index")
                                    .map(|s| s.give(((eid.clone(), target_index), time, 1)));
                                sessions
                                    .get_mut("channels/target-port")
                                    .map(|s| s.give(((eid, target_port), time, 1)));
                            }
                            TimelyEvent::Schedule(x) => {
                                let eid = Eid(x.id as u64);
                                let is_started =
                                    Bool(x.start_stop == ::timely::logging::StartStop::Start);

                                sessions
                                    .get_mut("schedule/started?")
                                    .map(|s| s.give(((eid, is_started), time, 1)));
                            }
                            TimelyEvent::Messages(_x) => {
                                // @TODO
                                // (MessagesChannel, (x.seq_noValue::Usize(x.channel), Value::Bool(x.is_send), Value::Usize(x.source), Value::Usize(x.target), Value::Usize(x.seq_no), Value::Usize(x.length)
                            }
                            _ => {}
                        }
                    }
                });
            }
        });

        streams
    }
}
