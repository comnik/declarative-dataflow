//! Operator and utilities to source data from the underlying Timely
//! logging streams.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::{Rc, Weak};
use std::time::{Duration, Instant};

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::capture::event::link::EventLink;
use timely::dataflow::operators::capture::Replay;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::{Scope, Stream};
use timely::logging::{BatchLogger, TimelyEvent};

use crate::server::scheduler::Scheduler;
use crate::sources::Sourceable;
use crate::{Aid, Value};
use Value::{Bool, Eid};

/// One or more taps into Timely logging.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub struct TimelyLogging {
    /// The log attributes that should be materialized.
    pub attributes: Vec<Aid>,
}

impl<S: Scope<Timestamp = Duration>> Sourceable<S> for TimelyLogging {
    fn source(
        &self,
        scope: &mut S,
        _t0: Instant,
        _scheduler: Weak<RefCell<Scheduler>>,
    ) -> Vec<(Aid, Stream<S, ((Value, Value), Duration, isize)>)> {
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
                            TimelyEvent::Operates(mut x) => {
                                let eid = Eid(x.id as u64);
                                let name = Value::String(x.name);

                                // The id of this operator within its scope.
                                let local_id = Eid(x.addr.pop().unwrap() as u64);

                                if x.addr.is_empty() {
                                    // This is a top-level subgraph and thus lives in the root.
                                    sessions
                                        .get_mut("timely/scope")
                                        .map(|s| s.give(((eid.clone(), Eid(0)), time, 1)));
                                } else {
                                    // The leaf scope that this operator resides in.
                                    let scope = Eid(x.addr.pop().unwrap() as u64);

                                    sessions
                                        .get_mut("timely/scope")
                                        .map(|s| s.give(((eid.clone(), scope.clone()), time, 1)));

                                    if !x.addr.is_empty() {
                                        // This means that there are one or more parent scopes.
                                        // We want to make sure our scope is linked to its parent. But
                                        // we can assume all others are doing the same, so we don't need to
                                        // re-introduce edges for higher-level ancestors.
                                        let parent = Eid(x.addr.pop().unwrap() as u64);
                                        sessions
                                            .get_mut("timely/scope")
                                            .map(|s| s.give(((scope, parent), time, 1)));
                                    }
                                }

                                sessions
                                    .get_mut("timely.event.operates/local-id")
                                    .map(|s| s.give(((eid.clone(), local_id), time, 1)));
                                sessions
                                    .get_mut("timely.event.operates/name")
                                    .map(|s| s.give(((eid, name), time, 1)));
                            }
                            TimelyEvent::Shutdown(x) => {
                                let eid = Eid(x.id as u64);
                                sessions
                                    .get_mut("timely.event.operates/shutdown?")
                                    .map(|s| s.give(((eid, Bool(true)), time, 1)));
                            }
                            TimelyEvent::Channels(mut x) => {
                                let eid = Eid(x.id as u64);
                                let src_index = Eid(x.source.0 as u64);
                                let src_port = Eid(x.source.1 as u64);
                                let target_index = Eid(x.target.0 as u64);
                                let target_port = Eid(x.target.1 as u64);

                                // The leaf scope that this channel resides in.
                                let scope = Eid(x.scope_addr.pop().unwrap() as u64);

                                sessions
                                    .get_mut("timely/scope")
                                    .map(|s| s.give(((eid.clone(), scope.clone()), time, 1)));
                                sessions
                                    .get_mut("timely.event.channels/src-index")
                                    .map(|s| s.give(((eid.clone(), src_index), time, 1)));
                                sessions
                                    .get_mut("timely.event.channels/src-port")
                                    .map(|s| s.give(((eid.clone(), src_port), time, 1)));
                                sessions
                                    .get_mut("timely.event.channels/target-index")
                                    .map(|s| s.give(((eid.clone(), target_index), time, 1)));
                                sessions
                                    .get_mut("timely.event.channels/target-port")
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

        self.attributes
            .iter()
            .map(|aid| (aid.to_string(), streams.remove(aid).unwrap()))
            .collect()
    }
}
