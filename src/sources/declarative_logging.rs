//! Operator and utilities to source data from the underlying
//! Declarative logging streams.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::{Rc, Weak};
use std::time::{Duration, Instant};

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::capture::event::link::EventLink;
use timely::dataflow::operators::capture::Replay;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::{Scope, Stream};
use timely::logging::BatchLogger;

use crate::logging::DeclarativeEvent;
use crate::server::scheduler::Scheduler;
use crate::sources::Sourceable;
use crate::{AttributeConfig, InputSemantics};
use crate::{Aid, Value};
use Value::{Eid, Number};

/// One or more taps into Declarative logging.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub struct DeclarativeLogging {
    /// The log attributes that should be materialized.
    pub attributes: Vec<Aid>,
}

impl<S: Scope<Timestamp = Duration>> Sourceable<S> for DeclarativeLogging {
    fn source(
        &self,
        scope: &mut S,
        _t0: Instant,
        _scheduler: Weak<RefCell<Scheduler>>,
    ) -> Vec<(Aid, AttributeConfig, Stream<S, ((Value, Value), Duration, isize)>)> {
        let events = Rc::new(EventLink::new());
        let mut logger = BatchLogger::new(events.clone());

        scope
            .log_register()
            .insert::<DeclarativeEvent, _>("declarative", move |time, data| {
                logger.publish_batch(time, data)
            });

        let input = Some(events).replay_into(scope);

        let mut demux =
            OperatorBuilder::new("Declarative Logging Demux".to_string(), scope.clone());

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

                    for (time, worker, datum) in demux_buffer.drain(..) {
                        match datum {
                            DeclarativeEvent::JoinTuples(x) => {
                                let worker = Eid(worker as u64);
                                let cardinality = Number(x.cardinality as i64);

                                sessions
                                    .get_mut("declarative.event.join/tuples")
                                    .map(|s| s.give(((worker, cardinality), time, 1)));
                            }
                        }
                    }
                });
            }
        });

        self.attributes
            .iter()
            .map(|aid| (aid.to_string(), AttributeConfig::real_time(InputSemantics::Raw), streams.remove(aid).unwrap()))
            .collect()
    }
}
