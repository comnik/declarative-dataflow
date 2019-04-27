//! Operator and utilities to source data from the underlying
//! Differential logging streams.

use std::collections::HashMap;
use std::time::Duration;

use timely::communication::message::RefOrMut;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::capture::Replay;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::{Scope, Stream};

use differential_dataflow::logging::DifferentialEvent;

use crate::sources::{Sourceable, SourcingContext};
use crate::{Aid, Value};
use crate::{AttributeConfig, InputSemantics};
use Value::{Eid, Number};

/// One or more taps into Differential logging.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub struct DifferentialLogging {
    /// The log attributes that should be materialized.
    pub attributes: Vec<Aid>,
}

impl<S: Scope<Timestamp = Duration>> Sourceable<S> for DifferentialLogging {
    fn source(
        &self,
        scope: &mut S,
        context: SourcingContext<S::Timestamp>,
    ) -> Vec<(
        Aid,
        AttributeConfig,
        Stream<S, ((Value, Value), Duration, isize)>,
    )> {
        let input = Some(context.differential_events).replay_into(scope);

        let mut demux =
            OperatorBuilder::new("Differential Logging Demux".to_string(), scope.clone());

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

                input.for_each(|time, data: RefOrMut<Vec<_>>| {
                    data.swap(&mut demux_buffer);

                    let mut sessions = HashMap::with_capacity(num_interests);
                    for (aid, handle) in handles.iter_mut() {
                        sessions.insert(aid.to_string(), handle.session(&time));
                    }

                    for (time, _worker, datum) in demux_buffer.drain(..) {
                        match datum {
                            DifferentialEvent::Batch(x) => {
                                let operator = Eid((x.operator as u64).into());
                                let length = Number(x.length as i64);

                                sessions
                                    .get_mut("differential.event/size")
                                    .map(|s| s.give(((operator, length), time, 1)));
                            }
                            DifferentialEvent::Merge(x) => {
                                trace!("[DIFFERENTIAL] {:?}", x);

                                if let Some(complete_size) = x.complete {
                                    let operator = Eid((x.operator as u64).into());
                                    let size_diff =
                                        (complete_size as i64) - (x.length1 + x.length2) as i64;

                                    sessions
                                        .get_mut("differential.event/size")
                                        .map(|s| s.give(((operator, Number(size_diff)), time, 1)));
                                }
                            }
                            _ => {}
                        }
                    }
                });
            }
        });

        self.attributes
            .iter()
            .map(|aid| {
                (
                    aid.to_string(),
                    AttributeConfig::real_time(InputSemantics::Raw),
                    streams.remove(aid).unwrap(),
                )
            })
            .collect()
    }
}
