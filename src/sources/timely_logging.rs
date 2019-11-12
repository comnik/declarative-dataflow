//! Operator and utilities to source data from the underlying Timely
//! logging streams.

use std::collections::HashMap;
use std::time::Duration;

use timely::communication::message::RefOrMut;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::capture::Replay;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::{Scope, Stream};
use timely::logging::{TimelyEvent, WorkerIdentifier};

use crate::sources::{Sourceable, SourcingContext};
use crate::{AsAid, Value};
use crate::{AttributeConfig, InputSemantics};
use Value::{Bool, Eid};

/// One or more taps into Timely logging.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub struct TimelyLogging<A: AsAid> {
    /// The log attributes that should be materialized.
    pub attributes: Vec<A>,
    /// Optionally listen for events from a number of remote workers,
    /// rather than introspectively.
    pub remote_peers: Option<usize>,
}

impl<A, S> Sourceable<A, S> for TimelyLogging<A>
where
    A: AsAid + From<&'static str>,
    S: Scope<Timestamp = Duration>,
{
    fn source(
        &self,
        scope: &mut S,
        context: SourcingContext<S::Timestamp>,
    ) -> Vec<(
        A,
        AttributeConfig,
        Stream<S, ((Value, Value), Duration, isize)>,
    )> {
        let input = match self.remote_peers {
            None => {
                // Read events introspectively.
                Some(context.timely_events).replay_into(scope)
            }
            Some(source_peers) => {
                // Listen for events from a remote computation.
                let sockets = open_sockets(source_peers);
                make_replayers(sockets, scope.index(), scope.peers()).replay_into(scope)
            }
        };

        let mut demux = OperatorBuilder::new("Timely Logging Demux".to_string(), scope.clone());

        let mut input = demux.new_input(&input, Pipeline);

        let mut wrappers = HashMap::with_capacity(self.attributes.len());
        let mut streams = HashMap::with_capacity(self.attributes.len());

        for aid in self.attributes.iter() {
            let (wrapper, stream) = demux.new_output();
            wrappers.insert(aid.clone(), wrapper);
            streams.insert(aid.clone(), stream);
        }

        let mut demux_buffer = Vec::new();
        let num_interests = self.attributes.len();

        let timely__scope = A::from("timely/scope");

        let timely__event__operates__local_id = A::from("timely.event.operates/local-id");
        let timely__event__operates__name = A::from("timely.event.operates/name");
        let timely__event__operates__shutdown = A::from("timely.event.operates/shutdown?");

        let timely__event__channels__src_index = A::from("timely.event.channels/src-index");
        let timely__event__channels__src_port = A::from("timely.event.channels/src-port");
        let timely__event__channels__target_index = A::from("timely.event.channels/target-index");
        let timely__event__channels__target_port = A::from("timely.event.channels/target-port");

        let schedule__started = A::from("schedule/started?");

        demux.build(move |_capability| {
            move |_frontiers| {
                let mut handles = HashMap::with_capacity(num_interests);
                for (aid, wrapper) in wrappers.iter_mut() {
                    handles.insert(aid.clone(), wrapper.activate());
                }

                input.for_each(|time, data: RefOrMut<Vec<_>>| {
                    data.swap(&mut demux_buffer);

                    let mut sessions = HashMap::with_capacity(num_interests);
                    for (aid, handle) in handles.iter_mut() {
                        sessions.insert(aid.clone(), handle.session(&time));
                    }

                    for (time, _worker, datum) in demux_buffer.drain(..) {
                        match datum {
                            TimelyEvent::Operates(mut x) => {
                                let eid = Eid((x.id as u64).into());
                                let name = Value::String(x.name);

                                // The id of this operator within its scope.
                                let local_id = Eid((x.addr.pop().unwrap() as u64).into());

                                if x.addr.is_empty() {
                                    // This is a top-level subgraph and thus lives in the root.
                                    sessions
                                        .get_mut(&timely__scope)
                                        .map(|s| s.give(((eid.clone(), Eid(0)), time, 1)));
                                } else {
                                    // The leaf scope that this operator resides in.
                                    let scope = Eid((x.addr.pop().unwrap() as u64).into());

                                    sessions
                                        .get_mut(&timely__scope)
                                        .map(|s| s.give(((eid.clone(), scope.clone()), time, 1)));

                                    if !x.addr.is_empty() {
                                        // This means that there are one or more parent scopes.
                                        // We want to make sure our scope is linked to its parent. But
                                        // we can assume all others are doing the same, so we don't need to
                                        // re-introduce edges for higher-level ancestors.
                                        let parent = Eid((x.addr.pop().unwrap() as u64).into());
                                        sessions
                                            .get_mut(&timely__scope)
                                            .map(|s| s.give(((scope, parent), time, 1)));
                                    }
                                }

                                sessions
                                    .get_mut(&timely__event__operates__local_id)
                                    .map(|s| s.give(((eid.clone(), local_id), time, 1)));
                                sessions
                                    .get_mut(&timely__event__operates__name)
                                    .map(|s| s.give(((eid, name), time, 1)));
                            }
                            TimelyEvent::Shutdown(x) => {
                                let eid = Eid((x.id as u64).into());
                                sessions
                                    .get_mut(&timely__event__operates__shutdown)
                                    .map(|s| s.give(((eid, Bool(true)), time, 1)));
                            }
                            TimelyEvent::Channels(mut x) => {
                                let eid = Eid((x.id as u64).into());
                                let src_index = Eid((x.source.0 as u64).into());
                                let src_port = Eid((x.source.1 as u64).into());
                                let target_index = Eid((x.target.0 as u64).into());
                                let target_port = Eid((x.target.1 as u64).into());

                                // The leaf scope that this channel resides in.
                                let scope = Eid((x.scope_addr.pop().unwrap() as u64).into());

                                sessions
                                    .get_mut(&timely__scope)
                                    .map(|s| s.give(((eid.clone(), scope.clone()), time, 1)));
                                sessions
                                    .get_mut(&timely__event__channels__src_index)
                                    .map(|s| s.give(((eid.clone(), src_index), time, 1)));
                                sessions
                                    .get_mut(&timely__event__channels__src_port)
                                    .map(|s| s.give(((eid.clone(), src_port), time, 1)));
                                sessions
                                    .get_mut(&timely__event__channels__target_index)
                                    .map(|s| s.give(((eid.clone(), target_index), time, 1)));
                                sessions
                                    .get_mut(&timely__event__channels__target_port)
                                    .map(|s| s.give(((eid, target_port), time, 1)));
                            }
                            TimelyEvent::Schedule(x) => {
                                let eid = Eid((x.id as u64).into());
                                let is_started =
                                    Bool(x.start_stop == ::timely::logging::StartStop::Start);

                                sessions
                                    .get_mut(&schedule__started)
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
            .map(|aid| {
                (
                    aid.clone(),
                    AttributeConfig::real_time(InputSemantics::Distinct),
                    streams.remove(aid).unwrap(),
                )
            })
            .collect()
    }
}

use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};

use timely::dataflow::operators::capture::EventReader;

/// Listens on 127.0.0.1:8000 and opens `source_peers` sockets from
/// the computations we're examining.
fn open_sockets(source_peers: usize) -> Arc<Mutex<Vec<Option<TcpStream>>>> {
    let listener = TcpListener::bind("127.0.0.1:8000").unwrap();
    let sockets = (0..source_peers)
        .map(|_| Some(listener.incoming().next().unwrap().unwrap()))
        .collect::<Vec<_>>();

    Arc::new(Mutex::new(sockets))
}

/// Construct replayers that read data from sockets and can stream it
/// into timely dataflow.
fn make_replayers(
    sockets: Arc<Mutex<Vec<Option<TcpStream>>>>,
    index: usize,
    peers: usize,
) -> Vec<EventReader<Duration, (Duration, WorkerIdentifier, TimelyEvent), TcpStream>> {
    sockets
        .lock()
        .unwrap()
        .iter_mut()
        .enumerate()
        .filter(|(i, _)| *i % peers == index)
        .map(move |(_, s)| s.take().unwrap())
        .map(|r| EventReader::<Duration, (Duration, WorkerIdentifier, TimelyEvent), _>::new(r))
        .collect::<Vec<_>>()
}
