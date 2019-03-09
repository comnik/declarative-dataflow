//! Operator and utilities to source data from the underlying Timely
//! logging streams.

use std::rc::Rc;
use std::time::Duration;

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::capture::event::link::EventLink;
use timely::dataflow::operators::capture::Replay;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::Map;
use timely::dataflow::{Scope, Stream};
use timely::logging::{BatchLogger, TimelyEvent};

use crate::sources::Sourceable;
use crate::Value;
use Value::{Address, Bool, Eid};

#[derive(Clone)]
enum LogAid {
    OperatesAddress,
    OperatesName,
    ChannelsAddress,
    ChannelsSrcIndex,
    ChannelsSrcPort,
    ChannelsTargetIndex,
    ChannelsTargetPort,
    Schedule,
}

trait Datafy {
    fn datafy(self) -> Vec<(LogAid, (Value, Value))>;
}

impl Datafy for TimelyEvent {
    fn datafy(self) -> Vec<(LogAid, (Value, Value))> {
        match self {
            TimelyEvent::Operates(x) => vec![
                (LogAid::OperatesAddress, (Eid(x.id as u64), Address(x.addr))),
                (
                    LogAid::OperatesName,
                    (Eid(x.id as u64), Value::String(x.name)),
                ),
            ],
            TimelyEvent::Channels(x) => vec![
                (
                    LogAid::ChannelsAddress,
                    (Eid(x.id as u64), Address(x.scope_addr)),
                ),
                (
                    LogAid::ChannelsSrcIndex,
                    (Eid(x.id as u64), Eid(x.source.0 as u64)),
                ),
                (
                    LogAid::ChannelsSrcPort,
                    (Eid(x.id as u64), Eid(x.source.1 as u64)),
                ),
                (
                    LogAid::ChannelsTargetIndex,
                    (Eid(x.id as u64), Eid(x.target.0 as u64)),
                ),
                (
                    LogAid::ChannelsTargetPort,
                    (Eid(x.id as u64), Eid(x.target.1 as u64)),
                ),
            ],
            TimelyEvent::Schedule(x) => vec![(
                LogAid::Schedule,
                (
                    Eid(x.id as u64),
                    Bool(x.start_stop == ::timely::logging::StartStop::Start),
                ),
            )],
            TimelyEvent::Messages(x) => vec![
                // @TODO
                // (LogAid::MessagesChannel, (x.seq_noValue::Usize(x.channel), Value::Bool(x.is_send), Value::Usize(x.source), Value::Usize(x.target), Value::Usize(x.seq_no), Value::Usize(x.length)
            ],
            _ => vec![],
        }
    }
}

/// One or more taps into Timely logging.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub struct TimelyLogging {}

impl Sourceable<Duration> for TimelyLogging {
    fn source<S: Scope<Timestamp = Duration>>(
        &self,
        scope: &mut S,
        names: Vec<String>,
    ) -> Stream<S, (usize, ((Value, Value), Duration, isize))> {
        let events = Rc::new(EventLink::new());
        let mut logger = BatchLogger::new(events.clone());

        scope
            .log_register()
            .insert::<TimelyEvent, _>("timely", move |time, data| logger.publish_batch(time, data));

        let input = Some(events).replay_into(scope);

        input.flat_map(|(time, worker, datum)| {
            datum
                .datafy()
                .drain(..)
                .map(move |(aid, x)| (aid as usize, (x, time, 1)))
                .collect::<Vec<(usize, ((Value, Value), Duration, isize))>>()
        })

        // let mut demux = OperatorBuilder::new("Timely Logging Demux".to_string(), scope.clone());

        // let mut input = demux.new_input(&input, Pipeline);

        // let (mut operates_out, operates) = demux.new_output();
        // let (mut channels_out, channels) = demux.new_output();
        // let (mut schedule_out, schedule) = demux.new_output();
        // let (mut messages_out, messages) = demux.new_output();

        // let mut demux_buffer = Vec::new();

        // demux.build(move |_capability| {

        //     move |_frontiers| {

        //         let mut operates = operates_out.activate();
        //         let mut channels = channels_out.activate();
        //         let mut schedule = schedule_out.activate();
        //         let mut messages = messages_out.activate();

        //         input.for_each(|time, data| {
        //             data.swap(&mut demux_buffer);
        //             let mut operates_session = operates.session(&time);
        //             let mut channels_session = channels.session(&time);
        //             let mut schedule_session = schedule.session(&time);
        //             let mut messages_session = messages.session(&time);

        //             for (time, _worker, datum) in demux_buffer.drain(..) {
        //                 match datum {
        //                     TimelyEvent::Operates(_) => {
        //                         operates_session.give((datum.as_vector(), time, 1));
        //                     },
        //                     TimelyEvent::Channels(_) => {
        //                         channels_session.give((datum.as_vector(), time, 1));
        //                     },
        //                     TimelyEvent::Schedule(_) => {
        //                         schedule_session.give((datum.as_vector(), time, 1));
        //                     },
        //                     TimelyEvent::Messages(_) => {
        //                         messages_session.give((datum.as_vector(), time, 1));
        //                     },
        //                     _ => { },
        //                 }
        //             }
        //         });
        //     }
        // });
    }
}
