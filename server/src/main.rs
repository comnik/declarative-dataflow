#[global_allocator]
static ALLOCATOR: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate log;

use std::collections::{HashSet, VecDeque};
use std::time::{Duration, Instant};

use getopts::Options;

use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::operators::generic::OutputHandle;
use timely::dataflow::operators::{Operator, Probe};
use timely::logging::{Logger, TimelyEvent};
use timely::synchronization::Sequencer;

use differential_dataflow::logging::DifferentialEvent;
use differential_dataflow::operators::Consolidate;

use declarative_dataflow::server::{scheduler, Config, CreateAttribute, Request, Server, TxId};
use declarative_dataflow::sinks::{Sinkable, SinkingContext};
use declarative_dataflow::timestamp::{Coarsen, Time};
use declarative_dataflow::{Output, ResultDiff};

mod networking;
use crate::networking::{DomainEvent, Token, IO, SYSTEM};

/// Server timestamp type.
#[cfg(all(not(feature = "real-time"), not(feature = "bitemporal")))]
type T = u64;

/// Server timestamp type.
#[cfg(feature = "real-time")]
type T = Duration;

#[cfg(feature = "bitemporal")]
use declarative_dataflow::timestamp::pair::Pair;
/// Server timestamp type.
#[cfg(feature = "bitemporal")]
type T = Pair<Duration, u64>;

/// A mutation of server state.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Serialize, Deserialize, Debug)]
struct Command {
    /// The worker that received this command from a client originally
    /// and is therefore the one that should receive all outputs.
    pub owner: usize,
    /// The client token that issued the command. Only relevant to the
    /// owning worker, as no one else has the connection.
    pub client: usize,
    /// Requests issued by the client.
    pub requests: Vec<Request>,
}

fn main() {
    env_logger::init();

    let args: Vec<String> = std::env::args().collect();
    let timely_args = std::env::args().take_while(|ref arg| *arg != "--");
    let timely_config = timely::Configuration::from_args(timely_args).unwrap();

    timely::execute(timely_config, move |worker| {
        // Read configuration.
        let config = {
            let server_args = args.iter().rev().take_while(|arg| *arg != "--");
            let default_config: Config = Default::default();

            let opts = options();

            match opts.parse(server_args) {
                Err(err) => panic!(err),
                Ok(matches) => {
                    let starting_port = matches
                        .opt_str("port")
                        .map(|x| x.parse().expect("failed to parse port"))
                        .unwrap_or(default_config.port);

                    let tick: Option<Duration> = matches
                        .opt_str("tick")
                        .map(|x| Duration::from_secs(x.parse().expect("failed to parse tick duration")));

                    Config {
                        port: starting_port + (worker.index() as u16),
                        tick,
                        manual_advance: matches.opt_present("manual-advance"),
                        enable_logging: matches.opt_present("enable-logging"),
                        enable_optimizer: matches.opt_present("enable-optimizer"),
                        enable_meta: matches.opt_present("enable-meta"),
                    }
                }
            }
        };

        // Initialize server state (no networking).
        let mut server = Server::<T, Token>::new_at(config.clone(), worker.timer());

        if config.enable_logging {
            #[cfg(feature = "real-time")]
            server.enable_logging(worker).unwrap();
        }

        // The server might specify a sequence of requests for
        // setting-up built-in arrangements. We serialize those here
        // and pre-load the sequencer with them, such that they will
        // flow through the regular request handling.
        let builtins = Server::<T, Token>::builtins();
        let preload_command = Command {
            owner: worker.index(),
            client: SYSTEM.0,
            requests: builtins,
        };

        // Setup serializing command stream between all workers.
        let mut sequencer: Sequencer<Command> =
            Sequencer::preloaded(worker, Instant::now(), VecDeque::from(vec![preload_command]));

        // Kickoff ticking, if configured. We only want to issue ticks
        // from a single worker, to avoid redundant ticking.
        if worker.index() == 0 && config.tick.is_some() {
            sequencer.push(Command {
                owner: 0,
                client: SYSTEM.0,
                requests: vec![Request::Tick],
            });
        }

        // Set up I/O event loop.
        let mut io = {
            use std::net::{IpAddr, Ipv4Addr, SocketAddr};

            // let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), config.port);
            let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0,0,0,0)), config.port);

            IO::new(addr)
        };

        info!(
            "[WORKER {}] running with config {:?}, {} peers",
            worker.index(),
            config,
            worker.peers(),
        );

        // Sequence counter for commands.
        let mut next_tx: TxId = 0;

        let mut shutdown = false;

        while !shutdown {
            // each worker has to...
            //
            // ...accept new client connections
            // ...accept commands on a client connection and push them to the sequencer
            // ...step computations
            // ...send results to clients
            //
            // by having everything inside a single event loop, we can
            // easily make trade-offs such as limiting the number of
            // commands consumed, in order to ensure timely progress
            // on registered queues

            // polling - should usually be driven completely
            // non-blocking (i.e. timeout 0), but higher timeouts can
            // be used for debugging or artificial braking

            if server.scheduler.borrow().has_pending() {
                let mut scheduler = server.scheduler.borrow_mut();
                while let Some(activator) = scheduler.next() {
                    if let Some(event) = activator.schedule() {
                        match event {
                            scheduler::Event::Tick => {
                                sequencer.push(Command {
                                    owner: worker.index(),
                                    client: SYSTEM.0,
                                    requests: vec![Request::Tick],
                                });
                            }
                        }
                    }
                }
            } else {
                // @TODO in blocking mode, we could check whether
                // worker 'would park', and block for input here
                // poll.poll(&mut events, None).expect("failed to poll I/O events");
            }

            // Transform low-level I/O events into domain events.
            io.step(next_tx, &server.interests);

            while let Some(event) = io.next() {
                match event {
                    DomainEvent::Requests(token, requests) => {
                        trace!("[IO] command");
                        sequencer.push(Command {
                            owner: worker.index(),
                            client: token.into(),
                            requests,
                        });
                    }
                    DomainEvent::Disconnect(token) => {
                        info!("[IO] token={:?} disconnected", token);
                        sequencer.push(Command {
                            owner: worker.index(),
                            client: token.into(),
                            requests: vec![Request::Disconnect],
                        });
                    }
                }
            }

            // handle commands

            while let Some(mut command) = sequencer.next() {

                // Count-up sequence numbers.
                next_tx += 1;

                info!("[WORKER {}] {} requests by client {} at {}", worker.index(), command.requests.len(), command.client, next_tx);

                let owner = command.owner;
                let client = command.client;
                let last_tx = next_tx - 1;

                for req in command.requests.drain(..) {

                    // @TODO only create a single dataflow, but only if req != Transact

                    trace!("[WORKER {}] {:?}", worker.index(), req);

                    let result = match req {
                        Request::Transact(req) => server.transact(req, owner, worker.index()),
                        Request::Interest(req) => {
                            let interests = server.interests
                                .entry(req.name.clone())
                                .or_insert_with(HashSet::new);

                            // We need to check this, because we only want to setup
                            // the dataflow on the first interest.
                            let was_first = interests.is_empty();

                            // All workers keep track of every client's interests, s.t. they
                            // know when to clean up unused dataflows.
                            interests.insert(Token(client));

                            if was_first {
                                let send_results = io.send.clone();

                                let disable_logging = req.disable_logging.unwrap_or(false);
                                let mut timely_logger = None;
                                let mut differential_logger = None;

                                if disable_logging {
                                    info!("Disabling logging");
                                    timely_logger = worker.log_register().remove("timely");
                                    differential_logger = worker.log_register().remove("differential/arrange");
                                }

                                let result = worker.dataflow::<T, _, _>(|scope| {
                                    let sink_context: SinkingContext = (&req).into();

                                    let relation = match server.interest(&req.name, scope) {
                                        Err(error) => { return Err(error); }
                                        Ok(relation) => relation,
                                    };

                                    let delayed = match req.granularity {
                                        None => relation.consolidate(),
                                        Some(granularity) => {
                                            let granularity: T = granularity.into();
                                            relation
                                                .delay(move |t| t.coarsen(&granularity))
                                                .consolidate()
                                        }
                                    };

                                    let pact = Exchange::new(move |_| owner as u64);

                                    match req.sink {
                                        Some(sink) => {
                                            let sunk = match sink.sink(&delayed.inner, pact, &mut server.probe, sink_context) {
                                                Err(error) => { return Err(error); }
                                                Ok(sunk) => sunk,
                                            };

                                            if let Some(sunk) = sunk {
                                                let mut vector = Vec::new();
                                                sunk
                                                    .unary(Pipeline, "SinkResults", move |_cap, _info| {
                                                        move |input, _output: &mut OutputHandle<_, ResultDiff<T>, _>| {
                                                            input.for_each(|_time, data| {
                                                                data.swap(&mut vector);

                                                                for out in vector.drain(..) {
                                                                    send_results.send(out)
                                                                        .expect("internal channel send failed");
                                                                }
                                                            });
                                                        }
                                                    })
                                                    .probe_with(&mut server.probe);
                                            }

                                            Ok(())
                                        }
                                        None => {
                                            delayed
                                                .inner
                                                .unary(pact, "ResultsRecv", move |_cap, _info| {
                                                    move |input, _output: &mut OutputHandle<_, ResultDiff<T>, _>| {
                                                        // due to the exchange pact, this closure is only
                                                        // executed by the owning worker

                                                        // @TODO only forward inputs up to the frontier!

                                                        input.for_each(|_time, data| {
                                                            let data = data.iter()
                                                                .map(|(tuple, t, diff)| (tuple.clone(), t.clone().into(), *diff))
                                                                .collect::<Vec<ResultDiff<Time>>>();

                                                            send_results
                                                                .send(Output::QueryDiff(sink_context.name.clone(), data))
                                                                .expect("internal channel send failed");
                                                        });
                                                    }
                                                })
                                                .probe_with(&mut server.probe);

                                            Ok(())
                                        }
                                    }
                                });

                                if disable_logging {
                                    if let Some(logger) = timely_logger {
                                        if let Ok(logger) = logger.downcast::<Logger<TimelyEvent>>() {
                                            worker
                                                .log_register()
                                                .insert_logger::<TimelyEvent>("timely", *logger);
                                        }
                                    }

                                    if let Some(logger) = differential_logger {
                                        if let Ok(logger) = logger.downcast::<Logger<DifferentialEvent>>() {
                                            worker
                                                .log_register()
                                                .insert_logger::<DifferentialEvent>("differential/arrange", *logger);
                                        }
                                    }
                                }

                                result
                            } else {
                                Ok(())
                            }
                        }
                        Request::Uninterest(name) => server.uninterest(Token(command.client), &name),
                        Request::Register(req) => server.register(req),
                        Request::RegisterSource(source) => {
                            worker.dataflow::<T, _, _>(|scope| {
                                server.register_source(Box::new(source), scope)
                            })
                        }
                        Request::CreateAttribute(CreateAttribute { name, config }) => {
                            worker.dataflow::<T, _, _>(|scope| {
                                server.context.internal.create_transactable_attribute(&name, config, scope)
                            })
                        }
                        Request::AdvanceDomain(name, next) => server.advance_domain(name, next.into()),
                        Request::CloseInput(name) => server.context.internal.close_input(name),
                        Request::Disconnect => server.disconnect_client(Token(command.client)),
                        Request::Setup => unimplemented!(),
                        Request::Tick => {
                            // We don't actually have to do any actual worker here, because we are
                            // ticking the domain on each command anyways. We do have to schedule
                            // the next tick, however.

                            // We only want to issue ticks from a single worker, to avoid
                            // redundant ticking.
                            if worker.index() == 0 {
                                if let Some(tick) = config.tick {
                                    let interval_end = Instant::now().duration_since(worker.timer()).coarsen(&tick);
                                    let at = worker.timer() + interval_end;
                                    server.scheduler.borrow_mut().event_at(at, scheduler::Event::Tick);
                                }
                            }

                            Ok(())
                        }
                        Request::Status => {
                            let status = serde_json::json!({
                                "category": "df/status",
                                "message": "running",
                            });

                            io.send.send(Output::Message(client, status)).unwrap();

                            Ok(())
                        }
                        Request::Shutdown => {
                            shutdown = true;
                            Ok(())
                        }
                    };

                    if let Err(error) = result {
                        io.send.send(Output::Error(client, error, last_tx)).unwrap();
                    }
                }

                if !config.manual_advance {
                    #[cfg(all(not(feature = "real-time"), not(feature = "bitemporal")))]
                    let next = next_tx as u64;
                    #[cfg(feature = "real-time")]
                    let next = Instant::now().duration_since(worker.timer());
                    #[cfg(feature = "bitemporal")]
                    let next = Pair::new(Instant::now().duration_since(worker.timer()), next_tx as u64);

                    server.context.internal.advance_epoch(next).expect("failed to advance epoch");
                }
            }

            // We must always ensure that workers step in every
            // iteration, even if no queries registered, s.t. the
            // sequencer can continue propagating commands. We also
            // want to limit the maximal number of steps here to avoid
            // stalling user inputs.
            for _i in 0..32 {
                worker.step();
            }

            // We advance before `step_or_park`, because advancing
            // might take a decent amount of time, in case traces get
            // compacted. If that happens, we can park less before
            // scheduling the next activator.
            server.context.internal.advance().expect("failed to advance domain");

            // Finally, we give the CPU a chance to chill, if no work
            // remains.
            let delay = server.scheduler.borrow().until_next().unwrap_or(Duration::from_millis(100));
            worker.step_or_park(Some(delay));
        }

        info!("[WORKER {}] shutting down", worker.index());

        drop(sequencer);

        // Shutdown loggers s.t. logging dataflows can shut down.
        #[cfg(feature = "real-time")]
        server.shutdown_logging(worker).unwrap();

    }).expect("Timely computation did not exit cleanly");
}

fn options() -> Options {
    let mut opts = Options::new();
    opts.optopt("", "port", "server port", "PORT");
    opts.optopt(
        "",
        "tick",
        "advance domain at a regular interval",
        "SECONDS",
    );
    opts.optflag(
        "",
        "manual-advance",
        "forces clients to call AdvanceDomain explicitely",
    );
    opts.optflag("", "enable-logging", "enable log event sources");
    opts.optflag("", "enable-history", "enable historical queries");
    opts.optflag("", "enable-optimizer", "enable WCO queries");
    opts.optflag("", "enable-meta", "enable queries on the query graph");

    opts
}
