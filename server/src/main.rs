#[global_allocator]
static ALLOCATOR: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate log;

use std::collections::{HashSet, VecDeque};
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::time::{Duration, Instant};

use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::operators::generic::OutputHandle;
use timely::dataflow::operators::{Operator, Probe};
use timely::logging::{Logger, TimelyEvent};
use timely::synchronization::Sequencer;

use differential_dataflow::logging::DifferentialEvent;
use differential_dataflow::operators::Consolidate;

use declarative_dataflow::scheduling::{AsScheduler, SchedulingEvent};
use declarative_dataflow::server;
use declarative_dataflow::server::{CreateAttribute, Request, Server, TxId};
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

#[derive(Debug, Clone)]
struct Configuration {
    /// Port at which client connections should be accepted.
    pub port: u16,
    /// File from which to read server configuration.
    pub config: Option<String>,
    /// Number of threads to use.
    pub threads: usize,
    /// Number of processes to expect over the entire cluster.
    pub processes: usize,
    /// Host addresses.
    pub addresses: Vec<String>,
    /// ID of this process within the cluster.
    pub timely_pid: usize,
    /// Whether to report connection progress.
    pub report: bool,
}

impl Default for Configuration {
    fn default() -> Self {
        Configuration {
            port: 6262,
            config: None,
            threads: 1,
            processes: 1,
            addresses: vec!["localhost:2101".to_string()],
            timely_pid: 0,
            report: false,
        }
    }
}

impl Configuration {
    /// Returns a `getopts::Options` struct describing all available
    /// configuration options.
    pub fn options() -> getopts::Options {
        let mut opts = getopts::Options::new();

        opts.optopt("", "port", "server port", "PORT");
        opts.optopt("", "config", "server configuration file", "FILE");

        // Timely arguments.
        opts.optopt(
            "w",
            "threads",
            "number of per-process worker threads",
            "NUM",
        );
        opts.optopt("p", "process", "identity of this process", "IDX");
        opts.optopt("n", "processes", "number of processes", "NUM");
        opts.optopt(
            "h",
            "hostfile",
            "text file whose lines are process addresses",
            "FILE",
        );
        opts.optflag("r", "report", "reports connection progress");

        opts
    }

    /// Parses configuration options from the provided arguments.
    pub fn from_args<I: Iterator<Item = String>>(args: I) -> Self {
        let default: Self = Default::default();
        let opts = Self::options();

        let matches = opts.parse(args).expect("failed to parse arguments");

        let port = matches
            .opt_str("port")
            .map(|x| x.parse().expect("failed to parse port"))
            .unwrap_or(default.port);

        let threads = matches
            .opt_str("w")
            .map(|x| x.parse().expect("failed to parse threads"))
            .unwrap_or(default.threads);

        let timely_pid = matches
            .opt_str("p")
            .map(|x| x.parse().expect("failed to parse process id"))
            .unwrap_or(default.timely_pid);

        let processes = matches
            .opt_str("n")
            .map(|x| x.parse().expect("failed to parse processes"))
            .unwrap_or(default.processes);

        let mut addresses = Vec::new();
        if let Some(hosts) = matches.opt_str("h") {
            let reader = BufReader::new(File::open(hosts.clone()).unwrap());
            for x in reader.lines().take(processes) {
                addresses.push(x.unwrap());
            }
            if addresses.len() < processes {
                panic!(
                    "could only read {} addresses from {}, but -n: {}",
                    addresses.len(),
                    hosts,
                    processes
                );
            }
        } else {
            for index in 0..processes {
                addresses.push(format!("localhost:{}", 2101 + index));
            }
        }

        assert!(processes == addresses.len());
        assert!(timely_pid < processes);

        let report = matches.opt_present("report");

        Self {
            port,
            config: matches.opt_str("config"),
            threads,
            processes,
            addresses,
            timely_pid,
            report,
        }
    }
}

impl Into<server::Configuration> for Configuration {
    fn into(self) -> server::Configuration {
        match self.config {
            None => server::Configuration::default(),
            Some(ref path) => {
                let mut config_file =
                    File::open(path).expect("failed to open server configuration file");

                let mut contents = String::new();
                config_file
                    .read_to_string(&mut contents)
                    .expect("failed to read configuration file");

                serde_json::from_str(&contents).expect("failed to parse configuration")
            }
        }
    }
}

impl Into<timely::Configuration> for Configuration {
    fn into(self) -> timely::Configuration {
        if self.processes > 1 {
            timely::Configuration::Cluster {
                threads: self.threads,
                process: self.timely_pid,
                addresses: self.addresses,
                report: self.report,
                log_fn: Box::new(|_| None),
            }
        } else if self.threads > 1 {
            timely::Configuration::Process(self.threads)
        } else {
            timely::Configuration::Thread
        }
    }
}

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

    let config = Configuration::from_args(std::env::args());
    let timely_config: timely::Configuration = config.clone().into();
    let server_config: server::Configuration = config.clone().into();

    timely::execute(timely_config, move |worker| {
        // Initialize server state (no networking).
        let mut server = Server::<T, Token>::new_at(server_config.clone(), worker.timer());

        if server_config.enable_logging {
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
        if worker.index() == 0 && server_config.tick.is_some() {
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
            "[W{}] running with config {:?}, {} peers",
            worker.index(),
            config,
            worker.peers(),
        );

        info!(
            "[W{}] running with server_config {:?}",
            worker.index(),
            server_config,
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
                while let Some(activator) = scheduler.realtime.next() {
                    if let Some(event) = activator.schedule() {
                        match event {
                            SchedulingEvent::Tick => {
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

                trace!("[W{}] {} requests by client {} at {}", worker.index(), command.requests.len(), command.client, next_tx);

                let owner = command.owner;
                let client = command.client;
                let last_tx = next_tx - 1;

                for req in command.requests.drain(..) {

                    // @TODO only create a single dataflow, but only if req != Transact

                    trace!("[W{}] {:?}", worker.index(), req);

                    let result = match req {
                        Request::Transact(req) => server.transact(req, owner, worker.index()),
                        Request::Subscribe(aid) => {
                            let interests = server.interests
                                .entry(aid.clone())
                                .or_insert_with(HashSet::new);

                            // All workers keep track of every client's interests, s.t. they
                            // know when to clean up unused dataflows.
                            interests.insert(Token(client));

                            if interests.len() > 1 {
                                // We only want to setup the dataflow on
                                // the first interest.
                                Ok(())
                            } else {
                                let send_results = io.send.clone();

                                let result = worker.dataflow::<T, _, _>(|scope| {
                                    let (propose, shutdown) = server
                                        .internal
                                        .forward_propose(&aid)
                                        .unwrap()
                                        .import_frontier(scope, &aid);

                                    // @TODO stash this somewhere
                                    std::mem::forget(shutdown);

                                    let pact = Exchange::new(move |_| owner as u64);

                                    propose
                                        .as_collection(|e, v| vec![e.clone(), v.clone()])
                                        .inner
                                        .unary(pact, "Subscription", move |_cap, _info| {
                                            move |input, _output: &mut OutputHandle<_, ResultDiff<T>, _>| {
                                                // Due to the exchange pact, this closure is only
                                                // executed by the owning worker.

                                                input.for_each(|_time, data| {
                                                    let data = data.iter()
                                                        .map(|(tuple, t, diff)| (tuple.clone(), t.clone().into(), *diff))
                                                        .collect::<Vec<ResultDiff<Time>>>();

                                                    send_results
                                                        .send(Output::QueryDiff(aid.clone(), data))
                                                        .expect("internal channel send failed");
                                                });
                                            }
                                        })
                                        .probe_with(&mut server.probe);

                                    Ok(())
                                });

                                result
                            }
                        }
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
                                server.create_attribute(scope, &name, config)
                            })
                        }
                        Request::AdvanceDomain(name, next) => server.advance_domain(name, next.into()),
                        Request::CloseInput(name) => server.internal.close_input(name),
                        Request::Disconnect => server.disconnect_client(Token(command.client)),
                        Request::Setup => unimplemented!(),
                        Request::Tick => {
                            // We don't actually have to do any actual worker here, because we are
                            // ticking the domain on each command anyways. We do have to schedule
                            // the next tick, however.

                            // We only want to issue ticks from a single worker, to avoid
                            // redundant ticking.
                            if worker.index() == 0 {
                                if let Some(tick) = server_config.tick {
                                    let interval_end = Instant::now().duration_since(worker.timer()).coarsen(&tick);
                                    let at = worker.timer() + interval_end;
                                    server.scheduler.borrow_mut().realtime.event_at(at, SchedulingEvent::Tick);
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

                if !server_config.manual_advance {
                    #[cfg(all(not(feature = "real-time"), not(feature = "bitemporal")))]
                    let next = next_tx as u64;
                    #[cfg(feature = "real-time")]
                    let next = Instant::now().duration_since(worker.timer());
                    #[cfg(feature = "bitemporal")]
                    let next = Pair::new(Instant::now().duration_since(worker.timer()), next_tx as u64);

                    server.internal.advance_epoch(next).expect("failed to advance epoch");
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
            server.internal.advance().expect("failed to advance domain");

            // Finally, we give the CPU a chance to chill, if no work
            // remains.
            let delay = server.scheduler.borrow().realtime.until_next().unwrap_or(Duration::from_millis(100));
            worker.step_or_park(Some(delay));
        }

        info!("[W{}] shutting down", worker.index());

        drop(sequencer);

        // Shutdown loggers s.t. logging dataflows can shut down.
        #[cfg(feature = "real-time")]
        server.shutdown_logging(worker).unwrap();

    }).expect("Timely computation did not exit cleanly");
}
