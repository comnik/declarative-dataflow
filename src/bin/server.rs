#[global_allocator]
static ALLOCATOR: jemallocator::Jemalloc = jemallocator::Jemalloc;

extern crate declarative_dataflow;
extern crate differential_dataflow;
extern crate getopts;
extern crate mio;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate slab;
extern crate timely;
extern crate ws;

#[macro_use]
extern crate log;
extern crate env_logger;

#[macro_use]
extern crate abomonation_derive;
extern crate abomonation;

use std::collections::VecDeque;
use std::io::BufRead;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::{Duration, Instant};
use std::{thread, usize};

use getopts::Options;

use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::generic::OutputHandle;
use timely::dataflow::operators::{Operator, Probe};
use timely::synchronization::Sequencer;

use mio::net::TcpListener;
use mio::*;

use slab::Slab;

use ws::connection::{ConnEvent, Connection};

use declarative_dataflow::server::{Config, CreateAttribute, Request, Server};
use declarative_dataflow::{Error, ResultDiff};

const SERVER: Token = Token(usize::MAX - 1);
const RESULTS: Token = Token(usize::MAX - 2);
const ERRORS: Token = Token(usize::MAX - 3);
const SYSTEM: Token = Token(usize::MAX - 4);
const CLI: Token = Token(usize::MAX - 5);

/// A mutation of server state.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Serialize, Deserialize, Debug)]
pub struct Command {
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

    let mut opts = Options::new();
    opts.optopt("", "port", "server port", "PORT");
    opts.optflag("", "enable-cli", "enable the CLI interface");
    opts.optflag("", "enable-history", "enable historical queries");
    opts.optflag("", "enable-optimizer", "enable WCO queries");
    opts.optflag("", "enable-meta", "enable queries on the query graph");

    let args: Vec<String> = std::env::args().collect();
    let timely_args = std::env::args().take_while(|ref arg| *arg != "--");

    timely::execute_from_args(timely_args, move |worker| {
        // read configuration
        let server_args = args.iter().rev().take_while(|arg| *arg != "--");
        let default_config: Config = Default::default();
        let config = match opts.parse(server_args) {
            Err(err) => panic!(err),
            Ok(matches) => {
                let starting_port = matches
                    .opt_str("port")
                    .map(|x| x.parse().unwrap_or(default_config.port))
                    .unwrap_or(default_config.port);

                Config {
                    port: starting_port + (worker.index() as u16),
                    enable_cli: matches.opt_present("enable-cli"),
                    enable_history: matches.opt_present("enable-history"),
                    enable_optimizer: matches.opt_present("enable-optimizer"),
                    enable_meta: matches.opt_present("enable-meta"),
                }
            }
        };

        // setup interpretation context
        let mut server = Server::<Token>::new(config.clone());

        // The server might specify a sequence of requests for
        // setting-up built-in arrangements. We serialize those here
        // and pre-load the sequencer with them, such that they will
        // flow through the regular request handling.
        let builtins = Server::<Token>::builtins();
        let preload_command = Command {
            owner: worker.index(),
            client: SYSTEM.0,
            requests: builtins,
        };

        // setup serialized command queue (shared between all workers)
        let mut sequencer: Sequencer<Command> =
            Sequencer::preloaded(worker, Instant::now(), VecDeque::from(vec![preload_command]));

        // configure websocket server
        let ws_settings = ws::Settings {
            max_connections: 1024,
            ..ws::Settings::default()
        };

        // setup CLI channel
        let (send_cli, recv_cli) = mio::channel::channel();

        // setup results channel
        let (send_results, recv_results) = mio::channel::channel::<(String, Vec<ResultDiff>)>();

        // setup errors channel
        let (send_errors, recv_errors) = mio::channel::channel::<(Vec<Token>, Vec<Error>)>();

        // setup server socket
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), config.port);
        let server_socket = TcpListener::bind(&addr).unwrap();
        let mut connections = Slab::with_capacity(ws_settings.max_connections);
        let mut next_connection_id: u32 = 0;

        // setup event loop
        let poll = Poll::new().unwrap();
        let mut events = Events::with_capacity(1024);

        if config.enable_cli {
            poll.register(
                &recv_cli,
                CLI,
                Ready::readable(),
                PollOpt::edge() | PollOpt::oneshot(),
            ).unwrap();

            thread::spawn(move || {
                info!("[CLI] accepting cli commands");

                let input = std::io::stdin();
                while let Some(line) = input.lock().lines().map(|x| x.unwrap()).next() {
                    send_cli
                        .send(line.to_string())
                        .expect("failed to send command");
                }
            });
        }

        poll.register(
            &recv_results,
            RESULTS,
            Ready::readable(),
            PollOpt::edge() | PollOpt::oneshot(),
        ).unwrap();

        poll.register(
            &recv_errors,
            ERRORS,
            Ready::readable(),
            PollOpt::edge() | PollOpt::oneshot(),
        ).unwrap();

        poll.register(&server_socket, SERVER, Ready::readable(), PollOpt::level())
            .unwrap();

        info!(
            "[WORKER {}] running with config {:?}",
            worker.index(),
            config
        );

        loop {
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
            //
            // @TODO handle errors
            poll.poll(&mut events, Some(Duration::from_millis(0)))
                .unwrap();

            for event in events.iter() {
                trace!(
                    "[WORKER {}] recv event on {:?}",
                    worker.index(),
                    event.token()
                );

                match event.token() {
                    CLI => {
                        while let Ok(cli_input) = recv_cli.try_recv() {
                            match serde_json::from_str::<Vec<Request>>(&cli_input) {
                                Err(serde_error) => {
                                    let error = Error {
                                        category: "df.error.category/incorrect",
                                        message: serde_error.to_string(),
                                    };

                                    send_errors.send((vec![], vec![error])).unwrap();
                                }
                                Ok(requests) => {
                                    sequencer.push(Command {
                                        owner: worker.index(),
                                        client: SYSTEM.0,
                                        requests,
                                    });
                                }
                            }
                        }

                        poll.reregister(
                            &recv_cli,
                            CLI,
                            Ready::readable(),
                            PollOpt::edge() | PollOpt::oneshot(),
                        ).unwrap();
                    }
                    SERVER => {
                        if event.readiness().is_readable() {
                            // new connection arrived on the server socket
                            match server_socket.accept() {
                                Err(err) => error!(
                                    "[WORKER {}] error while accepting connection {:?}",
                                    worker.index(),
                                    err
                                ),
                                Ok((socket, addr)) => {
                                    info!(
                                        "[WORKER {}] new tcp connection from {}",
                                        worker.index(),
                                        addr
                                    );

                                    // @TODO to nagle or not to nagle?
                                    // sock.set_nodelay(true)

                                    let token = {
                                        let entry = connections.vacant_entry();
                                        let token = Token(entry.key());
                                        let connection_id = next_connection_id;
                                        next_connection_id = next_connection_id.wrapping_add(1);

                                        entry.insert(Connection::new(
                                            token,
                                            socket,
                                            ws_settings,
                                            connection_id,
                                        ));

                                        token
                                    };

                                    let conn = &mut connections[token.into()];

                                    conn.as_server().unwrap();

                                    poll.register(
                                        conn.socket(),
                                        conn.token(),
                                        conn.events(),
                                        PollOpt::edge() | PollOpt::oneshot(),
                                    ).unwrap();
                                }
                            }
                        }
                    }
                    RESULTS => {
                        while let Ok((query_name, results)) = recv_results.try_recv() {
                            info!("[WORKER {}] {:?} {:?}", worker.index(), query_name, results);

                            match server.interests.get(&query_name) {
                                None => {
                                    /* @TODO unregister this flow */
                                    info!("NO INTEREST FOR THIS RESULT");
                                }
                                Some(tokens) => {
                                    let serialized = serde_json::to_string::<(String, Vec<ResultDiff>)>(
                                        &(query_name, results),
                                    ).expect("failed to serialize outputs");
                                    let msg = ws::Message::text(serialized);

                                    for &token in tokens.iter() {
                                        // @TODO check whether connection still exists
                                        let conn = &mut connections[token.into()];

                                        conn.send_message(msg.clone())
                                            .expect("failed to send message");

                                        poll.reregister(
                                            conn.socket(),
                                            conn.token(),
                                            conn.events(),
                                            PollOpt::edge() | PollOpt::oneshot(),
                                        ).unwrap();
                                    }
                                }
                            }
                        }

                        poll.reregister(
                            &recv_results,
                            RESULTS,
                            Ready::readable(),
                            PollOpt::edge() | PollOpt::oneshot(),
                        ).unwrap();
                    }
                    ERRORS => {
                        while let Ok((tokens, mut errors)) = recv_errors.try_recv() {
                            info!("[WORKER {}] sending errors {:?}", worker.index(), errors);

                            let serializable = errors.drain(..).map(|error| {
                                let mut serializable = serde_json::Map::new();
                                serializable.insert("df.error/category".to_string(), serde_json::Value::String(error.category.to_string()));
                                serializable.insert("df.error/message".to_string(), serde_json::Value::String(error.message.to_string()));

                                serializable
                            }).collect();

                            let serialized = serde_json::to_string::<(String, Vec<serde_json::Map<_,_>>)>(
                                &("df.error".to_string(), serializable)
                            ).expect("failed to serialize errors");
                            let msg = ws::Message::text(serialized);

                            for &token in tokens.iter() {
                                // @TODO check whether connection still exists
                                let conn = &mut connections[token.into()];

                                conn.send_message(msg.clone())
                                    .expect("failed to send message");

                                poll.reregister(
                                    conn.socket(),
                                    conn.token(),
                                    conn.events(),
                                    PollOpt::edge() | PollOpt::oneshot(),
                                ).unwrap();
                            }
                        }

                        poll.reregister(
                            &recv_results,
                            ERRORS,
                            Ready::readable(),
                            PollOpt::edge() | PollOpt::oneshot(),
                        ).unwrap();
                    }
                    _ => {
                        let token = event.token();
                        let active = {
                            let readiness = event.readiness();
                            let conn_events = connections[token.into()].events();

                            // @TODO refactor connection to accept a
                            // vector in which to place events and
                            // rename conn_events to avoid name clash

                            if (readiness & conn_events).is_readable() {
                                match connections[token.into()].read() {
                                    Err(err) => {
                                        trace!(
                                            "[WORKER {}] error while reading: {}",
                                            worker.index(),
                                            err
                                        );
                                        // @TODO error handling
                                        connections[token.into()].error(err)
                                    }
                                    Ok(mut conn_events) => {
                                        for conn_event in conn_events.drain(0..) {
                                            match conn_event {
                                                ConnEvent::Message(msg) => {
                                                    match serde_json::from_str::<Vec<Request>>(&msg.into_text().unwrap()) {
                                                        Err(serde_error) => {
                                                            let error = Error {
                                                                category: "df.error.category/incorrect",
                                                                message: serde_error.to_string(),
                                                            };

                                                            send_errors.send((vec![token], vec![error])).unwrap();
                                                        }
                                                        Ok(requests) => {
                                                            let command = Command {
                                                                owner: worker.index(),
                                                                client: token.into(),
                                                                requests,
                                                            };

                                                            trace!("[WORKER {}] {:?}", worker.index(), command);

                                                            sequencer.push(command);
                                                        }
                                                    }
                                                }
                                                _ => {
                                                    println!("other");
                                                }
                                            }
                                        }
                                    }
                                }
                            }

                            let conn_events = connections[token.into()].events();

                            if (readiness & conn_events).is_writable() {
                                if let Err(err) = connections[token.into()].write() {
                                    trace!(
                                        "[WORKER {}] error while writing: {}",
                                        worker.index(),
                                        err
                                    );
                                    // @TODO error handling
                                    connections[token.into()].error(err)
                                }
                            }

                            // connection events may have changed
                            connections[token.into()].events().is_readable()
                                || connections[token.into()].events().is_writable()
                        };

                        // NOTE: Closing state only applies after a ws connection was successfully
                        // established. It's possible that we may go inactive while in a connecting
                        // state if the handshake fails.
                        if !active {
                            if let Ok(addr) = connections[token.into()].socket().peer_addr() {
                                debug!("WebSocket connection to {} disconnected.", addr);
                            } else {
                                trace!("WebSocket connection to token={:?} disconnected.", token);
                            }
                            connections.remove(token.into());
                        } else {
                            let conn = &connections[token.into()];
                            poll.reregister(
                                conn.socket(),
                                conn.token(),
                                conn.events(),
                                PollOpt::edge() | PollOpt::oneshot(),
                            ).unwrap();
                        }
                    }
                }
            }

            // handle commands

            while let Some(mut command) = sequencer.next() {
                info!("[WORKER {}] {:?}", worker.index(), command);

                for req in command.requests.drain(..) {
                    let owner = command.owner;
                    let client = command.client;

                    // @TODO only create a single dataflow, but only if req != Transact

                    match req {
                        Request::Datom(e, a, v, diff, tx) => server.datom(owner, worker.index(), e, a, v, diff, tx),
                        Request::Transact(req) => server.transact(req, owner, worker.index()),
                        Request::Interest(req) => {
                            if owner == worker.index() {
                                // we are the owning worker and thus have to
                                // keep track of this client's new interest

                                let client_token = Token(command.client);
                                server.interests
                                    .entry(req.name.clone())
                                    .or_insert_with(Vec::new)
                                    .push(client_token);
                            }

                            if !server.context.global_arrangements.contains_key(&req.name) {

                                let send_results_handle = send_results.clone();

                                worker.dataflow::<u64, _, _>(|scope| {
                                    let name = req.name.clone();

                                    match server.interest(&req.name, scope) {
                                        Err(error) => {
                                            send_errors.send((vec![Token(client)], vec![error])).unwrap();
                                        }
                                        Ok(trace) => {
                                            trace
                                                .import_named(scope, &req.name)
                                            // @TODO clone entire batches instead of flattening
                                                .as_collection(|tuple,_| tuple.clone())
                                                .inner
                                            // .stream
                                            // .map(|batch| (*batch).clone())
                                                .unary_notify(
                                                    Exchange::new(move |_| owner as u64),
                                                    "ResultsRecv",
                                                    vec![],
                                                    move |input, _output: &mut OutputHandle<_, (), _>, _notificator| {

                                                        // due to the exchange pact, this closure is only
                                                        // executed by the owning worker

                                                        input.for_each(|_time, data| {
                                                            send_results_handle
                                                                .send((name.clone(), data.to_vec()))
                                                                .unwrap();
                                                        });
                                                    })
                                                .probe_with(&mut server.probe);
                                        }
                                    }
                                });
                            }
                        }
                        Request::Register(req) => server.register(req),
                        Request::RegisterSource(req) => {
                            worker.dataflow::<u64, _, _>(|scope| {
                                if let Err(error) = server.register_source(req, scope) {
                                    send_errors.send((vec![Token(client)], vec![error])).unwrap();
                                }
                            });
                        }
                        Request::CreateAttribute(CreateAttribute { name }) => {
                            worker.dataflow::<u64, _, _>(|scope| {
                                if let Err(error) = server.create_attribute(&name, scope) {
                                    send_errors.send((vec![Token(client)], vec![error])).unwrap();
                                }
                            });
                        }
                        Request::AdvanceInput(name, tx) => {
                            if let Err(error) = server.advance_input(name, tx) {
                                send_errors.send((vec![Token(client)], vec![error])).unwrap();
                            }
                        },
                        Request::CloseInput(name) => server.close_input(name),
                    }
                }
            }

            // ensure work continues, even if no queries registered,
            // s.t. the sequencer continues issuing commands
            worker.step();

            worker.step_while(|| server.is_any_outdated());
        }
    }).unwrap(); // asserts error-free execution
}
