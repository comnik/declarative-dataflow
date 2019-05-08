use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::time::Duration;

use slab::Slab;

use mio::net::TcpListener;
pub use mio::Token;
use mio::*;
use mio_extras::channel;

use ws::connection::{ConnEvent, Connection};

use declarative_dataflow::server::Request;
use declarative_dataflow::{Error, Output};

const SERVER: Token = Token(std::usize::MAX - 1);
const RESULTS: Token = Token(std::usize::MAX - 2);
pub const SYSTEM: Token = Token(std::usize::MAX - 3);

/// A high-level event devoid of I/O details.
pub enum DomainEvent {
    /// A client sent one or more requests.
    Requests(Token, Vec<Request>),
    /// A client has went away.
    Disconnect(Token),
}

use DomainEvent::*;

/// State for translating low-level I/O events into domain events.
pub struct IO {
    // Event loop.
    poll: Poll,
    // Buffer space for I/O events.
    events: Events,
    // Buffer space for connection events.
    conn_events: Vec<ConnEvent>,
    // Queue of resulting domain events.
    domain_events: VecDeque<DomainEvent>,
    /// Input handle to internal channel.
    pub send: channel::Sender<Output>,
    /// Receive handle to internal channel.
    pub recv: channel::Receiver<Output>,
    // TCP server socket.
    server_socket: TcpListener,
    // Client connections.
    connections: Slab<Connection>,
    next_connection_id: u32,
    // WebSocket settings.
    ws_settings: ws::Settings,
}

impl IO {
    pub fn new(address: SocketAddr) -> Self {
        let poll = Poll::new().expect("failed to setup event loop");

        let (send, recv) = channel::channel::<Output>();

        let server_socket = TcpListener::bind(&address).expect("failed to create server socket");

        poll.register(
            &recv,
            RESULTS,
            Ready::readable(),
            PollOpt::edge() | PollOpt::oneshot(),
        )
        .expect("failed to register result channel");

        poll.register(&server_socket, SERVER, Ready::readable(), PollOpt::level())
            .expect("failed to register server socket");

        let ws_settings = ws::Settings {
            max_connections: 1024,
            ..ws::Settings::default()
        };

        IO {
            poll,
            events: Events::with_capacity(ws_settings.max_connections),
            conn_events: Vec::new(),
            domain_events: VecDeque::new(),
            send,
            recv,
            server_socket,
            connections: Slab::with_capacity(ws_settings.max_connections),
            next_connection_id: 0,
            ws_settings,
        }
    }

    /// Handle networking events.
    pub fn step(&mut self, t: u64, interests: &HashMap<String, HashSet<Token>>) {
        // We mustn't timeout here, we are not in charge of blocking.
        self.poll
            .poll(&mut self.events, Some(Duration::from_millis(0)))
            .expect("failed to poll I/O events");

        for event in self.events.iter() {
            trace!("[IO] recv event on {:?}", event.token());

            match event.token() {
                SERVER => {
                    if event.readiness().is_readable() {
                        // new connection arrived on the server socket
                        match self.server_socket.accept() {
                            Err(err) => error!("[IO] error while accepting connection {:?}", err),
                            Ok((socket, addr)) => {
                                let token = {
                                    let entry = self.connections.vacant_entry();
                                    let token = Token(entry.key());
                                    let connection_id = self.next_connection_id;

                                    self.next_connection_id =
                                        self.next_connection_id.wrapping_add(1);

                                    entry.insert(Connection::new(
                                        token,
                                        socket,
                                        self.ws_settings,
                                        connection_id,
                                    ));

                                    token
                                };

                                info!("[IO] new tcp connection from {} (token {:?})", addr, token);

                                let conn = &mut self.connections[token.into()];

                                conn.as_server().unwrap();

                                self.poll
                                    .register(
                                        conn.socket(),
                                        conn.token(),
                                        conn.events(),
                                        PollOpt::edge() | PollOpt::oneshot(),
                                    )
                                    .unwrap();
                            }
                        }
                    }
                }
                RESULTS => {
                    while let Ok(out) = self.recv.try_recv() {
                        let tokens: Box<dyn Iterator<Item = Token>> = match &out {
                            &Output::QueryDiff(ref name, ref results) => {
                                info!("[IO] {} {} results", name, results.len());

                                match interests.get(name) {
                                    None => {
                                        warn!("result on query {} w/o interested clients", name);
                                        Box::new(std::iter::empty())
                                    }
                                    Some(tokens) => Box::new(tokens.iter().cloned()),
                                }
                            }
                            &Output::Json(ref name, _, _, _) => {
                                info!("[IO] json on query {}", name);

                                match interests.get(name) {
                                    None => {
                                        warn!("result on query {} w/o interested clients", name);
                                        Box::new(std::iter::empty())
                                    }
                                    Some(tokens) => Box::new(tokens.iter().cloned()),
                                }
                            }
                            &Output::Message(client, ref msg) => {
                                info!("[IO] {:?}", msg);
                                Box::new(std::iter::once(client.into()))
                            }
                            &Output::Error(client, ref error, _) => {
                                error!("[IO] {:?}", error);
                                Box::new(std::iter::once(client.into()))
                            }
                        };

                        let serialized = serde_json::to_string::<Output>(&out)
                            .expect("failed to serialize output");

                        let msg = ws::Message::text(serialized);

                        for token in tokens {
                            match self.connections.get_mut(token.into()) {
                                None => {
                                    // @TODO we need to clean up the connection here
                                    warn!("client {:?} has gone away undetected", token);
                                    self.domain_events.push_back(Disconnect(token));
                                }
                                Some(conn) => {
                                    conn.send_message(msg.clone())
                                        .expect("failed to send message");

                                    self.poll
                                        .reregister(
                                            conn.socket(),
                                            conn.token(),
                                            conn.events(),
                                            PollOpt::edge() | PollOpt::oneshot(),
                                        )
                                        .unwrap();
                                }
                            }
                        }
                    }

                    self.poll
                        .reregister(
                            &self.recv,
                            RESULTS,
                            Ready::readable(),
                            PollOpt::edge() | PollOpt::oneshot(),
                        )
                        .unwrap();
                }
                _ => {
                    let token = event.token();
                    let active = {
                        let event_readiness = event.readiness();

                        let conn_readiness = self.connections[token.into()].events();
                        if (event_readiness & conn_readiness).is_readable() {
                            if let Err(err) =
                                self.connections[token.into()].read(&mut self.conn_events)
                            {
                                trace!("[IO] error while reading: {}", err);
                                // @TODO error handling
                                self.connections[token.into()].error(err)
                            }
                        }

                        let conn_readiness = self.connections[token.into()].events();
                        if (event_readiness & conn_readiness).is_writable() {
                            if let Err(err) =
                                self.connections[token.into()].write(&mut self.conn_events)
                            {
                                trace!("[IO] error while writing: {}", err);
                                // @TODO error handling
                                self.connections[token.into()].error(err)
                            }
                        }

                        trace!("read {} connection events", self.conn_events.len());

                        for conn_event in self.conn_events.drain(..) {
                            match conn_event {
                                ConnEvent::Message(msg) => {
                                    trace!("[WS] ConnEvent::Message");
                                    match msg {
                                        ws::Message::Text(string) => {
                                            match serde_json::from_str::<Vec<Request>>(&string) {
                                                Err(serde_error) => {
                                                    self.send
                                                        .send(Output::Error(
                                                            token.into(),
                                                            Error::incorrect(serde_error),
                                                            t,
                                                        ))
                                                        .unwrap();
                                                }
                                                Ok(requests) => {
                                                    self.domain_events
                                                        .push_back(Requests(token, requests));
                                                }
                                            }
                                        }
                                        ws::Message::Binary(_) => unimplemented!(),
                                    }
                                }
                                ConnEvent::Close(code, reason) => {
                                    trace!("[WS] ConnEvent::Close");
                                    info!(
                                        "[IO] connection closing (token {:?}, {:?}, {})",
                                        token, code, reason
                                    );
                                }
                                other => {
                                    trace!("[WS] {:?}", other);
                                }
                            }
                        }

                        // connection events may have changed
                        self.connections[token.into()].events().is_readable()
                            || self.connections[token.into()].events().is_writable()
                    };

                    // NOTE: Closing state only applies after a ws connection was successfully
                    // established. It's possible that we may go inactive while in a connecting
                    // state if the handshake fails.
                    if !active {
                        self.domain_events.push_back(Disconnect(token.clone()));
                        self.connections.remove(token.into());
                    } else {
                        let conn = &self.connections[token.into()];
                        self.poll
                            .reregister(
                                conn.socket(),
                                conn.token(),
                                conn.events(),
                                PollOpt::edge() | PollOpt::oneshot(),
                            )
                            .unwrap();
                    }
                }
            }
        }
    }
}

impl Iterator for IO {
    type Item = DomainEvent;
    fn next(&mut self) -> Option<DomainEvent> {
        self.domain_events.pop_front()
    }
}
