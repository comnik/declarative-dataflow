//! A dataflow operator handling server requests.

extern crate timely;

use std::hash::Hash;
use std::rc::Rc;
use std::cell::RefCell;

use timely::dataflow::{Scope, Stream};
use timely::dataflow::scopes::{Child, ScopeParent};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Operator;

use server::{Server, Request, CreateInput};

use super::{Command, Result};

type Owner = u64;
type Query = String;

/// Extension trait for `Stream`.
pub trait Handler<S: Scope<Timestamp = u64>,
                  Token: Hash+From<usize>> {
    /// Consumes each command on the stream, updates the sever state
    /// accordingly and outputs results to send to clients.
    fn handler(&self,
               scope: Rc<RefCell<S>>,
               server: Rc<RefCell<Server<Token>>>) -> Stream<S, (Owner, Query, Result)>;
}

impl<S: Scope<Timestamp = u64>,
     Token: Hash+From<usize>+'static> Handler<S, Token> for Stream<S, (usize, Command)> {

    fn handler(&self,
               scope: Rc<RefCell<S>>,
               server: Rc<RefCell<Server<Token>>>) -> Stream<S, (Owner, Query, Result)> {

        let worker_index = self.scope().index();
        
        self.unary_frontier(Pipeline, "Handler", move |_,_| {

            let mut recvd = Vec::new();
            let mut vector = Vec::new();

            move |input, output| {

                let mut region = scope.borrow_mut();
                let mut server_cell = server.borrow_mut();
                
                // This is taken directly from Timely's Sequencer
                // abstraction. We must be careful to construct dataflows
                // deterministically and in the same order across the
                // cluster.

                // Grab each command and queue it up.
                input.for_each(|time, data| {
                    data.swap(&mut vector);
                    for (_worker, element) in vector.drain(..) {
                        recvd.push((time.time().clone(), element));
                    }
                });

                recvd.sort();

                // Determine how many (which) elements to read from `recvd`.
                let count = recvd.iter().filter(|&(ref time, _)| !input.frontier().less_equal(time)).count();
                let commands = recvd.drain(..count);

                trace!("[WORKER {}] handling sequencer commands", worker_index);
                info!("[WORKER {}] {:?}", worker_index, commands);
                
                for (_, command) in commands {

                    match serde_json::from_str::<Vec<Request>>(&command.cmd) {
                        Err(msg) => panic!("failed to parse command: {:?}", msg),
                        Ok(mut requests) => {
                            for req in requests.drain(..) {

                                let owner = command.owner.clone();

                                match req {
                                    Request::Datom(e, a, v, diff, tx) => server_cell.datom(owner, worker_index, e, a, v, diff, tx),
                                    Request::Transact(req) => server_cell.transact(req, owner, worker_index),
                                    Request::Interest(req) => {
                                        if owner == worker_index {
                                            // we are the owning worker and thus have to
                                            // keep track of this client's new interest

                                            match command.client {
                                                None => {}
                                                Some(client) => {
                                                    let client_token = Token::from(client);
                                                    server_cell.interests
                                                        .entry(req.name.clone())
                                                        .or_insert(Vec::new())
                                                        .push(client_token);
                                                }
                                            };
                                        }
                                    },
                                    // Request::Register(req) => server_cell.register(req, &mut region),
                                    // Request::RegisterSource(req) => server_cell.register_source(req, &mut region),
                                    // Request::CreateInput(CreateInput { name }) => server_cell.create_input(&name, &mut region),
                                    Request::AdvanceInput(name, tx) => server_cell.advance_input(name, tx),
                                    Request::CloseInput(name) => server_cell.close_input(name),
                                    _ => panic!("")
                                }
                            }
                        }
                    }
                }
            }
        })
    }
}
