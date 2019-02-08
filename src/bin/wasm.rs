#[macro_use]
extern crate stdweb;

use std::cell::RefCell;
use std::rc::Rc;

use timely::communication::allocator::thread::{Thread, ThreadBuilder};
use timely::communication::allocator::AllocateBuilder;

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::{Operator, Probe};
use timely::worker::Worker;

use declarative_dataflow::server::{Config, CreateAttribute, Request, Server};
use declarative_dataflow::ResultDiff;

const WORKER_INDEX: usize = 0;

thread_local!(
    static WORKER: Rc<RefCell<Worker<Thread>>> = {
        let allocator = ThreadBuilder.build();
        let worker = Worker::new(allocator);

        Rc::new(RefCell::new(worker))
    };

    static SERVER: Rc<RefCell<Server<()>>> = {
        let config = Config {
            port: 0,
            enable_cli: false,
            enable_history: false,
            enable_optimizer: false,
            enable_meta: false,
        };
        let server = Server::<()>::new(config);

        Rc::new(RefCell::new(server))
    };

    static NEXT_TX: RefCell<u64> = RefCell::new(0);
);

#[js_export]
fn handle(requests: Vec<Request>) {
    let mut requests = requests.clone();

    NEXT_TX.with(move |next_tx| {
        WORKER.with(move |worker_cell| {
            SERVER.with(move |server_cell| {
                *next_tx.borrow_mut() += 1;

                let mut worker = worker_cell.borrow_mut();
                let mut server = server_cell.borrow_mut();

                // no notion of owner without multiple workers
                let owner = 0;

                for req in requests.drain(..) {
                    match req {
                        Request::Transact(req) => {
                            if let Err(error) = server.transact(req, owner, WORKER_INDEX) {
                                js! { console.error(@{error.message}) }
                            }
                        }
                        Request::Interest(req) => {
                            worker.dataflow::<u64, _, _>(|scope| {
                                let name = req.name.clone();

                                match server.interest(&req.name, scope) {
                                    Err(error) => {
                                        js! { console.error(@{error.message}) }
                                    }
                                    Ok(trace) => {
                                        trace
                                            .import_named(scope, &req.name)
                                            .as_collection(|tuple, _| tuple.clone())
                                            .inner
                                            .probe_with(&mut server.probe)
                                            .sink(Pipeline, "wasm_out", move |input| {
                                                while let Some((_time, data)) = input.next() {
                                                    let out: Vec<ResultDiff> = data
                                                        .iter()
                                                        .cloned()
                                                        .map(|(tuple, t, diff)| {
                                                            ResultDiff(tuple, t, diff)
                                                        })
                                                        .collect();

                                                    js! {
                                                        const name = @{name.clone()};
                                                        const batch = @{out};
                                                        __UGLY_DIFF_HOOK(name, batch);
                                                    }
                                                }
                                            });
                                    }
                                }
                            });
                        }
                        Request::Register(req) => {
                            if let Err(error) = server.register(req) {
                                js! { console.error(@{error.message}) }
                            }
                        }
                        Request::RegisterSource(_req) => unimplemented!(),
                        Request::CreateAttribute(CreateAttribute { name, semantics }) => {
                            worker.dataflow::<u64, _, _>(|scope| {
                                if let Err(error) = server
                                    .context
                                    .internal
                                    .create_attribute(&name, semantics, scope)
                                {
                                    js! { console.error(@{error.message}) }
                                }
                            });
                        }
                        Request::AdvanceDomain(name, next) => {
                            if let Err(error) = server.advance_domain(name, next) {
                                js! { console.error(@{error.message}) }
                            }
                        }
                        Request::CloseInput(name) => {
                            server.context.internal.close_input(name).unwrap()
                        }
                    }
                }

                if let Err(error) = server.advance_domain(None, *next_tx.borrow()) {
                    js! { console.error(@{error.message}) }
                }
            });
        });
    });
}

/// Steps all dataflows and returns true, if more work is remaining.
#[js_export]
fn step() -> bool {
    WORKER.with(|worker_cell| {
        let mut worker = worker_cell.borrow_mut();
        worker.step();
    });

    SERVER.with(|server_cell| server_cell.borrow().is_any_outdated())
}

fn main() {
    // WORKER.with(|worker_cell| {

    //     let mut worker = worker_cell.borrow_mut();

    //     worker.dataflow::<u64, _, _>(|scope| {
    //         let stream = (0..9)
    //             .to_stream(scope)
    //             .map(|x| x + 1)
    //             .sink(Pipeline, "wasm_out", |input| {
    //                 while let Some((time, data)) = input.next() {
    //                     js! {
    //                         var batch = @{data.clone()};
    //                         __UGLY_DIFF_HOOK(batch);
    //                     }
    //                 }
    //             });
    //     });
    // });
}
