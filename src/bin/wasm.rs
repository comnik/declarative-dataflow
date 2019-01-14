extern crate declarative_dataflow;
extern crate timely;

#[macro_use]
extern crate stdweb;

use timely::communication::allocator::AllocateBuilder;
use timely::communication::allocator::thread::{ThreadBuilder, Thread};

use timely::worker::Worker;
use timely::dataflow::operators::{Operator, Probe};
use timely::dataflow::channels::pact::Pipeline;

use declarative_dataflow::Out;
use declarative_dataflow::server::{Config, Request, Server, CreateInput};

use std::rc::Rc;
use std::cell::RefCell;

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
        };
        let server = Server::<()>::new(config);

        Rc::new(RefCell::new(server))
    };
);

#[js_export]
fn handle(requests: Vec<Request>) {

    let mut requests = requests.clone();
        
    WORKER.with(move |worker_cell| {
        SERVER.with(move |server_cell| {

            let mut worker = worker_cell.borrow_mut();
            let mut server = server_cell.borrow_mut();

            // no notion of owner without multiple workers
            let owner = 0;
            
            for req in requests.drain(..) {
                match req {
                    Request::Datom(e, a, v, diff, tx) => server.datom(owner, WORKER_INDEX, e, a, v, diff, tx),
                    Request::Transact(req) => server.transact(req, owner, WORKER_INDEX),
                    Request::Register(req) => {
                        worker.dataflow::<u64, _, _>(|scope| {
                            server.register(req, scope);
                        });
                    }
                    Request::RegisterSource(req) => { panic!("Not supported") }
                    Request::CreateInput(CreateInput { name }) => {
                        worker.dataflow::<u64, _, _>(|scope| {
                            server.create_input(&name, scope);
                        });
                    }
                    Request::AdvanceInput(name, tx) => server.advance_input(name, tx),
                    Request::CloseInput(name) => server.close_input(name),
                    Request::Interest(req) => {
                        worker.dataflow::<u64, _, _>(|scope| {
                            let name = req.name.clone();

                            server.interest(&req.name, scope)
                                .inner
                                .probe_with(&mut server.probe)
                                .sink(Pipeline, "wasm_out", move |input| {
                                    while let Some((_time, data)) = input.next() {
                                        let out: Vec<Out> = data.iter().cloned()
                                            .map(|(tuple, t, diff)| Out(tuple, t, diff))
                                            .collect();
                                        
                                        js! {
                                            const name = @{name.clone()};
                                            const batch = @{out};
                                            __UGLY_DIFF_HOOK(name, batch);
                                        }
                                    }
                                });
                        });
                    }
                }
            }
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
