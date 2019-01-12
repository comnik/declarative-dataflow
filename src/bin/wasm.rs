extern crate declarative_dataflow;
extern crate timely;

#[macro_use]
extern crate stdweb;

use timely::communication::allocator::AllocateBuilder;
use timely::communication::allocator::thread::{ThreadBuilder, Thread};

use timely::worker::Worker;
use timely::dataflow::operators::{Operator, Map, ToStream};
use timely::dataflow::channels::pact::Pipeline;

use declarative_dataflow::server::{Config, Request, Server, CreateInput};

use std::rc::Rc;
use std::cell::RefCell;

thread_local!(
    static WORKER: Rc<RefCell<Worker<Thread>>> = {
        let allocator = ThreadBuilder.build();
        let worker = Worker::new(allocator);
        
        Rc::new(RefCell::new(worker))
    }
);

#[js_export]
fn step() {
    js! { console.log("running"); }
    WORKER.with(|worker_cell| {
        let mut worker = worker_cell.borrow_mut();
        worker.step();
    })
}

fn main() {

    let server_config = Config {
        port: 0,
        enable_cli: false,
        enable_history: false,
    };

    WORKER.with(|worker_cell| {

        let mut worker = worker_cell.borrow_mut();

        worker.dataflow::<u64, _, _>(|scope| {
            let stream = (0..9)
                .to_stream(scope)
                .map(|x| x + 1)
                .sink(Pipeline, "wasm_out", |input| {
                    while let Some((time, data)) = input.next() {
                        js! {
                            var batch = @{data.clone()};
                            __UGLY_DIFF_HOOK(batch);
                        }
                    }
                });
        });
    });

    js! { alert("test"); }
}
