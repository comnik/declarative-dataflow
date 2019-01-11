extern crate declarative_dataflow;
extern crate timely;

#[macro_use]
extern crate stdweb;

use timely::communication::allocator::AllocateBuilder;
use timely::communication::allocator::thread::ThreadBuilder;

use timely::worker::Worker;
use timely::dataflow::operators::{Operator, Map, ToStream};
use timely::dataflow::channels::pact::Pipeline;

use declarative_dataflow::server::{Config, Request, Server, CreateInput};

fn main() {

    let server_config = Config {
        port: 0,
        enable_cli: false,
        enable_history: false,
    };

    let allocator = ThreadBuilder.build();
    let mut worker = Worker::new(allocator);

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

    // loop {
        // js! { console.log("running"); }
        worker.step();
    // }

    js! { alert("test"); }
}
