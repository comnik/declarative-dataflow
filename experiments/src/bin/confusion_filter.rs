extern crate declarative_dataflow;
extern crate differential_dataflow;
extern crate timely;

use std::time::Instant;

use timely::Configuration;

use differential_dataflow::operators::Count;

use declarative_dataflow::plan::Join;
use declarative_dataflow::server::{Register, RegisterSource, Server};
use declarative_dataflow::sources::{JsonFile, Source};
use declarative_dataflow::{Plan, Rule, Value};

fn main() {
    let filename = std::env::args().nth(1).unwrap();

    timely::execute(Configuration::Process(1), move |worker| {
        let mut server = Server::<u64, u64>::new(Default::default());

        let e = 1;
        let rules = vec![Rule {
            name: "q1".to_string(),
            plan: Plan::Join(Join {
                variables: vec![e],
                left_plan: Box::new(Plan::MatchAV(
                    e,
                    "target".to_string(),
                    Value::String("Russian".to_string()),
                )),
                right_plan: Box::new(Plan::MatchAV(
                    e,
                    "guess".to_string(),
                    Value::String("Russian".to_string()),
                )),
            }),
        }];

        let obj_source = Source::JsonFile(JsonFile {
            path: filename.clone(),
        });

        worker.dataflow::<u64, _, _>(|scope| {
            server.register_source(
                RegisterSource {
                    names: vec!["target".to_string(), "guess".to_string()],
                    source: obj_source,
                },
                scope,
            );

            server.register(
                Register {
                    rules,
                    publish: vec!["q1".to_string()],
                },
                scope,
            );

            server
                .interest("q1", scope)
                .map(|_x| ())
                .count()
                .inspect(|x| println!("RESULT {:?}", x));
        });

        let timer = Instant::now();
        while !server.probe.done() {
            worker.step();
        }

        println!("finished at {:?}", timer.elapsed());
    })
    .unwrap();
}
