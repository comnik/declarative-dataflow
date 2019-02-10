extern crate declarative_dataflow;
extern crate differential_dataflow;
extern crate timely;

use std::time::Instant;

use timely::Configuration;

use differential_dataflow::operators::Count;

use declarative_dataflow::plan::{Aggregate, AggregationFn, Join};
use declarative_dataflow::server::{Register, RegisterSource, Server};
use declarative_dataflow::sources::{JsonFile, Source};
use declarative_dataflow::{Plan, Rule};

fn main() {
    let filename = std::env::args().nth(1).unwrap();

    timely::execute(Configuration::Process(1), move |worker| {
        let mut server = Server::<u64, u64>::new(Default::default());

        let (e, country, target, count) = (1, 2, 3, 4);
        let rules = vec![Rule {
            name: "q2".to_string(),
            plan: Plan::Aggregate(Aggregate {
                variables: vec![country, target, count],
                plan: Box::new(Plan::Join(Join {
                    variables: vec![e],
                    left_plan: Box::new(Plan::MatchA(e, "country".to_string(), country)),
                    right_plan: Box::new(Plan::MatchA(e, "target".to_string(), target)),
                })),
                aggregation_fns: vec![AggregationFn::COUNT],
                key_symbols: vec![country, target],
                with_symbols: vec![],
            }),
        }];

        let obj_source = Source::JsonFile(JsonFile {
            path: filename.clone(),
        });

        worker.dataflow::<u64, _, _>(|scope| {
            server.register_source(
                RegisterSource {
                    names: vec![
                        "country".to_string(),
                        "target".to_string(),
                        "guess".to_string(),
                    ],
                    source: obj_source,
                },
                scope,
            );

            server.register(
                Register {
                    rules,
                    publish: vec!["q2".to_string()],
                },
                scope,
            );

            server
                .interest("q2", scope)
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
