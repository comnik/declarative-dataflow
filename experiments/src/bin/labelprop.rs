#[global_allocator]
static ALLOCATOR: jemallocator::Jemalloc = jemallocator::Jemalloc;

extern crate declarative_dataflow;
extern crate differential_dataflow;
extern crate timely;

use std::sync::mpsc::channel;
use std::time::Instant;

use timely::Configuration;

use differential_dataflow::operators::group::Count;
use differential_dataflow::operators::Consolidate;

use declarative_dataflow::plan::{Aggregate, AggregationFn, Join, Union};
use declarative_dataflow::server::{Register, RegisterSource, Server};
use declarative_dataflow::sources::{CsvFile, Source};
use declarative_dataflow::{Plan, Rule, Value};

fn main() {
    timely::execute(Configuration::Thread, move |worker| {
        let mut server = Server::<u64, u64>::new(Default::default());
        let (send_results, results) = channel();

        let (x, y, z) = (0, 1, 2);
        let rules = vec![
            Rule {
                name: "label".to_string(),
                plan: Plan::Union(Union {
                    variables: vec![x, y],
                    plans: vec![
                        Plan::MatchA(x, ":node".to_string(), y),
                        Plan::Join(Join {
                            variables: vec![z],
                            left_plan: Box::new(Plan::MatchA(z, ":edge".to_string(), y)),
                            right_plan: Box::new(Plan::NameExpr(vec![x, z], "label".to_string())),
                        }),
                    ],
                }),
            },
            Rule {
                name: "labelprop".to_string(),
                plan: Plan::NameExpr(vec![x, y], "label".to_string()),
                // plan: Plan::Aggregate(Aggregate {
                //     variables: vec![x, y],
                //     key_variables: vec![],
                //     plan: Box::new(Plan::NameExpr(vec![x, y], "label".to_string())),
                //     aggregation_fn: AggregationFn::COUNT
                // })
            },
        ];

        let edge_source = RegisterSource {
            names: vec![":edge".to_string()],
            source: Source::CsvFile(CsvFile {
                separator: ' ',
                path: "/Users/niko/data/labelprop/edges.httpd_df".to_string(),
                schema: vec![(1, Value::Eid(0))],
            }),
        };
        let node_source = RegisterSource {
            names: vec![":node".to_string()],
            source: Source::CsvFile(CsvFile {
                separator: ' ',
                path: "/Users/niko/data/labelprop/nodes.httpd_df".to_string(),
                schema: vec![(1, Value::Eid(0))],
            }),
        };

        worker.dataflow::<u64, _, _>(|scope| {
            server.register_source(edge_source, scope);
            server.register_source(node_source, scope);

            server.register(Register {
                rules,
                publish: vec!["labelprop".to_string()],
            });

            server
                .interest("labelprop", scope)
                .import(scope)
                .as_collection(|tuple, _| tuple.clone())
                .map(|_x| ())
                .consolidate()
                .count()
                .probe_with(&mut server.probe)
                .inspect(move |x| {
                    send_results.send((x.0.clone(), x.2)).unwrap();
                });
        });

        let timer = Instant::now();
        while !server.probe.done() {
            worker.step();
        }

        dbg!(results.recv().unwrap());
        // assert_eq!(dbg!(results.recv().unwrap()), (vec![Value::Number(9393283)], 1));
        println!("Finished. {:?}", timer.elapsed());
    })
    .unwrap();
}
