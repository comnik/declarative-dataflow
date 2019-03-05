#[global_allocator]
static ALLOCATOR: jemallocator::Jemalloc = jemallocator::Jemalloc;

use std::sync::mpsc::channel;
use std::time::Instant;

use timely::Configuration;

use differential_dataflow::operators::{Consolidate, Count};

use declarative_dataflow::plan::{Aggregate, AggregationFn, Join, Union};
use declarative_dataflow::server::{Register, RegisterSource, Server};
use declarative_dataflow::sources::{CsvFile, Source};
use declarative_dataflow::{Plan, Rule, Value};
use Value::Eid;

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
                has_headers: false,
                delimiter: b' ',
                path: "/Users/niko/data/labelprop/edges.httpd_df".to_string(),
                eid_offset: 0,
                timestamp_offset: None,
                flexible: false,
                comment: None,
                schema: vec![(1, Eid(0))],
            }),
        };
        let node_source = RegisterSource {
            names: vec![":node".to_string()],
            source: Source::CsvFile(CsvFile {
                has_headers: false,
                delimiter: b' ',
                path: "/Users/niko/data/labelprop/nodes.httpd_df".to_string(),
                eid_offset: 0,
                timestamp_offset: None,
                flexible: false,
                comment: None,
                schema: vec![(1, Eid(0))],
            }),
        };

        worker.dataflow::<u64, _, _>(|scope| {
            server.register_source(edge_source, scope).unwrap();
            server.register_source(node_source, scope).unwrap();

            server
                .register(Register {
                    rules,
                    publish: vec!["labelprop".to_string()],
                })
                .unwrap();

            match server.interest("labelprop", scope) {
                Err(error) => panic!(error),
                Ok(relation) => {
                    relation
                        .map(|_x| ())
                        .consolidate()
                        .count()
                        .probe_with(&mut server.probe)
                        .inspect(move |x| {
                            send_results.send((x.0.clone(), x.2)).unwrap();
                        });
                }
            }
        });

        let timer = Instant::now();
        worker.step_while(|| !server.probe.done());

        assert_eq!(results.recv().unwrap(), (((), 9393283), 1));
        // assert_eq!(dbg!(results.recv().unwrap()), (vec![Value::Number(9393283)], 1));
        println!("Finished. {:?}", timer.elapsed());
    })
    .unwrap();
}
