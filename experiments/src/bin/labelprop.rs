extern crate timely;
extern crate differential_dataflow;
extern crate declarative_dataflow;

use std::thread;
use std::time::{Instant};
use std::sync::mpsc::{channel};

use timely::{Configuration};

use differential_dataflow::operators::group::Count;

use declarative_dataflow::{Plan, Rule, Value};
use declarative_dataflow::plan::{Join, Union, Aggregate, AggregationFn};
use declarative_dataflow::sources::{Source, CsvFile};
use declarative_dataflow::server::{Server, Register, RegisterSource};

fn main() {
    timely::execute(Configuration::Thread, move |worker| {

        let mut server = Server::<u64>::new(Default::default());
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
                            right_plan: Box::new(Plan::RuleExpr(vec![x, z], "label".to_string()))
                        })
                    ]
                })
            },
            Rule {
                name: "labelprop".to_string(),
                plan: Plan::RuleExpr(vec![x, y], "label".to_string()),
                // plan: Plan::Aggregate(Aggregate {
                //     variables: vec![x, y],
                //     key_symbols: vec![],
                //     plan: Box::new(Plan::RuleExpr(vec![x, y], "label".to_string())),
                //     aggregation_fn: AggregationFn::COUNT
                // })
            }
        ];

        let edge_source = RegisterSource {
            names: vec![":edge".to_string()],
            source: Source::CsvFile(CsvFile { path: "../data/labelprop/edges.httpd_df".to_string() })
        };
        let node_source = RegisterSource {
            names: vec![":node".to_string()],
            source: Source::CsvFile(CsvFile { path: "../data/labelprop/nodes.httpd_df".to_string() })
        };

        worker.dataflow::<u64, _, _>(|mut scope| {
            
            server.register_source(edge_source, &mut scope);
            server.register_source(node_source, &mut scope);
            
            server.register(Register { rules, publish: vec!["labelprop".to_string()] }, &mut scope);

            server.interest("labelprop", &mut scope)
                .inspect(move |x| { send_results.send((x.0.clone(), x.2)).unwrap(); });
        });

        let timer = Instant::now();
        while !server.probe.done() { worker.step(); }

        thread::spawn(move || {
            // assert_eq!(results.recv().unwrap(), (vec![Value::Number(9393283)], 1));
            println!("Finished. {:?}", timer.elapsed());
        });
    }).unwrap();
}
