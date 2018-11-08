extern crate timely;
extern crate declarative_dataflow;

use std::thread;
use std::time::{Instant};
use std::sync::mpsc::{channel};

use timely::{Configuration};

use declarative_dataflow::{Plan, Rule, Value};
use declarative_dataflow::plan::{Join, Union, Aggregate, AggregationFn};
use declarative_dataflow::sources::{Source, PlainFile};
use declarative_dataflow::server::{Server, Register, RegisterSource};

fn main() {
    timely::execute(Configuration::Process(4), move |worker| {

        let mut server = Server::new(Default::default());
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
                plan: Plan::Aggregate(Aggregate {
                    variables: vec![x, y],
                    key_symbols: vec![x, y],
                    plan: Box::new(Plan::RuleExpr(vec![x, y], "label".to_string())),
                    aggregation_fn: AggregationFn::COUNT
                })
            }
        ];

        let edge_source = Source::PlainFile(PlainFile { path: "../data/labelprop/edges.httpd_df".to_string() });
        let node_source = Source::PlainFile(PlainFile { path: "../data/labelprop/nodes.httpd_df".to_string() });

        worker.dataflow::<usize, _, _>(|mut scope| {
            
            server.register_source(RegisterSource { name: ":edge".to_string(), source: edge_source }, &mut scope);
            server.register_source(RegisterSource { name: ":node".to_string(), source: node_source }, &mut scope);
            
            server.register(Register { rules, publish: vec!["labelprop".to_string()] }, &mut scope);

            server.interest("labelprop".to_string(), &mut scope)
                .inspect(move |x| { send_results.send((x.0.clone(), x.2)).unwrap(); });
        });

        let timer = Instant::now();
        while !server.probe.done() { worker.step(); }

        thread::spawn(move || {
            assert_eq!(results.recv().unwrap(), (vec![Value::Number(9393283)], 1));
            println!("Finished. {:?}", timer.elapsed());
        });
    }).unwrap();
}
