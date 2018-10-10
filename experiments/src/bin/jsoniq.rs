extern crate timely;
extern crate declarative_dataflow;

use std::thread;
use std::time::{Instant};
use std::sync::mpsc::{channel};

use timely::{Configuration};

use declarative_dataflow::{Plan, Rule, Value};
use declarative_dataflow::plan::{Filter, Join, Union, Aggregate, AggregationFn, Predicate};
use declarative_dataflow::sources::{Source, JsonFile};
use declarative_dataflow::server::{Server, Interest, Register, RegisterSource};

fn main() {
    timely::execute(Configuration::Process(1), move |worker| {

        let mut server = Server::new(Default::default());
        let (send_results, results) = channel();

        // [:find ?e :where [?e1 target ?x] [?e2 guess ?x]]
        let (e, target, guess) = (1, 2, 3);
        let rules = vec![
            Rule {
                name: "q1".to_string(),
                plan: Plan::Join(Join {
                    variables: vec![e],
                    left_plan: Box::new(Plan::MatchAV(e, "target".to_string(), Value::String("russian".to_string()))),
                    right_plan: Box::new(Plan::MatchAV(e, "guess".to_string(), Value::String("russian".to_string()))),
                })
            }
        ];

        let obj_source = Source::JsonFile(JsonFile { path: "../data/jsoniq/test.json".to_string() });

        worker.dataflow::<usize, _, _>(|mut scope| {
            
            server.register_source(RegisterSource {
                names: vec!["target".to_string(), "guess".to_string()],
                source: obj_source
            }, &mut scope);
            
            server.register(Register { rules, publish: vec!["q1".to_string()] }, &mut scope);

            server.interest(Interest { name: "q1".to_string() }, &mut scope)
                .inspect(move |x| { send_results.send((x.0.clone(), x.2)).unwrap(); });
        });

        let timer = Instant::now();
        while !server.probe.done() { worker.step(); }

        thread::spawn(move || {
            while let Ok(result) = results.recv() {
                println!("{:?}", result);
            }
            // assert_eq!(results.recv().unwrap(), (vec![Value::Number(9393283)], 1));
            // println!("Finished. {:?}", timer.elapsed());
        });
    }).unwrap();
}
