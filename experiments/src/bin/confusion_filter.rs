extern crate timely;
extern crate differential_dataflow;
extern crate declarative_dataflow;

use std::time::{Instant};

use timely::{Configuration};

use differential_dataflow::operators::Count;

use declarative_dataflow::{Plan, Rule, Value};
use declarative_dataflow::plan::{Join};
use declarative_dataflow::sources::{Source, JsonFile};
use declarative_dataflow::server::{Server, Register, RegisterSource};

fn main() {

    let filename = std::env::args().nth(1).unwrap();

    timely::execute(Configuration::Process(1), move |worker| {

        let mut server = Server::new(Default::default());

        let e = 1;
        let rules = vec![
            Rule {
                name: "q1".to_string(),
                plan: Plan::Join(Join {
                    variables: vec![e],
                    left_plan: Box::new(Plan::MatchAV(e, "target".to_string(), Value::String("Russian".to_string()))),
                    right_plan: Box::new(Plan::MatchAV(e, "guess".to_string(), Value::String("Russian".to_string()))),
                })
            }
        ];

        let obj_source = Source::JsonFile(JsonFile { path: filename.clone() });

        worker.dataflow::<u64, _, _>(|mut scope| {
            
            server.register_source(RegisterSource {
                names: vec!["target".to_string(), "guess".to_string()],
                source: obj_source
            }, &mut scope);
            
            server.register(Register { rules, publish: vec!["q1".to_string()] }, &mut scope);

            server.interest("q1", &mut scope)
                .map(|_x| ())
                .count()
                .inspect(|x| println!("RESULT {:?}", x));
        });

        let timer = Instant::now();
        while !server.probe.done() { worker.step(); }

        println!("finished at {:?}", timer.elapsed());
    }).unwrap();
}
