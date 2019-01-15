extern crate timely;
extern crate graph_map;
extern crate differential_dataflow;
extern crate declarative_dataflow;

use graph_map::GraphMMap;

use declarative_dataflow::plan::{Hector, Binding};
use declarative_dataflow::server::{Register, Server, Transact, TxData};
use declarative_dataflow::{Plan, Rule, Value};

fn main() {

    let filename = std::env::args().nth(1).unwrap();
    let batching = std::env::args().nth(2).unwrap().parse::<usize>().unwrap();
    let inspect = std::env::args().any(|x| x == "inspect");

    timely::execute_from_args(std::env::args().skip(2), move |worker| {

        let timer = std::time::Instant::now();
        let graph = GraphMMap::new(&filename);
        let mut server = Server::<u64>::new(Default::default());

        // [?a :edge ?b] [?b :edge ?c] [?a :edge ?c]
        let (a,b,c) = (1,2,3);
        let plan = Plan::Hector(Hector {
            bindings: vec![
                Binding { symbols: (a,b), source_name: "edge".to_string() },
                Binding { symbols: (b,c), source_name: "edge".to_string() },
                Binding { symbols: (a,c), source_name: "edge".to_string() },
            ]
        });

        let peers = worker.peers();
        let index = worker.index();

        worker.dataflow::<u64,_,_>(|scope| {
            server.create_attribute("edge", scope);

            server.register(
                Register {
                    rules: vec![
                        Rule { name: "triangles".to_string(), plan }
                    ],
                    publish: vec!["triangles".to_string()],
                },
                scope,
            );

            server
                .interest("triangles", scope)
                .filter(move |_| inspect)
                .inspect(|x| println!("\tTriangle: {:?}", x));
        });

        let mut index = index;
        while index < graph.nodes() {
            server.transact(
                Transact {
                    tx: Some(index as u64),
                    tx_data: graph.edges(index).iter()
                        .map(|y| TxData(1, index as u64, "edge".to_string(), Value::Eid(*y as u64)))
                        .collect()
                },
                0,
                0,
            );
                        
            index += peers;
            if (index / peers) % batching == 0 {
                worker.step_while(|| server.is_any_outdated());
                println!("{:?}\tRound {} complete", timer.elapsed(), index);
            }
        }

    }).unwrap();
}
