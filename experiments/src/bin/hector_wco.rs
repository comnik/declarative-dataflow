#[global_allocator]
static ALLOCATOR: jemallocator::Jemalloc = jemallocator::Jemalloc;

use graph_map::GraphMMap;

use declarative_dataflow::server::Server;
use declarative_dataflow::plan::{Plan, Join};
use declarative_dataflow::{q, Binding, AttributeConfig, InputSemantics, Rule, TxData, Value};
use Value::Eid;

fn main() {
    let filename = std::env::args().nth(1).unwrap();
    let batching = std::env::args().nth(2).unwrap().parse::<usize>().unwrap();
    let query = std::env::args().nth(3).unwrap().parse::<usize>().unwrap();
    let inspect = std::env::args().any(|x| x == "inspect");

    timely::execute_from_args(std::env::args().skip(3), move |worker| {
        let mut timer = std::time::Instant::now();
        let graph = GraphMMap::new(&filename);
        let mut server = Server::<u64, u64>::new(Default::default());

        // [?a :edge ?b] [?b :edge ?c] [?a :edge ?c]
        let (a, b, c) = (1, 2, 3);
        let plan =
            if query == 0 {
                println!("hector 1");
                q(
                    vec![a, b, c],
                    vec![
                        Binding::attribute(a, "edge", b),
                        Binding::attribute(b, "edge", c),
                        Binding::attribute(a, "edge", c),
                    ],
                )
            } else if query == 1 {
                println!("hector 2");
                q(
                    vec![a, b, c],
                    vec![
                        Binding::attribute(a, "edge", b),
                        Binding::attribute(a, "edge", c),
                        Binding::attribute(b, "edge", c),
                    ],
                )
            } else if query == 2 {
                println!("hector 3");
                q(
                    vec![a, b, c],
                    vec![
                        Binding::attribute(b, "edge", c),
                        Binding::attribute(a, "edge", c),
                        Binding::attribute(a, "edge", b),
                    ],
                )
            } else if query == 3 {
                println!("([?a ?b] [?b ?c]) [?a ?c]");
                Plan::Join(Join {
                    variables: vec![a,c],
                    left_plan: Box::new(Plan::MatchA(a, "edge".to_string(), c)),
                    right_plan: Box::new(Plan::Join(Join {
                        variables: vec![b],
                        left_plan: Box::new(Plan::MatchA(a, "edge".to_string(), b)),
                        right_plan: Box::new(Plan::MatchA(b, "edge".to_string(), c)),
                    }))
                })
            } else if query == 4 {
                println!("([?a ?b] [?a ?c]) [?b ?c]");
                Plan::Join(Join {
                    variables: vec![b, c],
                    left_plan: Box::new(Plan::MatchA(b, "edge".to_string(), c)),
                    right_plan: Box::new(Plan::Join(Join {
                        variables: vec![a],
                        left_plan: Box::new(Plan::MatchA(a, "edge".to_string(), b)),
                        right_plan: Box::new(Plan::MatchA(a, "edge".to_string(), c)),
                    }))
                })
            } else {
                println!("([?a ?c] [?b ?c]) [?a ?b]");
                Plan::Join(Join {
                    variables: vec![a, b],
                    left_plan: Box::new(Plan::MatchA(a, "edge".to_string(), b)),
                    right_plan: Box::new(Plan::Join(Join {
                        variables: vec![c],
                        left_plan: Box::new(Plan::MatchA(a, "edge".to_string(), c)),
                        right_plan: Box::new(Plan::MatchA(b, "edge".to_string(), c)),
                    }))
                })
            };

        let peers = worker.peers();
        let index = worker.index();

        worker.dataflow::<u64, _, _>(|scope| {
            server
                .context
                .internal
                .create_transactable_attribute("edge", AttributeConfig::tx_time(InputSemantics::Raw), scope)
                .unwrap();

            server
                .test_single(
                    scope,
                    Rule {
                        name: "triangles".to_string(),
                        plan,
                    },
                )
                .filter(move |_| inspect)
                .inspect(|x| println!("\tTriangle: {:?}", x));

            server.advance_domain(None, 1).unwrap();
        });

        let mut index = index;
        let mut next_tx = 1;

        while index < graph.nodes() {
            server
                .transact(
                    graph
                        .edges(index)
                        .iter()
                        .map(|y| TxData(1, index as u64, "edge".to_string(), Eid(*y as u64)))
                        .collect(),
                    0,
                    0,
                )
                .unwrap();

            server.advance_domain(None, next_tx).unwrap();
            next_tx += 1;

            index += peers;
            if (index / peers) % batching == 0 {
                worker.step_while(|| server.is_any_outdated());
                println!("{},{}", index, timer.elapsed().as_millis());
                timer = std::time::Instant::now();
            }
        }
    })
    .unwrap();
}
