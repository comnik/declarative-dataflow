extern crate declarative_dataflow;
extern crate timely;

use std::sync::mpsc::channel;

use timely::Configuration;

use declarative_dataflow::plan::{Join, Project};
use declarative_dataflow::server::{Server, Transact, TxData};
use declarative_dataflow::{Plan, Rule, Value};

#[test]
fn match_ea() {
    timely::execute(Configuration::Thread, move |worker| {
        let mut server = Server::<u64>::new(Default::default());
        let (send_results, results) = channel();

        // [:find ?v :where [1 :name ?n]]
        let plan = Plan::MatchEA(1, ":name".to_string(), 1);

        worker.dataflow::<u64, _, _>(|scope| {
            server.create_attribute(":name", scope);

            server
                .test_single(scope, Rule { name: "match_ea".to_string(), plan, })
                .inspect(move |x| {
                    send_results.send((x.0.clone(), x.2)).unwrap();
                });
        });

        server.transact(
            Transact {
                tx: Some(0),
                tx_data: vec![
                    TxData(
                        1,
                        1,
                        ":name".to_string(),
                        Value::String("Dipper".to_string()),
                    ),
                    TxData(
                        1,
                        1,
                        ":name".to_string(),
                        Value::String("Alias".to_string()),
                    ),
                    TxData(
                        1,
                        2,
                        ":name".to_string(),
                        Value::String("Mabel".to_string()),
                    ),
                ],
            },
            0,
            0,
        );

        worker.step_while(|| server.is_any_outdated());

        assert_eq!(results.recv().unwrap(), (vec![Value::String("Alias".to_string())], 1));
        assert_eq!(results.recv().unwrap(), (vec![Value::String("Dipper".to_string())], 1));
    }).unwrap();
}

#[test]
fn join() {
    timely::execute(Configuration::Thread, move |worker| {
        let mut server = Server::<u64>::new(Default::default());
        let (send_results, results) = channel();

        // [:find ?e ?n ?a :where [?e :age ?a] [?e :name ?n]]
        let (e, a, n) = (1, 2, 3);
        let plan = Plan::Project(Project {
            variables: vec![e, n, a],
            plan: Box::new(Plan::Join(Join {
                variables: vec![e],
                left_plan: Box::new(Plan::MatchA(e, ":name".to_string(), n)),
                right_plan: Box::new(Plan::MatchA(e, ":age".to_string(), a)),
            })),
        });

        worker.dataflow::<u64, _, _>(|scope| {
            server.create_attribute(":name", scope);
            server.create_attribute(":age", scope);

            server
                .test_single(scope, Rule { name: "join".to_string(), plan, })
                .inspect(move |x| {
                    send_results.send((x.0.clone(), x.2)).unwrap();
                });
        });

        server.transact(
            Transact {
                tx: Some(0),
                tx_data: vec![
                    TxData(1, 1, ":name".to_string(), Value::String("Dipper".to_string())),
                    TxData(1, 1, ":age".to_string(), Value::Number(12)),
                ],
            },
            0,
            0,
        );

        worker.step_while(|| server.is_any_outdated());

        assert_eq!(results.recv().unwrap(), (vec![Value::Eid(1), Value::String("Dipper".to_string()), Value::Number(12)], 1));
    }).unwrap();
}
