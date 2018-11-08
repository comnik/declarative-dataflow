extern crate declarative_dataflow;
extern crate timely;

use std::sync::mpsc::channel;
use std::thread;

use timely::Configuration;

use declarative_dataflow::plan::{Join, Project};
use declarative_dataflow::server::{Register, Server, Transact, TxData};
use declarative_dataflow::{Plan, Rule, Value};

#[test]
fn match_ea_after_input() {
    timely::execute(Configuration::Thread, move |worker| {
        let mut server = Server::new(Default::default());
        let (send_results, results) = channel();

        // [:find ?v :where [1 :name ?n]]
        let plan = Plan::MatchEA(1, ":name".to_string(), 1);

        worker.dataflow::<u64, _, _>(|mut scope| {
            server.create_input(":name".to_string(), &mut scope);
        });

        let tx_data = vec![
            TxData(1, 1, ":name".to_string(), Value::String("Dipper".to_string())),
            TxData(1, 1, ":name".to_string(), Value::String("Alias".to_string())),
            TxData(1, 2, ":name".to_string(), Value::String("Mabel".to_string())),
        ];
        let tx0 = Transact { tx: Some(0), tx_data };
        server.transact(tx0, 0, 0);

        worker.step_while(|| server.is_any_outdated());
        
        worker.dataflow::<u64, _, _>(|mut scope| {
            let query_name = "match_ea";
            server.register(
                Register {
                    rules: vec![Rule {
                        name: query_name.to_string(),
                        plan: plan,
                    }],
                    publish: vec![query_name.to_string()],
                },
                scope,
            );

            server
                .interest(query_name.to_string(), &mut scope)
                .inspect(move |x| {
                    send_results.send((x.0.clone(), x.2)).unwrap();
                });
        });

        worker.step_while(|| server.is_any_outdated());

        thread::spawn(move || {
            assert_eq!(
                results.recv().unwrap(),
                (vec![Value::String("Alias".to_string())], 1)
            );
            assert_eq!(
                results.recv().unwrap(),
                (vec![Value::String("Dipper".to_string())], 1)
            );
        }).join().unwrap();
    }).unwrap();
}

#[test]
fn join_after_input() {
    timely::execute(Configuration::Thread, move |worker| {
        let mut server = Server::new(Default::default());
        let (send_results, results) = channel();

        worker.dataflow::<u64, _, _>(|mut scope| {
            server.create_input(":transfer/from".to_string(), &mut scope);
            server.create_input(":user/id".to_string(), &mut scope);
        });

        worker.step_while(|| server.is_any_outdated());

        {
            let tx_data = vec![
                TxData(1, 1, ":user/id".to_string(), Value::String("123-456-789".to_string())),
            ];
            let tx0 = Transact { tx: None, tx_data };
            server.transact(tx0, 0, 0);

            worker.step_while(|| server.is_any_outdated());
        }

        {
            let tx_data = vec![
                TxData(1, 101, ":transfer/from".to_string(), Value::String("123-456-789".to_string())),
            ];
            let tx1 = Transact { tx: None, tx_data };
            server.transact(tx1, 0, 0);

            worker.step_while(|| server.is_any_outdated());
        }

        worker.dataflow::<u64, _, _>(|mut scope| {

            let (transfer, sender, uuid) = (1, 2, 3);
            let plan = Plan::Project(Project {
                variables: vec![transfer, sender],
                plan: Box::new(Plan::Join(Join {
                    variables: vec![uuid],
                    left_plan: Box::new(Plan::MatchA(transfer, ":transfer/from".to_string(), uuid)),
                    right_plan: Box::new(Plan::MatchA(sender, ":user/id".to_string(), uuid)),
                })),
            });

            let query_name = "join";
            server.register(
                Register {
                    rules: vec![Rule {
                        name: query_name.to_string(),
                        plan: plan,
                    }],
                    publish: vec![query_name.to_string()],
                },
                &mut scope,
            );

            server
                .interest(query_name.to_string(), &mut scope)
                .inspect(move |x| {
                    send_results.send((x.0.clone(), x.2)).unwrap();
                });
        });

        worker.step_while(|| server.is_any_outdated());

        thread::spawn(move || {
            assert_eq!(
                results.recv().unwrap(),
                (vec![Value::Eid(101), Value::Eid(1)], 1)
            );
        }).join().unwrap();
    }).unwrap();
}
