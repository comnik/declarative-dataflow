extern crate declarative_dataflow;
extern crate timely;

use std::time::Duration;
use std::collections::HashSet;
use std::sync::mpsc::channel;

use timely::Configuration;

use declarative_dataflow::plan::PullLevel;
use declarative_dataflow::server::{Server, Transact, TxData};
use declarative_dataflow::{Plan, Rule, Value};

#[test]
fn pull_level() {
    timely::execute(Configuration::Thread, |worker| {
        let mut server = Server::<u64>::new(Default::default());
        let (send_results, results) = channel();

        let (e,) = (1,);
        let plan = Plan::PullLevel(PullLevel {
            variables: vec![],
            plan: Box::new(Plan::MatchAV(e.clone(), "admin?".to_string(), Value::Bool(false))),
            attributes: vec!["name".to_string(), "age".to_string()],
        });

        worker.dataflow::<u64, _, _>(|scope| {
            server.create_input("admin?", scope);
            server.create_input("name", scope);
            server.create_input("age", scope);

            server
                .test_single(scope, Rule { name: "pull_level".to_string(), plan: plan, })
                .inspect(move |x| { send_results.send((x.0.clone(), x.2)).unwrap(); });
        });

        server.transact(
            Transact {
                tx: Some(0),
                tx_data: vec![
                    TxData(1, 100, "admin?".to_string(), Value::Bool(true)),
                    TxData(1, 200, "admin?".to_string(), Value::Bool(false)),
                    TxData(1, 300, "admin?".to_string(), Value::Bool(false)),
                    TxData(1, 100, "name".to_string(), Value::String("Mabel".to_string())),
                    TxData(1, 200, "name".to_string(), Value::String("Dipper".to_string())),
                    TxData(1, 300, "name".to_string(), Value::String("Soos".to_string())),
                    TxData(1, 100, "age".to_string(), Value::Number(12)),
                    TxData(1, 200, "age".to_string(), Value::Number(13)),
                ],
            },
            0,
            0,
        );

        worker.step_while(|| server.is_any_outdated());

        let mut expected = HashSet::new();
        expected.insert((vec![Value::Eid(200), Value::Attribute("age".to_string()), Value::Number(13)], 1));
        expected.insert((vec![Value::Eid(200), Value::Attribute("name".to_string()), Value::String("Dipper".to_string())], 1));
        expected.insert((vec![Value::Eid(300), Value::Attribute("name".to_string()), Value::String("Soos".to_string())], 1));

        for _i in 0..expected.len() {
            let result = results.recv().unwrap();
            if !expected.remove(&result) { panic!("unknown result {:?}", result); }
        }

        assert!(results.recv_timeout(Duration::from_millis(400)).is_err());
    }).unwrap();
}

#[test]
fn pull_children() {
    timely::execute(Configuration::Thread, |worker| {
        let mut server = Server::<u64>::new(Default::default());
        let (send_results, results) = channel();

        let (parent, child,) = (1, 2,);
        let plan = Plan::PullLevel(PullLevel {
            variables: vec![],
            plan: Box::new(Plan::MatchA(parent, "parent/child".to_string(), child)),
            attributes: vec!["name".to_string(), "age".to_string()],
        });
        
        worker.dataflow::<u64, _, _>(|scope| {
            server.create_input("parent/child", scope);
            server.create_input("name", scope);
            server.create_input("age", scope);

            server
                .test_single(scope, Rule { name: "pull_children".to_string(), plan: plan, })
                .inspect(move |x| { send_results.send((x.0.clone(), x.2)).unwrap(); });
        });

        server.transact(
            Transact {
                tx: Some(0),
                tx_data: vec![
                    TxData(1, 100, "name".to_string(), Value::String("Alice".to_string())),
                    TxData(1, 100, "parent/child".to_string(), Value::Eid(300)),
                    TxData(1, 200, "name".to_string(), Value::String("Bob".to_string())),
                    TxData(1, 200, "parent/child".to_string(), Value::Eid(400)),
                    TxData(1, 300, "name".to_string(), Value::String("Mabel".to_string())),
                    TxData(1, 300, "age".to_string(), Value::Number(13)),
                    TxData(1, 400, "name".to_string(), Value::String("Dipper".to_string())),
                    TxData(1, 400, "age".to_string(), Value::Number(12)),
                ],
            },
            0,
            0,
        );

        worker.step_while(|| server.is_any_outdated());

        let mut expected = HashSet::new();
        expected.insert((vec![Value::Eid(100), Value::Eid(300), Value::Attribute("age".to_string()), Value::Number(13)], 1));
        expected.insert((vec![Value::Eid(100), Value::Eid(300), Value::Attribute("name".to_string()), Value::String("Mabel".to_string())], 1));
        expected.insert((vec![Value::Eid(200), Value::Eid(400), Value::Attribute("age".to_string()), Value::Number(12)], 1));
        expected.insert((vec![Value::Eid(200), Value::Eid(400), Value::Attribute("name".to_string()), Value::String("Dipper".to_string())], 1));

        for _i in 0..expected.len() {
            let result = results.recv().unwrap();
            if !expected.remove(&result) { panic!("unknown result {:?}", result); }
        }

        assert!(results.recv_timeout(Duration::from_millis(400)).is_err());
    }).unwrap();
}
