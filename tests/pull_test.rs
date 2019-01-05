extern crate declarative_dataflow;
extern crate timely;

use std::time::Duration;
use std::collections::HashSet;
use std::sync::mpsc::channel;

use timely::Configuration;

use declarative_dataflow::plan::PullLevel;
use declarative_dataflow::server::{Register, Server, Transact, TxData};
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
            eid_sym: e.clone(),
            attributes: vec!["name".to_string(), "age".to_string()],
        });

        worker.dataflow::<u64, _, _>(|scope| {
            server.create_input("admin?", scope);
            server.create_input("name", scope);
            server.create_input("age", scope);

            let query_name = "pull_level";
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
                .interest(query_name, scope)
                .inspect(move |x| {
                    send_results.send((x.0.clone(), x.2)).unwrap();
                });
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
