extern crate declarative_dataflow;
extern crate timely;

use std::rc::Rc;
use std::cell::RefCell;
use std::time::Duration;
use std::collections::{HashSet, HashMap};
use std::sync::mpsc::channel;

use timely::Configuration;

use declarative_dataflow::plan::{Pull, PullLevel};
use declarative_dataflow::server::{Server, Transact, TxData};
use declarative_dataflow::{Plan, Rule, Value, Entity};

#[test]
fn pull_level() {
    timely::execute(Configuration::Thread, |worker| {
        let mut server = Server::<u64>::new(Default::default());
        let (send_results, results) = channel();

        let (e,) = (1,);
        let plan = Plan::PullLevel(PullLevel {
            variables: vec![],
            plan: Box::new(Plan::MatchAV(e.clone(), "admin?".to_string(), Value::Bool(false))),
            pull_attributes: vec!["name".to_string(), "age".to_string()],
            path_attributes: vec![],
        });

        worker.dataflow::<u64, _, _>(|scope| {
            server.create_input("admin?", scope);
            server.create_input("name", scope);
            server.create_input("age", scope);

            server
                .test_single(scope, Rule { name: "pull_level".to_string(), plan })
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
            pull_attributes: vec!["name".to_string(), "age".to_string()],
            path_attributes: vec!["parent/child".to_string()],
        });
        
        worker.dataflow::<u64, _, _>(|scope| {
            server.create_input("parent/child", scope);
            server.create_input("name", scope);
            server.create_input("age", scope);

            server
                .test_single(scope, Rule { name: "pull_children".to_string(), plan })
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
        expected.insert((vec![Value::Eid(100), Value::Attribute("parent/child".to_string()), Value::Eid(300), Value::Attribute("age".to_string()), Value::Number(13)], 1));
        expected.insert((vec![Value::Eid(100), Value::Attribute("parent/child".to_string()), Value::Eid(300), Value::Attribute("name".to_string()), Value::String("Mabel".to_string())], 1));
        expected.insert((vec![Value::Eid(200), Value::Attribute("parent/child".to_string()), Value::Eid(400), Value::Attribute("age".to_string()), Value::Number(12)], 1));
        expected.insert((vec![Value::Eid(200), Value::Attribute("parent/child".to_string()), Value::Eid(400), Value::Attribute("name".to_string()), Value::String("Dipper".to_string())], 1));

        for _i in 0..expected.len() {
            let result = results.recv().unwrap();
            if !expected.remove(&result) { panic!("unknown result {:?}", result); }
        }

        assert!(results.recv_timeout(Duration::from_millis(400)).is_err());
    }).unwrap();
}

// #[test]
// fn pull_maps() {
//     timely::execute(Configuration::Thread, |worker| {
//         let mut server = Server::<u64>::new(Default::default());

//         let (parent, child,) = (1, 2,);
//         let plan = Plan::PullLevel(PullLevel {
//             variables: vec![],
//             plan: Box::new(Plan::MatchA(parent, "parent/child".to_string(), child)),
//             attributes: vec!["name".to_string(), "age".to_string()],
//         });

//         let view = Rc::new(RefCell::new(HashMap::<Value,Value>::new()));
//         let view_handle = Rc::downgrade(&view);
        
//         worker.dataflow::<u64, _, _>(|scope| {
//             server.create_input("parent/child", scope);
//             server.create_input("name", scope);
//             server.create_input("age", scope);

//             server
//                 .test_single(scope, Rule { name: "pull_children".to_string(), plan })
//                 .inspect(move |&(ref data, _t, diff)| {
//                     if let Some(view_cell) = view_handle.upgrade() {

//                         let mut view_borrow = view_cell.borrow_mut();
                        
//                         if diff > 0 {
//                             view_borrow.insert(data[2].clone(), data[3].clone());
//                         } else if diff < 0 {
//                             view_borrow.remove(data[2]);
//                         }
//                     }
//                 });
//         });

//         server.transact(
//             Transact {
//                 tx: Some(0),
//                 tx_data: vec![
//                     TxData(1, 100, "name".to_string(), Value::String("Alice".to_string())),
//                     TxData(1, 100, "parent/child".to_string(), Value::Eid(300)),
//                     TxData(1, 200, "name".to_string(), Value::String("Bob".to_string())),
//                     TxData(1, 200, "parent/child".to_string(), Value::Eid(400)),
//                     TxData(1, 300, "name".to_string(), Value::String("Mabel".to_string())),
//                     TxData(1, 300, "age".to_string(), Value::Number(13)),
//                     TxData(1, 400, "name".to_string(), Value::String("Dipper".to_string())),
//                     TxData(1, 400, "age".to_string(), Value::Number(12)),
//                 ],
//             },
//             0,
//             0,
//         );

//         worker.step_while(|| server.is_any_outdated());

//         println!("{:?}", view);
//     }).unwrap();
// }

#[test]
fn pull() {
    timely::execute(Configuration::Thread, |worker| {
        let mut server = Server::<u64>::new(Default::default());
        let (send_results, results) = channel();

        let (a,b,c,) = (1,2,3,);
        let plan = Plan::Pull(Pull {
            paths: vec![
                PullLevel {
                    variables: vec![],
                    plan: Box::new(Plan::MatchA(a, "join/binding".to_string(), b)),
                    pull_attributes: vec!["pattern/e".to_string(),
                                          "pattern/a".to_string(),
                                          "pattern/v".to_string()],
                    path_attributes: vec!["join/binding".to_string()],
                },
                PullLevel {
                    variables: vec![],
                    plan: Box::new(Plan::MatchA(a, "name".to_string(), c)),
                    pull_attributes: vec![],
                    path_attributes: vec!["name".to_string()],
                }
            ]
        });

        worker.dataflow::<u64, _, _>(|scope| {
            server.create_input("name", scope);
            server.create_input("join/binding", scope);
            server.create_input("pattern/e", scope);
            server.create_input("pattern/a", scope);
            server.create_input("pattern/v", scope);

            server
                .test_single(scope, Rule { name: "pull".to_string(), plan })
                .inspect(move |x| { send_results.send((x.0.clone(), x.2)).unwrap(); });
        });

        server.transact(
            Transact {
                tx: Some(0),
                tx_data: vec![
                    TxData(1, 100, "name".to_string(), Value::String("rule".to_string())),
                    TxData(1, 100, "join/binding".to_string(), Value::Eid(200)),
                    TxData(1, 100, "join/binding".to_string(), Value::Eid(300)),
                    TxData(1, 200, "pattern/a".to_string(), Value::Attribute("xyz".to_string())),
                    TxData(1, 300, "pattern/e".to_string(), Value::Eid(12345)),
                    TxData(1, 300, "pattern/a".to_string(), Value::Attribute("asd".to_string())),
                ],
            },
            0,
            0,
        );

        worker.step_while(|| server.is_any_outdated());

        let mut expected = HashSet::new();
        expected.insert((vec![Value::Eid(100), Value::Attribute("name".to_string()), Value::String("rule".to_string())], 1));
        expected.insert((vec![Value::Eid(100), Value::Attribute("join/binding".to_string()), Value::Eid(200), Value::Attribute("pattern/a".to_string()), Value::Attribute("xyz".to_string())], 1));
        expected.insert((vec![Value::Eid(100), Value::Attribute("join/binding".to_string()), Value::Eid(300), Value::Attribute("pattern/e".to_string()), Value::Eid(12345)], 1));
        expected.insert((vec![Value::Eid(100), Value::Attribute("join/binding".to_string()), Value::Eid(300), Value::Attribute("pattern/a".to_string()), Value::Attribute("asd".to_string())], 1));
        
        for _i in 0..expected.len() {
            let result = results.recv_timeout(Duration::from_millis(400)).unwrap();
            if !expected.remove(&result) { panic!("unknown result {:?}", result); }
        }

        assert!(results.recv_timeout(Duration::from_millis(400)).is_err());
    }).unwrap();
}
