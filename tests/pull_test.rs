extern crate declarative_dataflow;
extern crate timely;

use std::collections::HashSet;
use std::sync::mpsc::channel;
use std::time::Duration;

use timely::Configuration;

use declarative_dataflow::plan::{Pull, PullLevel};
use declarative_dataflow::server::{Server, Transact, TxData};
use declarative_dataflow::{Plan, Rule, Value};
use Value::{Aid, Bool, Eid, Number, String};

#[test]
fn pull_level() {
    timely::execute(Configuration::Thread, |worker| {
        let mut server = Server::<u64>::new(Default::default());
        let (send_results, results) = channel();

        let (e,) = (1,);
        let plan = Plan::PullLevel(PullLevel {
            variables: vec![],
            plan: Box::new(Plan::MatchAV(e, "admin?".to_string(), Bool(false))),
            pull_attributes: vec!["name".to_string(), "age".to_string()],
            path_attributes: vec![],
        });

        worker.dataflow::<u64, _, _>(|scope| {
            server.create_attribute("admin?", scope).unwrap();
            server.create_attribute("name", scope).unwrap();
            server.create_attribute("age", scope).unwrap();

            server
                .test_single(
                    scope,
                    Rule {
                        name: "pull_level".to_string(),
                        plan,
                    },
                )
                .inspect(move |x| {
                    send_results.send((x.0.clone(), x.2)).unwrap();
                });
        });

        server.transact(
            Transact {
                tx: Some(0),
                tx_data: vec![
                    TxData(1, 100, "admin?".to_string(), Bool(true)),
                    TxData(1, 200, "admin?".to_string(), Bool(false)),
                    TxData(1, 300, "admin?".to_string(), Bool(false)),
                    TxData(1, 100, "name".to_string(), String("Mabel".to_string())),
                    TxData(1, 200, "name".to_string(), String("Dipper".to_string())),
                    TxData(1, 300, "name".to_string(), String("Soos".to_string())),
                    TxData(1, 100, "age".to_string(), Number(12)),
                    TxData(1, 200, "age".to_string(), Number(13)),
                ],
            },
            0,
            0,
        );

        worker.step_while(|| server.is_any_outdated());

        let mut expected = HashSet::new();
        expected.insert((vec![Eid(200), Aid("age".to_string()), Number(13)], 1));
        expected.insert((
            vec![
                Eid(200),
                Aid("name".to_string()),
                String("Dipper".to_string()),
            ],
            1,
        ));
        expected.insert((
            vec![
                Eid(300),
                Aid("name".to_string()),
                String("Soos".to_string()),
            ],
            1,
        ));

        for _i in 0..expected.len() {
            let result = results.recv().unwrap();
            if !expected.remove(&result) {
                panic!("unknown result {:?}", result);
            }
        }

        assert!(results.recv_timeout(Duration::from_millis(400)).is_err());
    })
    .unwrap();
}

#[test]
fn pull_children() {
    timely::execute(Configuration::Thread, |worker| {
        let mut server = Server::<u64>::new(Default::default());
        let (send_results, results) = channel();

        let (parent, child) = (1, 2);
        let plan = Plan::PullLevel(PullLevel {
            variables: vec![],
            plan: Box::new(Plan::MatchA(parent, "parent/child".to_string(), child)),
            pull_attributes: vec!["name".to_string(), "age".to_string()],
            path_attributes: vec!["parent/child".to_string()],
        });

        worker.dataflow::<u64, _, _>(|scope| {
            server.create_attribute("parent/child", scope).unwrap();
            server.create_attribute("name", scope).unwrap();
            server.create_attribute("age", scope).unwrap();

            server
                .test_single(
                    scope,
                    Rule {
                        name: "pull_children".to_string(),
                        plan,
                    },
                )
                .inspect(move |x| {
                    send_results.send((x.0.clone(), x.2)).unwrap();
                });
        });

        server.transact(
            Transact {
                tx: Some(0),
                tx_data: vec![
                    TxData(1, 100, "name".to_string(), String("Alice".to_string())),
                    TxData(1, 100, "parent/child".to_string(), Eid(300)),
                    TxData(1, 200, "name".to_string(), String("Bob".to_string())),
                    TxData(1, 200, "parent/child".to_string(), Eid(400)),
                    TxData(1, 300, "name".to_string(), String("Mabel".to_string())),
                    TxData(1, 300, "age".to_string(), Number(13)),
                    TxData(1, 400, "name".to_string(), String("Dipper".to_string())),
                    TxData(1, 400, "age".to_string(), Number(12)),
                ],
            },
            0,
            0,
        );

        worker.step_while(|| server.is_any_outdated());

        let mut expected = HashSet::new();
        expected.insert((
            vec![
                Eid(100),
                Aid("parent/child".to_string()),
                Eid(300),
                Aid("age".to_string()),
                Number(13),
            ],
            1,
        ));
        expected.insert((
            vec![
                Eid(100),
                Aid("parent/child".to_string()),
                Eid(300),
                Aid("name".to_string()),
                String("Mabel".to_string()),
            ],
            1,
        ));
        expected.insert((
            vec![
                Eid(200),
                Aid("parent/child".to_string()),
                Eid(400),
                Aid("age".to_string()),
                Number(12),
            ],
            1,
        ));
        expected.insert((
            vec![
                Eid(200),
                Aid("parent/child".to_string()),
                Eid(400),
                Aid("name".to_string()),
                String("Dipper".to_string()),
            ],
            1,
        ));

        for _i in 0..expected.len() {
            let result = results.recv().unwrap();
            if !expected.remove(&result) {
                panic!("unknown result {:?}", result);
            }
        }

        assert!(results.recv_timeout(Duration::from_millis(400)).is_err());
    })
    .unwrap();
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
//             server.create_attribute("parent/child", scope).unwrap();
//             server.create_attribute("name", scope).unwrap();
//             server.create_attribute("age", scope).unwrap();

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
//                     TxData(1, 100, "name".to_string(), String("Alice".to_string())),
//                     TxData(1, 100, "parent/child".to_string(), Eid(300)),
//                     TxData(1, 200, "name".to_string(), String("Bob".to_string())),
//                     TxData(1, 200, "parent/child".to_string(), Eid(400)),
//                     TxData(1, 300, "name".to_string(), String("Mabel".to_string())),
//                     TxData(1, 300, "age".to_string(), Number(13)),
//                     TxData(1, 400, "name".to_string(), String("Dipper".to_string())),
//                     TxData(1, 400, "age".to_string(), Number(12)),
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

        let (a, b, c) = (1, 2, 3);
        let plan = Plan::Pull(Pull {
            variables: vec![],
            paths: vec![
                PullLevel {
                    variables: vec![],
                    plan: Box::new(Plan::MatchA(a, "join/binding".to_string(), b)),
                    pull_attributes: vec![
                        "pattern/e".to_string(),
                        "pattern/a".to_string(),
                        "pattern/v".to_string(),
                    ],
                    path_attributes: vec!["join/binding".to_string()],
                },
                PullLevel {
                    variables: vec![],
                    plan: Box::new(Plan::MatchA(a, "name".to_string(), c)),
                    pull_attributes: vec![],
                    path_attributes: vec!["name".to_string()],
                },
            ],
        });

        worker.dataflow::<u64, _, _>(|scope| {
            server.create_attribute("name", scope).unwrap();
            server.create_attribute("join/binding", scope).unwrap();
            server.create_attribute("pattern/e", scope).unwrap();
            server.create_attribute("pattern/a", scope).unwrap();
            server.create_attribute("pattern/v", scope).unwrap();

            server
                .test_single(
                    scope,
                    Rule {
                        name: "pull".to_string(),
                        plan,
                    },
                )
                .inspect(move |x| {
                    send_results.send((x.0.clone(), x.2)).unwrap();
                });
        });

        server.transact(
            Transact {
                tx: Some(0),
                tx_data: vec![
                    TxData(1, 100, "name".to_string(), String("rule".to_string())),
                    TxData(1, 100, "join/binding".to_string(), Eid(200)),
                    TxData(1, 100, "join/binding".to_string(), Eid(300)),
                    TxData(1, 200, "pattern/a".to_string(), Aid("xyz".to_string())),
                    TxData(1, 300, "pattern/e".to_string(), Eid(12345)),
                    TxData(1, 300, "pattern/a".to_string(), Aid("asd".to_string())),
                ],
            },
            0,
            0,
        );

        worker.step_while(|| server.is_any_outdated());

        let mut expected = HashSet::new();
        expected.insert((
            vec![
                Eid(100),
                Aid("name".to_string()),
                String("rule".to_string()),
            ],
            1,
        ));
        expected.insert((
            vec![
                Eid(100),
                Aid("join/binding".to_string()),
                Eid(200),
                Aid("pattern/a".to_string()),
                Aid("xyz".to_string()),
            ],
            1,
        ));
        expected.insert((
            vec![
                Eid(100),
                Aid("join/binding".to_string()),
                Eid(300),
                Aid("pattern/e".to_string()),
                Eid(12345),
            ],
            1,
        ));
        expected.insert((
            vec![
                Eid(100),
                Aid("join/binding".to_string()),
                Eid(300),
                Aid("pattern/a".to_string()),
                Aid("asd".to_string()),
            ],
            1,
        ));

        for _i in 0..expected.len() {
            let result = results.recv_timeout(Duration::from_millis(400)).unwrap();
            if !expected.remove(&result) {
                panic!("unknown result {:?}", result);
            }
        }

        assert!(results.recv_timeout(Duration::from_millis(400)).is_err());
    })
    .unwrap();
}
