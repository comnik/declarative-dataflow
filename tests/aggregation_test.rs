extern crate declarative_dataflow;
extern crate num_rational;
extern crate timely;

use std::sync::mpsc::channel;
use std::thread;

use timely::Configuration;

use declarative_dataflow::plan::{Aggregate, AggregationFn, Join, Project};
use declarative_dataflow::server::{Register, Server, Transact, TxData};
use declarative_dataflow::{Plan, Rule, Value};

use num_rational::Ratio;

// Single aggregations
#[test]
fn count() {
    timely::execute(Configuration::Thread, move |worker| {
        let mut server = Server::new(Default::default());
        let (send_results, results) = channel();
        let send_results_copy = send_results.clone();
        let send_results_copy_2 = send_results.clone();

        // [:find (count ?amount) :where [?e :amount ?amount]]
        let (e, amount) = (1, 2);
        let plan = Plan::Aggregate(Aggregate {
            variables: vec![amount],
            plan: Box::new(Plan::MatchA(e, ":amount".to_string(), amount)),
            aggregation_fns: vec![AggregationFn::COUNT],
            key_symbols: vec![],
            aggregation_symbols: vec![amount],
            with_symbols: vec![],
        });

        // [:find ?e (count ?amount) :where [?e :amount ?amount]]
        let (e, amount) = (1, 2);
        let plan_group_sing = Plan::Aggregate(Aggregate {
            variables: vec![e, amount],
            plan: Box::new(Plan::MatchA(e, ":amount".to_string(), amount)),
            aggregation_fns: vec![AggregationFn::COUNT],
            key_symbols: vec![e],
            aggregation_symbols: vec![amount],
        });

        // [:find ?e ?amount (count ?amount) :where [?e :amount ?amount]]
        let (e, amount) = (1, 2);
        let plan_group_mult = Plan::Aggregate(Aggregate {
            variables: vec![e, amount],
            plan: Box::new(Plan::MatchA(e, ":amount".to_string(), amount)),
            aggregation_fns: vec![AggregationFn::COUNT],
            key_symbols: vec![e, amount],
            aggregation_symbols: vec![amount],
            with_symbols: vec![],
        });

        worker.dataflow::<u64, _, _>(|mut scope| {
            server.create_input(":amount".to_string(), scope);

            let query_name = "count";
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

            let query_name = "count_group_single_key";
            server.register(
                Register {
                    rules: vec![Rule {
                        name: query_name.to_string(),
                        plan: plan_group_sing,
                    }],
                    publish: vec![query_name.to_string()],
                },
                &mut scope,
            );

            server
                .interest(query_name.to_string(), &mut scope)
                .inspect(move |x| {
                    send_results_copy.send((x.0.clone(), x.2)).unwrap();
                });

            let query_name = "count_group_multiple_keys";
            server.register(
                Register {
                    rules: vec![Rule {
                        name: query_name.to_string(),
                        plan: plan_group_mult,
                    }],
                    publish: vec![query_name.to_string()],
                },
                &mut scope,
            );

            server
                .interest(query_name.to_string(), &mut scope)
                .inspect(move |x| {
                    send_results_copy_2.send((x.0.clone(), x.2)).unwrap();
                });
        });

        server.transact(
            Transact {
                tx: Some(0),
                tx_data: vec![
                    TxData(1, 1, ":amount".to_string(), Value::Number(5)),
                    TxData(1, 2, ":amount".to_string(), Value::Number(10)),
                    TxData(1, 2, ":amount".to_string(), Value::Number(10)),
                    TxData(1, 1, ":amount".to_string(), Value::Number(2)),
                    TxData(1, 1, ":amount".to_string(), Value::Number(4)),
                    TxData(1, 1, ":amount".to_string(), Value::Number(6)),
                ],
            },
            0,
            0,
        );

        worker.step_while(|| server.is_any_outdated());

        thread::spawn(move || {
            assert_eq!(results.recv().unwrap(), (vec![Value::Number(5)], 1));
            assert_eq!(
                results.recv().unwrap(),
                (vec![Value::Eid(1), Value::Number(4)], 1)
            );
            assert_eq!(
                results.recv().unwrap(),
                (vec![Value::Eid(2), Value::Number(1)], 1)
            );
            assert_eq!(
                results.recv().unwrap(),
                (vec![Value::Eid(1), Value::Number(2), Value::Number(1)], 1)
            );
            assert_eq!(
                results.recv().unwrap(),
                (vec![Value::Eid(1), Value::Number(4), Value::Number(1)], 1)
            );
            assert_eq!(
                results.recv().unwrap(),
                (vec![Value::Eid(1), Value::Number(5), Value::Number(1)], 1)
            );
            assert_eq!(
                results.recv().unwrap(),
                (vec![Value::Eid(1), Value::Number(6), Value::Number(1)], 1)
            );
            assert_eq!(
                results.recv().unwrap(),
                (vec![Value::Eid(2), Value::Number(10), Value::Number(1)], 1)
            );
        }).join()
            .unwrap();
    }).unwrap();
}

#[test]
fn max() {
    timely::execute(Configuration::Thread, move |worker| {
        let mut server = Server::new(Default::default());
        let (send_results, results) = channel();
        let send_results_copy = send_results.clone();

        // [:find (max ?amount) :where [?e :amount ?amount]]
        let (e, amount) = (1, 2);
        let plan = Plan::Aggregate(Aggregate {
            variables: vec![amount],
            plan: Box::new(Plan::Project(Project {
                variables: vec![amount],
                plan: Box::new(Plan::MatchA(e, ":amount".to_string(), amount)),
            })),
            aggregation_fns: vec![AggregationFn::MAX],
            key_symbols: vec![],
            aggregation_symbols: vec![amount],
            with_symbols: vec![],
        });

        // [:find ?e (max ?amount) :where [?e :amount ?amount]]
        let plan_group = Plan::Aggregate(Aggregate {
            variables: vec![e, amount],
            plan: Box::new(Plan::MatchA(e, ":amount".to_string(), amount)),
            aggregation_fns: vec![AggregationFn::MAX],
            key_symbols: vec![e],
            aggregation_symbols: vec![amount],
            with_symbols: vec![],
        });

        worker.dataflow::<u64, _, _>(|mut scope| {
            server.create_input(":amount".to_string(), scope);

            let query_name = "max";
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

            let query_name = "max_group";
            server.register(
                Register {
                    rules: vec![Rule {
                        name: query_name.to_string(),
                        plan: plan_group,
                    }],
                    publish: vec![query_name.to_string()],
                },
                &mut scope,
            );

            server
                .interest(query_name.to_string(), &mut scope)
                .inspect(move |x| {
                    send_results_copy.send((x.0.clone(), x.2)).unwrap();
                });
        });

        server.transact(
            Transact {
                tx: Some(0),
                tx_data: vec![
                    TxData(1, 1, ":amount".to_string(), Value::Number(5)),
                    TxData(1, 2, ":amount".to_string(), Value::Number(10)),
                    TxData(1, 2, ":amount".to_string(), Value::Number(10)),
                    TxData(1, 1, ":amount".to_string(), Value::Number(2)),
                    TxData(1, 1, ":amount".to_string(), Value::Number(4)),
                    TxData(1, 1, ":amount".to_string(), Value::Number(6)),
                ],
            },
            0,
            0,
        );

        worker.step_while(|| server.is_any_outdated());

        thread::spawn(move || {
            assert_eq!(
                results.recv().unwrap(),
                (vec![Value::Eid(1), Value::Number(6)], 1)
            );
            assert_eq!(
                results.recv().unwrap(),
                (vec![Value::Eid(2), Value::Number(10)], 1)
            );
            assert_eq!(results.recv().unwrap(), (vec![Value::Number(10)], 1));
        }).join()
            .unwrap();
    }).unwrap();
}

#[test]
fn min() {
    timely::execute(Configuration::Thread, move |worker| {
        let mut server = Server::new(Default::default());
        let (send_results, results) = channel();
        let send_results_copy = send_results.clone();

        // [:find (min ?amount) :where [?e :amount ?amount]]
        let (e, amount) = (1, 2);
        let plan = Plan::Aggregate(Aggregate {
            variables: vec![amount],
            plan: Box::new(Plan::Project(Project {
                variables: vec![amount],
                plan: Box::new(Plan::MatchA(e, ":amount".to_string(), amount)),
            })),
            aggregation_fns: vec![AggregationFn::MIN],
            key_symbols: vec![],
            aggregation_symbols: vec![amount],
            with_symbols: vec![],
        });

        // [:find ?e (min ?amount) :where [?e :amount ?amount]]
        let plan_group = Plan::Aggregate(Aggregate {
            variables: vec![e, amount],
            plan: Box::new(Plan::MatchA(e, ":amount".to_string(), amount)),
            aggregation_fns: vec![AggregationFn::MIN],
            key_symbols: vec![e],
            aggregation_symbols: vec![amount],
            with_symbols: vec![],
        });

        worker.dataflow::<u64, _, _>(|mut scope| {
            server.create_input(":amount".to_string(), scope);

            let query_name = "min";
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

            let query_name = "min_group";
            server.register(
                Register {
                    rules: vec![Rule {
                        name: query_name.to_string(),
                        plan: plan_group,
                    }],
                    publish: vec![query_name.to_string()],
                },
                &mut scope,
            );

            server
                .interest(query_name.to_string(), &mut scope)
                .inspect(move |x| {
                    send_results_copy.send((x.0.clone(), x.2)).unwrap();
                });
        });

        server.transact(
            Transact {
                tx: Some(0),
                tx_data: vec![
                    TxData(1, 1, ":amount".to_string(), Value::Number(5)),
                    TxData(1, 2, ":amount".to_string(), Value::Number(10)),
                    TxData(1, 2, ":amount".to_string(), Value::Number(10)),
                    TxData(1, 1, ":amount".to_string(), Value::Number(2)),
                    TxData(1, 1, ":amount".to_string(), Value::Number(4)),
                    TxData(1, 1, ":amount".to_string(), Value::Number(6)),
                ],
            },
            0,
            0,
        );

        worker.step_while(|| server.is_any_outdated());

        thread::spawn(move || {
            assert_eq!(
                results.recv().unwrap(),
                (vec![Value::Eid(1), Value::Number(2)], 1)
            );
            assert_eq!(
                results.recv().unwrap(),
                (vec![Value::Eid(2), Value::Number(10)], 1)
            );
            assert_eq!(results.recv().unwrap(), (vec![Value::Number(2)], 1));
        }).join()
            .unwrap();
    }).unwrap();
}

#[test]
fn sum() {
    timely::execute(Configuration::Thread, move |worker| {
        let mut server = Server::new(Default::default());
        let (send_results, results) = channel();
        let send_results_copy = send_results.clone();

        // [:find (sum ?amount) :with ?e :where [?e :amount ?amount]]
        let (e, amount) = (1, 2);
        let plan = Plan::Aggregate(Aggregate {
            variables: vec![amount],
            plan: Box::new(Plan::Project(Project {
                variables: vec![amount],
                plan: Box::new(Plan::MatchA(e, ":amount".to_string(), amount)),
            })),
            aggregation_fns: vec![AggregationFn::SUM],
            key_symbols: vec![],
            aggregation_symbols: vec![amount],
            with_symbols: vec![],
        });

        // [:find ?e (sum ?amount) :where [?e :amount ?amount]]
        let plan_group = Plan::Aggregate(Aggregate {
            variables: vec![e, amount],
            plan: Box::new(Plan::MatchA(e, ":amount".to_string(), amount)),
            aggregation_fns: vec![AggregationFn::SUM],
            key_symbols: vec![e],
            aggregation_symbols: vec![amount],
            with_symbols: vec![],
        });

        worker.dataflow::<u64, _, _>(|mut scope| {
            server.create_input(":amount".to_string(), scope);

            let query_name = "sum";
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

            let query_name = "sum_group";
            server.register(
                Register {
                    rules: vec![Rule {
                        name: query_name.to_string(),
                        plan: plan_group,
                    }],
                    publish: vec![query_name.to_string()],
                },
                &mut scope,
            );

            server
                .interest(query_name.to_string(), &mut scope)
                .inspect(move |x| {
                    send_results_copy.send((x.0.clone(), x.2)).unwrap();
                });
        });

        server.transact(
            Transact {
                tx: Some(0),
                tx_data: vec![
                    TxData(1, 1, ":amount".to_string(), Value::Number(5)),
                    TxData(1, 2, ":amount".to_string(), Value::Number(10)),
                    TxData(1, 2, ":amount".to_string(), Value::Number(10)),
                    TxData(1, 1, ":amount".to_string(), Value::Number(2)),
                    TxData(1, 1, ":amount".to_string(), Value::Number(4)),
                    TxData(1, 1, ":amount".to_string(), Value::Number(6)),
                ],
            },
            0,
            0,
        );

        worker.step_while(|| server.is_any_outdated());

        thread::spawn(move || {
            assert_eq!(
                results.recv().unwrap(),
                (vec![Value::Eid(1), Value::Number(17)], 1)
            );
            assert_eq!(
                results.recv().unwrap(),
                (vec![Value::Eid(2), Value::Number(10)], 1)
            );
            assert_eq!(results.recv().unwrap(), (vec![Value::Number(27)], 1));
        }).join()
            .unwrap();
    }).unwrap();
}

#[test]
fn avg() {
    timely::execute(Configuration::Thread, move |worker| {
        let mut server = Server::new(Default::default());
        let (send_results, results) = channel();
        let send_results_copy = send_results.clone();

        // [:find (avg ?amount) :where [?e :amount ?amount]]
        let (e, amount) = (1, 2);
        let plan = Plan::Aggregate(Aggregate {
            variables: vec![amount],
            plan: Box::new(Plan::Project(Project {
                variables: vec![amount],
                plan: Box::new(Plan::MatchA(e, ":amount".to_string(), amount)),
            })),
            aggregation_fns: vec![AggregationFn::AVG],
            key_symbols: vec![],
            aggregation_symbols: vec![amount],
            with_symbols: vec![],
        });

        // [:find ?e (avg ?amount) :where [?e :amount ?amount]]
        let plan_group = Plan::Aggregate(Aggregate {
            variables: vec![e, amount],
            plan: Box::new(Plan::MatchA(e, ":amount".to_string(), amount)),
            aggregation_fns: vec![AggregationFn::AVG],
            key_symbols: vec![e],
            aggregation_symbols: vec![amount],
            with_symbols: vec![],
        });

        worker.dataflow::<u64, _, _>(|mut scope| {
            server.create_input(":amount".to_string(), scope);

            let query_name = "avg";
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

            let query_name = "avg_group";
            server.register(
                Register {
                    rules: vec![Rule {
                        name: query_name.to_string(),
                        plan: plan_group,
                    }],
                    publish: vec![query_name.to_string()],
                },
                &mut scope,
            );

            server
                .interest(query_name.to_string(), &mut scope)
                .inspect(move |x| {
                    send_results_copy.send((x.0.clone(), x.2)).unwrap();
                });
        });

        server.transact(
            Transact {
                tx: Some(0),
                tx_data: vec![
                    TxData(1, 1, ":amount".to_string(), Value::Number(5)),
                    TxData(1, 2, ":amount".to_string(), Value::Number(10)),
                    TxData(1, 2, ":amount".to_string(), Value::Number(10)),
                    TxData(1, 1, ":amount".to_string(), Value::Number(2)),
                    TxData(1, 1, ":amount".to_string(), Value::Number(4)),
                    TxData(1, 1, ":amount".to_string(), Value::Number(6)),
                ],
            },
            0,
            0,
        );

        worker.step_while(|| server.is_any_outdated());

        thread::spawn(move || {
            assert_eq!(
                results.recv().unwrap(),
                (vec![Value::Eid(1), Value::Rational32(Ratio::new(17, 4))], 1)
            );
            assert_eq!(
                results.recv().unwrap(),
                (vec![Value::Eid(2), Value::Rational32(Ratio::new(10, 1))], 1)
            );
            assert_eq!(
                results.recv().unwrap(),
                (vec![Value::Rational32(Ratio::new(27, 5))], 1)
            );
        }).join()
            .unwrap();
    }).unwrap();
}

// #[test]
// fn var() {
//     timely::execute(Configuration::Thread, move |worker| {
//         let mut server = Server::new(Default::default());
//         let (send_results, results) = channel();
//         let send_results_copy = send_results.clone();

//         // [:find (variance ?amount) :where [?e :amount ?amount]]
//         let (e, amount) = (1, 2);
//         let plan = Plan::Aggregate(Aggregate {
//             variables: vec![e, amount, amount],
//             plan: Box::new(Plan::Project(Project {
//                 variables: vec![amount],
//                 plan: Box::new(Plan::MatchA(e, ":amount".to_string(), amount)),
//             })),
//             aggregation_fns: vec![AggregationFn::VARIANCE],
//             key_symbols: vec![],
//         });

//         // [:find ?e (variance ?amount) :where [?e :amount ?amount]]
//         let plan_group = Plan::Aggregate(Aggregate {
//             variables: vec![e, amount],
//             plan: Box::new(Plan::MatchA(e, ":amount".to_string(), amount)),
//             aggregation_fn: AggregationFn::VARIANCE,
//             key_symbols: vec![e],
//         });

//         worker.dataflow::<u64, _, _>(|mut scope| {
//             server.create_input(":amount".to_string(), scope);

//             let query_name = "var";
//             server.register(
//                 Register {
//                     rules: vec![Rule {
//                         name: query_name.to_string(),
//                         plan: plan,
//                     }],
//                     publish: vec![query_name.to_string()],
//                 },
//                 &mut scope,
//             );

//             server
//                 .interest(query_name.to_string(), &mut scope)
//                 .inspect(move |x| {
//                     send_results.send((x.0.clone(), x.2)).unwrap();
//                 });

//             let query_name = "var_group";
//             server.register(
//                 Register {
//                     rules: vec![Rule {
//                         name: query_name.to_string(),
//                         plan: plan_group,
//                     }],
//                     publish: vec![query_name.to_string()],
//                 },
//                 &mut scope,
//             );

//             server
//                 .interest(query_name.to_string(), &mut scope)
//                 .inspect(move |x| {
//                     send_results_copy.send((x.0.clone(), x.2)).unwrap();
//                 });
//         });

//         server.transact(
//             Transact {
//                 tx: Some(0),
//                 tx_data: vec![
//                     TxData(1, 1, ":amount".to_string(), Value::Number(5)),
//                     TxData(1, 2, ":amount".to_string(), Value::Number(10)),
//                     TxData(1, 2, ":amount".to_string(), Value::Number(10)),
//                     TxData(1, 1, ":amount".to_string(), Value::Number(2)),
//                     TxData(1, 1, ":amount".to_string(), Value::Number(4)),
//                     TxData(1, 1, ":amount".to_string(), Value::Number(6)),
//                 ],
//             },
//             0,
//             0,
//         );

//         worker.step_while(|| server.is_any_outdated());

//         thread::spawn(move || {
//             assert_eq!(
//                 results.recv().unwrap(),
//                 (
//                     vec![Value::Eid(1), Value::Rational32(Ratio::new(35, 16))],
//                     1
//                 )
//             );
//             assert_eq!(
//                 results.recv().unwrap(),
//                 (vec![Value::Eid(2), Value::Rational32(Ratio::new(0, 1))], 1)
//             );
//             assert_eq!(
//                 results.recv().unwrap(),
//                 (vec![Value::Rational32(Ratio::new(176, 25))], 1)
//             );
//         }).join()
//             .unwrap();
//     }).unwrap();
// }

#[test]
fn median() {
    timely::execute(Configuration::Thread, move |worker| {
        let mut server = Server::new(Default::default());
        let (send_results, results) = channel();
        let send_results_copy = send_results.clone();

        // [:find (median ?amount) :where [?e :amount ?amount]]
        let (e, amount) = (1, 2);
        let plan = Plan::Aggregate(Aggregate {
            variables: vec![amount],
            plan: Box::new(Plan::Project(Project {
                variables: vec![amount],
                plan: Box::new(Plan::MatchA(e, ":amount".to_string(), amount)),
            })),
            aggregation_fns: vec![AggregationFn::MEDIAN],
            key_symbols: vec![],
            aggregation_symbols: vec![amount],
            with_symbols: vec![],
        });

        // [:find ?e (median ?amount) :where [?e :amount ?amount]]
        let plan_group = Plan::Aggregate(Aggregate {
            variables: vec![e, amount],
            plan: Box::new(Plan::MatchA(e, ":amount".to_string(), amount)),
            aggregation_fns: vec![AggregationFn::MEDIAN],
            key_symbols: vec![e],
            aggregation_symbols: vec![amount],
            with_symbols: vec![],
        });

        worker.dataflow::<u64, _, _>(|mut scope| {
            server.create_input(":amount".to_string(), scope);

            let query_name = "median";
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

            let query_name = "median_group";
            server.register(
                Register {
                    rules: vec![Rule {
                        name: query_name.to_string(),
                        plan: plan_group,
                    }],
                    publish: vec![query_name.to_string()],
                },
                &mut scope,
            );

            server
                .interest(query_name.to_string(), &mut scope)
                .inspect(move |x| {
                    send_results_copy.send((x.0.clone(), x.2)).unwrap();
                });
        });

        server.transact(
            Transact {
                tx: Some(0),
                tx_data: vec![
                    TxData(1, 1, ":amount".to_string(), Value::Number(5)),
                    TxData(1, 2, ":amount".to_string(), Value::Number(10)),
                    TxData(1, 2, ":amount".to_string(), Value::Number(10)),
                    TxData(1, 1, ":amount".to_string(), Value::Number(2)),
                    TxData(1, 1, ":amount".to_string(), Value::Number(4)),
                    TxData(1, 1, ":amount".to_string(), Value::Number(6)),
                ],
            },
            0,
            0,
        );

        worker.step_while(|| server.is_any_outdated());

        thread::spawn(move || {
            assert_eq!(
                results.recv().unwrap(),
                (vec![Value::Eid(1), Value::Number(5)], 1)
            );
            assert_eq!(
                results.recv().unwrap(),
                (vec![Value::Eid(2), Value::Number(10)], 1)
            );
            assert_eq!(results.recv().unwrap(), (vec![Value::Number(5)], 1));
        }).join()
            .unwrap();
    }).unwrap();
}

// Multiple Aggregations

#[test]
fn multi() {
    timely::execute(Configuration::Thread, move |worker| {
        let mut server = Server::new(Default::default());
        let (send_results, results) = channel();
        let send_results_copy = send_results.clone();

        // [:find (max ?amount) (min ?debt) (sum ?amount) (avg ?debt) :where [?e :amount ?amount][?e :debt ?debt]]
        let (e, amount, debt) = (1, 2, 3);
        let plan = Plan::Aggregate(Aggregate{
            variables: vec![amount, debt, amount, debt],
            plan: Box::new(Plan::Project(Project {
                variables: vec![amount, debt],
                plan: Box::new(Plan::Join(Join {
                    variables: vec![e],
                    left_plan: Box::new(Plan::MatchA(e, ":amount".to_string(), amount)),
                    right_plan: Box::new(Plan::MatchA(e, ":debt".to_string(), debt)),
                })),
            })),
            aggregation_fns: vec![
                AggregationFn::MAX,
                AggregationFn::MIN,
                AggregationFn::SUM,
                AggregationFn::AVG,
            ],
            key_symbols: vec![],
            aggregation_symbols: vec![amount, debt, amount, debt],
            with_symbols: vec![],
        });

        // [:find ?e (min ?amount) (max ?amount) (median ?amount) (count ?amount) (min ?debt)
        // (max ?debt) (median ?debt) (count ?debt) :where [?e :amount ?amount][?e :debt ?debt]]
        let plan_group = Plan::Aggregate(Aggregate{
            variables: vec![e, amount, amount, amount, amount, debt, debt, debt, debt],
            plan: Box::new(Plan::Project(Project{
                variables: vec![e, amount, debt],
                plan: Box::new(Plan::Join(Join {
                    variables: vec![e],
                    left_plan: Box::new(Plan::MatchA(e, ":amount".to_string(), amount)),
                    right_plan: Box::new(Plan::MatchA(e, ":debt".to_string(), debt)),
                })),
            })),
            aggregation_fns: vec![
                AggregationFn::MIN,
                AggregationFn::MAX,
                AggregationFn::MEDIAN,
                AggregationFn::COUNT,
                AggregationFn::MIN,
                AggregationFn::MAX,
                AggregationFn::MEDIAN,
                AggregationFn::COUNT,
            ],
            key_symbols: vec![e],
            aggregation_symbols: vec![amount, amount, amount, amount, debt, debt, debt, debt],
            with_symbols: vec![],
        });

        worker.dataflow::<u64, _, _>(|mut scope| {
            server.create_input(":amount".to_string(), scope);
            server.create_input(":debt".to_string(), scope);

            let query_name = "multi";
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

            let query_name = "multi_group";
            server.register(
                Register {
                    rules: vec![Rule {
                        name: query_name.to_string(),
                        plan: plan_group,
                    }],
                    publish: vec![query_name.to_string()],
                },
                &mut scope,
            );

            server
                .interest(query_name.to_string(), &mut scope)
                .inspect(move |x| {
                    send_results_copy.send((x.0.clone(), x.2)).unwrap();
                });
        });

        server.transact(
            Transact {
                tx: Some(0),
                tx_data: vec![
                    TxData(1, 1, ":amount".to_string(), Value::Number(5)),
                    TxData(1, 1, ":amount".to_string(), Value::Number(2)),
                    TxData(1, 1, ":amount".to_string(), Value::Number(6)),
                    TxData(1, 1, ":amount".to_string(), Value::Number(9)),
                    TxData(1, 1, ":amount".to_string(), Value::Number(10)),
                    TxData(1, 1, ":debt".to_string(), Value::Number(13)),
                    TxData(1, 1, ":debt".to_string(), Value::Number(4)),
                    TxData(1, 1, ":debt".to_string(), Value::Number(9)),
                    TxData(1, 1, ":debt".to_string(), Value::Number(15)),
                    TxData(1, 1, ":debt".to_string(), Value::Number(10)),
                    TxData(1, 2, ":amount".to_string(), Value::Number(2)),
                    TxData(1, 2, ":amount".to_string(), Value::Number(4)),
                    TxData(1, 2, ":debt".to_string(), Value::Number(5)),
                    TxData(1, 2, ":debt".to_string(), Value::Number(42)),
                ],
            },
            0,
            0,
        );

        worker.step_while(|| server.is_any_outdated());

        thread::spawn(move || {
            assert_eq!(
                results.recv().unwrap(),
                (
                    vec![
                        Value::Number(10),
                        Value::Number(4),
                        Value::Number(36),
                        Value::Rational32(Ratio::new(14, 1)),
                    ],
                    1
                )
            );
            assert_eq!(

                results.recv().unwrap(),
                (
                    vec![
                        Value::Eid(1),
                        Value::Number(2),
                        Value::Number(10),
                        Value::Number(6),
                        Value::Number(5),
                        Value::Number(4),
                        Value::Number(15),
                        Value::Number(10),
                        Value::Number(5),
                    ],
                    1
                )
            );
            assert_eq!(
                results.recv().unwrap(),
                (
                    vec![
                        Value::Eid(2),
                        Value::Number(2),
                        Value::Number(4),
                        Value::Number(4),
                        Value::Number(2),
                        Value::Number(5),
                        Value::Number(42),
                        Value::Number(42),
                        Value::Number(2),
                    ],
                    1
                )
            );
        }).join()
            .unwrap();
    }).unwrap();
}

#[test]
fn with() {
    timely::execute(Configuration::Thread, move |worker| {
        let mut server = Server::new(Default::default());
        let (send_results, results) = channel();

        // [:find (sum ?heads) :with ?monster :where [?e :monster ?monster][?e :head ?head]] 
        let (e, monster, heads) = (1, 2, 3);
        let plan = Plan::Aggregate(Aggregate{
            variables: vec![heads],
            plan: Box::new(Plan::Project(Project {
                variables: vec![heads, monster],
                plan: Box::new(Plan::Join(Join {
                    variables: vec![e],
                    left_plan: Box::new(Plan::MatchA(e, ":monster".to_string(), monster)),
                    right_plan: Box::new(Plan::MatchA(e, ":heads".to_string(), heads)),
                })),
            })),
            aggregation_fns: vec![
                AggregationFn::SUM,
            ],
            key_symbols: vec![],
            aggregation_symbols: vec![heads],
            with_symbols: vec![monster],
        });

        worker.dataflow::<u64, _, _>(|mut scope| {
            server.create_input(":monster".to_string(), scope);
            server.create_input(":heads".to_string(), scope);

            let query_name = "with";
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

        server.transact(
            Transact {
                tx: Some(0),
                tx_data: vec![
                    TxData(1, 1, ":monster".to_string(), Value::String("Cerberus".to_string())),
                    TxData(1, 1, ":heads".to_string(), Value::Number(3)),
                    TxData(1, 2, ":monster".to_string(), Value::String("Medusa".to_string())),
                    TxData(1, 2, ":heads".to_string(), Value::Number(1)),
                    TxData(1, 3, ":monster".to_string(), Value::String("Cyclops".to_string())),
                    TxData(1, 3, ":heads".to_string(), Value::Number(1)),
                    TxData(1, 4, ":monster".to_string(), Value::String("Chimera".to_string())),
                    TxData(1, 4, ":heads".to_string(), Value::Number(1)),
                ],
            },
            0,
            0,
        );

        worker.step_while(|| server.is_any_outdated());

        thread::spawn(move || {
            assert_eq!(
                results.recv().unwrap(),
                (
                    vec![
                        Value::Number(6),
                    ],
                    1
                )
            );
        }).join()
            .unwrap();
    }).unwrap();
}
