extern crate declarative_dataflow;
extern crate num_rational;
extern crate timely;

use std::sync::mpsc::channel;
use std::thread;

use timely::Configuration;

use declarative_dataflow::plan::{Aggregate, AggregationFn, Join, Project};
use declarative_dataflow::server::{CreateInput, Interest, Register, Server, Transact, TxData};
use declarative_dataflow::{Plan, Rule, Value};

use num_rational::Ratio;

#[test]
fn match_ea() {
    timely::execute(Configuration::Thread, move |worker| {
        let mut server = Server::new(Default::default());
        let (send_results, results) = channel();

        // [:find ?v :where [1 :name ?n]]
        let plan = Plan::MatchEA(1, ":name".to_string(), 1);

        worker.dataflow::<u64, _, _>(|scope| {
            server.create_input(
                CreateInput {
                    name: ":name".to_string(),
                },
                scope,
            );

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
                .interest(
                    Interest {
                        name: query_name.to_string(),
                    },
                    scope,
                )
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

        for handle in server.input_handles.values() {
            while server.probe.less_than(handle.time()) {
                worker.step();
            }
        }

        thread::spawn(move || {
            assert_eq!(
                results.recv().unwrap(),
                (vec![Value::String("Alias".to_string())], 1)
            );
            assert_eq!(
                results.recv().unwrap(),
                (vec![Value::String("Dipper".to_string())], 1)
            );
        }).join()
            .unwrap();
    }).unwrap();
}

#[test]
fn join() {
    timely::execute(Configuration::Thread, move |worker| {
        let mut server = Server::new(Default::default());
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

        worker.dataflow::<u64, _, _>(|mut scope| {
            server.create_input(
                CreateInput {
                    name: ":name".to_string(),
                },
                &mut scope,
            );
            server.create_input(
                CreateInput {
                    name: ":age".to_string(),
                },
                &mut scope,
            );

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
                .interest(
                    Interest {
                        name: query_name.to_string(),
                    },
                    &mut scope,
                )
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
                    TxData(1, 1, ":age".to_string(), Value::Number(12)),
                ],
            },
            0,
            0,
        );

        for handle in server.input_handles.values() {
            while server.probe.less_than(handle.time()) {
                worker.step();
            }
        }

        thread::spawn(move || {
            assert_eq!(
                results.recv().unwrap(),
                (
                    vec![
                        Value::Eid(1),
                        Value::String("Dipper".to_string()),
                        Value::Number(12),
                    ],
                    1
                )
            );
        }).join()
            .unwrap();
    }).unwrap();
}

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
            variables: vec![e, amount],
            plan: Box::new(Plan::MatchA(e, ":amount".to_string(), amount)),
            aggregation_fn: AggregationFn::COUNT,
            key_symbols: vec![],
        });

        // [:find ?e (count ?amount) :where [?e :amount ?amount]]
        let (e, amount) = (1, 2);
        let plan_group_sing = Plan::Aggregate(Aggregate {
            variables: vec![e, amount],
            plan: Box::new(Plan::MatchA(e, ":amount".to_string(), amount)),
            aggregation_fn: AggregationFn::COUNT,
            key_symbols: vec![e],
        });

        // [:find ?e ?amount (count ?amount) :where [?e :amount ?amount]]
        let (e, amount) = (1, 2);
        let plan_group_mult = Plan::Aggregate(Aggregate {
            variables: vec![e, amount],
            plan: Box::new(Plan::MatchA(e, ":amount".to_string(), amount)),
            aggregation_fn: AggregationFn::COUNT,
            key_symbols: vec![e, amount],
        });

        worker.dataflow::<u64, _, _>(|mut scope| {
            server.create_input(
                CreateInput {
                    name: ":amount".to_string(),
                },
                &mut scope,
            );

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
                .interest(
                    Interest {
                        name: query_name.to_string(),
                    },
                    &mut scope,
                )
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
                .interest(
                    Interest {
                        name: query_name.to_string(),
                    },
                    &mut scope,
                )
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
                .interest(
                    Interest {
                        name: query_name.to_string(),
                    },
                    &mut scope,
                )
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

        for handle in server.input_handles.values() {
            while server.probe.less_than(handle.time()) {
                worker.step();
            }
        }

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
            variables: vec![e, amount],
            plan: Box::new(Plan::Project(Project {
                variables: vec![amount],
                plan: Box::new(Plan::MatchA(e, ":amount".to_string(), amount)),
            })),
            aggregation_fn: AggregationFn::MAX,
            key_symbols: vec![],
        });

        // [:find ?e (max ?amount) :where [?e :amount ?amount]]
        let plan_group = Plan::Aggregate(Aggregate {
            variables: vec![e, amount],
            plan: Box::new(Plan::MatchA(e, ":amount".to_string(), amount)),
            aggregation_fn: AggregationFn::MAX,
            key_symbols: vec![e],
        });

        worker.dataflow::<u64, _, _>(|mut scope| {
            server.create_input(
                CreateInput {
                    name: ":amount".to_string(),
                },
                &mut scope,
            );

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
                .interest(
                    Interest {
                        name: query_name.to_string(),
                    },
                    &mut scope,
                )
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
                .interest(
                    Interest {
                        name: query_name.to_string(),
                    },
                    &mut scope,
                )
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

        for handle in server.input_handles.values() {
            while server.probe.less_than(handle.time()) {
                worker.step();
            }
        }

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
            variables: vec![e, amount],
            plan: Box::new(Plan::Project(Project {
                variables: vec![amount],
                plan: Box::new(Plan::MatchA(e, ":amount".to_string(), amount)),
            })),
            aggregation_fn: AggregationFn::MIN,
            key_symbols: vec![],
        });

        // [:find ?e (min ?amount) :where [?e :amount ?amount]]
        let plan_group = Plan::Aggregate(Aggregate {
            variables: vec![e, amount],
            plan: Box::new(Plan::MatchA(e, ":amount".to_string(), amount)),
            aggregation_fn: AggregationFn::MIN,
            key_symbols: vec![e],
        });

        worker.dataflow::<u64, _, _>(|mut scope| {
            server.create_input(
                CreateInput {
                    name: ":amount".to_string(),
                },
                &mut scope,
            );

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
                .interest(
                    Interest {
                        name: query_name.to_string(),
                    },
                    &mut scope,
                )
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
                .interest(
                    Interest {
                        name: query_name.to_string(),
                    },
                    &mut scope,
                )
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

        for handle in server.input_handles.values() {
            while server.probe.less_than(handle.time()) {
                worker.step();
            }
        }

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

        let (e, amount) = (1, 2);

        // [:find ?e (sum ?amount) :where [?e :amount ?amount]]
        let plan_group = Plan::Aggregate(Aggregate {
            variables: vec![e, amount],
            plan: Box::new(Plan::MatchA(e, ":amount".to_string(), amount)),
            aggregation_fn: AggregationFn::SUM,
            key_symbols: vec![e],
        });

        // [:find (sum ?amount) :with ?e :where [?e :amount ?amount]]
        let (e, amount) = (1, 2);
        let plan = Plan::Aggregate(Aggregate {
            variables: vec![e, amount, amount],
            plan: Box::new(Plan::Project(Project {
                variables: vec![amount],
                plan: Box::new(Plan::MatchA(e, ":amount".to_string(), amount)),
            })),
            aggregation_fn: AggregationFn::SUM,
            key_symbols: vec![],
        });

        worker.dataflow::<u64, _, _>(|mut scope| {
            server.create_input(
                CreateInput {
                    name: ":amount".to_string(),
                },
                &mut scope,
            );

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
                .interest(
                    Interest {
                        name: query_name.to_string(),
                    },
                    &mut scope,
                )
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
                .interest(
                    Interest {
                        name: query_name.to_string(),
                    },
                    &mut scope,
                )
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

        for handle in server.input_handles.values() {
            while server.probe.less_than(handle.time()) {
                worker.step();
            }
        }

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
            variables: vec![e, amount, amount],
            plan: Box::new(Plan::Project(Project {
                variables: vec![amount],
                plan: Box::new(Plan::MatchA(e, ":amount".to_string(), amount)),
            })),
            aggregation_fn: AggregationFn::AVG,
            key_symbols: vec![],
        });

        // [:find ?e (sum ?amount) :where [?e :amount ?amount]]
        let plan_group = Plan::Aggregate(Aggregate {
            variables: vec![e, amount],
            plan: Box::new(Plan::MatchA(e, ":amount".to_string(), amount)),
            aggregation_fn: AggregationFn::AVG,
            key_symbols: vec![e],
        });

        worker.dataflow::<u64, _, _>(|mut scope| {
            server.create_input(
                CreateInput {
                    name: ":amount".to_string(),
                },
                &mut scope,
            );

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
                .interest(
                    Interest {
                        name: query_name.to_string(),
                    },
                    &mut scope,
                )
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
                .interest(
                    Interest {
                        name: query_name.to_string(),
                    },
                    &mut scope,
                )
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

        for handle in server.input_handles.values() {
            while server.probe.less_than(handle.time()) {
                worker.step();
            }
        }

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

#[test]
fn var() {
    timely::execute(Configuration::Thread, move |worker| {
        let mut server = Server::new(Default::default());
        let (send_results, results) = channel();
        let send_results_copy = send_results.clone();

        // [:find (variance ?amount) :where [?e :amount ?amount]]
        let (e, amount) = (1, 2);
        let plan = Plan::Aggregate(Aggregate {
            variables: vec![e, amount, amount],
            plan: Box::new(Plan::Project(Project {
                variables: vec![amount],
                plan: Box::new(Plan::MatchA(e, ":amount".to_string(), amount)),
            })),
            aggregation_fn: AggregationFn::VARIANCE,
            key_symbols: vec![],
        });

        // [:find ?e (variance ?amount) :where [?e :amount ?amount]]
        let plan_group = Plan::Aggregate(Aggregate {
            variables: vec![e, amount],
            plan: Box::new(Plan::MatchA(e, ":amount".to_string(), amount)),
            aggregation_fn: AggregationFn::VARIANCE,
            key_symbols: vec![e],
        });

        worker.dataflow::<u64, _, _>(|mut scope| {
            server.create_input(
                CreateInput {
                    name: ":amount".to_string(),
                },
                &mut scope,
            );

            let query_name = "var";
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
                .interest(
                    Interest {
                        name: query_name.to_string(),
                    },
                    &mut scope,
                )
                .inspect(move |x| {
                    send_results.send((x.0.clone(), x.2)).unwrap();
                });

            let query_name = "var_group";
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
                .interest(
                    Interest {
                        name: query_name.to_string(),
                    },
                    &mut scope,
                )
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

        for handle in server.input_handles.values() {
            while server.probe.less_than(handle.time()) {
                worker.step();
            }
        }

        thread::spawn(move || {
            assert_eq!(
                results.recv().unwrap(),
                (
                    vec![Value::Eid(1), Value::Rational32(Ratio::new(35, 16))],
                    1
                )
            );
            assert_eq!(
                results.recv().unwrap(),
                (vec![Value::Eid(2), Value::Rational32(Ratio::new(0, 1))], 1)
            );
            assert_eq!(
                results.recv().unwrap(),
                (vec![Value::Rational32(Ratio::new(176, 25))], 1)
            );
        }).join()
            .unwrap();
    }).unwrap();
}

#[test]
fn median() {
    timely::execute(Configuration::Thread, move |worker| {
        let mut server = Server::new(Default::default());
        let (send_results, results) = channel();
        let send_results_copy = send_results.clone();

        // [:find (median ?amount) :where [?e :amount ?amount]]
        let (e, amount) = (1, 2);
        let plan = Plan::Aggregate(Aggregate {
            variables: vec![e, amount, amount],
            plan: Box::new(Plan::Project(Project {
                variables: vec![amount],
                plan: Box::new(Plan::MatchA(e, ":amount".to_string(), amount)),
            })),
            aggregation_fn: AggregationFn::MEDIAN,
            key_symbols: vec![],
        });

        // [:find ?e (median ?amount) :where [?e :amount ?amount]]
        let plan_group = Plan::Aggregate(Aggregate {
            variables: vec![e, amount],
            plan: Box::new(Plan::MatchA(e, ":amount".to_string(), amount)),
            aggregation_fn: AggregationFn::MEDIAN,
            key_symbols: vec![e],
        });

        worker.dataflow::<u64, _, _>(|mut scope| {
            server.create_input(
                CreateInput {
                    name: ":amount".to_string(),
                },
                &mut scope,
            );

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
                .interest(
                    Interest {
                        name: query_name.to_string(),
                    },
                    &mut scope,
                )
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
                .interest(
                    Interest {
                        name: query_name.to_string(),
                    },
                    &mut scope,
                )
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

        for handle in server.input_handles.values() {
            while server.probe.less_than(handle.time()) {
                worker.step();
            }
        }

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
