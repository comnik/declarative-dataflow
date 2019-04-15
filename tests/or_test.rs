use std::collections::HashSet;
use std::iter::FromIterator;
use std::sync::mpsc::channel;
use std::time::Duration;

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Operator;

use declarative_dataflow::binding::Binding;
use declarative_dataflow::plan::{Hector, Implementable, Union};
use declarative_dataflow::server::Server;
use declarative_dataflow::{Aid, Value};
use declarative_dataflow::{AttributeConfig, InputSemantics, Plan, Rule, TxData};
use Value::{Eid, Number, String};

struct Case {
    description: &'static str,
    plan: Plan,
    transactions: Vec<Vec<TxData>>,
    expectations: Vec<Vec<(Vec<Value>, u64, isize)>>,
}

fn dependencies(case: &Case) -> HashSet<Aid> {
    let mut deps = HashSet::new();

    for binding in case.plan.into_bindings().iter() {
        if let Binding::Attribute(binding) = binding {
            deps.insert(binding.source_attribute.clone());
        }
    }

    deps
}

fn run_cases(mut cases: Vec<Case>) {
    for case in cases.drain(..) {
        timely::execute_directly(move |worker| {
            let mut server = Server::<u64, u64>::new(Default::default());
            let (send_results, results) = channel();

            dbg!(case.description);

            let mut deps = dependencies(&case);
            let plan = case.plan.clone();

            for tx in case.transactions.iter() {
                for datum in tx {
                    deps.insert(datum.2.clone());
                }
            }

            worker.dataflow::<u64, _, _>(|scope| {
                for dep in deps.iter() {
                    server
                        .context
                        .internal
                        .create_transactable_attribute(
                            dep,
                            AttributeConfig::tx_time(InputSemantics::Raw),
                            scope,
                        )
                        .unwrap();
                }

                server
                    .test_single(
                        scope,
                        Rule {
                            name: "query".to_string(),
                            plan,
                        },
                    )
                    .inner
                    .sink(Pipeline, "Results", move |input| {
                        input.for_each(|_time, data| {
                            for datum in data.iter() {
                                send_results.send(datum.clone()).unwrap()
                            }
                        });
                    });
            });

            let mut transactions = case.transactions.clone();
            let mut next_tx = 0;

            for (tx_id, tx_data) in transactions.drain(..).enumerate() {
                next_tx += 1;

                server.transact(tx_data, 0, 0).unwrap();
                server.advance_domain(None, next_tx).unwrap();

                worker.step_while(|| server.is_any_outdated());

                let mut expected: HashSet<(Vec<Value>, u64, isize)> =
                    HashSet::from_iter(case.expectations[tx_id].iter().cloned());

                for _i in 0..expected.len() {
                    match results.recv_timeout(Duration::from_millis(400)) {
                        Err(_err) => {
                            panic!("No result.");
                        }
                        Ok(result) => {
                            if !expected.remove(&result) {
                                panic!("Unknown result {:?}.", result);
                            }
                        }
                    }
                }

                match results.recv_timeout(Duration::from_millis(400)) {
                    Err(_err) => {}
                    Ok(result) => {
                        panic!("Extraneous result {:?}", result);
                    }
                }
            }
        });
    }
}

#[test]
fn or() {
    let data = vec![
        TxData(1, 1, ":name".to_string(), String("Ivan".to_string())),
        TxData(1, 1, ":age".to_string(), Number(10)),
        TxData(1, 2, ":name".to_string(), String("Ivan".to_string())),
        TxData(1, 2, ":age".to_string(), Number(20)),
        TxData(1, 3, ":name".to_string(), String("Oleg".to_string())),
        TxData(1, 3, ":age".to_string(), Number(10)),
        TxData(1, 4, ":name".to_string(), String("Oleg".to_string())),
        TxData(1, 4, ":age".to_string(), Number(20)),
        TxData(1, 5, ":name".to_string(), String("Ivan".to_string())),
        TxData(1, 5, ":age".to_string(), Number(10)),
        TxData(1, 6, ":name".to_string(), String("Ivan".to_string())),
        TxData(1, 6, ":age".to_string(), Number(20)),
    ];

    run_cases(vec![
        Case {
            description: "[:find ?e :where (or [?e :name Oleg] [?e :age 10])]",
            plan: Plan::Union(Union {
                variables: vec![0],
                plans: vec![
                    Plan::Hector(Hector {
                        variables: vec![0],
                        bindings: vec![
                            Binding::attribute(0, ":name", 1),
                            Binding::constant(1, String("Oleg".to_string())),
                        ],
                    }),
                    Plan::Hector(Hector {
                        variables: vec![0],
                        bindings: vec![
                            Binding::attribute(0, ":age", 1),
                            Binding::constant(1, Number(10)),
                        ],
                    }),
                ],
            }),
            transactions: vec![data.clone()],
            expectations: vec![vec![
                (vec![Eid(1)], 0, 1),
                (vec![Eid(3)], 0, 1),
                (vec![Eid(4)], 0, 1),
                (vec![Eid(5)], 0, 1),
            ]],
        },
        Case {
            description: "[:find ?e :where (or [?e :name Oleg] [?e :age 30])] (one branch empty)",
            plan: Plan::Union(Union {
                variables: vec![0],
                plans: vec![
                    Plan::Hector(Hector {
                        variables: vec![0],
                        bindings: vec![
                            Binding::attribute(0, ":name", 1),
                            Binding::constant(1, String("Oleg".to_string())),
                        ],
                    }),
                    Plan::Hector(Hector {
                        variables: vec![0],
                        bindings: vec![
                            Binding::attribute(0, ":age", 1),
                            Binding::constant(1, Number(30)),
                        ],
                    }),
                ],
            }),
            transactions: vec![data.clone()],
            expectations: vec![vec![(vec![Eid(3)], 0, 1), (vec![Eid(4)], 0, 1)]],
        },
        Case {
            description: "[:find ?e :where (or [?e :name Petr] [?e :age 30])] (both empty)",
            plan: Plan::Union(Union {
                variables: vec![0],
                plans: vec![
                    Plan::Hector(Hector {
                        variables: vec![0],
                        bindings: vec![
                            Binding::attribute(0, ":name", 1),
                            Binding::constant(1, String("Petr".to_string())),
                        ],
                    }),
                    Plan::Hector(Hector {
                        variables: vec![0],
                        bindings: vec![
                            Binding::attribute(0, ":age", 1),
                            Binding::constant(1, Number(30)),
                        ],
                    }),
                ],
            }),
            transactions: vec![data.clone()],
            expectations: vec![vec![]],
        },
        // @TODO must be able to identify the conflict between the two
        // constant bindings
        //
        // Case {
        //     description: "[:find ?e :where [?e :name Ivan] (or [?e :name Oleg] [?e :age 10])] (join with 1 var)",
        //     plan: Plan::Union(Union {
        //         variables: vec![0],
        //         plans: vec![
        //             Plan::Hector(Hector {
        //                 variables: vec![0],
        //                 bindings: vec![
        //                     Binding::attribute(0, ":name", 1),
        //                     Binding::constant(1, String("Ivan".to_string())),
        //                     Binding::constant(1, String("Oleg".to_string())),
        //                 ],
        //             }),
        //             Plan::Hector(Hector {
        //                 variables: vec![0],
        //                 bindings: vec![
        //                     Binding::attribute(0, ":name", 1),
        //                     Binding::constant(1, String("Ivan".to_string())),
        //                     Binding::attribute(0, ":age", 2),
        //                     Binding::constant(2, Number(10)),
        //                 ],
        //             }),
        //         ],
        //     }),
        //     transactions: vec![data.clone()],
        //     expectations: vec![vec![
        //         (vec![Eid(1)], 0, 1),
        //         (vec![Eid(5)], 0, 1),
        //     ]],
        // },
        Case {
            description: "[:find ?e
                           :where
                           [?e :age ?a] 
                           (or (and [?e :name Ivan] [1 :age a])
                               (and [?e :name Oleg] [2 :age a]))] (join with 2 vars)",
            plan: Plan::Union(Union {
                variables: vec![0],
                plans: vec![
                    Plan::Hector(Hector {
                        variables: vec![0],
                        bindings: vec![
                            Binding::attribute(0, ":age", 2),
                            Binding::attribute(0, ":name", 1),
                            Binding::constant(1, String("Ivan".to_string())),
                            Binding::attribute(3, ":age", 2),
                            Binding::constant(3, Eid(1)),
                        ],
                    }),
                    Plan::Hector(Hector {
                        variables: vec![0],
                        bindings: vec![
                            Binding::attribute(0, ":age", 2),
                            Binding::attribute(0, ":name", 1),
                            Binding::constant(1, String("Oleg".to_string())),
                            Binding::attribute(3, ":age", 2),
                            Binding::constant(3, Eid(2)),
                        ],
                    }),
                ],
            }),
            transactions: vec![data.clone()],
            expectations: vec![vec![
                (vec![Eid(1)], 0, 1),
                (vec![Eid(5)], 0, 1),
                (vec![Eid(4)], 0, 1),
            ]],
        },
    ]);
}

#[test]
fn or_join() {
    let data = vec![
        TxData(1, 1, ":name".to_string(), String("Ivan".to_string())),
        TxData(1, 1, ":age".to_string(), Number(10)),
        TxData(1, 2, ":name".to_string(), String("Ivan".to_string())),
        TxData(1, 2, ":age".to_string(), Number(20)),
        TxData(1, 3, ":name".to_string(), String("Oleg".to_string())),
        TxData(1, 3, ":age".to_string(), Number(10)),
        TxData(1, 4, ":name".to_string(), String("Oleg".to_string())),
        TxData(1, 4, ":age".to_string(), Number(20)),
        TxData(1, 5, ":name".to_string(), String("Ivan".to_string())),
        TxData(1, 5, ":age".to_string(), Number(10)),
        TxData(1, 6, ":name".to_string(), String("Ivan".to_string())),
        TxData(1, 6, ":age".to_string(), Number(20)),
    ];

    run_cases(vec![Case {
        description: "[:find ?e 
                           :where
                           (or-join [?e]
                             [?e :name ?n]
                             (and [?e :age ?a] [?e :name ?n]))]",
        plan: Plan::Union(Union {
            variables: vec![0],
            plans: vec![
                Plan::Hector(Hector {
                    variables: vec![0],
                    bindings: vec![Binding::attribute(0, ":name", 2)],
                }),
                Plan::Hector(Hector {
                    variables: vec![0],
                    bindings: vec![
                        Binding::attribute(0, ":age", 1),
                        Binding::attribute(0, ":name", 2),
                    ],
                }),
            ],
        }),
        transactions: vec![data.clone()],
        expectations: vec![vec![
            (vec![Eid(1)], 0, 1),
            (vec![Eid(2)], 0, 1),
            (vec![Eid(3)], 0, 1),
            (vec![Eid(4)], 0, 1),
            (vec![Eid(5)], 0, 1),
            (vec![Eid(6)], 0, 1),
        ]],
    }]);
}
