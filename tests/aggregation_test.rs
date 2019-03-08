use std::collections::HashSet;
use std::iter::FromIterator;
use std::sync::mpsc::channel;
use std::time::Duration;

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Operator;

use declarative_dataflow::binding::Binding;
use declarative_dataflow::plan::{Aggregate, AggregationFn, Implementable, Join, Project};
use declarative_dataflow::server::Server;
use declarative_dataflow::{Aid, AttributeSemantics, Plan, Rule, TxData, Value};
use Value::{Eid, Number, Rational32, String};

use num_rational::Ratio;

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

            let deps = dependencies(&case);
            let plan = case.plan.clone();

            worker.dataflow::<u64, _, _>(|scope| {
                for dep in deps.iter() {
                    server
                        .context
                        .internal
                        .create_attribute(dep, AttributeSemantics::Raw, scope)
                        .unwrap();
                }

                server
                    .test_single(
                        scope,
                        Rule {
                            name: "hector".to_string(),
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
fn count() {
    let (e, amount) = (1, 2);
    let data = vec![
        TxData(1, 1, ":amount".to_string(), Number(5)),
        TxData(1, 2, ":amount".to_string(), Number(10)),
        TxData(1, 2, ":amount".to_string(), Number(10)),
        TxData(1, 1, ":amount".to_string(), Number(2)),
        TxData(1, 1, ":amount".to_string(), Number(4)),
        TxData(1, 1, ":amount".to_string(), Number(6)),
    ];

    run_cases(vec![
        Case {
            description: "[:find (count ?amount) :where [?e :amount ?amount]]",
            plan: Plan::Aggregate(Aggregate {
                variables: vec![amount],
                plan: Box::new(Plan::Project(Project {
                    variables: vec![amount],
                    plan: Box::new(Plan::MatchA(e, ":amount".to_string(), amount)),
                })),
                aggregation_fns: vec![AggregationFn::COUNT],
                key_variables: vec![],
                aggregation_variables: vec![amount],
                with_variables: vec![],
            }),
            transactions: vec![data.clone()],
            expectations: vec![vec![(vec![Number(6)], 0, 1)]],
            // set-semantics
            // expectations: vec![
            //     vec![(vec![Number(5)], 0, 1)],
            // ],
        },
        Case {
            description: "[:find ?e (count ?amount) :where [?e :amount ?amount]]",
            plan: Plan::Aggregate(Aggregate {
                variables: vec![e, amount],
                plan: Box::new(Plan::MatchA(e, ":amount".to_string(), amount)),
                aggregation_fns: vec![AggregationFn::COUNT],
                key_variables: vec![e],
                aggregation_variables: vec![amount],
                with_variables: vec![],
            }),
            transactions: vec![data.clone()],
            expectations: vec![vec![
                (vec![Eid(1), Number(4)], 0, 1),
                (vec![Eid(2), Number(2)], 0, 1),
            ]],
            // set-semantics
            // expectations: vec![
            //     vec![
            //         (vec![Eid(1), Number(4)], 0, 1),
            //         (vec![Eid(2), Number(1)], 0, 1),
            //     ],
            // ],
        },
    ]);
}

#[test]
fn max() {
    let (e, amount) = (1, 2);
    let data = vec![
        TxData(1, 1, ":amount".to_string(), Number(5)),
        TxData(1, 2, ":amount".to_string(), Number(10)),
        TxData(1, 2, ":amount".to_string(), Number(10)),
        TxData(1, 1, ":amount".to_string(), Number(2)),
        TxData(1, 1, ":amount".to_string(), Number(4)),
        TxData(1, 1, ":amount".to_string(), Number(6)),
    ];

    run_cases(vec![
        Case {
            description: "[:find (max ?amount) :where [?e :amount ?amount]]",
            plan: Plan::Aggregate(Aggregate {
                variables: vec![amount],
                plan: Box::new(Plan::Project(Project {
                    variables: vec![amount],
                    plan: Box::new(Plan::MatchA(e, ":amount".to_string(), amount)),
                })),
                aggregation_fns: vec![AggregationFn::MAX],
                key_variables: vec![],
                aggregation_variables: vec![amount],
                with_variables: vec![],
            }),
            transactions: vec![data.clone()],
            expectations: vec![vec![(vec![Number(10)], 0, 1)]],
        },
        Case {
            description: "[:find ?e (max ?amount) :where [?e :amount ?amount]]",
            plan: Plan::Aggregate(Aggregate {
                variables: vec![e, amount],
                plan: Box::new(Plan::MatchA(e, ":amount".to_string(), amount)),
                aggregation_fns: vec![AggregationFn::MAX],
                key_variables: vec![e],
                aggregation_variables: vec![amount],
                with_variables: vec![],
            }),
            transactions: vec![data.clone()],
            expectations: vec![vec![
                (vec![Eid(1), Number(6)], 0, 1),
                (vec![Eid(2), Number(10)], 0, 1),
            ]],
        },
    ]);
}

#[test]
fn min() {
    let (e, amount) = (1, 2);
    let data = vec![
        TxData(1, 1, ":amount".to_string(), Number(5)),
        TxData(1, 2, ":amount".to_string(), Number(10)),
        TxData(1, 2, ":amount".to_string(), Number(10)),
        TxData(1, 1, ":amount".to_string(), Number(2)),
        TxData(1, 1, ":amount".to_string(), Number(4)),
        TxData(1, 1, ":amount".to_string(), Number(6)),
    ];

    run_cases(vec![
        Case {
            description: "[:find (min ?amount) :where [?e :amount ?amount]]",
            plan: Plan::Aggregate(Aggregate {
                variables: vec![amount],
                plan: Box::new(Plan::Project(Project {
                    variables: vec![amount],
                    plan: Box::new(Plan::MatchA(e, ":amount".to_string(), amount)),
                })),
                aggregation_fns: vec![AggregationFn::MIN],
                key_variables: vec![],
                aggregation_variables: vec![amount],
                with_variables: vec![],
            }),
            transactions: vec![data.clone()],
            expectations: vec![vec![(vec![Number(2)], 0, 1)]],
        },
        Case {
            description: "[:find ?e (min ?amount) :where [?e :amount ?amount]]",
            plan: Plan::Aggregate(Aggregate {
                variables: vec![e, amount],
                plan: Box::new(Plan::MatchA(e, ":amount".to_string(), amount)),
                aggregation_fns: vec![AggregationFn::MIN],
                key_variables: vec![e],
                aggregation_variables: vec![amount],
                with_variables: vec![],
            }),
            transactions: vec![data.clone()],
            expectations: vec![vec![
                (vec![Eid(1), Number(2)], 0, 1),
                (vec![Eid(2), Number(10)], 0, 1),
            ]],
        },
    ]);
}

#[test]
fn sum() {
    let (e, amount) = (1, 2);
    let data = vec![
        TxData(1, 1, ":amount".to_string(), Number(5)),
        TxData(1, 2, ":amount".to_string(), Number(10)),
        TxData(1, 2, ":amount".to_string(), Number(10)),
        TxData(1, 1, ":amount".to_string(), Number(2)),
        TxData(1, 1, ":amount".to_string(), Number(4)),
        TxData(1, 1, ":amount".to_string(), Number(6)),
    ];

    run_cases(vec![
        Case {
            description: "[:find (sum ?amount) :with ?e :where [?e :amount ?amount]]",
            plan: Plan::Aggregate(Aggregate {
                variables: vec![amount],
                plan: Box::new(Plan::Project(Project {
                    variables: vec![amount],
                    plan: Box::new(Plan::MatchA(e, ":amount".to_string(), amount)),
                })),
                aggregation_fns: vec![AggregationFn::SUM],
                key_variables: vec![],
                aggregation_variables: vec![amount],
                with_variables: vec![],
            }),
            transactions: vec![data.clone()],
            expectations: vec![vec![(vec![Number(37)], 0, 1)]],
            // set-semantics
            // expectations: vec![
            //     vec![(vec![Number(27)], 0, 1)],
            // ],
        },
        Case {
            description: "[:find ?e (sum ?amount) :where [?e :amount ?amount]]",
            plan: Plan::Aggregate(Aggregate {
                variables: vec![e, amount],
                plan: Box::new(Plan::MatchA(e, ":amount".to_string(), amount)),
                aggregation_fns: vec![AggregationFn::SUM],
                key_variables: vec![e],
                aggregation_variables: vec![amount],
                with_variables: vec![],
            }),
            transactions: vec![data.clone()],
            expectations: vec![vec![
                (vec![Eid(1), Number(17)], 0, 1),
                (vec![Eid(2), Number(20)], 0, 1),
            ]],
            // set-semantics
            // expectations: vec![
            //     vec![
            //         (vec![Eid(1), Number(17)], 0, 1),
            //         (vec![Eid(2), Number(10)], 0, 1),
            //     ],
            // ],
        },
    ]);
}

#[test]
fn avg() {
    let (e, amount) = (1, 2);
    let data = vec![
        TxData(1, 1, ":amount".to_string(), Number(5)),
        TxData(1, 2, ":amount".to_string(), Number(10)),
        TxData(1, 2, ":amount".to_string(), Number(10)),
        TxData(1, 1, ":amount".to_string(), Number(2)),
        TxData(1, 1, ":amount".to_string(), Number(4)),
        TxData(1, 1, ":amount".to_string(), Number(6)),
    ];

    run_cases(vec![
        Case {
            description: "[:find (avg ?amount) :where [?e :amount ?amount]]",
            plan: Plan::Aggregate(Aggregate {
                variables: vec![amount],
                plan: Box::new(Plan::Project(Project {
                    variables: vec![amount],
                    plan: Box::new(Plan::MatchA(e, ":amount".to_string(), amount)),
                })),
                aggregation_fns: vec![AggregationFn::AVG],
                key_variables: vec![],
                aggregation_variables: vec![amount],
                with_variables: vec![],
            }),
            transactions: vec![data.clone()],
            expectations: vec![vec![(vec![Rational32(Ratio::new(37, 6))], 0, 1)]],
            // set-semantics
            // expectations: vec![
            //     vec![(vec![Rational32(Ratio::new(27, 5))], 0, 1)],
            // ],
        },
        Case {
            description: "[:find ?e (avg ?amount) :where [?e :amount ?amount]]",
            plan: Plan::Aggregate(Aggregate {
                variables: vec![e, amount],
                plan: Box::new(Plan::MatchA(e, ":amount".to_string(), amount)),
                aggregation_fns: vec![AggregationFn::AVG],
                key_variables: vec![e],
                aggregation_variables: vec![amount],
                with_variables: vec![],
            }),
            transactions: vec![data.clone()],
            expectations: vec![vec![
                (vec![Eid(1), Rational32(Ratio::new(17, 4))], 0, 1),
                (vec![Eid(2), Rational32(Ratio::new(20, 2))], 0, 1),
            ]],
            // set-semantics
            // expectations: vec![
            //     vec![
            //         (vec![Eid(1), Rational32(Ratio::new(17, 4))], 0, 1),
            //         (vec![Eid(2), Rational32(Ratio::new(10, 1))], 0, 1),
            //     ],
            // ],
        },
    ]);
}

#[test]
fn variance() {
    let (e, amount) = (1, 2);
    let data = vec![
        TxData(1, 1, ":amount".to_string(), Number(5)),
        TxData(1, 2, ":amount".to_string(), Number(10)),
        TxData(1, 2, ":amount".to_string(), Number(10)),
        TxData(1, 1, ":amount".to_string(), Number(2)),
        TxData(1, 1, ":amount".to_string(), Number(4)),
        TxData(1, 1, ":amount".to_string(), Number(6)),
    ];

    run_cases(vec![
        Case {
            description: "[:find (variance ?amount) :where [?e :amount ?amount]]",
            plan: Plan::Aggregate(Aggregate {
                variables: vec![amount],
                plan: Box::new(Plan::Project(Project {
                    variables: vec![amount],
                    plan: Box::new(Plan::MatchA(e, ":amount".to_string(), amount)),
                })),
                aggregation_fns: vec![AggregationFn::VARIANCE],
                key_variables: vec![],
                aggregation_variables: vec![amount],
                with_variables: vec![],
            }),
            transactions: vec![data.clone()],
            expectations: vec![vec![(vec![Rational32(Ratio::new(317, 36))], 0, 1)]],
            // set-semantics
            // expectations: vec![
            //     vec![(vec![Rational32(Ratio::new(176, 25))], 0, 1)],
            // ],
        },
        Case {
            description: "[:find ?e (variance ?amount) :where [?e :amount ?amount]]",
            plan: Plan::Aggregate(Aggregate {
                variables: vec![e, amount],
                plan: Box::new(Plan::MatchA(e, ":amount".to_string(), amount)),
                aggregation_fns: vec![AggregationFn::VARIANCE],
                key_variables: vec![e],
                aggregation_variables: vec![amount],
                with_variables: vec![],
            }),
            transactions: vec![data.clone()],
            expectations: vec![vec![
                (vec![Eid(1), Rational32(Ratio::new(35, 16))], 0, 1),
                (vec![Eid(2), Rational32(Ratio::new(0, 1))], 0, 1),
            ]],
        },
    ]);
}

#[test]
fn median() {
    let (e, amount) = (1, 2);
    let data = vec![
        TxData(1, 1, ":amount".to_string(), Number(5)),
        TxData(1, 2, ":amount".to_string(), Number(10)),
        TxData(1, 2, ":amount".to_string(), Number(10)),
        TxData(1, 1, ":amount".to_string(), Number(2)),
        TxData(1, 1, ":amount".to_string(), Number(4)),
        TxData(1, 1, ":amount".to_string(), Number(6)),
    ];

    run_cases(vec![
        Case {
            description: "[:find (median ?amount) :where [?e :amount ?amount]]",
            plan: Plan::Aggregate(Aggregate {
                variables: vec![amount],
                plan: Box::new(Plan::Project(Project {
                    variables: vec![amount],
                    plan: Box::new(Plan::MatchA(e, ":amount".to_string(), amount)),
                })),
                aggregation_fns: vec![AggregationFn::MEDIAN],
                key_variables: vec![],
                aggregation_variables: vec![amount],
                with_variables: vec![],
            }),
            transactions: vec![data.clone()],
            expectations: vec![vec![(vec![Number(5)], 0, 1)]],
        },
        Case {
            description: "[:find ?e (median ?amount) :where [?e :amount ?amount]]",
            plan: Plan::Aggregate(Aggregate {
                variables: vec![e, amount],
                plan: Box::new(Plan::MatchA(e, ":amount".to_string(), amount)),
                aggregation_fns: vec![AggregationFn::MEDIAN],
                key_variables: vec![e],
                aggregation_variables: vec![amount],
                with_variables: vec![],
            }),
            transactions: vec![data.clone()],
            expectations: vec![vec![
                (vec![Eid(1), Number(5)], 0, 1),
                (vec![Eid(2), Number(10)], 0, 1),
            ]],
        },
    ]);
}

#[test]
fn multiple_aggregations() {
    run_cases(vec![
        Case {
            description:
            "[:find (max ?amount) (min ?debt) (sum ?amount) (avg ?debt) \
             :where [?e :amount ?amount][?e :debt ?debt]]",
            plan: {
                let (e, amount, debt) = (1, 2, 3);
                Plan::Aggregate(Aggregate {
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
                    key_variables: vec![],
                    aggregation_variables: vec![amount, debt, amount, debt],
                    with_variables: vec![],
                })
            },
            transactions: vec![
                vec![
                    TxData(1, 1, ":amount".to_string(), Number(5)),
                    TxData(1, 1, ":amount".to_string(), Number(2)),
                    TxData(1, 1, ":amount".to_string(), Number(6)),
                    TxData(1, 1, ":amount".to_string(), Number(9)),
                    TxData(1, 1, ":amount".to_string(), Number(10)),
                    TxData(1, 1, ":debt".to_string(), Number(13)),
                    TxData(1, 1, ":debt".to_string(), Number(4)),
                    TxData(1, 1, ":debt".to_string(), Number(9)),
                    TxData(1, 1, ":debt".to_string(), Number(15)),
                    TxData(1, 1, ":debt".to_string(), Number(10)),
                    TxData(1, 2, ":amount".to_string(), Number(2)),
                    TxData(1, 2, ":amount".to_string(), Number(4)),
                    TxData(1, 2, ":debt".to_string(), Number(5)),
                    TxData(1, 2, ":debt".to_string(), Number(42)),
                ],
            ],
            expectations: vec![
                vec![
                    (vec![Number(10), Number(4), Number(172), Rational32(Ratio::new(349, 29))], 0, 1),
                ],
            ],
            // set-semantics
            // expectations: vec![
            //     vec![
            //         (vec![Number(10), Number(4), Number(36), Rational32(Ratio::new(14, 1))], 0, 1),
            //     ],
            // ],
        },
        Case {
            description:
            "[:find ?e (min ?amount) (max ?amount) (median ?amount) (count ?amount) (min ?debt) (max ?debt) (median ?debt) (count ?debt) \
             :where [?e :amount ?amount][?e :debt ?debt]]",
            plan: {
                let (e, amount, debt) = (1, 2, 3);
                Plan::Aggregate(Aggregate {
                    variables: vec![e, amount, amount, amount, amount, debt, debt, debt, debt],
                    plan: Box::new(Plan::Project(Project {
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
                    key_variables: vec![e],
                    aggregation_variables: vec![amount, amount, amount, amount, debt, debt, debt, debt],
                    with_variables: vec![],
                })
            },
            transactions: vec![
                vec![
                    TxData(1, 1, ":amount".to_string(), Number(5)),
                    TxData(1, 1, ":amount".to_string(), Number(2)),
                    TxData(1, 1, ":amount".to_string(), Number(6)),
                    TxData(1, 1, ":amount".to_string(), Number(9)),
                    TxData(1, 1, ":amount".to_string(), Number(10)),
                    TxData(1, 1, ":debt".to_string(), Number(13)),
                    TxData(1, 1, ":debt".to_string(), Number(4)),
                    TxData(1, 1, ":debt".to_string(), Number(9)),
                    TxData(1, 1, ":debt".to_string(), Number(15)),
                    TxData(1, 1, ":debt".to_string(), Number(10)),
                    TxData(1, 2, ":amount".to_string(), Number(2)),
                    TxData(1, 2, ":amount".to_string(), Number(4)),
                    TxData(1, 2, ":debt".to_string(), Number(5)),
                    TxData(1, 2, ":debt".to_string(), Number(42)),
                ],
            ],
            expectations: vec![
                vec![
                    (vec![Eid(1), Number(2), Number(10), Number(6), Number(25), Number(4), Number(15), Number(10), Number(25)], 0, 1),
                    (vec![Eid(2), Number(2), Number(4), Number(4), Number(4), Number(5), Number(42), Number(42), Number(4)], 0, 1),
                ],
            ],
            // set-semantics
            // expectations: vec![
            //     vec![
            //         (vec![Eid(1), Number(2), Number(10), Number(6), Number(5), Number(4), Number(15), Number(10), Number(5)], 0, 1),
            //         (vec![Eid(2), Number(2), Number(4), Number(4), Number(2), Number(5), Number(42), Number(42), Number(2)], 0, 1),
            //     ],
            // ],
        },
        Case {
            description:
            "[:find (sum ?heads) \
             :with ?monster \
             :where [?e :monster ?monster] [?e :head ?head]]",
            plan: {
                let (e, monster, heads) = (1, 2, 3);
                Plan::Aggregate(Aggregate {
                    variables: vec![heads],
                    plan: Box::new(Plan::Project(Project {
                        variables: vec![heads, monster],
                        plan: Box::new(Plan::Join(Join {
                            variables: vec![e],
                            left_plan: Box::new(Plan::MatchA(e, ":monster".to_string(), monster)),
                            right_plan: Box::new(Plan::MatchA(e, ":heads".to_string(), heads)),
                        })),
                    })),
                    aggregation_fns: vec![AggregationFn::SUM],
                    key_variables: vec![],
                    aggregation_variables: vec![heads],
                    with_variables: vec![monster],
                })
            },
            transactions: vec![
                vec![
                    TxData(1, 1, ":monster".to_string(), String("Cerberus".to_string())),
                    TxData(1, 1, ":heads".to_string(), Number(3)),
                    TxData(1, 2, ":monster".to_string(), String("Medusa".to_string())),
                    TxData(1, 2, ":heads".to_string(), Number(1)),
                    TxData(1, 3, ":monster".to_string(), String("Cyclops".to_string())),
                    TxData(1, 3, ":heads".to_string(), Number(1)),
                    TxData(1, 4, ":monster".to_string(), String("Chimera".to_string())),
                    TxData(1, 4, ":heads".to_string(), Number(1)),
                ],
            ],
            expectations: vec![
                vec![(vec![Number(6)], 0, 1)],
            ],
        },
    ]);
}
