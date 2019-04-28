use std::collections::HashSet;
use std::iter::FromIterator;
use std::sync::mpsc::channel;
use std::time::Duration;

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Operator;

use declarative_dataflow::binding::Binding;
use declarative_dataflow::plan::{Implementable, Join, Project};
use declarative_dataflow::server::Server;
use declarative_dataflow::{q, Aid, AttributeConfig, InputSemantics, Plan, Rule, TxData, Value};
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
                            AttributeConfig::tx_time(InputSemantics::CardinalityMany),
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
fn base_patterns() {
    let data = vec![
        TxData::add(100, ":name", String("Dipper".to_string())),
        TxData::add(100, ":name", String("Alias".to_string())),
        TxData::add(200, ":name", String("Mabel".to_string())),
    ];

    run_cases(vec![
        Case {
            description: "[:find ?e ?n :where [?e :name ?n]]",
            plan: Plan::MatchA(0, ":name".to_string(), 1),
            transactions: vec![data.clone()],
            expectations: vec![vec![
                (vec![Eid(100), String("Dipper".to_string())], 0, 1),
                (vec![Eid(100), String("Alias".to_string())], 0, 1),
                (vec![Eid(200), String("Mabel".to_string())], 0, 1),
            ]],
        },
        Case {
            description: "[:find ?n :where [100 :name ?n]]",
            plan: Plan::MatchEA(100, ":name".to_string(), 0),
            transactions: vec![data.clone()],
            expectations: vec![vec![
                (vec![String("Alias".to_string())], 0, 1),
                (vec![String("Dipper".to_string())], 0, 1),
            ]],
        },
        Case {
            description: "[:find ?e :where [?e :name Mabel]]",
            plan: Plan::MatchAV(0, ":name".to_string(), String("Mabel".to_string())),
            transactions: vec![data.clone()],
            expectations: vec![vec![(vec![Eid(200)], 0, 1)]],
        },
    ]);
}

#[test]
fn base_projections() {
    let data = vec![
        TxData::add(100, ":name", String("Dipper".to_string())),
        TxData::add(100, ":name", String("Alias".to_string())),
        TxData::add(200, ":name", String("Mabel".to_string())),
    ];

    run_cases(vec![
        Case {
            description: "[:find ?e :where [?e :name ?n]]",
            plan: Plan::Project(Project {
                variables: vec![0],
                plan: Box::new(Plan::MatchA(0, ":name".to_string(), 1)),
            }),
            transactions: vec![data.clone()],
            expectations: vec![vec![(vec![Eid(100)], 0, 2), (vec![Eid(200)], 0, 1)]],
        },
        Case {
            description: "[:find ?n :where [?e :name ?n]]",
            plan: Plan::Project(Project {
                variables: vec![1],
                plan: Box::new(Plan::MatchA(0, ":name".to_string(), 1)),
            }),
            transactions: vec![data.clone()],
            expectations: vec![vec![
                (vec![String("Dipper".to_string())], 0, 1),
                (vec![String("Alias".to_string())], 0, 1),
                (vec![String("Mabel".to_string())], 0, 1),
            ]],
        },
        Case {
            description: "[:find ?e ?n :where [?e :name ?n]]",
            plan: Plan::Project(Project {
                variables: vec![0, 1],
                plan: Box::new(Plan::MatchA(0, ":name".to_string(), 1)),
            }),
            transactions: vec![data.clone()],
            expectations: vec![vec![
                (vec![Eid(100), String("Dipper".to_string())], 0, 1),
                (vec![Eid(100), String("Alias".to_string())], 0, 1),
                (vec![Eid(200), String("Mabel".to_string())], 0, 1),
            ]],
        },
        Case {
            description: "[:find ?n ?e :where [?e :name ?n]]",
            plan: Plan::Project(Project {
                variables: vec![1, 0],
                plan: Box::new(Plan::MatchA(0, ":name".to_string(), 1)),
            }),
            transactions: vec![data.clone()],
            expectations: vec![vec![
                (vec![String("Dipper".to_string()), Eid(100)], 0, 1),
                (vec![String("Alias".to_string()), Eid(100)], 0, 1),
                (vec![String("Mabel".to_string()), Eid(200)], 0, 1),
            ]],
        },
    ]);
}

#[test]
fn wco_base_patterns() {
    let data = vec![
        TxData::add(100, ":name", String("Dipper".to_string())),
        TxData::add(100, ":name", String("Alias".to_string())),
        TxData::add(200, ":name", String("Mabel".to_string())),
    ];

    run_cases(vec![
        Case {
            description: "[:find ?e ?n :where [?e :name ?n]]",
            plan: q(vec![0, 1], vec![Binding::attribute(0, ":name", 1)]),
            transactions: vec![data.clone()],
            expectations: vec![vec![
                (vec![Eid(100), String("Dipper".to_string())], 0, 1),
                (vec![Eid(100), String("Alias".to_string())], 0, 1),
                (vec![Eid(200), String("Mabel".to_string())], 0, 1),
            ]],
        },
        Case {
            description: "[:find ?n :where [100 :name ?n]]",
            plan: q(
                vec![0, 1],
                vec![
                    Binding::attribute(0, ":name", 1),
                    Binding::constant(0, Eid(100)),
                ],
            ),
            transactions: vec![data.clone()],
            expectations: vec![vec![
                (vec![Eid(100), String("Alias".to_string())], 0, 1),
                (vec![Eid(100), String("Dipper".to_string())], 0, 1),
            ]],
        },
        Case {
            description: "[:find ?e :where [?e :name Mabel]]",
            plan: q(
                vec![0, 1],
                vec![
                    Binding::attribute(0, ":name", 1),
                    Binding::constant(1, String("Mabel".to_string())),
                ],
            ),
            transactions: vec![data.clone()],
            expectations: vec![vec![(vec![Eid(200), String("Mabel".to_string())], 0, 1)]],
        },
    ]);
}

#[test]
fn joins() {
    run_cases(vec![{
        let (e, a, n) = (1, 2, 3);
        Case {
            description: "[:find ?e ?n ?a :where [?e :age ?a] [?e :name ?n]]",
            plan: Plan::Project(Project {
                variables: vec![e, n, a],
                plan: Box::new(Plan::Join(Join {
                    variables: vec![e],
                    left_plan: Box::new(Plan::MatchA(e, ":name".to_string(), n)),
                    right_plan: Box::new(Plan::MatchA(e, ":age".to_string(), a)),
                })),
            }),
            transactions: vec![vec![
                TxData::add(1, ":name", String("Dipper".to_string())),
                TxData::add(1, ":age", Number(12)),
            ]],
            expectations: vec![vec![(
                vec![Eid(1), String("Dipper".to_string()), Number(12)],
                0,
                1,
            )]],
        }
    }]);
}

#[test]
fn wco_joins() {
    let data = vec![
        TxData::add(1, ":name", String("Ivan".to_string())),
        TxData::add(1, ":age", Number(15)),
        TxData::add(2, ":name", String("Petr".to_string())),
        TxData::add(2, ":age", Number(37)),
        TxData::add(3, ":name", String("Ivan".to_string())),
        TxData::add(3, ":age", Number(37)),
        TxData::add(4, ":age", Number(15)),
    ];

    run_cases(vec![
        Case {
            description: "[:find ?e :where [?e :name]]",
            plan: q(vec![0], vec![Binding::attribute(0, ":name", 1)]),
            transactions: vec![data.clone()],
            expectations: vec![vec![
                (vec![Eid(1)], 0, 1),
                (vec![Eid(2)], 0, 1),
                (vec![Eid(3)], 0, 1),
            ]],
        },
        Case {
            description: "[:find ?e ?v :where [?e :name Ivan] [?e :age ?v]]",
            plan: q(
                vec![0, 2],
                vec![
                    Binding::attribute(0, ":name", 1),
                    Binding::constant(1, String("Ivan".to_string())),
                    Binding::attribute(0, ":age", 2),
                ],
            ),
            transactions: vec![data.clone()],
            expectations: vec![vec![
                (vec![Eid(1), Number(15)], 0, 1),
                (vec![Eid(3), Number(37)], 0, 1),
            ]],
        },
        Case {
            description: "[:find ?e1 ?e2 :where [?e1 :name ?n] [?e2 :name ?n]]",
            plan: q(
                vec![0, 2],
                vec![
                    Binding::attribute(0, ":name", 1),
                    Binding::attribute(2, ":name", 1),
                ],
            ),
            transactions: vec![data.clone()],
            expectations: vec![vec![
                (vec![Eid(1), Eid(1)], 0, 1),
                (vec![Eid(2), Eid(2)], 0, 1),
                (vec![Eid(3), Eid(3)], 0, 1),
                (vec![Eid(1), Eid(3)], 0, 1),
                (vec![Eid(3), Eid(1)], 0, 1),
            ]],
        },
        {
            let (e, c, e2, a, n) = (0, 1, 2, 3, 4);
            Case {
                description: "[:find ?e ?e2 ?n :where [?e :name Ivan] [?e :age ?a] [?e2 :age ?a] [?e2 :name ?n]]",
                plan: q(vec![e, e2, n], vec![
                    Binding::attribute(e, ":name", c),
                    Binding::constant(c, String("Ivan".to_string())),
                    Binding::attribute(e, ":age", a),
                    Binding::attribute(e2, ":age", a),
                    Binding::attribute(e2, ":name", n),
                ]),
                transactions: vec![data.clone()],
                expectations: vec![vec![
                    (vec![Eid(1), Eid(1), String("Ivan".to_string())], 0, 1),
                    (vec![Eid(3), Eid(3), String("Ivan".to_string())], 0, 1),
                    (vec![Eid(3), Eid(2), String("Petr".to_string())], 0, 1),
                ]],
            }
        },
    ]);
}

#[test]
fn wco_join_many() {
    let data = vec![
        TxData::add(1, ":name", String("Ivan".to_string())),
        TxData::add(1, ":aka", String("ivolga".to_string())),
        TxData::add(1, ":aka", String("pi".to_string())),
        TxData::add(2, ":name", String("Petr".to_string())),
        TxData::add(2, ":aka", String("porosenok".to_string())),
        TxData::add(2, ":aka", String("pi".to_string())),
    ];

    let (e1, x, e2, n1, n2) = (0, 1, 2, 3, 4);

    run_cases(vec![Case {
        description:
            "[:find ?n1 ?n2 :where [?e1 :aka ?x] [?e2 :aka ?x] [?e1 :name ?n1] [?e2 :name ?n2]]",
        plan: q(
            vec![n1, n2],
            vec![
                Binding::attribute(e1, ":aka", x),
                Binding::attribute(e2, ":aka", x),
                Binding::attribute(e1, ":name", n1),
                Binding::attribute(e2, ":name", n2),
            ],
        ),
        transactions: vec![data.clone()],
        expectations: vec![vec![
            (
                vec![String("Ivan".to_string()), String("Ivan".to_string())],
                0,
                2,
            ),
            (
                vec![String("Petr".to_string()), String("Petr".to_string())],
                0,
                2,
            ),
            (
                vec![String("Ivan".to_string()), String("Petr".to_string())],
                0,
                1,
            ),
            (
                vec![String("Petr".to_string()), String("Ivan".to_string())],
                0,
                1,
            ),
        ]],
    }]);
}

// @TODO
// {
//     let (e, a, n) = (1, 2, 3);

//     let mut constants: HashMap<u32, Value> = HashMap::new();
//     constants.insert(0, Number(18));

//     Case {
//         description: "[:find ?e ?n ?a :where [?e :age ?a] [?e :name ?n] [(<= 18 ?a)]]",
//         plan: Plan::Project(Project {
//             variables: vec![e, n, a],
//             plan: Box::new(Plan::Join(Join {
//                 variables: vec![e],
//                 left_plan: Box::new(Plan::MatchA(e, ":name".to_string(), n)),
//                 right_plan: Box::new(Plan::Filter(Filter {
//                     variables: vec![a],
//                     predicate: Predicate::LTE,
//                     plan: Box::new(Plan::MatchA(e, ":age".to_string(), a)),
//                     constants: constants,
//                 })),
//             })),
//         }),
//         transactions: vec![
//             vec![
//                 TxData::add(100, ":name", String("Dipper".to_string())),
//                 TxData::add(100, ":age", Number(12)),
//                 TxData::add(100, ":name", String("Soos".to_string())),
//                 TxData::add(100, ":age", Number(30)),
//             ],
//         ],
//         expectations: vec![
//             vec![(vec![Eid(100), String("Dipper".to_string()), Number(12)], 0, 1)],
//         ],
//     }
// },
