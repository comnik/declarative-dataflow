extern crate declarative_dataflow;
extern crate timely;

use std::collections::HashSet;
use std::iter::FromIterator;
use std::sync::mpsc::channel;
use std::time::Duration;

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Operator;
use timely::Configuration;

use declarative_dataflow::binding::BinaryPredicate::LT;
use declarative_dataflow::binding::{
    AttributeBinding, BinaryPredicateBinding, Binding, ConstantBinding,
};
use declarative_dataflow::plan::Hector;
use declarative_dataflow::server::{Server, Transact, TxData};
use declarative_dataflow::{Aid, Plan, Rule, Value};
use Binding::{Attribute, BinaryPredicate, Constant};
use Value::{Eid, Number, String};

struct Case {
    description: &'static str,
    plan: Hector,
    transactions: Vec<Vec<TxData>>,
    expectations: Vec<Vec<(Vec<Value>, u64, isize)>>,
}

fn dependencies(case: &Case) -> HashSet<Aid> {
    let mut deps = HashSet::new();

    for binding in case.plan.bindings.iter() {
        if let Attribute(binding) = binding {
            deps.insert(binding.source_attribute.clone());
        }
    }

    deps
}

#[test]
fn run_hector_cases() {
    let mut cases: Vec<Case> = vec![
        Case {
            description: "[?e :name ?n]",
            plan: Hector {
                variables: vec![0, 1],
                bindings: vec![Attribute(AttributeBinding {
                    symbols: (0, 1),
                    source_attribute: ":name".to_string(),
                })],
            },
            transactions: vec![vec![
                TxData(1, 1, ":name".to_string(), String("Dipper".to_string())),
                TxData(1, 2, ":name".to_string(), String("Mabel".to_string())),
                TxData(1, 3, ":name".to_string(), String("Soos".to_string())),
            ]],
            expectations: vec![vec![
                (vec![Eid(1), String("Dipper".to_string())], 0, 1),
                (vec![Eid(2), String("Mabel".to_string())], 0, 1),
                (vec![Eid(3), String("Soos".to_string())], 0, 1),
            ]],
        },
        Case {
            description: "[?e :name ?n] (constant ?n 'Dipper')",
            plan: Hector {
                variables: vec![0, 1],
                bindings: vec![
                    Attribute(AttributeBinding {
                        symbols: (0, 1),
                        source_attribute: ":name".to_string(),
                    }),
                    Constant(ConstantBinding {
                        symbol: 1,
                        value: String("Dipper".to_string()),
                    }),
                ],
            },
            transactions: vec![vec![
                TxData(1, 1, ":name".to_string(), String("Dipper".to_string())),
                TxData(1, 2, ":name".to_string(), String("Mabel".to_string())),
                TxData(1, 3, ":name".to_string(), String("Soos".to_string())),
            ]],
            expectations: vec![vec![(vec![Eid(1), String("Dipper".to_string())], 0, 1)]],
        },
        {
            let (e, a, n) = (1, 2, 3);
            Case {
                description: "[?e :age ?a] [?e :name ?n]",
                plan: Hector {
                    variables: vec![e, a, n],
                    bindings: vec![
                        Attribute(AttributeBinding {
                            symbols: (e, n),
                            source_attribute: ":name".to_string(),
                        }),
                        Attribute(AttributeBinding {
                            symbols: (e, a),
                            source_attribute: ":age".to_string(),
                        }),
                    ],
                },
                transactions: vec![vec![
                    TxData(1, 1, ":name".to_string(), String("Dipper".to_string())),
                    TxData(1, 1, ":age".to_string(), Number(12)),
                    TxData(1, 2, ":name".to_string(), String("Mabel".to_string())),
                    TxData(1, 2, ":age".to_string(), Number(13)),
                    TxData(1, 3, ":name".to_string(), String("Soos".to_string())),
                ]],
                expectations: vec![vec![
                    (vec![Eid(1), Number(12), String("Dipper".to_string())], 0, 1),
                    (vec![Eid(2), Number(13), String("Mabel".to_string())], 0, 1),
                ]],
            }
        },
        {
            let (a, b, c) = (1, 2, 3);
            Case {
                description: "[?a :edge ?b] [?b :edge ?c] [?a :edge ?c]",
                plan: Hector {
                    variables: vec![a, b, c],
                    bindings: vec![
                        Attribute(AttributeBinding {
                            symbols: (a, b),
                            source_attribute: "edge".to_string(),
                        }),
                        Attribute(AttributeBinding {
                            symbols: (b, c),
                            source_attribute: "edge".to_string(),
                        }),
                        Attribute(AttributeBinding {
                            symbols: (a, c),
                            source_attribute: "edge".to_string(),
                        }),
                    ],
                },
                transactions: vec![vec![
                    TxData(1, 100, "edge".to_string(), Eid(200)),
                    TxData(1, 200, "edge".to_string(), Eid(300)),
                    TxData(1, 100, "edge".to_string(), Eid(300)),
                    TxData(1, 100, "edge".to_string(), Eid(400)),
                    TxData(1, 400, "edge".to_string(), Eid(500)),
                    TxData(1, 500, "edge".to_string(), Eid(100)),
                ]],
                expectations: vec![vec![(vec![Eid(100), Eid(200), Eid(300)], 0, 1)]],
            }
        },
        {
            let (e, a, b, c, d) = (1, 2, 3, 4, 5);
            Case {
                description: "[?e :age ?a] [?e :name ?b] [?e :likes ?c] [?e :fears ?d]",
                plan: Hector {
                    variables: vec![e, a, b, c, d],
                    bindings: vec![
                        Attribute(AttributeBinding {
                            symbols: (e, a),
                            source_attribute: ":age".to_string(),
                        }),
                        Attribute(AttributeBinding {
                            symbols: (e, b),
                            source_attribute: ":name".to_string(),
                        }),
                        Attribute(AttributeBinding {
                            symbols: (e, c),
                            source_attribute: ":likes".to_string(),
                        }),
                        Attribute(AttributeBinding {
                            symbols: (e, d),
                            source_attribute: ":fears".to_string(),
                        }),
                    ],
                },
                transactions: vec![vec![
                    TxData(1, 100, ":name".to_string(), String("Dipper".to_string())),
                    TxData(1, 100, ":age".to_string(), Number(12)),
                    TxData(1, 100, ":likes".to_string(), Eid(200)),
                    TxData(1, 100, ":fears".to_string(), Eid(300)),
                    TxData(1, 200, ":name".to_string(), String("Mabel".to_string())),
                    TxData(1, 200, ":age".to_string(), Number(13)),
                    TxData(1, 300, ":name".to_string(), String("Soos".to_string())),
                ]],
                expectations: vec![vec![(
                    vec![
                        Eid(100),
                        Number(12),
                        String("Dipper".to_string()),
                        Eid(200),
                        Eid(300),
                    ],
                    0,
                    1,
                )]],
            }
        },
        Case {
            description: "[?a :num ?b] [?a :num ?c] (< ?b ?c)",
            plan: Hector {
                variables: vec![0, 1, 2],
                bindings: vec![
                    Attribute(AttributeBinding {
                        symbols: (0, 1),
                        source_attribute: ":num".to_string(),
                    }),
                    Attribute(AttributeBinding {
                        symbols: (0, 2),
                        source_attribute: ":num".to_string(),
                    }),
                    BinaryPredicate(BinaryPredicateBinding {
                        symbols: (1, 2),
                        predicate: LT,
                    }),
                ],
            },
            transactions: vec![vec![
                TxData(1, 100, ":num".to_string(), Number(1)),
                TxData(1, 100, ":num".to_string(), Number(2)),
                TxData(1, 100, ":num".to_string(), Number(3)),
            ]],
            expectations: vec![vec![
                (vec![Eid(100), Number(2), Number(1)], 0, 1),
                (vec![Eid(100), Number(3), Number(1)], 0, 1),
                (vec![Eid(100), Number(3), Number(2)], 0, 1),
            ]],
        },
        Case {
            description:
                "[?a :num ?b] [?a :num ?c] (< ?const0 ?c) (constant ?const0 18) (constant ?b 10)",
            plan: Hector {
                variables: vec![0, 1, 3, 2],
                bindings: vec![
                    Attribute(AttributeBinding {
                        symbols: (0, 1),
                        source_attribute: ":num".to_string(),
                    }),
                    Attribute(AttributeBinding {
                        symbols: (0, 2),
                        source_attribute: ":num".to_string(),
                    }),
                    Constant(ConstantBinding {
                        symbol: 3,
                        value: Number(18),
                    }),
                    Constant(ConstantBinding {
                        symbol: 1,
                        value: Number(10),
                    }),
                    BinaryPredicate(BinaryPredicateBinding {
                        symbols: (2, 3),
                        predicate: LT,
                    }),
                ],
            },
            transactions: vec![vec![
                TxData(1, 100, ":num".to_string(), Number(1)),
                TxData(1, 100, ":num".to_string(), Number(10)),
                TxData(1, 100, ":num".to_string(), Number(20)),
            ]],
            expectations: vec![vec![(
                vec![Eid(100), Number(10), Number(18), Number(20)],
                0,
                1,
            )]],
        },
    ];

    for case in cases.drain(..) {
        timely::execute(Configuration::Thread, move |worker| {
            let mut server = Server::<u64>::new(Default::default());
            let (send_results, results) = channel();

            dbg!(case.description);

            let deps = dependencies(&case);
            let plan = Plan::Hector(case.plan.clone());

            worker.dataflow::<u64, _, _>(|scope| {
                for dep in deps.iter() {
                    server.create_attribute(dep, scope).unwrap();
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

            for (tx_id, tx_data) in transactions.drain(..).enumerate() {
                let tx = Some(tx_id as u64);
                server.transact(Transact { tx, tx_data }, 0, 0);

                worker.step_while(|| server.is_any_outdated());

                let mut expected: HashSet<(Vec<Value>, u64, isize)> =
                    HashSet::from_iter(case.expectations[tx_id].iter().cloned());

                for _i in 0..expected.len() {
                    match results.recv_timeout(Duration::from_millis(400)) {
                        Err(_err) => {
                            eprint!("No result.");
                            dbg!(&case.plan.bindings);
                        }
                        Ok(result) => {
                            if !expected.remove(&result) {
                                eprint!("Unknown result {:?}.", result);
                                dbg!(&case.plan.bindings);
                            }
                        }
                    }
                }

                match results.recv_timeout(Duration::from_millis(400)) {
                    Err(_err) => {}
                    Ok(result) => {
                        eprint!("Extraneous result {:?}", result);
                        dbg!(&case.plan.bindings);
                    }
                }
            }
        })
        .unwrap();
    }
}
