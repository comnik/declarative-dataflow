use std::collections::HashSet;
use std::iter::FromIterator;
use std::sync::mpsc::channel;
use std::time::Duration;

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Operator;

use declarative_dataflow::binding::BinaryPredicate::LT;
use declarative_dataflow::binding::{AsBinding, Binding};
use declarative_dataflow::plan::hector::{plan_order, source_conflicts};
use declarative_dataflow::plan::{Hector, Implementable};
use declarative_dataflow::server::Server;
use declarative_dataflow::timestamp::Time;
use declarative_dataflow::{Aid, Datom, Plan, Rule, Value};
use declarative_dataflow::{AttributeConfig, IndexDirection, QuerySupport};
use Value::{Bool, Eid, Number, String};

struct Case {
    description: &'static str,
    plan: Hector,
    transactions: Vec<Vec<Datom<Aid>>>,
    expectations: Vec<Vec<(Vec<Value>, u64, isize)>>,
}

/// Ensures bindings report correct dependencies before being asked to
/// extend a prefix.
#[test]
fn binding_requirements() {
    let (a, b, c, d) = (0, 1, 2, 3);

    assert_eq!(
        Binding::attribute(a, ":edge", b).required_to_extend(&vec![a, c], d),
        None
    );
    assert_eq!(
        Binding::attribute(a, ":edge", b).required_to_extend(&vec![a, c], b),
        Some(None)
    );
    assert_eq!(
        Binding::attribute(a, ":edge", b).required_to_extend(&vec![c, d], a),
        Some(Some(b))
    );
    assert_eq!(
        Binding::attribute(a, ":edge", b).required_to_extend(&vec![c, d], b),
        Some(Some(a))
    );
}

/// Ensures bindings honor the correct dependencies before offering to
/// extend a prefix to a new variable.
#[test]
fn binding_readiness() {
    let (a, b, c, d) = (0, 1, 2, 3);

    assert_eq!(
        Binding::constant(a, Eid(100)).ready_to_extend(&vec![a, b]),
        None
    );
    assert_eq!(
        Binding::constant(a, Eid(100)).ready_to_extend(&vec![c, d]),
        Some(a)
    );
    assert_eq!(
        Binding::attribute(a, ":edge", b).ready_to_extend(&vec![c, d]),
        None
    );
    assert_eq!(
        Binding::attribute(a, ":edge", b).ready_to_extend(&vec![a, c]),
        Some(b)
    );
    assert_eq!(
        Binding::attribute(a, ":edge", b).ready_to_extend(&vec![c, a]),
        Some(b)
    );
    assert_eq!(
        Binding::attribute(a, ":edge", b).ready_to_extend(&vec![c, b]),
        Some(a)
    );
    assert_eq!(
        Binding::attribute(a, ":edge", b).ready_to_extend(&vec![b, c]),
        Some(a)
    );
}

/// Ensures that conflicts involving the source binding are identified
/// correctly.
#[test]
fn conflicts() {
    let (e, c, e2, a, n) = (0, 1, 2, 3, 4);
    let bindings = vec![
        Binding::attribute(e2, ":age", a),
        Binding::attribute(e, ":age", a),
        Binding::attribute(e, ":name", c),
        Binding::attribute(e2, ":name", n),
        Binding::constant(c, String("Ivan".to_string())),
        Binding::not(Binding::constant(c, String("Petr".to_string()))),
    ];

    assert_eq!(source_conflicts(0, &bindings), Vec::<&Binding>::new());
    assert_eq!(
        source_conflicts(2, &bindings),
        vec![
            &Binding::constant(c, String("Ivan".to_string())),
            &Binding::not(Binding::constant(c, String("Petr".to_string()))),
        ]
    );
}

/// Ensures that a valid variable order is chosen depending on the
/// current source binding.
#[test]
fn ordering() {
    let (e, c, e2, a, n) = (0, 1, 2, 3, 4);
    let bindings = vec![
        Binding::attribute(e2, ":age", a),
        Binding::attribute(e, ":age", a),
        Binding::attribute(e, ":name", c),
        Binding::attribute(e2, ":name", n),
        Binding::constant(c, String("Ivan".to_string())),
    ];

    {
        let (variable_order, binding_order) = plan_order(0, &bindings);

        assert_eq!(variable_order, vec![e2, a, e, n, c]);
        assert_eq!(
            binding_order,
            vec![
                Binding::attribute(e, ":age", a),
                Binding::attribute(e2, ":name", n),
                Binding::attribute(e, ":name", c),
                Binding::constant(c, String("Ivan".to_string())),
            ]
        );
    }
    {
        let (variable_order, binding_order) = plan_order(1, &bindings);

        assert_eq!(variable_order, vec![e, a, c, e2, n]);
        assert_eq!(
            binding_order,
            vec![
                Binding::attribute(e, ":name", c),
                Binding::attribute(e2, ":age", a),
                Binding::attribute(e2, ":name", n),
                Binding::constant(c, String("Ivan".to_string())),
            ]
        );
    }
    {
        let (variable_order, binding_order) = plan_order(2, &bindings);

        assert_eq!(variable_order, vec![e, c, a, e2, n]);
        assert_eq!(
            binding_order,
            vec![
                Binding::attribute(e, ":age", a),
                Binding::attribute(e2, ":age", a),
                Binding::attribute(e2, ":name", n),
                Binding::constant(c, String("Ivan".to_string())),
            ]
        );
    }
}

#[test]
fn run_hector_cases() {
    let mut cases: Vec<Case> = vec![
        Case {
            description: "[?e :name ?n]",
            plan: Hector {
                variables: vec![0, 1],
                bindings: vec![Binding::attribute(0, ":name", 1)],
            },
            transactions: vec![vec![
                Datom::add(1, ":name", String("Dipper".to_string())),
                Datom::add(2, ":name", String("Mabel".to_string())),
                Datom::add(3, ":name", String("Soos".to_string())),
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
                    Binding::attribute(0, ":name", 1),
                    Binding::constant(1, String("Dipper".to_string())),
                ],
            },
            transactions: vec![vec![
                Datom::add(1, ":name", String("Dipper".to_string())),
                Datom::add(2, ":name", String("Mabel".to_string())),
                Datom::add(3, ":name", String("Soos".to_string())),
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
                        Binding::attribute(e, ":name", n),
                        Binding::attribute(e, ":age", a),
                    ],
                },
                transactions: vec![vec![
                    Datom::add(1, ":name", String("Dipper".to_string())),
                    Datom::add(1, ":age", Number(12)),
                    Datom::add(2, ":name", String("Mabel".to_string())),
                    Datom::add(2, ":age", Number(13)),
                    Datom::add(3, ":name", String("Soos".to_string())),
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
                        Binding::attribute(a, "edge", b),
                        Binding::attribute(b, "edge", c),
                        Binding::attribute(a, "edge", c),
                    ],
                },
                transactions: vec![vec![
                    Datom::add(100, "edge", Eid(200)),
                    Datom::add(200, "edge", Eid(300)),
                    Datom::add(100, "edge", Eid(300)),
                    Datom::add(100, "edge", Eid(400)),
                    Datom::add(400, "edge", Eid(500)),
                    Datom::add(500, "edge", Eid(100)),
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
                        Binding::attribute(e, ":age", a),
                        Binding::attribute(e, ":name", b),
                        Binding::attribute(e, ":likes", c),
                        Binding::attribute(e, ":fears", d),
                    ],
                },
                transactions: vec![vec![
                    Datom::add(100, ":name", String("Dipper".to_string())),
                    Datom::add(100, ":age", Number(12)),
                    Datom::add(100, ":likes", Eid(200)),
                    Datom::add(100, ":fears", Eid(300)),
                    Datom::add(200, ":name", String("Mabel".to_string())),
                    Datom::add(200, ":age", Number(13)),
                    Datom::add(300, ":name", String("Soos".to_string())),
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
                    Binding::attribute(0, ":num", 1),
                    Binding::attribute(0, ":num", 2),
                    Binding::binary_predicate(LT, 1, 2),
                ],
            },
            transactions: vec![vec![
                Datom::add(100, ":num", Number(1)),
                Datom::add(100, ":num", Number(2)),
                Datom::add(100, ":num", Number(3)),
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
                    Binding::attribute(0, ":num", 1),
                    Binding::attribute(0, ":num", 2),
                    Binding::constant(3, Number(18)),
                    Binding::constant(1, Number(10)),
                    Binding::binary_predicate(LT, 2, 3),
                ],
            },
            transactions: vec![vec![
                Datom::add(100, ":num", Number(1)),
                Datom::add(100, ":num", Number(10)),
                Datom::add(100, ":num", Number(20)),
            ]],
            expectations: vec![vec![(
                vec![Eid(100), Number(10), Number(18), Number(20)],
                0,
                1,
            )]],
        },
        {
            let (e, n, a, admin) = (1, 2, 3, 4);
            Case {
                description:
                    "[?e :name ?n] [?e :age ?a] [?e :admin? ?admin] (constant ?admin true)",
                plan: Hector {
                    variables: vec![e, n, a, admin],
                    bindings: vec![
                        Binding::attribute(e, ":name", n),
                        Binding::attribute(e, ":age", a),
                        Binding::attribute(e, ":admin?", admin),
                        Binding::constant(admin, Bool(true)),
                    ],
                },
                transactions: vec![vec![
                    Datom::add(100, ":name", String("Dipper".to_string())),
                    Datom::add(100, ":age", Number(12)),
                    Datom::add(100, ":admin?", Bool(true)),
                    Datom::add(200, ":name", String("Mabel".to_string())),
                    Datom::add(100, ":age", Number(12)),
                    Datom::add(100, ":admin?", Bool(false)),
                ]],
                expectations: vec![vec![(
                    vec![
                        Eid(100),
                        String("Dipper".to_string()),
                        Number(12),
                        Bool(true),
                    ],
                    0,
                    1,
                )]],
            }
        },
    ];

    for case in cases.drain(..) {
        timely::execute_directly(move |worker| {
            let mut server = Server::<u64, u64>::new(Default::default());
            let (send_results, results) = channel();

            dbg!(case.description);

            let deps = case.plan.dependencies();
            let plan = Plan::Hector(case.plan.clone());

            worker.dataflow::<u64, _, _>(|scope| {
                for dep in deps.attributes.iter() {
                    let config = AttributeConfig {
                        trace_slack: Some(Time::TxId(1)),
                        query_support: QuerySupport::AdaptiveWCO,
                        index_direction: IndexDirection::Both,
                        ..Default::default()
                    };

                    server.create_attribute(scope, dep, config).unwrap();
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
                            eprint!("Missing results: {:?}", expected);
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
        });
    }
}
