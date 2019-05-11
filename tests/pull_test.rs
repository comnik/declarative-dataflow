use std::collections::HashSet;
use std::iter::FromIterator;
use std::sync::mpsc::channel;
use std::time::Duration;

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Operator;

use declarative_dataflow::plan::{Dependencies, Implementable, Pull, PullLevel};
use declarative_dataflow::server::{Register, Server};
use declarative_dataflow::{AttributeConfig, InputSemantics, Plan, Rule, TxData, Value};
use Value::{Aid, Bool, Eid, Number, String};

struct Case {
    description: &'static str,
    plan: Plan,
    root_plan: Option<Plan>,
    transactions: Vec<Vec<TxData>>,
    expectations: Vec<Vec<(Vec<Value>, u64, isize)>>,
}

fn run_cases(mut cases: Vec<Case>) {
    for case in cases.drain(..) {
        timely::execute_directly(move |worker| {
            let mut server = Server::<u64, u64>::new(Default::default());
            let (send_results, results) = channel();

            dbg!(case.description);

            let mut deps = case.plan.dependencies();
            let plan = case.plan.clone();
            let root_plan = case.root_plan.clone();

            dbg!(&plan);

            if let Some(ref root_plan) = root_plan {
                deps = Dependencies::merge(deps, root_plan.dependencies());
            }

            for tx in case.transactions.iter() {
                for datum in tx {
                    deps.attributes.insert(datum.2.clone());
                }
            }

            worker.dataflow::<u64, _, _>(|scope| {
                for dep in deps.attributes.iter() {
                    let mut config = AttributeConfig::tx_time(InputSemantics::Raw);
                    // @TODO get rid of this eventually, we only need delta queries
                    config.enable_wco = true;

                    server
                        .context
                        .internal
                        .create_transactable_attribute(dep, config, scope)
                        .unwrap();
                }

                if let Some(root_plan) = root_plan {
                    server
                        .register(Register {
                            rules: vec![Rule {
                                name: "hero".to_string(),
                                plan: root_plan,
                            }],
                            publish: vec!["hero".to_string()],
                        })
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
fn pull_level() {
    run_cases(vec![Case {
        description: "[:find (pull ?e [:name :age]) :where [?e :admin? false]]",
        plan: Plan::PullLevel(PullLevel {
            variables: vec![],
            pull_variable: 0,
            plan: Box::new(Plan::MatchAV(0, "admin?".to_string(), Bool(false))),
            pull_attributes: vec!["name".to_string(), "age".to_string()],
            path_attributes: vec![],
        }),
        root_plan: None,
        transactions: vec![vec![
            TxData::add(100, "admin?", Bool(true)),
            TxData::add(200, "admin?", Bool(false)),
            TxData::add(300, "admin?", Bool(false)),
            TxData::add(100, "name", String("Mabel".to_string())),
            TxData::add(200, "name", String("Dipper".to_string())),
            TxData::add(300, "name", String("Soos".to_string())),
            TxData::add(100, "age", Number(12)),
            TxData::add(200, "age", Number(13)),
        ]],
        expectations: vec![vec![
            (vec![Eid(200), Aid("age".to_string()), Number(13)], 0, 1),
            (
                vec![
                    Eid(200),
                    Aid("name".to_string()),
                    String("Dipper".to_string()),
                ],
                0,
                1,
            ),
            (
                vec![
                    Eid(300),
                    Aid("name".to_string()),
                    String("Soos".to_string()),
                ],
                0,
                1,
            ),
        ]],
    }]);
}

#[test]
fn pull_children() {
    let (parent, child) = (1, 2);

    run_cases(vec![Case {
        description: "[:find (pull ?child [:name :age]) :where [_ :parent/child ?child]]",
        plan: Plan::PullLevel(PullLevel {
            variables: vec![],
            pull_variable: child,
            plan: Box::new(Plan::MatchA(parent, "parent/child".to_string(), child)),
            pull_attributes: vec!["name".to_string(), "age".to_string()],
            path_attributes: vec!["parent/child".to_string()],
        }),
        root_plan: None,
        transactions: vec![vec![
            TxData::add(100, "name", String("Alice".to_string())),
            TxData::add(100, "parent/child", Eid(300)),
            TxData::add(200, "name", String("Bob".to_string())),
            TxData::add(200, "parent/child", Eid(400)),
            TxData::add(300, "name", String("Mabel".to_string())),
            TxData::add(300, "age", Number(13)),
            TxData::add(400, "name", String("Dipper".to_string())),
            TxData::add(400, "age", Number(12)),
        ]],
        expectations: vec![vec![
            (
                vec![
                    Eid(100),
                    Aid("parent/child".to_string()),
                    Eid(300),
                    Aid("age".to_string()),
                    Number(13),
                ],
                0,
                1,
            ),
            (
                vec![
                    Eid(100),
                    Aid("parent/child".to_string()),
                    Eid(300),
                    Aid("name".to_string()),
                    String("Mabel".to_string()),
                ],
                0,
                1,
            ),
            (
                vec![
                    Eid(200),
                    Aid("parent/child".to_string()),
                    Eid(400),
                    Aid("age".to_string()),
                    Number(12),
                ],
                0,
                1,
            ),
            (
                vec![
                    Eid(200),
                    Aid("parent/child".to_string()),
                    Eid(400),
                    Aid("name".to_string()),
                    String("Dipper".to_string()),
                ],
                0,
                1,
            ),
        ]],
    }]);
}

#[test]
fn pull() {
    let (a, b, c) = (1, 2, 3);

    run_cases(vec![Case {
        description:
            "[:find (pull ?a [:name {:join/binding #:pattern[e a v]}]) :where [?a :join/binding]]",
        root_plan: None,
        plan: Plan::Pull(Pull {
            variables: vec![],
            paths: vec![
                PullLevel {
                    variables: vec![],
                    pull_variable: b,
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
                    pull_variable: c,
                    plan: Box::new(Plan::MatchA(a, "name".to_string(), c)),
                    pull_attributes: vec![],
                    path_attributes: vec!["name".to_string()],
                },
            ],
        }),
        transactions: vec![vec![
            TxData::add(100, "name", String("rule".to_string())),
            TxData::add(100, "join/binding", Eid(200)),
            TxData::add(100, "join/binding", Eid(300)),
            TxData::add(200, "pattern/a", Aid("xyz".to_string())),
            TxData::add(300, "pattern/e", Eid(12345)),
            TxData::add(300, "pattern/a", Aid("asd".to_string())),
        ]],
        expectations: vec![vec![
            (
                vec![
                    Eid(100),
                    Aid("name".to_string()),
                    String("rule".to_string()),
                ],
                0,
                1,
            ),
            (
                vec![
                    Eid(100),
                    Aid("join/binding".to_string()),
                    Eid(200),
                    Aid("pattern/a".to_string()),
                    Aid("xyz".to_string()),
                ],
                0,
                1,
            ),
            (
                vec![
                    Eid(100),
                    Aid("join/binding".to_string()),
                    Eid(300),
                    Aid("pattern/e".to_string()),
                    Eid(12345),
                ],
                0,
                1,
            ),
            (
                vec![
                    Eid(100),
                    Aid("join/binding".to_string()),
                    Eid(300),
                    Aid("pattern/a".to_string()),
                    Aid("asd".to_string()),
                ],
                0,
                1,
            ),
        ]],
    }]);
}

#[cfg(feature = "graphql")]
#[test]
#[rustfmt::skip]
fn graph_ql() {
    use declarative_dataflow::plan::GraphQl;

    let transactions = vec![vec![
        TxData::add(100, "hero", Eid(100)),
        TxData::add(100, "hero", Eid(200)),
        TxData::add(100, "hero", Eid(300)),
        TxData::add(100, "hero", Eid(400)),
        
        TxData::add(100, "name", Value::from("Alice")),
        TxData::add(200, "name", Value::from("Bob")),
        TxData::add(300, "name", Value::from("Mabel")),
        TxData::add(400, "name", Value::from("Dipper")),
        
        TxData::add(300, "bested", Eid(400)),
        TxData::add(200, "bested", Eid(100)),

        TxData::add(300, "age", Number(13)),
        TxData::add(400, "age", Number(12)),
    ]];

    run_cases(vec![
        {
            let q = "{hero {name age height mass}}";

            let expectations = vec![vec![
                (vec![Eid(100), Value::aid("hero"), Eid(100), Value::aid("name"), Value::from("Alice")], 0, 1),
                (vec![Eid(100), Value::aid("hero"), Eid(200), Value::aid("name"), Value::from("Bob")], 0, 1),
                (vec![Eid(100), Value::aid("hero"), Eid(300), Value::aid("name"), Value::from("Mabel")], 0, 1),
                (vec![Eid(100), Value::aid("hero"), Eid(400), Value::aid("name"), Value::from("Dipper")], 0, 1),
                
                (vec![Eid(100), Value::aid("hero"), Eid(300), Value::aid("age"), Number(13)], 0, 1),
                (vec![Eid(100), Value::aid("hero"), Eid(400), Value::aid("age"), Number(12)], 0, 1),
            ]];
            
            Case {
                description: q,
                plan: Plan::GraphQl(GraphQl::new(q.to_string())),
                root_plan: Some(Plan::MatchA(0, "hero".to_string(), 1)),
                transactions: transactions.clone(),
                expectations,
            }
        },
        {
            let q = "{hero {name bested { name }}}";
            
            let expectations = vec![vec![
                (vec![Eid(100), Value::aid("hero"), Eid(100), Value::aid("name"), Value::from("Alice")], 0, 1),
                (vec![Eid(100), Value::aid("hero"), Eid(200), Value::aid("name"), Value::from("Bob")], 0, 1),
                (vec![Eid(100), Value::aid("hero"), Eid(300), Value::aid("name"), Value::from("Mabel")], 0, 1),
                (vec![Eid(100), Value::aid("hero"), Eid(400), Value::aid("name"), Value::from("Dipper")], 0, 1),

                (vec![Eid(100), Value::aid("hero"), Eid(300), Value::aid("bested"), Eid(400), Value::aid("name"), Value::from("Dipper")], 0, 1),
                (vec![Eid(100), Value::aid("hero"), Eid(200), Value::aid("bested"), Eid(100), Value::aid("name"), Value::from("Alice")], 0, 1),
            ]];
            
            Case {
                description: q,
                plan: Plan::GraphQl(GraphQl::new(q.to_string())),
                root_plan: Some(Plan::MatchA(0, "hero".to_string(), 1)),
                transactions: transactions.clone(),
                expectations,
            }
        },
        {
            let q = "{hero(name: \"Mabel\") {bested { name age }}}";

            let expectations = vec![vec![
                (vec![Eid(100), Value::aid("hero"), Eid(300),
                      Value::aid("bested"), Eid(400),
                      Value::aid("name"), Value::from("Dipper")
                ], 0, 1),
            ]];

            Case {
                description: q,
                plan: Plan::GraphQl(GraphQl::new(q.to_string())),
                root_plan: Some(Plan::MatchA(0, "hero".to_string(), 1)),
                transactions: transactions.clone(),
                expectations,
            }
        },
        {
            let q = "{hero {bested(name: \"Alice\") { name age }}}";

            let expectations = vec![vec![
                (vec![Eid(100), Value::aid("hero"), Eid(300),
                      Value::aid("bested"), Eid(400),
                      Value::aid("name"), Value::from("Dipper")
                ], 0, 1),
            ]];

            Case {
                description: q,
                plan: Plan::GraphQl(GraphQl::new(q.to_string())),
                root_plan: Some(Plan::MatchA(0, "hero".to_string(), 1)),
                transactions: transactions.clone(),
                expectations,
            }
        }
    ]);
}
