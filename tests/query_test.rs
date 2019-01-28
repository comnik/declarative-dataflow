extern crate declarative_dataflow;
extern crate timely;

use std::collections::{HashMap, HashSet};
use std::iter::FromIterator;
use std::sync::mpsc::channel;
use std::time::Duration;

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Operator;
use timely::Configuration;

use declarative_dataflow::binding::Binding;
use declarative_dataflow::plan::{Filter, Implementable, Join, Predicate, Project};
use declarative_dataflow::server::{Server, Transact, TxData};
use declarative_dataflow::{Aid, Plan, Rule, Value};
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
        match binding {
            Binding::Attribute(binding) => {
                deps.insert(binding.source_attribute.clone());
            }
            _ => {}
        }
    }

    deps
}

#[test]
fn run_query_cases() {
    let mut cases = vec![
        Case {
            description: "[:find ?e ?n :where [?e :name ?n]]",
            plan: Plan::MatchA(0, ":name".to_string(), 1),
            transactions: vec![vec![
                TxData(1, 100, ":name".to_string(), String("Dipper".to_string())),
                TxData(1, 100, ":name".to_string(), String("Alias".to_string())),
                TxData(1, 200, ":name".to_string(), String("Mabel".to_string())),
            ]],
            expectations: vec![vec![
                (vec![Eid(100), String("Dipper".to_string())], 0, 1),
                (vec![Eid(100), String("Alias".to_string())], 0, 1),
                (vec![Eid(200), String("Mabel".to_string())], 0, 1),
            ]],
        },
        Case {
            description: "[:find ?n :where [1 :name ?n]]",
            plan: Plan::MatchEA(100, ":name".to_string(), 0),
            transactions: vec![vec![
                TxData(1, 100, ":name".to_string(), String("Dipper".to_string())),
                TxData(1, 100, ":name".to_string(), String("Alias".to_string())),
                TxData(1, 200, ":name".to_string(), String("Mabel".to_string())),
            ]],
            expectations: vec![vec![
                (vec![String("Alias".to_string())], 0, 1),
                (vec![String("Dipper".to_string())], 0, 1),
            ]],
        },
        Case {
            description: "[:find ?e :where [?e :name 'Mabel']]",
            plan: Plan::MatchAV(0, ":name".to_string(), String("Mabel".to_string())),
            transactions: vec![vec![
                TxData(1, 100, ":name".to_string(), String("Dipper".to_string())),
                TxData(1, 100, ":name".to_string(), String("Alias".to_string())),
                TxData(1, 200, ":name".to_string(), String("Mabel".to_string())),
            ]],
            expectations: vec![vec![(vec![Eid(200)], 0, 1)]],
        },
        {
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
                    TxData(1, 1, ":name".to_string(), String("Dipper".to_string())),
                    TxData(1, 1, ":age".to_string(), Number(12)),
                ]],
                expectations: vec![vec![(
                    vec![Eid(1), String("Dipper".to_string()), Number(12)],
                    0,
                    1,
                )]],
            }
        },
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
        //                 TxData(1, 100, ":name".to_string(), String("Dipper".to_string())),
        //                 TxData(1, 100, ":age".to_string(), Number(12)),
        //                 TxData(1, 100, ":name".to_string(), String("Soos".to_string())),
        //                 TxData(1, 100, ":age".to_string(), Number(30)),
        //             ],
        //         ],
        //         expectations: vec![
        //             vec![(vec![Eid(100), String("Dipper".to_string()), Number(12)], 0, 1)],
        //         ],
        //     }
        // },
    ];

    for case in cases.drain(..) {
        timely::execute(Configuration::Thread, move |worker| {
            let mut server = Server::<u64>::new(Default::default());
            let (send_results, results) = channel();

            dbg!(case.description);

            let deps = dependencies(&case);
            let plan = case.plan.clone();

            worker.dataflow::<u64, _, _>(|scope| {
                for dep in deps.iter() {
                    server.create_attribute(dep, scope);
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
        })
        .unwrap();
    }
}
