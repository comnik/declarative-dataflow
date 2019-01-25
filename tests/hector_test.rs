extern crate declarative_dataflow;
extern crate timely;

use std::collections::HashSet;
use std::iter::FromIterator;
use std::time::Duration;
use std::sync::mpsc::channel;

use timely::Configuration;
use timely::dataflow::operators::Operator;
use timely::dataflow::channels::pact::Pipeline;

use declarative_dataflow::plan::Hector;
use declarative_dataflow::binding::{Binding, AttributeBinding, ConstantBinding};
use declarative_dataflow::server::{Server, Transact, TxData};
use declarative_dataflow::{Plan, Rule, Value, Aid};
use Value::{Eid, Number, String};

struct Case {
    description: &'static str,
    plan: Hector,
    transactions: Vec<Vec<TxData>>,
    expectations: Vec<Vec<(Vec<Value>, u64, isize)>>
}

fn dependencies(case: &Case) -> HashSet<Aid> {
    let mut deps = HashSet::new();

    for binding in case.plan.bindings.iter() {
        match binding {
            Binding::Attribute(binding) => { deps.insert(binding.source_attribute.clone()); }
            _ => {}
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
                bindings: vec![
                    Binding::Attribute(AttributeBinding { symbols: (0,1), source_attribute: ":name".to_string() })
                ],
            },
            transactions: vec![
                vec![
                    TxData(1, 1, ":name".to_string(), String("Dipper".to_string())),
                    TxData(1, 2, ":name".to_string(), String("Mabel".to_string())),
                    TxData(1, 3, ":name".to_string(), String("Soos".to_string())),
                ],
            ],
            expectations: vec![
                vec![
                    (vec![Eid(1), String("Dipper".to_string())], 0, 1),
                    (vec![Eid(2), String("Mabel".to_string())], 0, 1),
                    (vec![Eid(3), String("Soos".to_string())], 0, 1),
                ],
            ],
        },
        Case {
            description: "[?e :name ?n] (constant ?n 'Dipper')",
            plan: Hector {
                variables: vec![0, 1],
                bindings: vec![
                    Binding::Attribute(AttributeBinding { symbols: (0,1), source_attribute: ":name".to_string() }),
                    Binding::Constant(ConstantBinding { symbol: 1, value: String("Dipper".to_string()) }),
                ],
            },
            transactions: vec![
                vec![
                    TxData(1, 1, ":name".to_string(), String("Dipper".to_string())),
                    TxData(1, 2, ":name".to_string(), String("Mabel".to_string())),
                    TxData(1, 3, ":name".to_string(), String("Soos".to_string())),
                ],
            ],
            expectations: vec![
                vec![(vec![Eid(1), String("Dipper".to_string())], 0, 1)],
            ],
        },
        {
            let (e, a, n) = (1, 2, 3);
            Case {
                description: "[?e :age ?a] [?e :name ?n]",
                plan: Hector {
                    variables: vec![e, a, n],
                    bindings: vec![
                        Binding::Attribute(AttributeBinding { symbols: (e,n), source_attribute: ":name".to_string() }),
                        Binding::Attribute(AttributeBinding { symbols: (e,a), source_attribute: ":age".to_string() }),
                    ]
                },
                transactions: vec![
                    vec![
                        TxData(1, 1, ":name".to_string(), String("Dipper".to_string())),
                        TxData(1, 1, ":age".to_string(), Number(12)),
                        TxData(1, 2, ":name".to_string(), String("Mabel".to_string())),
                        TxData(1, 2, ":age".to_string(), Number(13)),
                        TxData(1, 3, ":name".to_string(), String("Soos".to_string())),
                    ],
                ],
                expectations: vec![
                    vec![
                        (vec![Eid(1), Number(12), String("Dipper".to_string())], 0, 1),
                        (vec![Eid(2), Number(13), String("Mabel".to_string())], 0, 1),
                    ],
                ],
            }
        },
        {
            let (a,b,c) = (1,2,3);
            Case {
                description: "[?a :edge ?b] [?b :edge ?c] [?a :edge ?c]",
                plan: Hector {
                    variables: vec![a, b, c],
                    bindings: vec![
                        Binding::Attribute(AttributeBinding { symbols: (a,b), source_attribute: "edge".to_string() }),
                        Binding::Attribute(AttributeBinding { symbols: (b,c), source_attribute: "edge".to_string() }),
                        Binding::Attribute(AttributeBinding { symbols: (a,c), source_attribute: "edge".to_string() }),
                    ]
                },
                transactions: vec![
                    vec![
                        TxData(1, 100, "edge".to_string(), Eid(200)),
                        TxData(1, 200, "edge".to_string(), Eid(300)),
                        TxData(1, 100, "edge".to_string(), Eid(300)),
                        TxData(1, 100, "edge".to_string(), Eid(400)),
                        TxData(1, 400, "edge".to_string(), Eid(500)),
                        TxData(1, 500, "edge".to_string(), Eid(100)),
                    ],
                ],
                expectations: vec![
                    vec![(vec![Eid(100), Eid(300), Eid(200)], 0, 1)],
                ],
            }
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
                    server.create_attribute(dep, scope);
                }
                
                server
                    .test_single(scope, Rule { name: "hector".to_string(), plan, })
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
                server.transact(Transact { tx, tx_data, }, 0, 0);

                worker.step_while(|| server.is_any_outdated());

                let mut expected: HashSet<(Vec<Value>, u64, isize)> =
                    HashSet::from_iter(case.expectations[tx_id].iter().cloned());

                for _i in 0..expected.len() {
                    let result = results.recv_timeout(Duration::from_millis(400)).expect("no result");
                    if !expected.remove(&result) { panic!("unknown result {:?}", result); }
                }
                
                assert!(results.recv_timeout(Duration::from_millis(400)).is_err());
            }
        }).unwrap();
    }
}
