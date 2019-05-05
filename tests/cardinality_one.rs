use std::collections::HashSet;
use std::iter::FromIterator;
use std::sync::mpsc::channel;
use std::time::Duration;

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Operator;

use declarative_dataflow::server::Server;
use declarative_dataflow::timestamp::Time;
use declarative_dataflow::{AttributeConfig, InputSemantics, Plan, Rule, TxData, Value};
use Time::TxId;
use Value::{Eid, Number};

struct Case {
    description: &'static str,
    plan: Plan,
    transactions: Vec<Vec<TxData>>,
    expectations: Vec<Vec<(Vec<Value>, u64, isize)>>,
}

fn run_cases_monotemp(mut cases: Vec<Case>) {
    for mut case in cases.drain(..) {
        timely::execute_directly(move |worker| {
            let mut server = Server::<u64, u64>::new(Default::default());
            let (send_results, results) = channel();

            dbg!(case.description);

            let plan = case.plan;

            let mut deps = HashSet::new();
            for tx in case.transactions.iter() {
                for datum in tx {
                    deps.insert(datum.2.clone());
                }
            }

            worker.dataflow::<u64, _, _>(|scope| {
                for dep in deps.iter() {
                    let config = AttributeConfig::tx_time(InputSemantics::CardinalityOne);
                    server
                        .context
                        .internal
                        .create_transactable_attribute(dep, config, scope)
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

            let mut next_tx = 0;

            // Make sure we can pop off transactions one-by-one.
            case.transactions.reverse();

            for mut expected_tuples in case.expectations.drain(..) {
                next_tx += 1;

                if let Some(tx_data) = case.transactions.pop() {
                    server.transact(tx_data, 0, 0).unwrap();
                }

                server.advance_domain(None, next_tx).unwrap();

                worker.step_while(|| server.is_any_outdated());

                let mut expected: HashSet<(Vec<Value>, u64, isize)> =
                    HashSet::from_iter(expected_tuples.drain(..));

                for _i in 0..expected.len() {
                    let result = results
                        .recv_timeout(Duration::from_millis(400))
                        .expect("no result");

                    if !expected.remove(&result) {
                        panic!("Unknown result {:?}.", result);
                    }
                }

                if let Ok(result) = results.recv_timeout(Duration::from_millis(400)) {
                    panic!("Extraneous result {:?}", result);
                }
            }
        });
    }
}

#[test]
fn cardinality_one() {
    run_cases_monotemp(vec![
        Case {
            description: "happy case",
            plan: Plan::MatchA(0, ":amount".to_string(), 1),
            transactions: vec![
                vec![
                    TxData::add(100, ":amount", Number(5)),
                    TxData::add(200, ":amount", Number(100)),
                ],
                vec![TxData::add(100, ":amount", Number(10))],
            ],
            expectations: vec![
                vec![
                    (vec![Eid(100), Number(5)], 0, 1),
                    (vec![Eid(200), Number(100)], 0, 1),
                ],
                vec![
                    (vec![Eid(100), Number(5)], 1, -1),
                    (vec![Eid(100), Number(10)], 1, 1),
                ],
            ],
        },
        Case {
            description: "happy case reversed",
            plan: Plan::MatchA(0, ":amount".to_string(), 1),
            transactions: vec![
                vec![
                    TxData::add(100, ":amount", Number(10)),
                    TxData::add(200, ":amount", Number(100)),
                ],
                vec![TxData::add(100, ":amount", Number(5))],
            ],
            expectations: vec![
                vec![
                    (vec![Eid(100), Number(10)], 0, 1),
                    (vec![Eid(200), Number(100)], 0, 1),
                ],
                vec![
                    (vec![Eid(100), Number(10)], 1, -1),
                    (vec![Eid(100), Number(5)], 1, 1),
                ],
            ],
        },
        Case {
            description: "retraction",
            plan: Plan::MatchA(0, ":amount".to_string(), 1),
            transactions: vec![
                vec![
                    TxData::add(100, ":amount", Number(5)),
                    TxData::add(200, ":amount", Number(100)),
                ],
                vec![TxData::retract(200, ":amount", Number(100))],
            ],
            expectations: vec![
                vec![
                    (vec![Eid(100), Number(5)], 0, 1),
                    (vec![Eid(200), Number(100)], 0, 1),
                ],
                vec![(vec![Eid(200), Number(100)], 1, -1)],
            ],
        },
    ]);
}

#[test]
fn cardinality_one_unordered() {
    run_cases_monotemp(vec![Case {
        description: "late arrival",
        plan: Plan::MatchA(0, ":amount".to_string(), 1),
        transactions: vec![
            vec![
                TxData::add(100, ":amount", Number(0)),
                TxData::add_at(100, ":amount", Number(2), TxId(2)),
            ],
            vec![TxData::add(100, ":amount", Number(1))],
        ],
        expectations: vec![
            vec![(vec![Eid(100), Number(0)], 0, 1)],
            vec![
                (vec![Eid(100), Number(0)], 1, -1),
                (vec![Eid(100), Number(1)], 1, 1),
            ],
            vec![
                (vec![Eid(100), Number(1)], 2, -1),
                (vec![Eid(100), Number(2)], 2, 1),
            ],
        ],
    }]);
}
