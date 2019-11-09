use std::collections::{HashMap, HashSet};
use std::default::Default;
use std::iter::FromIterator;
use std::sync::mpsc::channel;
use std::time::Duration;

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Operator;

use declarative_dataflow::server::Server;
use declarative_dataflow::timestamp::pair::Pair;
use declarative_dataflow::timestamp::Time;
use declarative_dataflow::{Aid, AttributeConfig, Datom, InputSemantics, Plan, Rule, Value};
use Time::TxId;
use Value::{Eid, Number};

struct Case<T> {
    description: &'static str,
    plan: Plan,
    transactions: Vec<Vec<Datom<Aid>>>,
    expectations: Vec<Vec<(Vec<Value>, T, isize)>>,
}

trait Run {
    fn run(self);
}

impl Run for Vec<Case<u64>> {
    fn run(mut self) {
        for mut case in self.drain(..) {
            timely::execute_directly(move |worker| {
                let mut server = Server::<u64, u64>::new(Default::default());
                let (send_results, results) = channel();

                dbg!(case.description);

                let mut deps = HashMap::new();
                for tx in case.transactions.iter() {
                    for datum in tx {
                        deps.entry(datum.2.clone()).or_insert_with(|| {
                            AttributeConfig::tx_time(InputSemantics::LastWriteWins)
                        });
                    }
                }

                let plan = case.plan;

                worker.dataflow::<u64, _, _>(|scope| {
                    for (dep, config) in deps.drain() {
                        server.create_attribute(scope, &dep, config).unwrap();
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
}

impl Run for Vec<Case<Pair<Duration, u64>>> {
    fn run(mut self) {
        for mut case in self.drain(..) {
            timely::execute_directly(move |worker| {
                let mut server = Server::<Pair<Duration, u64>, u64>::new(Default::default());
                let (send_results, results) = channel();

                dbg!(case.description);

                let mut deps = HashMap::new();
                for tx in case.transactions.iter() {
                    for datum in tx {
                        deps.entry(datum.2.clone())
                            .or_insert_with(|| AttributeConfig {
                                input_semantics: InputSemantics::LastWriteWins,
                                trace_slack: Some(Time::Bi(Duration::from_secs(0), 1)),
                                ..Default::default()
                            });
                    }
                }

                let plan = case.plan;

                worker.dataflow::<Pair<Duration, u64>, _, _>(|scope| {
                    for (dep, config) in deps.drain() {
                        server.create_attribute(scope, &dep, config).unwrap();
                    }

                    server
                        .test_single(
                            scope,
                            Rule {
                                name: "query".to_string(),
                                plan,
                            },
                        )
                        .inspect(|x| println!("{:?}", x))
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

                    server
                        .advance_domain(None, Pair::new(Duration::from_secs(next_tx), next_tx))
                        .unwrap();

                    worker.step_while(|| server.is_any_outdated());

                    let mut expected: HashSet<(Vec<Value>, Pair<Duration, u64>, isize)> =
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
}

#[test]
fn last_write_wins() {
    vec![
        Case {
            description: "happy case",
            plan: Plan::MatchA(0, ":amount".to_string(), 1),
            transactions: vec![
                vec![
                    Datom::add(100, ":amount", Number(5)),
                    Datom::add(200, ":amount", Number(100)),
                ],
                vec![Datom::add(100, ":amount", Number(10))],
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
                    Datom::add(100, ":amount", Number(10)),
                    Datom::add(200, ":amount", Number(100)),
                ],
                vec![Datom::add(100, ":amount", Number(5))],
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
                    Datom::add(100, ":amount", Number(5)),
                    Datom::add(200, ":amount", Number(100)),
                ],
                vec![Datom::retract(200, ":amount", Number(100))],
            ],
            expectations: vec![
                vec![
                    (vec![Eid(100), Number(5)], 0, 1),
                    (vec![Eid(200), Number(100)], 0, 1),
                ],
                vec![(vec![Eid(200), Number(100)], 1, -1)],
            ],
        },
        Case {
            description: "toggle",
            plan: Plan::MatchA(0, ":amount".to_string(), 1),
            transactions: vec![
                vec![Datom::add(100, ":amount", Number(5))],
                vec![Datom::add(100, ":amount", Number(10))],
                vec![Datom::add(100, ":amount", Number(5))],
            ],
            expectations: vec![
                vec![(vec![Eid(100), Number(5)], 0, 1)],
                vec![
                    (vec![Eid(100), Number(5)], 1, -1),
                    (vec![Eid(100), Number(10)], 1, 1),
                ],
                vec![
                    (vec![Eid(100), Number(10)], 2, -1),
                    (vec![Eid(100), Number(5)], 2, 1),
                ],
            ],
        },
    ]
    .run();
}

#[test]
fn last_write_wins_unordered() {
    vec![Case {
        description: "late arrival",
        plan: Plan::MatchA(0, ":amount".to_string(), 1),
        transactions: vec![
            vec![
                Datom::add(100, ":amount", Number(0)),
                Datom::add_at(100, ":amount", Number(2), TxId(2)),
            ],
            vec![Datom::add(100, ":amount", Number(1))],
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
    }]
    .run();
}

#[test]
#[cfg(feature = "real")]
fn bitemporal() {
    vec![
        Case {
            description: "bitemporal conflict",
            plan: Plan::MatchA(0, ":amount".to_string(), 1),
            transactions: vec![vec![
                Datom::add_at(
                    100,
                    ":amount",
                    Number(0),
                    Time::Bi(Duration::from_secs(0), 0),
                ),
                Datom::add_at(
                    100,
                    ":amount",
                    Number(2),
                    Time::Bi(Duration::from_secs(0), 2),
                ),
                Datom::add_at(
                    100,
                    ":amount",
                    Number(1),
                    Time::Bi(Duration::from_secs(1), 1),
                ),
            ]],
            expectations: vec![vec![
                (
                    vec![Eid(100), Number(0)],
                    Pair::new(Duration::from_secs(0), 0),
                    1,
                ),
                (
                    vec![Eid(100), Number(0)],
                    Pair::new(Duration::from_secs(0), 2),
                    -1,
                ),
                (
                    vec![Eid(100), Number(2)],
                    Pair::new(Duration::from_secs(0), 2),
                    1,
                ),
            ]],
        },
        Case {
            description: "bitemporal correction",
            plan: Plan::MatchA(0, ":amount".to_string(), 1),
            transactions: vec![
                vec![
                    Datom::add_at(
                        100,
                        ":amount",
                        Number(0),
                        Time::Bi(Duration::from_secs(0), 0),
                    ),
                    Datom::add_at(
                        100,
                        ":amount",
                        Number(2),
                        Time::Bi(Duration::from_secs(0), 2),
                    ),
                ],
                vec![Datom::add_at(
                    100,
                    ":amount",
                    Number(1),
                    Time::Bi(Duration::from_secs(1), 1),
                )],
            ],
            expectations: vec![
                vec![
                    (
                        vec![Eid(100), Number(0)],
                        Pair::new(Duration::from_secs(0), 0),
                        1,
                    ),
                    (
                        vec![Eid(100), Number(0)],
                        Pair::new(Duration::from_secs(0), 2),
                        -1,
                    ),
                    (
                        vec![Eid(100), Number(2)],
                        Pair::new(Duration::from_secs(0), 2),
                        1,
                    ),
                ],
                vec![
                    (
                        vec![Eid(100), Number(0)],
                        Pair::new(Duration::from_secs(1), 1),
                        -1,
                    ),
                    (
                        vec![Eid(100), Number(1)],
                        Pair::new(Duration::from_secs(1), 1),
                        1,
                    ),
                ],
            ],
        },
        Case {
            description: "bitemporal toggle",
            plan: Plan::MatchA(0, ":flow".to_string(), 1),
            transactions: vec![vec![
                Datom(
                    1,
                    Value::uuid_str("71828aae-4fc8-421b-82ca-68c5f4981d74"),
                    ":flow".to_string(),
                    Value::from(30.006),
                    Some(Time::Bi(Duration::from_secs(0), 1_554_120_030_000)), // 2019-04-01T12:00:30+00:00
                ),
                Datom(
                    1,
                    Value::uuid_str("71828aae-4fc8-421b-82ca-68c5f4981d74"),
                    ":flow".to_string(),
                    Value::from(31.006),
                    Some(Time::Bi(Duration::from_secs(0), 1_554_120_061_000)), // 2019-04-01T12:01:01+00:00
                ),
                Datom(
                    1,
                    Value::uuid_str("71828aae-4fc8-421b-82ca-68c5f4981d74"),
                    ":flow".to_string(),
                    Value::from(30.006),
                    Some(Time::Bi(Duration::from_secs(0), 1_554_120_150_000)), // 2019-04-01T12:02:30+00:00
                ),
            ]],
            expectations: vec![vec![
                (
                    vec![
                        Value::uuid_str("71828aae-4fc8-421b-82ca-68c5f4981d74"),
                        Value::from(30.006),
                    ],
                    Pair::new(Duration::from_secs(0), 1_554_120_030_000),
                    1,
                ),
                (
                    vec![
                        Value::uuid_str("71828aae-4fc8-421b-82ca-68c5f4981d74"),
                        Value::from(30.006),
                    ],
                    Pair::new(Duration::from_secs(0), 1_554_120_061_000),
                    -1,
                ),
                (
                    vec![
                        Value::uuid_str("71828aae-4fc8-421b-82ca-68c5f4981d74"),
                        Value::from(31.006),
                    ],
                    Pair::new(Duration::from_secs(0), 1_554_120_061_000),
                    1,
                ),
                (
                    vec![
                        Value::uuid_str("71828aae-4fc8-421b-82ca-68c5f4981d74"),
                        Value::from(31.006),
                    ],
                    Pair::new(Duration::from_secs(0), 1_554_120_150_000),
                    -1,
                ),
                (
                    vec![
                        Value::uuid_str("71828aae-4fc8-421b-82ca-68c5f4981d74"),
                        Value::from(30.006),
                    ],
                    Pair::new(Duration::from_secs(0), 1_554_120_150_000),
                    1,
                ),
            ]],
        },
    ]
    .run();
}
