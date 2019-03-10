use std::collections::HashSet;
use std::iter::FromIterator;
use std::sync::mpsc::channel;
use std::time::Duration;

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Operator;

use declarative_dataflow::binding::Binding;
use declarative_dataflow::plan::{Aggregate, AggregationFn, Implementable, Join, Project};
use declarative_dataflow::server::Server;
use declarative_dataflow::{Aid, Value};
use declarative_dataflow::{AttributeConfig, InputSemantics, Plan, Rule, TxData};
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
                        .create_attribute(dep, AttributeConfig::tx_time(InputSemantics::Raw), scope)
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
fn multiple_aggregations() {
    run_cases(vec![
        Case {
            description:
            "[:find (sum ?amount) (sum ?debt) (count ?amount) (count ?debt) \
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
                        AggregationFn::SUM,
                        AggregationFn::SUM,
                        AggregationFn::COUNT,
                        AggregationFn::COUNT,
                    ],
                    key_variables: vec![],
                    aggregation_variables: vec![amount, debt, amount, debt],
                    with_variables: vec![],
                })
            },
            transactions: vec![
                vec![
                    TxData(1, 1, ":amount".to_string(), Number(5)),
                    TxData(1, 2, ":amount".to_string(), Number(5)),
                    TxData(1, 1, ":debt".to_string(), Number(12)),
                    TxData(1, 2, ":debt".to_string(), Number(15)),
                ],
            ],
            expectations: vec![
                vec![
                    (vec![Number(10), Number(12)], 0, 1),
                ],
            ],
            // set-semantics
            // expectations: vec![
            //     vec![
            //         (vec![Number(10), Number(4), Number(36), Rational32(Ratio::new(14, 1))], 0, 1),
            //     ],
            // ],
        },
    ]);
}
