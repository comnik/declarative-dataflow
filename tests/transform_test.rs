use std::collections::HashSet;
use std::iter::FromIterator;
use std::sync::mpsc::channel;
use std::time::Duration;

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Operator;

use declarative_dataflow::binding::Binding;
use declarative_dataflow::plan::{Function, Implementable, Transform};
use declarative_dataflow::server::Server;
use declarative_dataflow::{Aid, Value};
use declarative_dataflow::{AttributeConfig, InputSemantics, Plan, Rule, TxData};
use Value::{Eid, Instant};

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

#[test]
fn run_transform_cases() {
    let mut cases = vec![Case {
        description: "[:find ?h :where [?e :timestamp ?t] [(interval ?t) ?h]]",
        plan: {
            let (e, t, h) = (1, 2, 3);
            let constants = vec![None, None];
            // let constants = vec![None, Some(Value::String(String::from("hour")))];
            Plan::Transform(Transform {
                variables: vec![t],
                result_variable: h,
                plan: Box::new(Plan::MatchA(e, ":timestamp".to_string(), t)),
                function: Function::TRUNCATE,
                constants,
            })
        },
        transactions: vec![vec![
            TxData::add(1, ":timestamp", Instant(1_540_048_515_500)),
            TxData::add(2, ":timestamp", Instant(1_540_048_515_616)),
        ]],
        expectations: vec![vec![
            (
                vec![
                    Eid(1),
                    Instant(1_540_048_515_500),
                    Instant(1_540_047_600_000),
                ],
                0,
                1,
            ),
            (
                vec![
                    Eid(2),
                    Instant(1_540_048_515_616),
                    Instant(1_540_047_600_000),
                ],
                0,
                1,
            ),
        ]],
    }];

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
                        .create_transactable_attribute(
                            dep,
                            AttributeConfig::tx_time(InputSemantics::Raw),
                            scope,
                        )
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
