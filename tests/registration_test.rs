use std::sync::mpsc::channel;

use declarative_dataflow::plan::{Join, Project};
use declarative_dataflow::server::Server;
use declarative_dataflow::timestamp::Time;
use declarative_dataflow::{AttributeConfig, IndexDirection, QuerySupport};
use declarative_dataflow::{Plan, Rule, TxData, Value};
use Value::{Eid, String};

#[test]
fn match_ea_after_input() {
    timely::execute_directly(move |worker| {
        let mut server = Server::<u64, u64>::new(Default::default());
        let (send_results, results) = channel();

        // [:find ?v :where [1 :name ?n]]
        let plan = Plan::MatchEA(1, ":name".to_string(), 1);

        worker.dataflow::<u64, _, _>(|scope| {
            let config = AttributeConfig {
                index_direction: IndexDirection::Both,
                query_support: QuerySupport::Basic,
                trace_slack: Some(Time::TxId(1)),
                ..Default::default()
            };

            server
                .context
                .internal
                .create_transactable_attribute(":name", config, scope)
                .unwrap();
        });

        let tx_data = vec![
            TxData::add(1, ":name", String("Dipper".to_string())),
            TxData::add(1, ":name", String("Alias".to_string())),
            TxData::add(2, ":name", String("Mabel".to_string())),
        ];

        server.transact(tx_data, 0, 0).unwrap();

        server.advance_domain(None, 1).unwrap();

        worker.step_while(|| server.is_any_outdated());

        worker.dataflow::<u64, _, _>(|scope| {
            server
                .test_single(
                    scope,
                    Rule {
                        name: "match_ea".to_string(),
                        plan,
                    },
                )
                .inspect(move |x| {
                    send_results.send((x.0.clone(), x.2)).unwrap();
                });
        });

        server.advance_domain(None, 2).unwrap();

        worker.step_while(|| server.is_any_outdated());

        assert_eq!(
            results.recv().unwrap(),
            (vec![String("Alias".to_string())], 1)
        );
        assert_eq!(
            results.recv().unwrap(),
            (vec![String("Dipper".to_string())], 1)
        );
    });
}

#[test]
fn join_after_input() {
    timely::execute_directly(move |worker| {
        let mut server = Server::<u64, u64>::new(Default::default());
        let (send_results, results) = channel();

        worker.dataflow::<u64, _, _>(|scope| {
            server
                .context
                .internal
                .create_transactable_attribute(
                    ":transfer/from",
                    AttributeConfig {
                        index_direction: IndexDirection::Both,
                        query_support: QuerySupport::Basic,
                        trace_slack: Some(Time::TxId(1)),
                        ..Default::default()
                    },
                    scope,
                )
                .unwrap();
            server
                .context
                .internal
                .create_transactable_attribute(
                    ":user/id",
                    AttributeConfig {
                        index_direction: IndexDirection::Both,
                        query_support: QuerySupport::Basic,
                        trace_slack: Some(Time::TxId(1)),
                        ..Default::default()
                    },
                    scope,
                )
                .unwrap();
        });

        server.advance_domain(None, 1).unwrap();

        worker.step_while(|| server.is_any_outdated());

        {
            server
                .transact(
                    vec![TxData::add(
                        1,
                        ":user/id",
                        String("123-456-789".to_string()),
                    )],
                    0,
                    0,
                )
                .unwrap();

            server.advance_domain(None, 2).unwrap();

            worker.step_while(|| server.is_any_outdated());
        }

        {
            server
                .transact(
                    vec![TxData::add(
                        101,
                        ":transfer/from",
                        String("123-456-789".to_string()),
                    )],
                    0,
                    0,
                )
                .unwrap();

            server.advance_domain(None, 3).unwrap();

            worker.step_while(|| server.is_any_outdated());
        }

        worker.dataflow::<u64, _, _>(|scope| {
            let (transfer, sender, uuid) = (1, 2, 3);
            let plan = Plan::Project(Project {
                variables: vec![transfer, sender],
                plan: Box::new(Plan::Join(Join {
                    variables: vec![uuid],
                    left_plan: Box::new(Plan::MatchA(transfer, ":transfer/from".to_string(), uuid)),
                    right_plan: Box::new(Plan::MatchA(sender, ":user/id".to_string(), uuid)),
                })),
            });

            server
                .test_single(
                    scope,
                    Rule {
                        name: "join".to_string(),
                        plan,
                    },
                )
                .inspect(move |x| {
                    send_results.send((x.0.clone(), x.2)).unwrap();
                });
        });

        server.advance_domain(None, 4).unwrap();

        worker.step_while(|| server.is_any_outdated());

        assert_eq!(results.recv().unwrap(), (vec![Eid(101), Eid(1)], 1));
    });
}
