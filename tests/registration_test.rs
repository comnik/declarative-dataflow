use std::sync::mpsc::channel;

use declarative_dataflow::plan::{Join, Project};
use declarative_dataflow::server::Server;
use declarative_dataflow::{AttributeConfig, InputSemantics, Plan, Rule, TxData, Value};
use InputSemantics::Raw;
use Value::{Eid, String};

#[test]
fn match_ea_after_input() {
    timely::execute_directly(move |worker| {
        let mut server = Server::<u64, u64>::new(Default::default());
        let (send_results, results) = channel();

        // [:find ?v :where [1 :name ?n]]
        let plan = Plan::MatchEA(1, ":name".to_string(), 1);

        worker.dataflow::<u64, _, _>(|scope| {
            server
                .context
                .internal
                .create_transactable_attribute(":name", AttributeConfig::tx_time(Raw), scope)
                .unwrap();
        });

        let tx_data = vec![
            TxData(1, 1, ":name".to_string(), String("Dipper".to_string())),
            TxData(1, 1, ":name".to_string(), String("Alias".to_string())),
            TxData(1, 2, ":name".to_string(), String("Mabel".to_string())),
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
                    AttributeConfig::tx_time(Raw),
                    scope,
                )
                .unwrap();
            server
                .context
                .internal
                .create_transactable_attribute(":user/id", AttributeConfig::tx_time(Raw), scope)
                .unwrap();
        });

        server.advance_domain(None, 1).unwrap();

        worker.step_while(|| server.is_any_outdated());

        {
            let tx_data = vec![TxData(
                1,
                1,
                ":user/id".to_string(),
                String("123-456-789".to_string()),
            )];

            server.transact(tx_data, 0, 0).unwrap();

            server.advance_domain(None, 2).unwrap();

            worker.step_while(|| server.is_any_outdated());
        }

        {
            let tx_data = vec![TxData(
                1,
                101,
                ":transfer/from".to_string(),
                String("123-456-789".to_string()),
            )];

            server.transact(tx_data, 0, 0).unwrap();

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
