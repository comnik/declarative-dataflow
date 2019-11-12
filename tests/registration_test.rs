use std::sync::mpsc::channel;

use declarative_dataflow::plan::{Join, Project};
use declarative_dataflow::server::Server;
use declarative_dataflow::timestamp::Time;
use declarative_dataflow::{Aid, Datom, Plan, Rule, Value};
use declarative_dataflow::{AttributeConfig, IndexDirection, QuerySupport};
use Value::{Eid, String};

#[test]
fn match_ea_after_input() {
    timely::execute_directly(move |worker| {
        let mut server = Server::<Aid, u64, u64>::new(Default::default());
        let (send_results, results) = channel();

        // [:find ?v :where [1 :name ?n]]
        let plan = Plan::match_ea(1, ":name", 1);

        worker.dataflow::<u64, _, _>(|scope| {
            let config = AttributeConfig {
                index_direction: IndexDirection::Both,
                query_support: QuerySupport::Basic,
                trace_slack: Some(Time::TxId(1)),
                ..Default::default()
            };

            server.create_attribute(scope, ":name", config).unwrap();
        });

        let tx_data = vec![
            Datom::add(1, ":name", String("Dipper".to_string())),
            Datom::add(1, ":name", String("Alias".to_string())),
            Datom::add(2, ":name", String("Mabel".to_string())),
        ];

        server.transact(tx_data, 0, 0).unwrap();

        server.advance_domain(None, 1).unwrap();

        worker.step_while(|| server.is_any_outdated());

        worker.dataflow::<u64, _, _>(|scope| {
            server
                .test_single(scope, Rule::named("match_ea", plan))
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
        let mut server = Server::<Aid, u64, u64>::new(Default::default());
        let (send_results, results) = channel();

        worker.dataflow::<u64, _, _>(|scope| {
            server
                .create_attribute(
                    scope,
                    ":transfer/from",
                    AttributeConfig {
                        index_direction: IndexDirection::Both,
                        query_support: QuerySupport::Basic,
                        trace_slack: Some(Time::TxId(1)),
                        ..Default::default()
                    },
                )
                .unwrap();
            server
                .create_attribute(
                    scope,
                    ":user/id",
                    AttributeConfig {
                        index_direction: IndexDirection::Both,
                        query_support: QuerySupport::Basic,
                        trace_slack: Some(Time::TxId(1)),
                        ..Default::default()
                    },
                )
                .unwrap();
        });

        server.advance_domain(None, 1).unwrap();

        worker.step_while(|| server.is_any_outdated());

        {
            server
                .transact(
                    vec![Datom::add(1, ":user/id", String("123-456-789".to_string()))],
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
                    vec![Datom::add(
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
                    left_plan: Box::new(Plan::match_a(transfer, ":transfer/from", uuid)),
                    right_plan: Box::new(Plan::match_a(sender, ":user/id", uuid)),
                })),
            });

            server
                .test_single(scope, Rule::named("join", plan))
                .inspect(move |x| {
                    send_results.send((x.0.clone(), x.2)).unwrap();
                });
        });

        server.advance_domain(None, 4).unwrap();

        worker.step_while(|| server.is_any_outdated());

        assert_eq!(results.recv().unwrap(), (vec![Eid(101), Eid(1)], 1));
    });
}
