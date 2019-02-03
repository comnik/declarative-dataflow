extern crate declarative_dataflow;
extern crate timely;

use std::sync::mpsc::channel;

use timely::Configuration;

use declarative_dataflow::plan::{Function, Transform};
use declarative_dataflow::server::{Server, Transact, TxData};
use declarative_dataflow::{Plan, Rule, Value};
use Value::{Eid, Instant};

#[test]
fn truncate() {
    timely::execute(Configuration::Thread, move |worker| {
        let mut server = Server::<u64>::new(Default::default());
        let (send_results, results) = channel();

        // [:find ?h :where [?e :timestamp ?t] [(interval ?t) ?h]]
        let (e, t, h) = (1, 2, 3);
        let constants = vec![None, None];
        // let constants = vec![None, Some(Value::String(String::from("hour")))];
        let plan = Plan::Transform(Transform {
            variables: vec![t],
            result_sym: h,
            plan: Box::new(Plan::MatchA(e, ":timestamp".to_string(), t)),
            function: Function::TRUNCATE,
            constants,
        });

        worker.dataflow::<u64, _, _>(|scope| {
            server.create_attribute(":timestamp", scope).unwrap();

            server
                .test_single(
                    scope,
                    Rule {
                        name: "truncate".to_string(),
                        plan,
                    },
                )
                .inspect(move |x| {
                    send_results.send((x.0.clone(), x.2)).unwrap();
                });
        });

        server.transact(
            Transact {
                tx: Some(0),
                tx_data: vec![
                    TxData(1, 1, ":timestamp".to_string(), Instant(1_540_048_515_500)),
                    TxData(1, 2, ":timestamp".to_string(), Instant(1_540_048_515_616)),
                ],
            },
            0,
            0,
        );

        worker.step_while(|| server.is_any_outdated());

        assert_eq!(
            results.recv().unwrap(),
            (
                vec![
                    Eid(1),
                    Instant(1_540_048_515_500),
                    Instant(1_540_047_600_000)
                ],
                1
            )
        );
        assert_eq!(
            results.recv().unwrap(),
            (
                vec![
                    Eid(2),
                    Instant(1_540_048_515_616),
                    Instant(1_540_047_600_000)
                ],
                1
            )
        );
    })
    .unwrap();
}
