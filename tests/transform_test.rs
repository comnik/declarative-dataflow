extern crate declarative_dataflow;
extern crate timely;

use std::sync::mpsc::channel;
use std::thread;

use timely::Configuration;

use declarative_dataflow::plan::{Function, Transform};
use declarative_dataflow::server::{CreateInput, Interest, Register, Server, Transact, TxData};
use declarative_dataflow::{Plan, Rule, Value};

#[test]
fn interval() {
    timely::execute(Configuration::Thread, move |worker| {
        let mut server = Server::new(Default::default());
        let (send_results, results) = channel();

        // [:find ?t :where [?e :timestamp ?t] [(interval ?t) ?t]]
        let (e, t) = (1, 2);
        let plan = Plan::Transform(Transform {
            variables: vec![t],
            plan: Box::new(Plan::MatchA(e, ":timestamp".to_string(), t)),
            function: Function::INTERVAL,
        });

        worker.dataflow::<u64, _, _>(|mut scope| {
            server.create_input(
                CreateInput {
                    name: ":timestamp".to_string(),
                },
                &mut scope,
            );

            let query_name = "interval";
            server.register(
                Register {
                    rules: vec![Rule {
                        name: query_name.to_string(),
                        plan: plan,
                    }],
                    publish: vec![query_name.to_string()],
                },
                &mut scope,
            );

            server
                .interest(
                    Interest {
                        name: query_name.to_string(),
                    },
                    &mut scope,
                )
                .inspect(move |x| {
                    send_results.send((x.0.clone(), x.2)).unwrap();
                });
        });

        server.transact(
            Transact {
                tx: Some(0),
                tx_data: vec![
                    TxData(
                        1,
                        1,
                        ":timestamp".to_string(),
                        Value::Instant(1540048515500),
                    ),
                    TxData(
                        1,
                        2,
                        ":timestamp".to_string(),
                        Value::Instant(1540048515616),
                    ),
                ],
            },
            0,
            0,
        );

        for handle in server.input_handles.values() {
            while server.probe.less_than(handle.time()) {
                worker.step();
            }
        }

        thread::spawn(move || {
            assert_eq!(
                results.recv().unwrap(),
                (vec![Value::Eid(1), Value::Instant(1540047600000)], 1)
            );
            assert_eq!(
                results.recv().unwrap(),
                (vec![Value::Eid(2), Value::Instant(1540047600000)], 1)
            );
        }).join()
            .unwrap();
    }).unwrap();
}
