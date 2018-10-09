extern crate timely;
extern crate declarative_dataflow;

use std::thread;
use std::sync::mpsc::{channel};

use timely::{Configuration};

use declarative_dataflow::{Plan, Rule, Value};
use declarative_dataflow::plan::{Project, Join, Aggregate, AggregationFn};
use declarative_dataflow::server::{Server, Transact, CreateInput, Interest, Register, TxData};

#[test]
fn match_ea() {
    timely::execute(Configuration::Thread, move |worker| {

        let mut server = Server::new(Default::default());
        let (send_results, results) = channel();

        // [:find ?v :where [1 :name ?n]]
        let plan = Plan::MatchEA(1, ":name".to_string(), 1);

        worker.dataflow::<usize, _, _>(|mut scope| {
            
            server.create_input(CreateInput { name: ":name".to_string() }, &mut scope);
            
            let query_name = "match_ea";
            server.register(Register {
                rules: vec![Rule { name: query_name.to_string(), plan: plan }],
                publish: vec![query_name.to_string()],
            }, &mut scope);

            server.interest(Interest { name: query_name.to_string() }, &mut scope)
                .inspect(move |x| { send_results.send((x.0.clone(), x.2)).unwrap(); });
        });

        server.transact(Transact {
            tx: Some(0),
            tx_data: vec![
                TxData(1, 1, ":name".to_string(), Value::String("Dipper".to_string())),
                TxData(1, 1, ":name".to_string(), Value::String("Alias".to_string())),
                TxData(1, 2, ":name".to_string(), Value::String("Mabel".to_string())),
            ],
        }, 0, 0);

        for handle in server.input_handles.values() {
            while server.probe.less_than(handle.time()) {
                worker.step();
            }
        }

        thread::spawn(move || {
            assert_eq!(results.recv().unwrap(), (vec![Value::String("Alias".to_string())], 1));
            assert_eq!(results.recv().unwrap(), (vec![Value::String("Dipper".to_string())], 1));
        }).join().unwrap();
    }).unwrap();
}

#[test]
fn join() {
    timely::execute(Configuration::Thread, move |worker| {

        let mut server = Server::new(Default::default());
        let (send_results, results) = channel();

        // [:find ?e ?n ?a :where [?e :age ?a] [?e :name ?n]]
        let (e, a, n) = (1, 2, 3);
        let plan = Plan::Project(Project {
            variables: vec![e, n, a],
            plan: Box::new(Plan::Join(Join {
                variables: vec![e],
                left_plan: Box::new(Plan::MatchA(e, ":name".to_string(), n)),
                right_plan: Box::new(Plan::MatchA(e, ":age".to_string(), a)),
            }))
        });
        
        worker.dataflow::<usize, _, _>(|mut scope| {
            server.create_input(CreateInput { name: ":name".to_string() }, &mut scope);
            server.create_input(CreateInput { name: ":age".to_string() }, &mut scope);
            
            let query_name = "join";
            server.register(Register {
                rules: vec![Rule { name: query_name.to_string(), plan: plan }],
                publish: vec![query_name.to_string()],
            }, &mut scope);

            server.interest(Interest { name: query_name.to_string() }, &mut scope)
                .inspect(move |x| { send_results.send((x.0.clone(), x.2)).unwrap(); });
        });

        server.transact(Transact {
            tx: Some(0),
            tx_data: vec![
                TxData(1, 1, ":name".to_string(), Value::String("Dipper".to_string())),
                TxData(1, 1, ":age".to_string(), Value::Number(12)),
            ],
        }, 0, 0);
        
        for handle in server.input_handles.values() {
            while server.probe.less_than(handle.time()) {
                worker.step();
            }
        }
        
        thread::spawn(move || {
            assert_eq!(results.recv().unwrap(), (vec![Value::Eid(1), Value::String("Dipper".to_string()), Value::Number(12)], 1));
        }).join().unwrap();
    }).unwrap();
}

#[test]
fn count() {
    timely::execute(Configuration::Thread, move |worker| {

        let mut server = Server::new(Default::default());
        let (send_results, results) = channel();

        // [:find ?e (count ?amount) :where [?e :amount ?amount]]
        let (e, amount) = (1, 2);
        let plan = Plan::Aggregate(Aggregate {
            variables: vec![e, amount],
            plan: Box::new(Plan::MatchA(e, ":amount".to_string(), amount)),
            aggregation_fn: AggregationFn::COUNT,
            key_symbols: vec![e],
        });
        
        worker.dataflow::<usize, _, _>(|mut scope| {
            server.create_input(CreateInput { name: ":amount".to_string() }, &mut scope);
            
            let query_name = "count";
            server.register(Register {
                rules: vec![Rule { name: query_name.to_string(), plan: plan }],
                publish: vec![query_name.to_string()],
            }, &mut scope);

            server.interest(Interest { name: query_name.to_string() }, &mut scope)
                .inspect(move |x| { send_results.send((x.0.clone(), x.2)).unwrap(); });
        });

        server.transact(Transact {
            tx: Some(0),
            tx_data: vec![
                TxData(1, 1, ":amount".to_string(), Value::Number(5)),
                TxData(1, 2, ":amount".to_string(), Value::Number(10)),
                TxData(1, 2, ":amount".to_string(), Value::Number(10)),
                TxData(1, 1, ":amount".to_string(), Value::Number(2)),
                TxData(1, 1, ":amount".to_string(), Value::Number(4)),
                TxData(1, 1, ":amount".to_string(), Value::Number(6)),
            ],
        }, 0, 0);

        for handle in server.input_handles.values() {
            while server.probe.less_than(handle.time()) {
                worker.step();
            }
        }
        
        thread::spawn(move || {
            assert_eq!(results.recv().unwrap(), (vec![Value::Eid(1), Value::Number(17)], 1));
            assert_eq!(results.recv().unwrap(), (vec![Value::Eid(2), Value::Number(20)], 1));
        }).join().unwrap();
    }).unwrap();
}

#[test]
fn max() {
    timely::execute(Configuration::Thread, move |worker| {

        let mut server = Server::new(Default::default());
        let (send_results, results) = channel();

        // [:find ?e (max ?amount) :where [?e :amount ?amount]]
        let (e, amount) = (1, 2);
        let plan = Plan::Aggregate(Aggregate {
            variables: vec![e, amount],
            plan: Box::new(Plan::MatchA(e, ":amount".to_string(), amount)),
            aggregation_fn: AggregationFn::MAX,
            key_symbols: vec![e],
        });
        
        worker.dataflow::<usize, _, _>(|mut scope| {
            server.create_input(CreateInput { name: ":amount".to_string() }, &mut scope);
            
            let query_name = "max";
            server.register(Register {
                rules: vec![Rule { name: query_name.to_string(), plan: plan }],
                publish: vec![query_name.to_string()],
            }, &mut scope);

            server.interest(Interest { name: query_name.to_string() }, &mut scope)
                .inspect(move |x| { send_results.send((x.0.clone(), x.2)).unwrap(); });
        });

        server.transact(Transact {
            tx: Some(0),
            tx_data: vec![
                TxData(1, 1, ":amount".to_string(), Value::Number(5)),
                TxData(1, 2, ":amount".to_string(), Value::Number(10)),
                TxData(1, 2, ":amount".to_string(), Value::Number(10)),
                TxData(1, 1, ":amount".to_string(), Value::Number(2)),
                TxData(1, 1, ":amount".to_string(), Value::Number(4)),
                TxData(1, 1, ":amount".to_string(), Value::Number(6)),
            ],
        }, 0, 0);
        
        for handle in server.input_handles.values() {
            while server.probe.less_than(handle.time()) {
                worker.step();
            }
        }
        
        thread::spawn(move || {
            assert_eq!(results.recv().unwrap(), (vec![Value::Eid(1), Value::Number(6)], 1));
            assert_eq!(results.recv().unwrap(), (vec![Value::Eid(2), Value::Number(10)], 1));
        }).join().unwrap();
    }).unwrap();
}

#[test]
fn min() {
    timely::execute(Configuration::Thread, move |worker| {

        let mut server = Server::new(Default::default());
        let (send_results, results) = channel();

        // [:find ?e (min ?amount) :where [?e :amount ?amount]]
        let (e, amount) = (1, 2);
        let plan = Plan::Aggregate(Aggregate {
            variables: vec![e, amount],
            plan: Box::new(Plan::MatchA(e, ":amount".to_string(), amount)),
            aggregation_fn: AggregationFn::MIN,
            key_symbols: vec![e],
        });
        
        worker.dataflow::<usize, _, _>(|mut scope| {
            server.create_input(CreateInput { name: ":amount".to_string() }, &mut scope);
            
            let query_name = "min";
            server.register(Register {
                rules: vec![Rule { name: query_name.to_string(), plan: plan }],
                publish: vec![query_name.to_string()],
            }, &mut scope);

            server.interest(Interest { name: query_name.to_string() }, &mut scope)
                .inspect(move |x| { send_results.send((x.0.clone(), x.2)).unwrap(); });
        });

        server.transact(Transact {
            tx: Some(0),
            tx_data: vec![
                TxData(1, 1, ":amount".to_string(), Value::Number(5)),
                TxData(1, 2, ":amount".to_string(), Value::Number(10)),
                TxData(1, 2, ":amount".to_string(), Value::Number(10)),
                TxData(1, 1, ":amount".to_string(), Value::Number(2)),
                TxData(1, 1, ":amount".to_string(), Value::Number(4)),
                TxData(1, 1, ":amount".to_string(), Value::Number(6)),
            ],
        }, 0, 0);
        
        for handle in server.input_handles.values() {
            while server.probe.less_than(handle.time()) {
                worker.step();
            }
        }
        
        thread::spawn(move || {
            assert_eq!(results.recv().unwrap(), (vec![Value::Eid(1), Value::Number(2)], 1));
            assert_eq!(results.recv().unwrap(), (vec![Value::Eid(2), Value::Number(10)], 1));
        }).join().unwrap();
    }).unwrap();
}
