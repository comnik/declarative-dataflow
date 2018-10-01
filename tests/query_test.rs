extern crate timely;
extern crate declarative_dataflow;

use std::collections::{HashMap};
use std::sync::mpsc::{channel};

use timely::{Configuration};
use timely::dataflow::{ProbeHandle};

use declarative_dataflow::{Context, Plan, Rule, Datom, Value, create_db, implement};
use declarative_dataflow::plan::{Project, Join};

#[test]
fn match_ea() {
    timely::execute(Configuration::Thread, move |worker| {

        let n = 1;
        let attr_name = 100;

        // [:find ?v :where [1 :name ?n]]
        let plan = Plan::MatchEA(1, attr_name, n);
        
        let mut probe = ProbeHandle::new();
        let mut ctx = worker.dataflow(|scope| {
            let (input_handle, db) = create_db(scope);
            Context { db, input_handle, queries: HashMap::new(), }
        });

        let (send_results, results) = channel();
        
        worker.dataflow::<usize, _, _>(|scope| {
            let query_name = "match_ea";
            let rules = vec![Rule { name: query_name.to_string(), plan: plan }];
            let publish = vec![query_name.to_string()];

            let mut rel_map = implement(rules, publish, scope, &mut ctx, &mut probe);

            rel_map.get_mut(query_name).unwrap().import(scope)
                .as_collection(|tuple,_| tuple.clone())
                .inspect(move |x| { send_results.send((x.0.clone(), x.2)).unwrap(); })
                .probe_with(&mut probe);
        });

        ctx.input_handle.insert(Datom(1, attr_name, Value::String("Dipper".to_string())));
        ctx.input_handle.insert(Datom(1, attr_name, Value::String("Alias".to_string())));
        ctx.input_handle.insert(Datom(2, attr_name, Value::String("Mabel".to_string())));
        ctx.input_handle.advance_to(1);
        ctx.input_handle.flush();
        
        while probe.less_than(ctx.input_handle.time()) { worker.step(); }

        assert_eq!(results.recv().unwrap(), (vec![Value::String("Alias".to_string())], 1));
        assert_eq!(results.recv().unwrap(), (vec![Value::String("Dipper".to_string())], 1));
        
    }).unwrap();
}

#[test]
fn join() {
    timely::execute(Configuration::Thread, move |worker| {

        let (e, a, n) = (1, 2, 3);
        let (attr_name, attr_age) = (100, 200);

        // [:find ?e ?n ?a :where [?e :age ?a] [?e :name ?n]]
        let plan = Plan::Project(Project {
            variables: vec![e, n, a],
            plan: Box::new(Plan::Join(Join {
                variables: vec![e],
                left_plan: Box::new(Plan::MatchA(e, attr_name, n)),
                right_plan: Box::new(Plan::MatchA(e, attr_age, a)),
            }))
        });
        
        let mut probe = ProbeHandle::new();
        let mut ctx = worker.dataflow(|scope| {
            let (input_handle, db) = create_db(scope);
            Context { db, input_handle, queries: HashMap::new(), }
        });

        let (send_results, results) = channel();
        
        worker.dataflow::<usize, _, _>(|scope| {
            let query_name = "join";
            let rules = vec![Rule { name: query_name.to_string(), plan: plan }];
            let publish = vec![query_name.to_string()];

            let mut rel_map = implement(rules, publish, scope, &mut ctx, &mut probe);

            rel_map.get_mut(query_name).unwrap().import(scope)
                .as_collection(|tuple,_| tuple.clone())
                .inspect(move |x| { send_results.send((x.0.clone(), x.2)).unwrap(); })
                .probe_with(&mut probe);
        });

        ctx.input_handle.insert(Datom(1, attr_name, Value::String("Dipper".to_string())));
        ctx.input_handle.insert(Datom(1, attr_age, Value::Number(12)));
        ctx.input_handle.advance_to(1);
        ctx.input_handle.flush();
        
        while probe.less_than(ctx.input_handle.time()) { worker.step(); }

        assert_eq!(results.recv().unwrap(), (vec![Value::Eid(1), Value::String("Dipper".to_string()), Value::Number(12)], 1));
        
    }).unwrap();
}
