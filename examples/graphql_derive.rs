use timely::dataflow::operators::unordered_input::UnorderedInput;
use timely::dataflow::scopes::Scope;

use declarative_dataflow::domain::{AsSingletonDomain, Domain};
use declarative_dataflow::{TxData, Value};
use Value::{Eid, Number, String};

use declarative_dataflow::derive::graphql::GraphQl;

fn main() {
    timely::execute_directly(move |worker| {
        // let (send_results, results) = channel();

        let mut domain = worker.dataflow::<u64, _, _>(|scope| {
            let human_name = scope
                .new_unordered_input()
                .as_singleton_domain("human_name");

            let human_age = scope.new_unordered_input().as_singleton_domain("human_age");

            let available = scope.new_unordered_input().as_singleton_domain("available");

            let mut domain: Domain<u64> = (human_name + human_age + available).with_slack(1).into();

            domain
                .forward_propose("human_name")
                .unwrap()
                .import(scope)
                .as_collection(|e, v| (e.clone(), v.clone()))
                .inspect(|x| println!("{:?}", x));

            domain
                .forward_propose("human_age")
                .unwrap()
                .import(scope)
                .as_collection(|e, v| (e.clone(), v.clone()))
                .inspect(|x| println!("{:?}", x));

            let mut world = scope.iterative(|nested| {
                GraphQl::new("{ available { human_name human_age } }".to_string()).derive(
                    nested,
                    &mut domain,
                    "selected",
                )
            });

            println!("World contains attributes {:?}", &world.attributes);

            world
                .forward_propose("selected/human_name")
                .unwrap()
                .import(scope)
                .as_collection(|e, v| (e.clone(), v.clone()))
                .inspect(|x| println!("WORLD {:?}", x));

            world
                .forward_propose("selected/human_age")
                .unwrap()
                .import(scope)
                .as_collection(|e, v| (e.clone(), v.clone()))
                .inspect(|x| println!("WORLD {:?}", x));

            domain
        });

        domain
            .transact(vec![
                TxData(
                    1,
                    Eid(100),
                    "human_name".to_string(),
                    String("Alice".to_string()),
                    None,
                ),
                TxData(
                    1,
                    Eid(200),
                    "human_name".to_string(),
                    String("Bob".to_string()),
                    None,
                ),
                TxData(1, Eid(100), "human_age".to_string(), Number(40), None),
                TxData(1, Eid(200), "human_age".to_string(), Number(30), None),
                TxData(1, Eid(123), "available".to_string(), Eid(100), None),
                TxData(1, Eid(123), "available".to_string(), Eid(200), None),
            ])
            .unwrap();

        domain.advance_epoch(1).unwrap();
        domain.advance().unwrap();

        domain
            .transact(vec![TxData(
                -1,
                Eid(123),
                "available".to_string(),
                Eid(200),
                None,
            )])
            .unwrap();

        domain.advance_epoch(2).unwrap();
        domain.advance().unwrap();
    });
}
