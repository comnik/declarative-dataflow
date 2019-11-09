use timely::dataflow::operators::unordered_input::UnorderedInput;
use timely::dataflow::scopes::Scope;

use declarative_dataflow::domain::{AsSingletonDomain, Domain};
use declarative_dataflow::{Aid, Datom, Value};
use Value::{Eid, Number, String};

#[cfg(feature = "graphql")]
use declarative_dataflow::derive::graphql::GraphQl;

#[cfg(not(feature = "graphql"))]
fn main() {}

#[cfg(feature = "graphql")]
fn main() {
    timely::execute_directly(move |worker| {
        // let (send_results, results) = channel();

        let mut domain = worker.dataflow::<u64, _, _>(|scope| {
            let human_name = scope
                .new_unordered_input()
                .as_singleton_domain("human_name".to_string());

            let human_age = scope
                .new_unordered_input()
                .as_singleton_domain("human_age".to_string());

            let available = scope
                .new_unordered_input()
                .as_singleton_domain("available".to_string());

            let mut domain: Domain<Aid, u64> =
                (human_name + human_age + available).with_slack(1).into();

            domain
                .forward_propose(&"human_name".to_string())
                .unwrap()
                .import(scope)
                .as_collection(|e, v| (e.clone(), v.clone()))
                .inspect(|x| println!("{:?}", x));

            domain
                .forward_propose(&"human_age".to_string())
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
                .forward_propose(&"selected/human_name".to_string())
                .unwrap()
                .import(scope)
                .as_collection(|e, v| (e.clone(), v.clone()))
                .inspect(|x| println!("WORLD {:?}", x));

            world
                .forward_propose(&"selected/human_age".to_string())
                .unwrap()
                .import(scope)
                .as_collection(|e, v| (e.clone(), v.clone()))
                .inspect(|x| println!("WORLD {:?}", x));

            domain
        });

        domain
            .transact(vec![
                Datom(
                    Eid(100),
                    "human_name".to_string(),
                    String("Alice".to_string()),
                    None,
                    1,
                ),
                Datom(
                    Eid(200),
                    "human_name".to_string(),
                    String("Bob".to_string()),
                    None,
                    1,
                ),
                Datom(Eid(100), "human_age".to_string(), Number(40), None, 1),
                Datom(Eid(200), "human_age".to_string(), Number(30), None, 1),
                Datom(Eid(123), "available".to_string(), Eid(100), None, 1),
                Datom(Eid(123), "available".to_string(), Eid(200), None, 1),
            ])
            .unwrap();

        domain.advance_epoch(1).unwrap();
        domain.advance().unwrap();

        domain
            .transact(vec![Datom(
                Eid(123),
                "available".to_string(),
                Eid(200),
                None,
                -1,
            )])
            .unwrap();

        domain.advance_epoch(2).unwrap();
        domain.advance().unwrap();
    });
}
