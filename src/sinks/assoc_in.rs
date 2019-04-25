//! Operator and utilities to write output diffs into nested maps.

use std::collections::HashMap;

use timely::dataflow::channels::pact::ParallelizationContract;
use timely::dataflow::operators::generic::Operator;
use timely::dataflow::{Scope, Stream};
use timely::progress::Timestamp;

use differential_dataflow::lattice::Lattice;

use crate::{Error, ResultDiff};

use super::Sinkable;

/// A nested hash-map sink.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub struct AssocIn { }

impl<T: > Sinkable<T> for AssocIn
where
    T: Timestamp + Lattice,
{
    fn sink<S, P>(
        &self,
        stream: &Stream<S, ResultDiff<T>>,
        pact: P,
    ) -> Result<Stream<S, ResultDiff<T>>, Error>
    where
        S: Scope<Timestamp = T>,
        P: ParallelizationContract<S::Timestamp, ResultDiff<T>>,
    {
        let mut paths = HashMap::new();
        let mut vector = Vec::new();

        let sunk = stream
            .unary_notify(pact, "AssocIn", vec![], move |input, output, notificator| {
                input.for_each(|cap, data| {
                    data.swap(&mut vector);

                    let mut paths_at_time = paths
                        .entry(cap.time().clone())
                        .or_insert_with(Vec::new);

                    paths_at_time.extend(vector.drain(..).map(|(x,_t,diff)| (x, diff)));
                    
                    notificator.notify_at(cap.retain());
                });

                // pop completed views
                notificator.for_each(|cap,_,_| {
                    if let Some(paths_at_time) = paths.remove(cap.time()) {
                        output
                            .session(&cap)
                            .give(paths_to_nested(paths_at_time));
                    }
                });
            });

        Ok(sunk)
    }
}

/// Outbound direction: Transform the provided query result paths into
/// a GraphQL-like / JSONy nested value to be provided to the user.
fn paths_to_nested(paths: Vec<(Vec<crate::Value>, isize)>) -> serde_json::Value {
    
    use serde_json::map::Map;
    use serde_json::Value::Object;

    use crate::Value::{Aid, Eid};

    let mut acc = Map::new();

    // `paths` has the structure `[[aid/eid val aid/eid val last-aid
    // last-val], ...]` Starting from the result map `acc`, for each
    // path, we want to navigate down the map, optionally creating
    // intermediate map levels if they don't exist yet, and finally
    // insert `last-val` at the lowest level.
    //
    // In short: We're rebuilding Clojure's `assoc-in` here:
    // `(assoc-in acc (pop path) (peek path))`
    for (mut path, diff) in paths {

        // @TODO handle retractions
        
        let mut current_map = &mut acc;
        let last_val = path.pop().unwrap();

        if let Aid(last_key) = path.pop().unwrap() {
            // If there are already values for the looked at attribute, we obtain
            // a reference to it. Otherwise, we create a new `Map` and obtain a
            // reference to it, which will be used in the next iteration.
            // We repeat this process of traversing down the map and optionally creating
            // new map levels until we've run out of intermediate attributes.
            for attribute in path {
                // Keys have to be `String`s and are either `Aid`s (such as "age")
                // or `Eid`s (linking to a nested `PullPath`).
                let k = match attribute {
                    Aid(x) => x,
                    Eid(x) => x.to_string(),
                    _ => unreachable!(),
                };

                let entry = current_map
                    .entry(k)
                    .or_insert_with(|| Object(Map::new()));

                *entry = match entry {
                    Object(m) => Object(std::mem::replace(m, Map::new())),
                    serde_json::Value::Array(_) => unreachable!(),
                    _ => Object(Map::new()),
                };

                match entry {
                    Object(m) => current_map = m,
                    _ => unreachable!(),
                };
            }

            // At the lowest level, we finally insert the path's value
            match current_map.get(&last_key) {
                Some(Object(_)) => (),
                _ => {
                    current_map.insert(last_key, serde_json::json!(last_val));
                }
            };
        } else {
            unreachable!();
        }
    }

    serde_json::Value::Object(acc)
}
