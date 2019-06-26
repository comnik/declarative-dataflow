//! Operator and utilities to write output diffs into nested maps.

use std::collections::HashMap;

use timely::dataflow::channels::pact::ParallelizationContract;
use timely::dataflow::operators::generic::Operator;
use timely::dataflow::{ProbeHandle, Scope, Stream};
use timely::progress::Timestamp;

use differential_dataflow::lattice::Lattice;

use serde_json::map::Map;
use serde_json::Value as JValue;
use serde_json::Value::Object;

use crate::{Error, Output, ResultDiff, Time};

use super::{Sinkable, SinkingContext};

/// A nested hash-map sink. Intra timestamp. For an inter timestamp version
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub struct AssocIn {
    /// Specifies whether to output full snapshots or just
    /// consolidated differences. None means no history and thus,
    /// diffs will merely be consolidated into nested maps as they
    /// pass through. Some(granularity) indicates that history is
    /// kept, and changes are reported at the specified granularity. A
    /// granularity of 1 means the full result set is forwarded on
    /// each change. A granularity of n means that only the
    /// sub-structure at level n will be forwarded on a change.
    pub stateful: Option<usize>,
}

impl<T> Sinkable<T> for AssocIn
where
    T: Timestamp + Lattice + std::convert::Into<Time>,
{
    fn sink<S, P>(
        &self,
        stream: &Stream<S, ResultDiff<T>>,
        pact: P,
        _probe: &mut ProbeHandle<T>,
        context: SinkingContext,
    ) -> Result<Option<Stream<S, Output>>, Error>
    where
        S: Scope<Timestamp = T>,
        P: ParallelizationContract<S::Timestamp, ResultDiff<T>>,
    {
        // This is basically Timely's Aggregate, reimplemented here,
        // s.t. we can pass in the pact instead of a routing function.

        let mut paths = HashMap::new();
        let mut states = if self.stateful.is_some() {
            Some(Map::new())
        } else {
            None
        };

        let granularity = self.stateful.unwrap_or(1);

        let mut vector = Vec::new();

        let name = context.name;

        let sunk = stream.unary_notify(
            pact,
            "AssocIn",
            vec![],
            move |input, output, notificator| {
                input.for_each(|cap, data| {
                    data.swap(&mut vector);

                    paths
                        .entry(cap.time().clone())
                        .or_insert_with(Vec::new)
                        .extend(vector.drain(..));

                    notificator.notify_at(cap.retain());
                });

                // pop completed views
                notificator.for_each(|cap, _, _| {
                    if let Some(paths_at_time) = paths.remove(cap.time()) {
                        match states {
                            None => {
                                let t = cap.time();

                                let mut map = Map::new();
                                merge_paths(&mut map, paths_at_time, granularity);

                                let keys: Vec<String> = map.keys().cloned().collect();

                                output.session(&cap).give_iterator(keys.iter().map(|key| {
                                    Output::Json(
                                        name.clone(),
                                        map.remove(key).unwrap(),
                                        t.clone().into(),
                                        1,
                                    )
                                }));
                            }
                            Some(ref mut states) => {
                                let t = cap.time();

                                // @TODO group and sort paths by their
                                // Differential time, apply each group
                                // and produce outputs

                                let changes = merge_paths(states, paths_at_time, granularity);

                                output.session(&cap).give_iterator(changes.iter().map(
                                    |change_key| {
                                        let mut snapshot = &states[&change_key[0]];
                                        for key in &change_key[1..] {
                                            if let Object(map) = snapshot {
                                                snapshot = &map[key];
                                            }
                                        }
                                        Output::Json(
                                            name.clone(),
                                            snapshot.clone(),
                                            t.clone().into(),
                                            1,
                                        )
                                    },
                                ));
                            }
                        }
                    }
                });
            },
        );

        Ok(Some(sunk))
    }
}

/// Outbound direction: Transform the provided query result paths into
/// a GraphQL-like / JSONy nested value to be provided to the user.
fn merge_paths<T>(
    acc: &mut Map<String, JValue>,
    mut paths: Vec<(Vec<crate::Value>, T, isize)>,
    granularity: usize,
) -> Vec<Vec<String>>
where
    T: Timestamp + Lattice + std::convert::Into<Time>,
{
    use crate::Value;

    // `paths` has the structure `[[aid/eid val aid/eid val leaf-aid
    // leaf-val], ...]` Starting from the result map `acc`, for each
    // path, we want to navigate down the map, optionally creating
    // intermediate map levels if they don't exist yet, and finally
    // insert `leaf-val` at the lowest level.
    //
    // In short: We're rebuilding Clojure's `assoc-in` here:
    // `(assoc-in acc (pop path) (peek path))`

    // Keys have to be either aids (attributes on an entity)
    // or eids (nested entities). Both types will be converted
    // to strings.
    let parse_key = |v: Value| match v {
        Value::Aid(x) => x,
        Value::Eid(x) => x.to_string(),
        Value::String(x) => x,
        _ => panic!("Malformed pull path. Expected a key."),
    };

    // It's important to treat retractions first, otherwise we might
    // dissoc the new value.
    paths.sort_by_key(|(_path, _t, diff)| *diff);
    paths.sort_by_key(|(_path, t, _diff)| t.clone());

    let mut changes: Vec<Vec<String>> = Vec::new();

    for (mut path, _t, diff) in paths {
        let mut change_key: Vec<String> = Vec::new();

        let leaf_val: Value = path.pop().expect("leaf value missing");
        let leaf_key: String = parse_key(path.pop().expect("leaf keay missing"));

        if path.is_empty() {
            if change_key.len() < granularity {
                change_key.push(leaf_key.clone());
            }
            acc.insert(leaf_key, serde_json::Value::from(leaf_val));
        } else {
            let first_key = parse_key(path[0].clone());

            if change_key.len() < granularity {
                change_key.push(first_key.clone());
            }

            let mut entry = acc.entry(first_key).or_insert_with(|| Object(Map::new()));

            for key in path.drain(1..) {
                // If there are already values for the looked at attribute, we
                // obtain a reference to it. Otherwise, we create a new `Map`
                // and obtain a reference to it, which will be used in the
                // next iteration.  We repeat this process of traversing down
                // the map and optionally creating new map levels until we've
                // run out of intermediate attributes.

                if change_key.len() < granularity {
                    change_key.push(parse_key(key.clone()));
                }

                if let Object(map) = entry {
                    entry = map
                        .entry(parse_key(key))
                        .or_insert_with(|| Object(Map::new()));
                }
            }

            // At the lowest level, we finally insert the leaf value.
            if let Object(map) = entry {
                if diff > 0 {
                    map.insert(leaf_key, serde_json::Value::from(leaf_val));
                } else {
                    map.remove(&leaf_key);
                }
            }
        }

        changes.push(change_key);
    }

    changes.sort();
    changes.dedup();
    changes
}
