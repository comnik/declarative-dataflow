//! Extension traits for `Stream` implementing various
//! declarative-specific operators.

use timely::dataflow::operators::aggregation::StateMachine;
use timely::dataflow::operators::Map;
use timely::dataflow::Scope;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::Consolidate;
use differential_dataflow::{AsCollection, Collection};

use crate::Value;

/// Provides the `cardinality_one` method.
pub trait CardinalityOne<S: Scope> {
    /// Ensures that only a single value per eid exists within an
    /// attribute, by retracting any previous values upon new
    /// updates. Therefore this stream does not expect explicit
    /// retractions.
    fn cardinality_one(&self) -> Collection<S, (Value, Value), isize>;
}

impl<S> CardinalityOne<S> for Collection<S, (Value, Value), isize>
where
    S: Scope,
    S::Timestamp: Lattice + Ord,
{
    fn cardinality_one(&self) -> Collection<S, (Value, Value), isize> {
        self.consolidate()
            .inner
            .map(|((e, next_v), t, diff)| (e, (next_v, t, diff)))
            .state_machine(
                |e, (next_v, t, diff), v| {
                    match v {
                        None => {
                            assert!(
                                diff > 0,
                                "Received a retraction of a new key on a CardinalityOne attribute"
                            );
                            *v = Some(next_v.clone());
                            (false, vec![((e.clone(), next_v), t, 1)])
                        }
                        Some(old_v) => {
                            let old_v = old_v.clone();
                            if diff > 0 {
                                *v = Some(next_v.clone());
                                (
                                    false,
                                    vec![
                                        ((e.clone(), old_v), t.clone(), -1),
                                        ((e.clone(), next_v), t, 1),
                                    ],
                                )
                            } else {
                                // Retraction received. Can clean up state.
                                (true, vec![((e.clone(), old_v), t, -1)])
                            }
                        }
                    }
                },
                |e| {
                    if let Value::Eid(eid) = e {
                        *eid as u64
                    } else {
                        panic!("Expected an eid.");
                    }
                },
            )
            .as_collection()
    }
}
