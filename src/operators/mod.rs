//! Extension traits for `Stream` implementing various
//! declarative-specific operators.

use timely::dataflow::operators::aggregation::StateMachine;
use timely::dataflow::operators::Map;
use timely::dataflow::{Scope, Stream};

use crate::Value;

/// Provides the `cardinality_single` method.
pub trait CardinalitySingle<S: Scope> {
    /// Ensures that only a single value per eid exists within an
    /// attribute, by retracting any previous values upon new
    /// updates. Therefore this stream does not expect explicit
    /// retractions.
    fn cardinality_single(&self) -> Stream<S, ((Value, Value), S::Timestamp, isize)>;
}

impl<S: Scope> CardinalitySingle<S> for Stream<S, ((Value, Value), S::Timestamp, isize)> {
    fn cardinality_single(&self) -> Stream<S, ((Value, Value), S::Timestamp, isize)> {
        self.map(|((e, next_v), t, diff)| (e, (next_v, t, diff)))
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
    }
}
