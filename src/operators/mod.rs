//! Extension traits for `Stream` implementing various
//! declarative-specific operators.

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::aggregation::StateMachine;
use timely::dataflow::operators::{generic::operator::Operator, Map};
use timely::dataflow::Scope;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::{Arrange, Arranged};
use differential_dataflow::trace::{cursor::Cursor, BatchReader};
use differential_dataflow::{AsCollection, Collection};

use crate::{TraceValHandle, Value};

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
        let arranged: Arranged<S, TraceValHandle<Value, Value, S::Timestamp, isize>> =
            self.arrange();

        arranged
            .stream
            .unary(Pipeline, "AsCollection", move |_, _| {
                move |input, output| {
                    input.for_each(|time, data| {
                        let mut session = output.session(&time);
                        for wrapper in data.iter() {
                            let batch = &wrapper;
                            let mut cursor = batch.cursor();
                            while let Some(key) = cursor.get_key(batch) {
                                let mut tuples = Vec::new();
                                while let Some(val) = cursor.get_val(batch) {
                                    cursor.map_times(batch, |time, diff| {
                                        tuples.push((
                                            (key.clone(), val.clone()),
                                            time.clone(),
                                            diff.clone(),
                                        ));
                                    });
                                    cursor.step_val(batch);
                                }

                                tuples.sort_by_key(|(_, ref t, _)| t.clone());
                                session.give_iterator(tuples.drain(..));

                                cursor.step_key(batch);
                            }
                        }
                    });
                }
            })
            .map(
                |((e, next_v), t, diff): ((Value, Value), S::Timestamp, isize)| {
                    (e, (next_v, t, diff))
                },
            )
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
                |e| match e {
                    Value::Eid(eid) => *eid as u64,
                    #[cfg(feature = "uuid")]
                    Value::Uuid(uuid) => {
                        use differential_dataflow::hashable::Hashable;
                        uuid.hashed()
                    }
                    _ => panic!("Not an eid or uuid: {:?}.", e),
                },
            )
            .as_collection()
    }
}
