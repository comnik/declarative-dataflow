//! Logic for working with attributes under a shared timestamp
//! semantics.

use std::collections::HashMap;
use std::ops::Sub;

use timely::dataflow::{ProbeHandle, Scope, Stream};
use timely::order::TotalOrder;
use timely::progress::Timestamp;

use differential_dataflow::input::{Input, InputSession};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::Threshold;
use differential_dataflow::trace::TraceReader;
use differential_dataflow::AsCollection;

use crate::operators::CardinalitySingle;
use crate::{Aid, Error, Time, TxData, Value};
use crate::{AttributeConfig, CollectionIndex, InputSemantics, RelationConfig, RelationHandle};

/// A domain manages attributes (and their inputs) that share a
/// timestamp semantics (e.g. come from the same logical source).
pub struct Domain<T: Timestamp + Lattice + TotalOrder> {
    /// The current timestamp.
    now_at: T,
    /// Input handles to attributes in this domain.
    input_sessions: HashMap<String, InputSession<T, (Value, Value), isize>>,
    /// The probe keeping track of progress in this domain.
    probe: ProbeHandle<T>,
    /// Configurations for attributes in this domain.
    pub attributes: HashMap<Aid, AttributeConfig>,
    /// Forward attribute indices eid -> v.
    pub forward: HashMap<Aid, CollectionIndex<Value, Value, T>>,
    /// Reverse attribute indices v -> eid.
    pub reverse: HashMap<Aid, CollectionIndex<Value, Value, T>>,
    /// Configuration for relations in this domain.
    pub relations: HashMap<Aid, RelationConfig<T>>,
    /// Relation traces.
    pub arrangements: HashMap<Aid, RelationHandle<T>>,
}

impl<T> Domain<T>
where
    T: Timestamp + Lattice + TotalOrder + Sub<Output = T> + std::convert::From<Time>,
{
    /// Creates a new domain.
    pub fn new(start_at: T) -> Self {
        Domain {
            now_at: start_at,
            input_sessions: HashMap::new(),
            probe: ProbeHandle::new(),
            attributes: HashMap::new(),
            forward: HashMap::new(),
            reverse: HashMap::new(),
            relations: HashMap::new(),
            arrangements: HashMap::new(),
        }
    }

    /// Creates a new collection of (e,v) tuples and indexes it in
    /// various ways. Stores forward, and reverse indices, as well as
    /// the input handle in the server state.
    pub fn create_attribute<S: Scope<Timestamp = T>>(
        &mut self,
        name: &str,
        config: AttributeConfig,
        scope: &mut S,
    ) -> Result<(), Error>
    where
        T: TotalOrder,
    {
        if self.forward.contains_key(name) {
            Err(Error {
                category: "df.error.category/conflict",
                message: format!("An attribute of name {} already exists.", name),
            })
        } else {
            let (handle, tuples) = scope.new_collection::<(Value, Value), isize>();

            self.input_sessions.insert(name.to_string(), handle);

            self.create_source(name, config, &tuples.inner)?;

            Ok(())
        }
    }

    /// Creates attributes from an external datoms source.
    pub fn create_source<S: Scope + ScopeParent<Timestamp = T>>(
        &mut self,
        name: &str,
        config: AttributeConfig,
        datoms: &Stream<S, ((Value, Value), T, isize)>,
    ) -> Result<(), Error> {
        if self.forward.contains_key(name) {
            Err(Error {
                category: "df.error.category/conflict",
                message: format!("An attribute of name {} already exists.", name),
            })
        } else {
            let tuples = match config.input_semantics {
                InputSemantics::Raw => datoms.as_collection(),
                InputSemantics::CardinalityOne => datoms.cardinality_single().as_collection(),
                InputSemantics::CardinalityMany => {
                    // Ensure that redundant (e,v) pairs don't cause
                    // misleading proposals during joining.
                    datoms.as_collection().distinct()
                }
            };

            self.attributes.insert(name.to_string(), config);

            let forward = CollectionIndex::index(&name, &tuples);
            let reverse = CollectionIndex::index(&name, &tuples.map(|(e, v)| (v, e)));

            self.forward.insert(name.to_string(), forward);
            self.reverse.insert(name.to_string(), reverse);

            info!("Created attribute {}", name);

            Ok(())
        }
    }

    /// Inserts a new named relation.
    pub fn register_arrangement(
        &mut self,
        name: String,
        config: RelationConfig<T>,
        mut trace: RelationHandle<T>,
    ) {
        // decline the capability for that trace handle to subset its
        // view of the data
        trace.distinguish_since(&[]);

        self.relations.insert(name.clone(), config);
        self.arrangements.insert(name, trace);
    }

    /// Transact data into one or more inputs.
    pub fn transact(&mut self, tx_data: Vec<TxData>) -> Result<(), Error> {
        // @TODO do this smarter, e.g. grouped by handle
        for TxData(op, e, a, v) in tx_data {
            match self.input_sessions.get_mut(&a) {
                None => {
                    return Err(Error {
                        category: "df.error.category/not-found",
                        message: format!("Attribute {} does not exist.", a),
                    });
                }
                Some(handle) => {
                    handle.update((Value::Eid(e), v), op);
                }
            }
        }

        Ok(())
    }

    /// Closes and drops an existing input.
    pub fn close_input(&mut self, name: String) -> Result<(), Error> {
        match self.input_sessions.remove(&name) {
            None => Err(Error {
                category: "df.error.category/not-found",
                message: format!("Input {} does not exist.", name),
            }),
            Some(handle) => {
                handle.close();
                Ok(())
            }
        }
    }

    /// Advances the domain to `next`. Advances all traces
    /// accordingly, depending on their configured slack.
    pub fn advance_to(&mut self, next: T) -> Result<(), Error> {
        if !self.now_at.less_equal(&next) {
            // We can't rewind time.
            Err(Error {
                category: "df.error.category/conflict",
                message: format!(
                    "Domain is at {:?}, you attempted to rewind to {:?}.",
                    &self.now_at, &next
                ),
            })
        } else if !self.now_at.eq(&next) {
            self.now_at = next.clone();

            for handle in self.input_sessions.values_mut() {
                handle.advance_to(next.clone());
                handle.flush();
            }

            for (aid, config) in self.attributes.iter() {
                if let Some(ref trace_slack) = config.trace_slack {
                    let frontier = &[next.clone() - trace_slack.clone().into()];

                    self.forward
                        .get_mut(aid)
                        .unwrap_or_else(|| {
                            panic!("Configuration available for unknown attribute {}", aid)
                        })
                        .advance_by(frontier);

                    self.reverse
                        .get_mut(aid)
                        .unwrap_or_else(|| {
                            panic!("Configuration available for unknown attribute {}", aid)
                        })
                        .advance_by(frontier);
                }
            }

            for (name, config) in self.relations.iter() {
                if let Some(ref trace_slack) = config.trace_slack {
                    let frontier = &[next.clone() - trace_slack.clone()];

                    self.arrangements
                        .get_mut(name)
                        .unwrap_or_else(|| {
                            panic!("Configuration available for unknown relation {}", name)
                        })
                        .advance_by(frontier);
                }
            }

            Ok(())
        } else {
            Ok(())
        }
    }

    /// Reports the current timestamp.
    pub fn time(&self) -> &T {
        &self.now_at
    }
}
