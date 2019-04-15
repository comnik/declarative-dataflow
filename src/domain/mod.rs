//! Logic for working with attributes under a shared timestamp
//! semantics.

use std::collections::HashMap;
use std::ops::Sub;

use timely::dataflow::{ProbeHandle, Scope, ScopeParent, Stream};
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
pub struct Domain<T: Timestamp + Lattice> {
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
    T: Timestamp + Lattice + Sub<Output = T> + std::convert::From<Time>,
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

    /// Creates a new attribute that can be transacted upon by
    /// clients.
    pub fn create_transactable_attribute<S: Scope<Timestamp = T>>(
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
            let (handle, pairs) = scope.new_collection::<(Value, Value), isize>();

            self.input_sessions.insert(name.to_string(), handle);

            self.create_sourced_attribute(name, config, &pairs.inner)?;

            Ok(())
        }
    }

    /// Creates attributes from a stream of (entity,value)
    /// pairs. Attributes created this way can not be transacted upon
    /// by clients.
    pub fn create_sourced_attribute<S: Scope + ScopeParent<Timestamp = T>>(
        &mut self,
        name: &str,
        config: AttributeConfig,
        pairs: &Stream<S, ((Value, Value), T, isize)>,
    ) -> Result<(), Error> {
        if self.forward.contains_key(name) {
            Err(Error {
                category: "df.error.category/conflict",
                message: format!("An attribute of name {} already exists.", name),
            })
        } else {
            let tuples = {
                let unprobed = match config.input_semantics {
                    InputSemantics::Raw => pairs.as_collection(),
                    InputSemantics::CardinalityOne => pairs.cardinality_single().as_collection(),
                    InputSemantics::CardinalityMany => {
                        // Ensure that redundant (e,v) pairs don't cause
                        // misleading proposals during joining.
                        pairs.as_collection().distinct()
                    }
                };

                unprobed.probe_with(&mut self.probe)
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
        trace: RelationHandle<T>,
    ) {
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

    /// @DEPRECATED in favor of advance_by
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

                    let forward_index = self.forward.get_mut(aid).unwrap_or_else(|| {
                        panic!("Configuration available for unknown attribute {}", aid)
                    });

                    forward_index.advance_by(frontier);
                    forward_index.distinguish_since(frontier);

                    let reverse_index = self.reverse.get_mut(aid).unwrap_or_else(|| {
                        panic!("Configuration available for unknown attribute {}", aid)
                    });

                    reverse_index.advance_by(frontier);
                    reverse_index.distinguish_since(frontier);
                }
            }

            for (name, config) in self.relations.iter() {
                if let Some(ref trace_slack) = config.trace_slack {
                    let frontier = &[next.clone() - trace_slack.clone()];

                    let trace = self.arrangements.get_mut(name).unwrap_or_else(|| {
                        panic!("Configuration available for unknown relation {}", name)
                    });

                    trace.advance_by(frontier);
                    trace.distinguish_since(frontier);
                }
            }

            Ok(())
        } else {
            Ok(())
        }
    }

    /// Advances all handles of the domain to its current frontier.
    pub fn advance_domain_to_source(&mut self) -> Result<(), Error> {
        let frontier = self.probe.with_frontier(|frontier| (*frontier).to_vec());
        self.advance_by(&frontier)
    }

    /// Advances the domain up to the specified frontier. Advances all
    /// traces accordingly, depending on their configured slack.
    pub fn advance_by(&mut self, frontier: &[T]) -> Result<(), Error> {
        if frontier.is_empty() {
            self.input_sessions.clear();
        } else if frontier.len() == 1 {
            for handle in self.input_sessions.values_mut() {
                handle.advance_to(frontier[0].clone());
                handle.flush();
            }
        } else {
            unimplemented!();
        }

        for (aid, config) in self.attributes.iter() {
            if let Some(ref trace_slack) = config.trace_slack {
                let slacking_frontier = frontier
                    .iter()
                    .cloned()
                    .map(|t| t - trace_slack.clone().into())
                    .collect::<Vec<T>>();;

                let forward_index = self.forward.get_mut(aid).unwrap_or_else(|| {
                    panic!("Configuration available for unknown attribute {}", aid)
                });

                forward_index.advance_by(&slacking_frontier);
                forward_index.distinguish_since(&slacking_frontier);

                let reverse_index = self.reverse.get_mut(aid).unwrap_or_else(|| {
                    panic!("Configuration available for unknown attribute {}", aid)
                });

                reverse_index.advance_by(&slacking_frontier);
                reverse_index.distinguish_since(&slacking_frontier);
            }
        }

        for (name, config) in self.relations.iter() {
            if let Some(ref trace_slack) = config.trace_slack {
                let slacking_frontier = frontier
                    .iter()
                    .cloned()
                    .map(|t| t - trace_slack.clone().into())
                    .collect::<Vec<T>>();

                let trace = self.arrangements.get_mut(name).unwrap_or_else(|| {
                    panic!("Configuration available for unknown relation {}", name)
                });

                trace.advance_by(&slacking_frontier);
                trace.distinguish_since(&slacking_frontier);
            }
        }

        Ok(())
    }

    /// Reports the current timestamp.
    pub fn time(&self) -> &T {
        &self.now_at
    }

    /// Returns a handle to the domain's input probe.
    pub fn probe(&self) -> &ProbeHandle<T> {
        &self.probe
    }
}
