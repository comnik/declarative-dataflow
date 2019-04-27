//! Logic for working with attributes under a shared timestamp
//! semantics.

use std::collections::HashMap;

use timely::dataflow::operators::{Probe, UnorderedInput};
use timely::dataflow::{ProbeHandle, Scope, ScopeParent, Stream};
use timely::progress::frontier::AntichainRef;
use timely::progress::Timestamp;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::Threshold;
use differential_dataflow::trace::TraceReader;
use differential_dataflow::AsCollection;

use crate::operators::CardinalitySingle;
use crate::{Aid, Error, Rewind, TxData, Value};
use crate::{AttributeConfig, CollectionIndex, InputSemantics, RelationConfig, RelationHandle};

mod unordered_session;
use unordered_session::UnorderedSession;

/// A domain manages attributes (and their inputs) that share a
/// timestamp semantics (e.g. come from the same logical source).
pub struct Domain<T: Timestamp + Lattice> {
    /// The current timestamp.
    now_at: T,
    /// Input handles to attributes in this domain.
    input_sessions: HashMap<String, UnorderedSession<T, (Value, Value), isize>>,
    /// The probe keeping track of source progress in this domain.
    domain_probe: ProbeHandle<T>,
    /// Configurations for attributes in this domain.
    pub attributes: HashMap<Aid, AttributeConfig>,
    /// Forward attribute indices eid -> v.
    pub forward: HashMap<Aid, CollectionIndex<Value, Value, T>>,
    /// Reverse attribute indices v -> eid.
    pub reverse: HashMap<Aid, CollectionIndex<Value, Value, T>>,
    /// Configuration for relations in this domain.
    pub relations: HashMap<Aid, RelationConfig>,
    /// Relation traces.
    pub arrangements: HashMap<Aid, RelationHandle<T>>,
}

impl<T> Domain<T>
where
    T: Timestamp + Lattice + Rewind,
{
    /// Creates a new domain.
    pub fn new(start_at: T) -> Self {
        Domain {
            now_at: start_at,
            input_sessions: HashMap::new(),
            domain_probe: ProbeHandle::new(),
            attributes: HashMap::new(),
            forward: HashMap::new(),
            reverse: HashMap::new(),
            relations: HashMap::new(),
            arrangements: HashMap::new(),
        }
    }

    /// Creates an attribute from a stream of (key,value)
    /// pairs. Applies operators to enforce input semantics, registers
    /// the attribute configuration, and installs appropriate indices.
    fn create_attribute<S: Scope + ScopeParent<Timestamp = T>>(
        &mut self,
        name: &str,
        config: AttributeConfig,
        pairs: &Stream<S, ((Value, Value), T, isize)>,
    ) -> Result<(), Error> {
        if self.forward.contains_key(name) {
            Err(Error::conflict(format!(
                "An attribute of name {} already exists.",
                name
            )))
        } else {
            let tuples = match config.input_semantics {
                InputSemantics::Raw => pairs.as_collection(),
                InputSemantics::CardinalityOne => pairs.cardinality_single().as_collection(),
                // Ensure that redundant (e,v) pairs don't cause
                // misleading proposals during joining.
                InputSemantics::CardinalityMany => pairs.as_collection().distinct(),
            };

            // This is crucial. If we forget to install the attribute
            // configuration, its traces will be ignored when
            // advancing the domain.
            self.attributes.insert(name.to_string(), config);

            let forward = CollectionIndex::index(&name, &tuples);
            let reverse = CollectionIndex::index(&name, &tuples.map(|(e, v)| (v, e)));

            self.forward.insert(name.to_string(), forward);
            self.reverse.insert(name.to_string(), reverse);

            info!("Created attribute {}", name);

            Ok(())
        }
    }

    /// Creates an attribute that can be transacted upon by clients.
    pub fn create_transactable_attribute<S: Scope<Timestamp = T>>(
        &mut self,
        name: &str,
        config: AttributeConfig,
        scope: &mut S,
    ) -> Result<(), Error> {
        let pairs = {
            let ((handle, cap), pairs) = scope.new_unordered_input::<((Value, Value), T, isize)>();
            let session = UnorderedSession::from(handle, cap);

            self.input_sessions.insert(name.to_string(), session);

            pairs
        };

        // We do not want to probe transactable attributes, because
        // the domain epoch is authoritative for them.
        self.create_attribute(name, config, &pairs)?;

        Ok(())
    }

    /// Creates an attribute that is controlled by a source and thus
    /// can not be transacted upon by clients.
    pub fn create_sourced_attribute<S: Scope + ScopeParent<Timestamp = T>>(
        &mut self,
        name: &str,
        config: AttributeConfig,
        pairs: &Stream<S, ((Value, Value), T, isize)>,
    ) -> Result<(), Error> {
        // We need to install a probe on source-fed attributes in
        // order to determine their progress.

        // We do not want to probe timeless attributes.
        // Sources of timeless attributes either are not able to or do not
        // want to provide valid domain timestamps.
        // Forcing to probe them would stall progress in the system.
        let source_pairs = if config.timeless {
            pairs.to_owned()
        } else {
            pairs.probe_with(&mut self.domain_probe)
        };

        self.create_attribute(name, config, &source_pairs)?;

        Ok(())
    }

    /// Inserts a new named relation.
    pub fn register_arrangement(
        &mut self,
        name: String,
        config: RelationConfig,
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
                    return Err(Error::not_found(format!("Attribute {} does not exist.", a)));
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
            None => Err(Error::not_found(format!("Input {} does not exist.", name))),
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
            Err(Error::conflict(format!(
                "Domain is at {:?}, you attempted to rewind to {:?}.",
                &self.now_at, &next
            )))
        } else if !self.now_at.eq(&next) {
            trace!(
                "Advancing domain to {:?} ({} attributes, {} handles)",
                next,
                self.attributes.len(),
                self.input_sessions.len()
            );

            self.now_at = next.clone();

            for handle in self.input_sessions.values_mut() {
                handle.advance_to(next.clone());
                handle.flush();
            }

            for (aid, config) in self.attributes.iter() {
                if let Some(ref trace_slack) = config.trace_slack {
                    let frontier = &[next.rewind(trace_slack.clone())];

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
                    let frontier = &[next.rewind(trace_slack.clone())];

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
        let frontier = self
            .domain_probe
            .with_frontier(|frontier| (*frontier).to_vec());
        self.advance_by(&frontier)
    }

    /// Advances the domain up to the specified frontier. Advances all
    /// traces accordingly, depending on their configured slack.
    pub fn advance_by(&mut self, frontier: &[T]) -> Result<(), Error> {
        let frontier = AntichainRef::new(frontier);

        if !frontier.less_equal(&self.now_at) {
            // Input handles have fallen behind the sources and need
            // to be advanced, such as not to block progress.
            if frontier.is_empty() {
                self.input_sessions.clear();
            } else if frontier.len() == 1 {
                for handle in self.input_sessions.values_mut() {
                    handle.advance_to(frontier[0].clone());
                    handle.flush();
                }
                self.now_at = frontier[0].clone();
            } else {
                // @TODO This can only happen with partially ordered
                // domain timestamps (e.g. bitemporal mode). We will
                // worry about when it happens.
                unimplemented!();
            }
        }

        for (aid, config) in self.attributes.iter() {
            if let Some(ref trace_slack) = config.trace_slack {
                let slacking_frontier = frontier
                    .iter()
                    .map(|t| t.rewind(trace_slack.clone()))
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
                    .map(|t| t.rewind(trace_slack.clone()))
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
    pub fn domain_probe(&self) -> &ProbeHandle<T> {
        &self.domain_probe
    }
}
