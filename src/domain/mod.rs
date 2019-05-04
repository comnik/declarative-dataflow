//! Logic for working with attributes under a shared timestamp
//! semantics.

use std::collections::HashMap;

use timely::dataflow::operators::{Probe, UnorderedInput};
use timely::dataflow::{ProbeHandle, Scope, ScopeParent, Stream};
use timely::progress::frontier::AntichainRef;
use timely::progress::Timestamp;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::TraceReader;
use differential_dataflow::AsCollection;

use crate::operators::CardinalitySingle;
use crate::{Aid, Error, Rewind, TxData, Value};
use crate::{AttributeConfig, CollectionIndex, InputSemantics, RelationConfig, RelationHandle};

mod unordered_session;
use unordered_session::UnorderedSession;

/// A domain manages attributes that share a timestamp semantics. Each
/// attribute within a domain can be either fed from an external
/// system, or from user transactions. The former are referred to as
/// *sourced*, the latter as *transactable* attributes.
///
/// Both types of input must make sure not to block overall domain
/// progress, s.t. results can be revealed and traces can be
/// compacted. For attributes with an opinion on time, users and
/// source operators are required to regularly downgrade their
/// capabilities. As they do so, the domain frontier advances.
///
/// Some attributes do not care about time. Such attributes want their
/// information to be immediately available to all
/// queries. Conceptually, they want all their inputs to happen at
/// t0. This is however not a practical solution, because holding
/// capabilities for t0 in perpetuity completely stalls monotemporal
/// domains and prevents trace compaction in multitemporal ones. We
/// refer to this type of attributes as *timeless*. Instead, timeless
/// attributes must be automatically advanced in lockstep with a
/// high-watermark of all timeful domain inputs. This ensures that
/// they will never block overall progress.
pub struct Domain<T: Timestamp + Lattice> {
    /// The current input epoch.
    now_at: T,
    /// Input handles to attributes in this domain.
    input_sessions: HashMap<String, UnorderedSession<T, (Value, Value), isize>>,
    /// The probe keeping track of source progress in this domain.
    domain_probe: ProbeHandle<T>,
    /// Maintaining the number of probed sources allows us to
    /// distinguish between a domain without sources, and one where
    /// sources have ceased producing inputs.
    probed_source_count: usize,
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
            probed_source_count: 0,
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
            let (forward, reverse) = match config.input_semantics {
                InputSemantics::Raw => {
                    let tuples = pairs.as_collection();

                    let forward = CollectionIndex::index(&name, &tuples);
                    let reverse = CollectionIndex::index(&name, &tuples.map(|(e, v)| (v, e)));

                    (forward, reverse)
                }
                InputSemantics::CardinalityOne => {
                    let tuples = pairs.cardinality_single().as_collection();

                    let forward = CollectionIndex::index(&name, &tuples);
                    let reverse = CollectionIndex::index(&name, &tuples.map(|(e, v)| (v, e)));

                    (forward, reverse)
                }
                InputSemantics::CardinalityMany => {
                    // Ensure that redundant (e,v) pairs don't cause
                    // misleading proposals during joining.
                    let tuples = pairs.as_collection();

                    let forward = CollectionIndex::index_distinct(&name, &tuples);
                    let reverse =
                        CollectionIndex::index_distinct(&name, &tuples.map(|(e, v)| (v, e)));

                    (forward, reverse)
                }
            };

            self.forward.insert(name.to_string(), forward);
            self.reverse.insert(name.to_string(), reverse);

            // This is crucial. If we forget to install the attribute
            // configuration, its traces will be ignored when
            // advancing the domain.
            self.attributes.insert(name.to_string(), config);

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
            self.probed_source_count += 1;
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
        for TxData(op, e, a, v, t) in tx_data {
            match self.input_sessions.get_mut(&a) {
                None => {
                    return Err(Error::not_found(format!("Attribute {} does not exist.", a)));
                }
                Some(handle) => match t {
                    None => handle.update((Value::Eid(e), v), op),
                    Some(t) => handle.update_at((Value::Eid(e), v), t.into(), op),
                },
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

    /// Advances the domain to the current domain frontier, thus
    /// allowing traces to compact. All domain input handles are
    /// forwarded up to the frontier, so as not to stall progress.
    pub fn advance(&mut self) -> Result<(), Error> {
        if self.probed_source_count() == 0 {
            // No sources registered.
            self.advance_traces(&[self.epoch().clone()])
        } else {
            let frontier = self
                .domain_probe
                .with_frontier(|frontier| (*frontier).to_vec());

            if frontier.is_empty() {
                // Even if all sources dropped their capabilities we
                // still want to advance all traces to the current
                // epoch, s.t. user created attributes are
                // continuously advanced and compacted.

                self.advance_traces(&[self.epoch().clone()])
            } else {
                if !AntichainRef::new(&frontier).less_equal(self.epoch()) {
                    // Input handles have fallen behind the sources and need
                    // to be advanced, such as not to block progress.

                    let max = frontier.iter().max().unwrap().clone();
                    self.advance_epoch(max)?;
                }

                self.advance_traces(&frontier)
            }
        }
    }

    /// Advances the domain epoch. The domain epoch can be in advance
    /// of or lag behind the domain frontier. It is used by timeless
    /// attributes to avoid stalling timeful inputs.
    pub fn advance_epoch(&mut self, next: T) -> Result<(), Error> {
        if !self.now_at.less_equal(&next) {
            // We can't rewind time.
            Err(Error::conflict(format!(
                "Domain is at {:?}, you attempted to rewind to {:?}.",
                &self.now_at, &next
            )))
        } else if !self.now_at.eq(&next) {
            trace!("Advancing domain epoch to {:?} ", next);

            for handle in self.input_sessions.values_mut() {
                handle.advance_to(next.clone());
                handle.flush();
            }
            self.now_at = next;

            Ok(())
        } else {
            Ok(())
        }
    }

    /// Advances domain traces up to the specified frontier minus
    /// their configured slack.
    pub fn advance_traces(&mut self, frontier: &[T]) -> Result<(), Error> {
        let frontier = AntichainRef::new(frontier);

        for (aid, config) in self.attributes.iter() {
            if let Some(ref trace_slack) = config.trace_slack {
                let slacking_frontier = frontier
                    .iter()
                    .map(|t| t.rewind(trace_slack.clone().into()))
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
                    .map(|t| t.rewind(trace_slack.clone().into()))
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

    /// Returns a handle to the domain's input probe.
    pub fn domain_probe(&self) -> &ProbeHandle<T> {
        &self.domain_probe
    }

    /// Reports the current input epoch.
    pub fn epoch(&self) -> &T {
        &self.now_at
    }

    /// Reports the number of probed (timeful) sources in the domain.
    pub fn probed_source_count(&self) -> usize {
        self.probed_source_count
    }

    /// Returns true iff the frontier dominates all domain inputs.
    pub fn dominates(&self, frontier: AntichainRef<T>) -> bool {
        // We must distinguish the scenario where the internal domain
        // has no sources from one where all its internal sources have
        // dropped their capabilities. We do this by checking the
        // probed_source_count of the domain.

        if self.probed_source_count() == 0 {
            frontier.less_than(self.epoch())
        } else {
            if frontier.is_empty() {
                false
            } else {
                self.domain_probe().with_frontier(|domain_frontier| {
                    domain_frontier.iter().all(|t| frontier.less_than(t))
                })
            }
        }
    }
}
