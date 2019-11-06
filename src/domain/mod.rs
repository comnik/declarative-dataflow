//! Logic for working with attributes under a shared timestamp
//! semantics.

use std::collections::HashMap;
use std::ops::{Add, AddAssign};

use timely::dataflow::operators::unordered_input::{ActivateCapability, UnorderedHandle};
use timely::dataflow::operators::Map;
use timely::dataflow::{ProbeHandle, Scope, Stream};
use timely::progress::frontier::AntichainRef;
use timely::progress::Timestamp;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::Arrange;
use differential_dataflow::trace::TraceReader;
use differential_dataflow::{AsCollection, Collection};

use crate::{Aid, Error, Rewind, Rule, TxData, Value};
use crate::{AttributeConfig, QuerySupport};
use crate::{ShutdownHandle, TraceKeyHandle, TraceValHandle};

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
    /// A namespace under which all attributes in this domain are
    /// contained.
    namespace: String,
    /// The current input epoch.
    now_at: T,
    /// Last trace advance.
    last_advance: Vec<T>,
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
    /// Forward count traces.
    pub forward_count: HashMap<Aid, TraceKeyHandle<Value, T, isize>>,
    /// Forward propose traces.
    pub forward_propose: HashMap<Aid, TraceValHandle<Value, Value, T, isize>>,
    /// Forward validate traces.
    pub forward_validate: HashMap<Aid, TraceKeyHandle<(Value, Value), T, isize>>,
    /// Reverse count traces.
    pub reverse_count: HashMap<Aid, TraceKeyHandle<Value, T, isize>>,
    /// Reverse propose traces.
    pub reverse_propose: HashMap<Aid, TraceValHandle<Value, Value, T, isize>>,
    /// Reverse validate traces.
    pub reverse_validate: HashMap<Aid, TraceKeyHandle<(Value, Value), T, isize>>,
    /// Representation of named rules.
    pub rules: HashMap<Aid, Rule>,
    /// Mapping from query names to their shutdown handles.
    pub shutdown_handles: HashMap<String, ShutdownHandle>,
}

// We're defining domain composition here.
impl<T> AddAssign for Domain<T>
where
    T: Timestamp + Lattice,
{
    fn add_assign(&mut self, other: Self) {
        assert_eq!(
            self.namespace, other.namespace,
            "Only domains within a namespace can be composed."
        );
        assert!(
            !self
                .attributes
                .keys()
                .any(|k| other.attributes.contains_key(k)),
            "Domain attributes clash."
        );

        self.now_at = self.now_at.meet(&other.now_at);
        // @TODO
        // self.last_advance = ???
        self.input_sessions.extend(other.input_sessions.into_iter());

        assert!(
            (other.probed_source_count == 0) || (self.probed_source_count == 0),
            "Domain composition not supported for two domains with external sources."
        );

        if self.probed_source_count == 0 {
            self.domain_probe = other.domain_probe;
            self.probed_source_count = other.probed_source_count;
        }

        self.attributes.extend(other.attributes.into_iter());

        self.forward_count.extend(other.forward_count.into_iter());
        self.forward_propose
            .extend(other.forward_propose.into_iter());
        self.forward_validate
            .extend(other.forward_validate.into_iter());

        self.reverse_count.extend(other.reverse_count.into_iter());
        self.reverse_propose
            .extend(other.reverse_propose.into_iter());
        self.reverse_validate
            .extend(other.reverse_validate.into_iter());

        self.rules.extend(other.rules.into_iter());

        self.shutdown_handles
            .extend(other.shutdown_handles.into_iter());
    }
}

impl<T> Add for Domain<T>
where
    T: Timestamp + Lattice,
{
    type Output = Self;

    fn add(self, other: Self) -> Self {
        let mut merged = self;
        merged += other;
        merged
    }
}

impl<T> Domain<T>
where
    T: Timestamp + Lattice + Rewind,
{
    /// Creates a new domain.
    pub fn new(start_at: T) -> Self {
        Domain {
            namespace: Default::default(),
            now_at: start_at,
            last_advance: vec![<T as Lattice>::minimum()],
            input_sessions: HashMap::new(),
            domain_probe: ProbeHandle::new(),
            probed_source_count: 0,
            attributes: HashMap::new(),
            forward_count: HashMap::new(),
            forward_propose: HashMap::new(),
            forward_validate: HashMap::new(),
            reverse_count: HashMap::new(),
            reverse_propose: HashMap::new(),
            reverse_validate: HashMap::new(),
            rules: HashMap::new(),
            shutdown_handles: HashMap::new(),
        }
    }

    /// Creates a new domain.
    pub fn new_from(namespace: &str, base: &Self) -> Self {
        Domain {
            namespace: namespace.to_string(),
            now_at: base.now_at.clone(),
            last_advance: base.last_advance.clone(),
            input_sessions: HashMap::new(),
            domain_probe: ProbeHandle::new(),
            probed_source_count: 0,
            attributes: HashMap::new(),
            forward_count: HashMap::new(),
            forward_propose: HashMap::new(),
            forward_validate: HashMap::new(),
            reverse_count: HashMap::new(),
            reverse_propose: HashMap::new(),
            reverse_validate: HashMap::new(),
            rules: HashMap::new(),
            shutdown_handles: HashMap::new(),
        }
    }

    /// Transact data into one or more inputs.
    pub fn transact(&mut self, tx_data: Vec<TxData>) -> Result<(), Error> {
        for TxData(op, e, a, v, t) in tx_data {
            match self.input_sessions.get_mut(&a) {
                None => {
                    return Err(Error::not_found(format!("Attribute {} does not exist.", a)));
                }
                Some(handle) => match t {
                    None => handle.update((e, v), op),
                    Some(t) => handle.update_at((e, v), t.into(), op),
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
        let last_advance = AntichainRef::new(&self.last_advance);

        if frontier.iter().any(|t| last_advance.less_than(t)) {
            trace!("Advancing traces to {:?}", frontier);

            self.last_advance = frontier.to_vec();
            let frontier = AntichainRef::new(frontier);

            for (aid, config) in self.attributes.iter() {
                if let Some(ref trace_slack) = config.trace_slack {
                    let slacking_frontier = frontier
                        .iter()
                        .map(|t| t.rewind(trace_slack.clone().into()))
                        .collect::<Vec<T>>();;

                    if let Some(trace) = self.forward_count.get_mut(aid) {
                        trace.advance_by(&slacking_frontier);
                        trace.distinguish_since(&slacking_frontier);
                    }

                    if let Some(trace) = self.forward_propose.get_mut(aid) {
                        trace.advance_by(&slacking_frontier);
                        trace.distinguish_since(&slacking_frontier);
                    }

                    if let Some(trace) = self.forward_validate.get_mut(aid) {
                        trace.advance_by(&slacking_frontier);
                        trace.distinguish_since(&slacking_frontier);
                    }

                    if let Some(trace) = self.reverse_count.get_mut(aid) {
                        trace.advance_by(&slacking_frontier);
                        trace.distinguish_since(&slacking_frontier);
                    }

                    if let Some(trace) = self.reverse_propose.get_mut(aid) {
                        trace.advance_by(&slacking_frontier);
                        trace.distinguish_since(&slacking_frontier);
                    }

                    if let Some(trace) = self.reverse_validate.get_mut(aid) {
                        trace.advance_by(&slacking_frontier);
                        trace.distinguish_since(&slacking_frontier);
                    }
                }
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
        } else if frontier.is_empty() {
            false
        } else {
            self.domain_probe().with_frontier(|domain_frontier| {
                domain_frontier.iter().all(|t| frontier.less_than(t))
            })
        }
    }

    /// Returns the definition for the rule of the given name.
    pub fn rule(&self, name: &str) -> Option<&Rule> {
        self.rules.get(name)
    }

    /// Checks whether an attribute of that name exists.
    pub fn has_attribute(&self, name: &str) -> bool {
        self.attributes.contains_key(name)
    }

    /// Retrieves the forward count trace for the specified aid.
    pub fn forward_count(&mut self, name: &str) -> Option<&mut TraceKeyHandle<Value, T, isize>> {
        self.forward_count.get_mut(name)
    }

    /// Retrieves the forward propose trace for the specified aid.
    pub fn forward_propose(
        &mut self,
        name: &str,
    ) -> Option<&mut TraceValHandle<Value, Value, T, isize>> {
        self.forward_propose.get_mut(name)
    }

    /// Retrieves the forward validate trace for the specified aid.
    pub fn forward_validate(
        &mut self,
        name: &str,
    ) -> Option<&mut TraceKeyHandle<(Value, Value), T, isize>> {
        self.forward_validate.get_mut(name)
    }

    /// Retrieves the reverse count trace for the specified aid.
    pub fn reverse_count(&mut self, name: &str) -> Option<&mut TraceKeyHandle<Value, T, isize>> {
        self.reverse_count.get_mut(name)
    }

    /// Retrieves the reverse propose trace for the specified aid.
    pub fn reverse_propose(
        &mut self,
        name: &str,
    ) -> Option<&mut TraceValHandle<Value, Value, T, isize>> {
        self.reverse_propose.get_mut(name)
    }

    /// Retrieves the reverse validate trace for the specified aid.
    pub fn reverse_validate(
        &mut self,
        name: &str,
    ) -> Option<&mut TraceKeyHandle<(Value, Value), T, isize>> {
        self.reverse_validate.get_mut(name)
    }
}

/// A domain that is still under construction in a specific scope.
pub struct ScopedDomain<S>
where
    S: Scope,
    S::Timestamp: Timestamp + Lattice + Rewind,
{
    raw: HashMap<Aid, Collection<S, (Value, Value), isize>>,
    domain: Domain<S::Timestamp>,
}

impl<S> AddAssign for ScopedDomain<S>
where
    S: Scope,
    S::Timestamp: Timestamp + Lattice + Rewind,
{
    fn add_assign(&mut self, other: Self) {
        self.raw.extend(other.raw.into_iter());
        self.domain += other.domain;
    }
}

impl<S> Add for ScopedDomain<S>
where
    S: Scope,
    S::Timestamp: Timestamp + Lattice + Rewind,
{
    type Output = Self;

    fn add(self, other: Self) -> Self {
        let mut merged = self;
        merged += other;
        merged
    }
}

impl<S> ScopedDomain<S>
where
    S: Scope,
    S::Timestamp: Timestamp + Lattice + Rewind,
{
    /// Installs indices required for the specified level of query
    /// support.
    pub fn with_query_support(mut self, query_support: QuerySupport) -> Self {
        for aid in self.domain.forward_propose.keys() {
            // Count traces are only required for use in worst-case
            // optimal joins.
            if query_support == QuerySupport::AdaptiveWCO {
                self.domain.forward_count.insert(
                    aid.clone(),
                    self.raw[aid]
                        .map(|(k, _v)| (k, ()))
                        .arrange_named(&format!("->Count({})", aid))
                        .trace,
                );
            }

            if query_support >= QuerySupport::Delta {
                self.domain.forward_validate.insert(
                    aid.clone(),
                    self.raw[aid]
                        .map(|t| (t, ()))
                        .arrange_named(&format!("->Validate({})", aid))
                        .trace,
                );
            }
        }

        self
    }

    /// Installs reverse indices for all attributes in the domain.
    pub fn with_reverse_indices(mut self) -> Self {
        for aid in self.domain.forward_count.keys() {
            self.domain.reverse_count.insert(
                aid.clone(),
                self.raw[aid]
                    .map(|(_e, v)| (v, ()))
                    .arrange_named(&format!("->_Count({})", aid))
                    .trace,
            );
        }

        for aid in self.domain.forward_propose.keys() {
            self.domain.reverse_propose.insert(
                aid.clone(),
                self.raw[aid]
                    .map(|(e, v)| (v, e))
                    .arrange_named(&format!("->_Propose({})", aid))
                    .trace,
            );
        }

        for aid in self.domain.forward_validate.keys() {
            self.domain.reverse_validate.insert(
                aid.clone(),
                self.raw[aid]
                    .map(|pair| (pair, ()))
                    .arrange_named(&format!("->_Validate({})", aid))
                    .trace,
            );
        }

        self
    }
}

impl<S> ScopedDomain<S>
where
    S: Scope,
    S::Timestamp: Timestamp + Lattice + Rewind + std::convert::Into<crate::timestamp::Time>,
{
    /// Configures the specified trace slack for all attributes in the
    /// domain.
    pub fn with_slack(mut self, slack: S::Timestamp) -> Self {
        for config in self.domain.attributes.values_mut() {
            config.trace_slack = Some(slack.clone().into());
        }

        self
    }
}

impl<S> Into<Domain<S::Timestamp>> for ScopedDomain<S>
where
    S: Scope,
    S::Timestamp: Timestamp + Lattice + Rewind,
{
    fn into(self) -> Domain<S::Timestamp> {
        self.domain
    }
}

/// Things that can be converted into a domain with a single
/// attribute.
pub trait AsSingletonDomain<S>
where
    S: Scope,
    S::Timestamp: Timestamp + Lattice + Rewind,
{
    /// Returns a domain containing only a single attribute of the
    /// specified name, with a single forward index installed.
    fn as_singleton_domain(self, name: &str) -> ScopedDomain<S>;
}

impl<S> AsSingletonDomain<S> for Stream<S, ((Value, Value), isize)>
where
    S: Scope,
    S::Timestamp: Timestamp + Lattice + Rewind,
{
    fn as_singleton_domain(self, name: &str) -> ScopedDomain<S> {
        let mut domain = Domain::new(Default::default());

        // When given only a stream without timestamps, we must assume
        // that this attribute is externally sourced and timeless,
        // meaning we have no control over its input handle and the
        // source is not able or not willing to provide timestamps. We
        // do not want to probe them, in order to not stall progress.
        let pairs = self
            .map(|(data, diff)| (data, Default::default(), diff))
            .as_collection();

        let mut raw = HashMap::new();
        raw.insert(name.to_string(), pairs);

        // Propose traces are used in general, whereas the other
        // indices are only relevant to Hector.
        domain.forward_propose.insert(
            name.to_string(),
            raw[name]
                .arrange_named(&format!("->Propose({})", &name))
                .trace,
        );

        // This is crucial. If we forget to install the attribute
        // configuration, its traces will be ignored when advancing
        // the domain.
        domain
            .attributes
            .insert(name.to_string(), Default::default());

        ScopedDomain { raw, domain }
    }
}

impl<S> AsSingletonDomain<S> for Collection<S, (Value, Value), isize>
where
    S: Scope,
    S::Timestamp: Timestamp + Lattice + Rewind,
{
    fn as_singleton_domain(self, name: &str) -> ScopedDomain<S> {
        let mut domain = Domain::new(Default::default());

        // When given only a collection we must assume that this
        // attribute is externally sourced, meaning we have no control
        // over its input handle. We therefore need to install a probe
        // in order to determine its progress.
        domain.probed_source_count += 1;
        let pairs = self.probe_with(&mut domain.domain_probe);

        let mut raw = HashMap::new();
        raw.insert(name.to_string(), pairs);

        // Propose traces are used in general, whereas the other
        // indices are only relevant to Hector.
        domain.forward_propose.insert(
            name.to_string(),
            raw[name]
                .arrange_named(&format!("->Propose({})", &name))
                .trace,
        );

        // This is crucial. If we forget to install the attribute
        // configuration, its traces will be ignored when advancing
        // the domain.
        domain
            .attributes
            .insert(name.to_string(), Default::default());

        ScopedDomain { raw, domain }
    }
}

impl<S> AsSingletonDomain<S> for Stream<S, ((Value, Value), S::Timestamp, isize)>
where
    S: Scope,
    S::Timestamp: Timestamp + Lattice + Rewind,
{
    fn as_singleton_domain(self, name: &str) -> ScopedDomain<S> {
        self.as_collection().as_singleton_domain(name)
    }
}

impl<S> AsSingletonDomain<S>
    for (
        (
            UnorderedHandle<S::Timestamp, ((Value, Value), S::Timestamp, isize)>,
            ActivateCapability<S::Timestamp>,
        ),
        Collection<S, (Value, Value), isize>,
    )
where
    S: Scope,
    S::Timestamp: Timestamp + Lattice + Rewind,
{
    fn as_singleton_domain(self, name: &str) -> ScopedDomain<S> {
        let mut domain = Domain::new(Default::default());

        // When a handle and a capability are available, we can infer
        // that this is an attribute that clients will issue
        // transactions against. We must not install a probe, because
        // the domain time will be authoritative for them.

        let ((handle, cap), pairs) = self;
        domain
            .input_sessions
            .insert(name.to_string(), UnorderedSession::from(handle, cap));

        let mut raw = HashMap::new();
        raw.insert(name.to_string(), pairs);

        // Propose traces are used in general, whereas the other
        // indices are only relevant to Hector.
        domain.forward_propose.insert(
            name.to_string(),
            raw[name]
                .arrange_named(&format!("->Propose({})", &name))
                .trace,
        );

        // This is crucial. If we forget to install the attribute
        // configuration, its traces will be ignored when advancing
        // the domain.
        domain
            .attributes
            .insert(name.to_string(), Default::default());

        ScopedDomain { raw, domain }
    }
}

impl<S> AsSingletonDomain<S>
    for (
        (
            UnorderedHandle<S::Timestamp, ((Value, Value), S::Timestamp, isize)>,
            ActivateCapability<S::Timestamp>,
        ),
        Stream<S, ((Value, Value), S::Timestamp, isize)>,
    )
where
    S: Scope,
    S::Timestamp: Timestamp + Lattice + Rewind,
{
    fn as_singleton_domain(self, name: &str) -> ScopedDomain<S> {
        let ((handle, cap), pairs) = self;
        ((handle, cap), pairs.as_collection()).as_singleton_domain(name)
    }
}
