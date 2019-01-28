//! Server logic for driving the library via commands.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::rc::Rc;
use std::time::{Duration, Instant};
#[cfg(feature="graphql")]
use std::collections::BTreeMap;
#[cfg(feature="graphql")]
use std::cmp::{Ordering, PartialOrd, PartialEq};

use timely::communication::Allocate;
use timely::dataflow::operators::capture::event::link::EventLink;
use timely::dataflow::{ProbeHandle, Scope};
use timely::logging::{BatchLogger, Logger, TimelyEvent};
use timely::progress::Timestamp;
use timely::worker::Worker;

use differential_dataflow::collection::Collection;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::logging::DifferentialEvent;

use crate::domain::Domain;
use crate::logging::DeclarativeEvent;
use crate::plan::{ImplContext, Implementable};
use crate::sinks::Sink;
use crate::sources::{Source, Sourceable, SourcingContext};
use crate::Rule;
use crate::{
    implement, implement_neu, AttributeConfig, CollectionIndex, RelationHandle, ShutdownHandle,
};
use crate::{Aid, Error, Rewind, Time, TxData, Value};

pub mod scheduler;
use self::scheduler::Scheduler;

/// Server configuration.
#[derive(Clone, Debug)]
pub struct Config {
    /// Port at which this server will listen at.
    pub port: u16,
    /// Do clients have to call AdvanceDomain explicitely?
    pub manual_advance: bool,
    /// Should logging streams be created?
    pub enable_logging: bool,
    /// Should inputs via CLI be accepted?
    pub enable_cli: bool,
    /// Should queries use the optimizer during implementation?
    pub enable_optimizer: bool,
    /// Should queries on the query graph be available?
    pub enable_meta: bool,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            port: 6262,
            manual_advance: false,
            enable_logging: false,
            enable_cli: false,
            enable_optimizer: false,
            enable_meta: false,
        }
    }
}

/// Transaction ids.
pub type TxId = u64;

/// A request expressing interest in receiving results published under
/// the specified name.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub struct Interest {
    /// The name of a previously registered dataflow.
    pub name: String,
    /// Result offset holding client token (for multi-tenant flows).
    pub tenant: Option<usize>,
    /// Granularity (in seconds or tx ids) at which to send
    /// results. None indicates no delay.
    pub granularity: Option<u64>,
    /// An optional sink configuration.
    pub sink: Option<Sink>,
    /// Whether or not to log events from this dataflow.
    pub disable_logging: Option<bool>,
}

/// A request with the intent of synthesising one or more new rules
/// and optionally publishing one or more of them.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub struct Register {
    /// A list of rules to synthesise in order.
    pub rules: Vec<Rule>,
    /// The names of rules that should be published.
    pub publish: Vec<String>,
}

/// A request with the intent of creating a new named, globally
/// available input that can be transacted upon.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub struct CreateAttribute {
    /// A globally unique name under which to publish data sent via
    /// this input.
    pub name: String,
    /// Semantics enforced on this attribute by 3DF.
    pub config: AttributeConfig,
}

/// Possible request types.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub enum Request {
    /// Sends inputs via one or more registered handles.
    Transact(Vec<TxData>),
    /// Expresses interest in a named relation.
    Interest(Interest),
    /// Expresses that the interest in a named relation has
    /// stopped. Once all interested clients have sent this, the
    /// dataflow can be cleaned up.
    Uninterest(String),
    /// Registers one or more named relations.
    Register(Register),
    /// A request with the intent of attaching to an external data
    /// source that publishes one or more attributes and relations.
    RegisterSource(Source),
    /// Creates a named input handle that can be `Transact`ed upon.
    CreateAttribute(CreateAttribute),
    /// Advances the specified domain to the specified time.
    AdvanceDomain(Option<String>, Time),
    /// Closes a named input handle.
    CloseInput(String),
    /// Client has disconnected.
    Disconnect,
    /// Requests any setup logic that needs to be executed
    /// deterministically across all workers.
    Setup,
    /// Requests orderly shutdown of the system.
    Shutdown,
    /// Register a query specified as GraphQL.
    #[cfg(feature="graphql")]
    GraphQl(String, String),
}

/// Nested value type for graphQL vec -> nested map transformation
#[cfg(feature="graphql")]
#[derive(Serialize, Deserialize, Debug, Clone, Ord, Eq)]
enum NestedVal<T: Eq + Hash + Ord> {
    Map(BTreeMap<T, NestedVal<T>>),
    Arr(Vec<NestedVal<T>>),
    Val(T),
}

#[cfg(feature="graphql")]
impl<T: Eq + Hash + Ord> PartialEq for NestedVal<T> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (&NestedVal::Map(ref a), &NestedVal::Map(ref b)) => a == b,
            (&NestedVal::Val(ref a), &NestedVal::Val(ref b)) => a == b,
            (&NestedVal::Arr(ref a), &NestedVal::Arr(ref b)) => a == b,
            _ => false,
        }
    }
}

#[cfg(feature="graphql")]
impl<T: Eq + Hash + Ord> PartialOrd for NestedVal<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (&NestedVal::Map(ref a), &NestedVal::Map(ref b)) => Some(a.values().cmp(b.values())),
            (&NestedVal::Map(_), _) => Some(Ordering::Greater),

            (&NestedVal::Arr(ref a), &NestedVal::Arr(ref b)) => Some(a.cmp(b)),
            (&NestedVal::Arr(_), &NestedVal::Map(_)) => Some(Ordering::Less),
            (&NestedVal::Arr(_), &NestedVal::Val(_)) => Some(Ordering::Greater),

            (&NestedVal::Val(ref a), &NestedVal::Val(ref b)) => Some(a.cmp(b)),
            (&NestedVal::Val(_), _) => Some(Ordering::Less),
        }
    }
}

#[cfg(feature="graphql")]
fn paths_to_nested<T: Eq + Hash + Ord + std::fmt::Debug>(paths: Vec<Vec<T>>) -> NestedVal<T> {
    let mut acc: BTreeMap<T, NestedVal<T>> = BTreeMap::new();
    for mut path in paths {
        let mut current_map = &mut acc;
        let last_val = path.pop().unwrap();
        let last_key = path.pop().unwrap();

        for attribute in path {
            let entry = current_map
                .entry(attribute)
                .or_insert_with(|| NestedVal::Map(BTreeMap::new()));

            *entry = match entry {
                NestedVal::Val(_) => NestedVal::Map(BTreeMap::new()),
                NestedVal::Map(m) => NestedVal::Map(std::mem::replace(m, BTreeMap::new())),
                NestedVal::Arr(_) => unreachable!(),
            };

            match entry {
                NestedVal::Map(m) => current_map = m,
                NestedVal::Val(_) => unreachable!(),
                NestedVal::Arr(_) => unreachable!(),
            };
        }

        current_map.insert(last_key, NestedVal::Val(last_val));
    }

    NestedVal::Map(acc)
}

#[cfg(feature="graphql")]
fn squash_nested<T: Eq + Hash + Ord + std::fmt::Debug>(nested: NestedVal<T>) -> NestedVal<T> {
    if let NestedVal::Map(m) = nested {
        let new = m.into_iter().fold(BTreeMap::new(), |mut acc, (k, v)| {
            let to_add = if let NestedVal::Map(nested_v) = v {
                let nested_squashed_v: Vec<_> = nested_v
                    .into_iter()
                    .map(|(_nested_k, nested_v)| squash_nested(nested_v))
                    .collect();
                NestedVal::Arr(nested_squashed_v)
            } else {
                v
            };

            acc.insert(k, to_add);
            acc
        });
        NestedVal::Map(new)
    } else {
        nested
    }
}

/// Server context maintaining globally registered arrangements and
/// input handles.
pub struct Server<T, Token>
where
    T: Timestamp + Lattice,
    Token: Hash + Eq + Copy,
{
    /// Server configuration.
    pub config: Config,
    /// A timer started at the initation of the timely computation
    /// (copied from worker).
    pub t0: Instant,
    /// Implementation context.
    pub context: Context<T>,
    /// Mapping from query names to interested client tokens.
    pub interests: HashMap<String, HashSet<Token>>,
    /// Mapping from client tokens to the workers managing their
    /// connections. Only maintained for clients participating in
    /// multi-tenant queries.
    pub tenant_owner: Rc<RefCell<HashMap<Token, u64>>>,
    // Mapping from query names to their shutdown handles.
    shutdown_handles: HashMap<String, ShutdownHandle>,
    /// Probe keeping track of overall dataflow progress.
    pub probe: ProbeHandle<T>,
    /// Scheduler managing deferred operator activations.
    pub scheduler: Rc<RefCell<Scheduler>>,
    // Link to replayable Timely logging events.
    timely_events: Option<Rc<EventLink<Duration, (Duration, usize, TimelyEvent)>>>,
    // Link to replayable Differential logging events.
    differential_events: Option<Rc<EventLink<Duration, (Duration, usize, DifferentialEvent)>>>,
}

/// Implementation context.
pub struct Context<T>
where
    T: Timestamp + Lattice,
{
    /// Representation of named rules.
    pub rules: HashMap<Aid, Rule>,
    /// Set of rules known to be underconstrained.
    pub underconstrained: HashSet<Aid>,
    /// Internal domain of command sequence numbers.
    pub internal: Domain<T>,
}

impl<T> ImplContext<T> for Context<T>
where
    T: Timestamp + Lattice,
{
    fn rule(&self, name: &str) -> Option<&Rule> {
        self.rules.get(name)
    }

    fn global_arrangement(&mut self, name: &str) -> Option<&mut RelationHandle<T>> {
        self.internal.arrangements.get_mut(name)
    }

    fn has_attribute(&self, name: &str) -> bool {
        self.internal.forward.contains_key(name)
    }

    fn forward_index(&mut self, name: &str) -> Option<&mut CollectionIndex<Value, Value, T>> {
        self.internal.forward.get_mut(name)
    }

    fn reverse_index(&mut self, name: &str) -> Option<&mut CollectionIndex<Value, Value, T>> {
        self.internal.reverse.get_mut(name)
    }

    fn is_underconstrained(&self, _name: &str) -> bool {
        // self.underconstrained.contains(name)
        true
    }
}

impl<T, Token> Server<T, Token>
where
    T: Timestamp + Lattice + Default + Rewind,
    Token: Hash + Eq + Copy,
{
    /// Creates a new server state from a configuration.
    pub fn new(config: Config) -> Self {
        Server::new_at(config, Instant::now())
    }

    /// Creates a new server state from a configuration with an
    /// additionally specified beginning of the computation: an
    /// instant in relation to which all durations will be measured.
    pub fn new_at(config: Config, t0: Instant) -> Self {
        let timely_events = Some(Rc::new(EventLink::new()));
        let differential_events = Some(Rc::new(EventLink::new()));

        Server {
            config,
            t0,
            context: Context {
                rules: HashMap::new(),
                internal: Domain::new(Default::default()),
                underconstrained: HashSet::new(),
            },
            interests: HashMap::new(),
            tenant_owner: Rc::new(RefCell::new(HashMap::new())),
            shutdown_handles: HashMap::new(),
            probe: ProbeHandle::new(),
            scheduler: Rc::new(RefCell::new(Scheduler::new())),
            timely_events,
            differential_events,
        }
    }

    /// Returns commands to install built-in plans.
    pub fn builtins() -> Vec<Request> {
        vec![
            // Request::CreateAttribute(CreateAttribute {
            //     name: "df.pattern/e".to_string(),
            //     semantics: InputSemantics::Raw,
            // }),
            // Request::CreateAttribute(CreateAttribute {
            //     name: "df.pattern/a".to_string(),
            //     semantics: InputSemantics::Raw,
            // }),
            // Request::CreateAttribute(CreateAttribute {
            //     name: "df.pattern/v".to_string(),
            //     semantics: InputSemantics::Raw,
            // }),
        ]
    }

    /// Drops all shutdown handles associated with the specified
    /// query, resulting in its dataflow getting cleaned up.
    fn shutdown_query(&mut self, name: &str) {
        info!("Shutting down {}", name);
        self.shutdown_handles.remove(name);
    }

    /// Handle a Transact request.
    pub fn transact(
        &mut self,
        tx_data: Vec<TxData>,
        owner: usize,
        worker_index: usize,
    ) -> Result<(), Error> {
        // only the owner should actually introduce new inputs
        if owner == worker_index {
            self.context.internal.transact(tx_data)
        } else {
            Ok(())
        }
    }

    /// Handles an Interest request.
    pub fn interest<S: Scope<Timestamp = T>>(
        &mut self,
        name: &str,
        scope: &mut S,
    ) -> Result<Collection<S, Vec<Value>, isize>, Error> {
        // We need to do a `contains_key` here to avoid taking
        // a mut ref on context.
        if self.context.internal.arrangements.contains_key(name) {
            // Rule is already implemented.
            let relation = self
                .context
                .global_arrangement(name)
                .unwrap()
                .import_named(scope, name)
                .as_collection(|tuple, _| tuple.clone());

            Ok(relation)
        } else {
            let (mut rel_map, shutdown_handle) = if self.config.enable_optimizer {
                implement_neu(name, scope, &mut self.context)?
            } else {
                implement(name, scope, &mut self.context)?
            };

            // @TODO when do we actually want to register result traces for re-use?
            // for (name, relation) in rel_map.into_iter() {
            // let trace = relation.map(|t| (t, ())).arrange_named(name).trace;
            //     self.context.register_arrangement(name, config, trace);
            // }

            match rel_map.remove(name) {
                None => Err(Error {
                    category: "df.error.category/fault",
                    message: format!(
                        "Relation of interest ({}) wasn't actually implemented.",
                        name
                    ),
                }),
                Some(relation) => {
                    self.shutdown_handles
                        .insert(name.to_string(), shutdown_handle);

                    Ok(relation)
                }
            }
        }
    }

    /// Handle a Register request.
    pub fn register(&mut self, req: Register) -> Result<(), Error> {
        let Register { rules, .. } = req;

        for rule in rules.into_iter() {
            if self.context.rules.contains_key(&rule.name) {
                // @TODO panic if hashes don't match
                // panic!("Attempted to re-register a named relation");
                continue;
            } else {
                if self.config.enable_meta {
                    let mut data = rule.plan.datafy();
                    let tx_data: Vec<TxData> =
                        data.drain(..).map(|(e, a, v)| TxData(1, e, a, v)).collect();

                    self.transact(tx_data, 0, 0)?;
                }

                self.context.rules.insert(rule.name.to_string(), rule);
            }
        }

        Ok(())
    }

    /// Handle a RegisterSource request.
    pub fn register_source<S: Scope<Timestamp = T>>(
        &mut self,
        source: Box<dyn Sourceable<S>>,
        scope: &mut S,
    ) -> Result<(), Error> {
        let timely_logger = scope.log_register().remove("timely");

        let differential_logger = scope.log_register().remove("differential/arrange");

        let context = SourcingContext {
            t0: self.t0,
            scheduler: Rc::downgrade(&self.scheduler),
            timely_events: self.timely_events.clone().unwrap(),
            differential_events: self.differential_events.clone().unwrap(),
        };

        // self.timely_events = None;
        // self.differential_events = None;

        let mut attribute_streams = source.source(scope, context);

        for (aid, config, datoms) in attribute_streams.drain(..) {
            self.context
                .internal
                .create_sourced_attribute(&aid, config, &datoms)?;
        }

        if let Some(logger) = timely_logger {
            if let Ok(logger) = logger.downcast::<Logger<TimelyEvent>>() {
                scope
                    .log_register()
                    .insert_logger::<TimelyEvent>("timely", *logger);
            }
        }

        if let Some(logger) = differential_logger {
            if let Ok(logger) = logger.downcast::<Logger<DifferentialEvent>>() {
                scope
                    .log_register()
                    .insert_logger::<DifferentialEvent>("differential/arrange", *logger);
            }
        }

        Ok(())
    }

    /// Handle an AdvanceDomain request.
    pub fn advance_domain(&mut self, name: Option<String>, next: T) -> Result<(), Error> {
        match name {
            None => self.context.internal.advance_to(next),
            Some(_) => Err(Error {
                category: "df.error.category/unsupported",
                message: "Named domains are not yet supported.".to_string(),
            }),
        }
    }

    /// Handles an Uninterest request, possibly cleaning up dataflows
    /// that are no longer interesting to any client.
    pub fn uninterest(&mut self, client: Token, name: &str) {
        // All workers keep track of every client's interests, s.t. they
        // know when to clean up unused dataflows.
        if let Some(entry) = self.interests.get_mut(name) {
            entry.remove(&client);

            if entry.is_empty() {
                self.shutdown_query(name);
                self.interests.remove(name);
            }
        }
    }

    /// Cleans up all bookkeeping state for the specified client.
    pub fn disconnect_client(&mut self, client: Token) {
        let names: Vec<String> = self.interests.keys().cloned().collect();

        for query_name in names.iter() {
            self.uninterest(client, query_name);
        }

        self.tenant_owner.borrow_mut().remove(&client);
    }

    /// Returns true iff the probe is behind any input handle. Mostly
    /// used as a convenience method during testing.
    pub fn is_any_outdated(&self) -> bool {
        if self.probe.less_than(self.context.internal.time()) {
            return true;
        }

        false
    }

    /// Register a GraphQL query
    #[cfg(feature="graphql")]
    pub fn register_graph_ql<S: Scope<Timestamp = u64>>(
        &mut self,
        query: String,
        name: &str,
        scope: &mut S,
    ) {
        let req = Register {
            rules: vec![Rule {
                name: name.to_string(),
                plan: Plan::GraphQl(plan::GraphQl { query }),
            }],
            publish: vec![name.to_string()],
        };

        self.register(req, scope);

        self.interest(name, scope)
            .map(|x| ((), x.to_vec()))
            .group(|_key, inp, out| {
                let paths: Vec<_> = inp
                    .into_iter()
                    .map(|(tuple, _diff)| (*tuple).clone())
                    .collect();

                let nested = paths_to_nested(paths);

                out.push((squash_nested(nested), 1));
            })
            .map(|(_g, x)| { println!("{:?}", x); x });
    }

    /// Helper for registering, publishing, and indicating interest in
    /// a single, named query. Used for testing.
    pub fn test_single<S: Scope<Timestamp = T>>(
        &mut self,
        scope: &mut S,
        rule: Rule,
    ) -> Collection<S, Vec<Value>, isize> {
        let interest_name = rule.name.clone();
        let publish_name = rule.name.clone();

        self.register(Register {
            rules: vec![rule],
            publish: vec![publish_name],
        })
        .unwrap();

        match self.interest(&interest_name, scope) {
            Err(error) => panic!("{:?}", error),
            Ok(relation) => relation.probe_with(&mut self.probe),
        }
    }
}

impl<Token> Server<Duration, Token>
where
    Token: Hash + Eq + Copy,
{
    /// Registers loggers for use in the various logging sources.
    pub fn enable_logging<A: Allocate>(&self, worker: &mut Worker<A>) -> Result<(), Error> {
        let mut timely_logger = BatchLogger::new(self.timely_events.clone().unwrap());
        worker
            .log_register()
            .insert::<TimelyEvent, _>("timely", move |time, data| {
                timely_logger.publish_batch(time, data)
            });

        let mut differential_logger = BatchLogger::new(self.differential_events.clone().unwrap());
        worker
            .log_register()
            .insert::<DifferentialEvent, _>("differential/arrange", move |time, data| {
                differential_logger.publish_batch(time, data)
            });

        Ok(())
    }

    /// Unregisters loggers.
    pub fn shutdown_logging<A: Allocate>(&self, worker: &mut Worker<A>) -> Result<(), Error> {
        worker
            .log_register()
            .insert::<TimelyEvent, _>("timely", move |_time, _data| {});

        worker
            .log_register()
            .insert::<DifferentialEvent, _>("differential/arrange", move |_time, _data| {});

        worker
            .log_register()
            .insert::<DeclarativeEvent, _>("declarative", move |_time, _data| {});

        Ok(())
    }
}
