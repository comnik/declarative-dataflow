//! Server logic for driving the library via commands.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::rc::Rc;
use std::time::{Duration, Instant};

use timely::communication::Allocate;
use timely::dataflow::operators::capture::event::link::EventLink;
use timely::dataflow::operators::UnorderedInput;
use timely::dataflow::{ProbeHandle, Scope};
use timely::logging::{BatchLogger, TimelyEvent};
use timely::progress::Timestamp;
use timely::worker::Worker;

use differential_dataflow::collection::{AsCollection, Collection};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::logging::DifferentialEvent;
use differential_dataflow::operators::Threshold;

use crate::domain::{AsSingletonDomain, Domain};
use crate::logging::DeclarativeEvent;
use crate::operators::LastWriteWins;
use crate::scheduling::Scheduler;
use crate::sinks::Sink;
use crate::sources::{Source, Sourceable, SourcingContext};
use crate::Rule;
use crate::{
    implement, implement_neu, AttributeConfig, IndexDirection, InputSemantics, ShutdownHandle,
};
use crate::{Aid, Datom, Error, Rewind, Time, Value};

/// Server configuration.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Configuration {
    /// Automatic domain tick interval.
    pub tick: Option<Duration>,
    /// Do clients have to call AdvanceDomain explicitely?
    pub manual_advance: bool,
    /// Should logging streams be created?
    pub enable_logging: bool,
    /// Should queries use the optimizer during implementation?
    pub enable_optimizer: bool,
}

impl Default for Configuration {
    fn default() -> Self {
        Configuration {
            tick: None,
            manual_advance: false,
            enable_logging: false,
            enable_optimizer: false,
        }
    }
}

#[cfg(feature = "getopts")]
impl Configuration {
    /// Returns a `getopts::Options` struct describing all available
    /// configuration options.
    pub fn options() -> getopts::Options {
        let mut opts = getopts::Options::new();

        opts.optopt(
            "",
            "tick",
            "advance domain at a regular interval",
            "SECONDS",
        );
        opts.optflag(
            "",
            "manual-advance",
            "forces clients to call AdvanceDomain explicitely",
        );
        opts.optflag("", "enable-logging", "enable log event sources");
        opts.optflag("", "enable-optimizer", "enable WCO queries");
        opts.optflag("", "enable-meta", "enable queries on the query graph");

        opts
    }

    /// Parses configuration options from the provided arguments.
    pub fn from_args<I: Iterator<Item = String>>(args: I) -> Result<Self, String> {
        let default: Self = Default::default();
        let opts = Self::options();

        let matches = opts.parse(args)?;

        let tick: Option<Duration> = matches
            .opt_str("tick")
            .map(|x| Duration::from_secs(x.parse().expect("failed to parse tick duration")));

        Self {
            tick,
            manual_advance: matches.opt_present("manual-advance"),
            enable_logging: matches.opt_present("enable-logging"),
            enable_optimizer: matches.opt_present("enable-optimizer"),
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
    /// Granularity at which to send results. None indicates no delay.
    pub granularity: Option<Time>,
    /// An optional sink configuration.
    pub sink: Option<Sink>,
    /// Whether or not to log events from this dataflow.
    pub disable_logging: Option<bool>,
}

impl std::convert::From<&Interest> for crate::sinks::SinkingContext {
    fn from(interest: &Interest) -> Self {
        Self {
            name: interest.name.clone(),
            granularity: interest.granularity.clone(),
        }
    }
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
    Transact(Vec<Datom<Aid>>),
    /// Expresses interest in an entire attribute.
    Subscribe(String),
    /// Derives new attributes under a new namespace.
    #[cfg(feature = "graphql")]
    Derive(String, String),
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
    /// Requests a domain advance to whatever epoch the server
    /// determines is *now*. Used by clients to enforce a minimum
    /// granularity of responses, if inputs happen only infrequently.
    Tick,
    /// Closes a named input handle.
    CloseInput(String),
    /// Client has disconnected.
    Disconnect,
    /// Requests any setup logic that needs to be executed
    /// deterministically across all workers.
    Setup,
    /// Requests a heartbeat containing status information.
    Status,
    /// Requests orderly shutdown of the system.
    Shutdown,
}

/// Server context maintaining globally registered arrangements and
/// input handles.
pub struct Server<T, Token>
where
    T: Timestamp + Lattice,
    Token: Hash + Eq + Copy,
{
    /// Server configuration.
    pub config: Configuration,
    /// A timer started at the initiation of the timely computation
    /// (copied from worker).
    pub t0: Instant,
    /// Internal domain in server time.
    pub internal: Domain<Aid, T>,
    /// Mapping from query names to interested client tokens.
    pub interests: HashMap<String, HashSet<Token>>,
    // Mapping from query names to their shutdown handles. This is
    // separate from internal shutdown handles on domains, because
    // user queries might be one-off and not result in a new domain
    // being created.
    shutdown_handles: HashMap<String, ShutdownHandle>,
    /// Probe keeping track of overall dataflow progress.
    pub probe: ProbeHandle<T>,
    /// Scheduler managing deferred operator activations.
    pub scheduler: Rc<RefCell<Scheduler<T>>>,
    // Link to replayable Timely logging events.
    timely_events: Option<Rc<EventLink<Duration, (Duration, usize, TimelyEvent)>>>,
    // Link to replayable Differential logging events.
    differential_events: Option<Rc<EventLink<Duration, (Duration, usize, DifferentialEvent)>>>,
}

impl<T, Token> Server<T, Token>
where
    T: Timestamp + Lattice + Default + Rewind,
    Token: Hash + Eq + Copy,
{
    /// Creates a new server state from a configuration.
    pub fn new(config: Configuration) -> Self {
        Server::new_at(config, Instant::now())
    }

    /// Creates a new server state from a configuration with an
    /// additionally specified beginning of the computation: an
    /// instant in relation to which all durations will be measured.
    pub fn new_at(config: Configuration, t0: Instant) -> Self {
        let timely_events = Some(Rc::new(EventLink::new()));
        let differential_events = Some(Rc::new(EventLink::new()));

        let probe = ProbeHandle::new();

        Server {
            config,
            t0,
            internal: Domain::new(Default::default()),
            interests: HashMap::new(),
            shutdown_handles: HashMap::new(),
            scheduler: Rc::new(RefCell::new(Scheduler::from(probe.clone()))),
            probe,
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

    /// Handles a Transact request.
    pub fn transact(
        &mut self,
        tx_data: Vec<Datom<Aid>>,
        owner: usize,
        worker_index: usize,
    ) -> Result<(), Error> {
        // only the owner should actually introduce new inputs
        if owner == worker_index {
            self.internal.transact(tx_data)
        } else {
            Ok(())
        }
    }

    /// Handles an Interest request.
    pub fn interest<S: Scope<Timestamp = T>>(
        &mut self,
        name: Aid,
        scope: &mut S,
    ) -> Result<Collection<S, Vec<Value>, isize>, Error> {
        let (mut rel_map, shutdown_handle) = if self.config.enable_optimizer {
            implement_neu(scope, &mut self.internal, name.clone())?
        } else {
            implement(scope, &mut self.internal, name.clone())?
        };

        match rel_map.remove(&name) {
            None => Err(Error::fault(format!(
                "Relation of interest ({}) wasn't actually implemented.",
                name
            ))),
            Some(relation) => {
                self.shutdown_handles
                    .insert(name.to_string(), shutdown_handle);

                Ok(relation)
            }
        }
    }

    /// Handles a Register request.
    pub fn register(&mut self, req: Register) -> Result<(), Error> {
        let Register { rules, .. } = req;

        for rule in rules.into_iter() {
            if self.internal.rules.contains_key(&rule.name) {
                // @TODO panic if hashes don't match
                // panic!("Attempted to re-register a named relation");
                continue;
            } else {
                self.internal.rules.insert(rule.name.to_string(), rule);
            }
        }

        Ok(())
    }

    /// Handles a CreateAttribute request.
    pub fn create_attribute<S>(
        &mut self,
        scope: &mut S,
        name: Aid,
        config: AttributeConfig,
    ) -> Result<(), Error>
    where
        S: Scope<Timestamp = T>,
        S::Timestamp: std::convert::Into<crate::timestamp::Time>,
    {
        let ((handle, cap), pairs) =
            scope.new_unordered_input::<((Value, Value), S::Timestamp, isize)>();

        let tuples = match config.input_semantics {
            InputSemantics::Raw => pairs.as_collection(),
            InputSemantics::LastWriteWins => pairs.as_collection().last_write_wins(),
            // Ensure that redundant (e,v) pairs don't cause
            // misleading proposals during joining.
            InputSemantics::Distinct => pairs.as_collection().distinct(),
        };

        let mut scoped_domain = ((handle, cap), tuples).as_singleton_domain(name.clone());

        if let Some(slack) = config.trace_slack {
            scoped_domain = scoped_domain.with_slack(slack.into());
        }

        // LastWriteWins is a special case, because count, propose,
        // and validate are all essentially the same.
        if config.input_semantics != InputSemantics::LastWriteWins {
            scoped_domain = scoped_domain.with_query_support(config.query_support);
        }

        if config.index_direction == IndexDirection::Both {
            scoped_domain = scoped_domain.with_reverse_indices();
        }

        self.internal += scoped_domain.into();

        info!("Created attribute {}", name);

        Ok(())
    }

    /// Returns a fresh sourcing context, useful for installing 3DF
    /// compatible sources manually.
    pub fn make_sourcing_context(&self) -> SourcingContext<T> {
        SourcingContext {
            t0: self.t0,
            scheduler: Rc::downgrade(&self.scheduler),
            domain_probe: self.internal.domain_probe().clone(),
            timely_events: self.timely_events.clone().unwrap(),
            differential_events: self.differential_events.clone().unwrap(),
        }
    }

    /// Handles a RegisterSource request.
    pub fn register_source<S>(
        &mut self,
        source: Box<dyn Sourceable<S>>,
        scope: &mut S,
    ) -> Result<(), Error>
    where
        S: Scope<Timestamp = T>,
        S::Timestamp: std::convert::Into<crate::timestamp::Time>,
    {
        // use timely::logging::Logger;
        // let timely_logger = scope.log_register().remove("timely");

        // let differential_logger = scope.log_register().remove("differential/arrange");

        let context = self.make_sourcing_context();

        // self.timely_events = None;
        // self.differential_events = None;

        let mut attribute_streams = source.source(scope, context);

        for (aid, config, pairs) in attribute_streams.drain(..) {
            let pairs = match config.input_semantics {
                InputSemantics::Raw => pairs.as_collection(),
                InputSemantics::LastWriteWins => pairs.as_collection().last_write_wins(),
                // Ensure that redundant (e,v) pairs don't cause
                // misleading proposals during joining.
                InputSemantics::Distinct => pairs.as_collection().distinct(),
            };

            let mut scoped_domain = pairs.as_singleton_domain(aid);

            if let Some(slack) = config.trace_slack {
                scoped_domain = scoped_domain.with_slack(slack.into());
            }

            // LastWriteWins is a special case, because count, propose,
            // and validate are all essentially the same.
            if config.input_semantics != InputSemantics::LastWriteWins {
                scoped_domain = scoped_domain.with_query_support(config.query_support);
            }

            if config.index_direction == IndexDirection::Both {
                scoped_domain = scoped_domain.with_reverse_indices();
            }

            self.internal += scoped_domain.into();
        }

        // if let Some(logger) = timely_logger {
        //     if let Ok(logger) = logger.downcast::<Logger<TimelyEvent>>() {
        //         scope
        //             .log_register()
        //             .insert_logger::<TimelyEvent>("timely", *logger);
        //     }
        // }

        // if let Some(logger) = differential_logger {
        //     if let Ok(logger) = logger.downcast::<Logger<DifferentialEvent>>() {
        //         scope
        //             .log_register()
        //             .insert_logger::<DifferentialEvent>("differential/arrange", *logger);
        //     }
        // }

        Ok(())
    }

    /// Handles an AdvanceDomain request.
    pub fn advance_domain(&mut self, name: Option<String>, next: T) -> Result<(), Error> {
        match name {
            None => self.internal.advance_epoch(next),
            Some(_) => Err(Error::unsupported("Named domains are not yet supported.")),
        }
    }

    /// Handles an Uninterest request, possibly cleaning up dataflows
    /// that are no longer interesting to any client.
    pub fn uninterest(&mut self, client: Token, name: &Aid) -> Result<(), Error> {
        // All workers keep track of every client's interests, s.t. they
        // know when to clean up unused dataflows.
        if let Some(entry) = self.interests.get_mut(name) {
            entry.remove(&client);

            if entry.is_empty() {
                self.shutdown_query(name);
                self.interests.remove(name);
            }
        }

        Ok(())
    }

    /// Cleans up all bookkeeping state for the specified client.
    pub fn disconnect_client(&mut self, client: Token) -> Result<(), Error> {
        let names: Vec<String> = self.interests.keys().cloned().collect();

        for query_name in names.iter() {
            self.uninterest(client, query_name)?
        }

        Ok(())
    }

    /// Returns true iff the probe is behind any input handle. Mostly
    /// used as a convenience method during testing. Using this within
    /// `step_while` is not safe in general and might lead to stalls.
    pub fn is_any_outdated(&self) -> bool {
        self.probe
            .with_frontier(|out_frontier| self.internal.dominates(out_frontier))
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

        match self.interest(interest_name, scope) {
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
