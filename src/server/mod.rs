//! Server logic for driving the library via commands.

use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::ops::Sub;
use std::time::Duration;

use timely::dataflow::{ProbeHandle, Scope};
use timely::order::TotalOrder;
use timely::progress::Timestamp;

use differential_dataflow::collection::Collection;
use differential_dataflow::input::Input;
use differential_dataflow::lattice::Lattice;

use crate::domain::Domain;
use crate::plan::{ImplContext, Implementable};
use crate::sinks::{Sink, Sinkable};
use crate::sources::{Source, Sourceable};
use crate::Rule;
use crate::{
    implement, implement_neu, CollectionIndex, InputSemantics, RelationHandle, ShutdownHandle,
};
use crate::{Aid, Error, Time, TxData, Value};

/// Server configuration.
#[derive(Clone, Debug)]
pub struct Config {
    /// Port at which this server will listen at.
    pub port: u16,
    /// Do clients have to call AdvanceDomain explicitely?
    pub manual_advance: bool,
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

/// A request with the intent of attaching to an external data source
/// and publishing it under a globally unique name.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub struct RegisterSource {
    /// One or more globally unique names.
    pub names: Vec<String>,
    /// A source configuration.
    pub source: Source,
}

/// A request with the intent of attaching an external system as a
/// named sink.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub struct RegisterSink {
    /// A globally unique name.
    pub name: String,
    /// A sink configuration.
    pub sink: Sink,
}

/// A request with the intent of creating a new named, globally
/// available input that can be transacted upon.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub struct CreateAttribute {
    /// A globally unique name under which to publish data sent via
    /// this input.
    pub name: String,
    /// Semantics enforced on this attribute by 3DF (vs those enforced
    /// by the external source).
    pub semantics: InputSemantics,
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
    /// Expresses interest in a named relation, but directing results
    /// to be forwarded to a sink.
    Flow(String, String),
    /// Registers one or more named relations.
    Register(Register),
    /// Registers an external data source.
    RegisterSource(RegisterSource),
    /// Registers an external data sink.
    RegisterSink(RegisterSink),
    /// Creates a named input handle that can be `Transact`ed upon.
    CreateAttribute(CreateAttribute),
    /// Advances the specified domain to the specified time.
    AdvanceDomain(Option<String>, Time),
    /// Closes a named input handle.
    CloseInput(String),
    /// Requests orderly shutdown of the system.
    Shutdown,
}

/// Server context maintaining globally registered arrangements and
/// input handles.
pub struct Server<T, Token>
where
    T: Timestamp + Lattice + TotalOrder,
    Token: Hash,
{
    /// Server configuration.
    pub config: Config,
    /// Implementation context.
    pub context: Context<T>,
    /// Mapping from query names to interested client tokens.
    pub interests: HashMap<String, HashSet<Token>>,
    /// Mapping from query names to their shutdown handles.
    pub shutdown_handles: HashMap<String, ShutdownHandle>,
    /// Probe keeping track of overall dataflow progress.
    pub probe: ProbeHandle<T>,
}

/// Implementation context.
pub struct Context<T>
where
    T: Timestamp + Lattice + TotalOrder,
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
    T: Timestamp + Lattice + TotalOrder,
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
    T: Timestamp + Lattice + TotalOrder + Default + Sub<Output = T>,
    Token: Hash,
{
    /// Creates a new server state from a configuration.
    pub fn new(config: Config) -> Self {
        Server {
            config,
            context: Context {
                rules: HashMap::new(),
                internal: Domain::new(Default::default()),
                underconstrained: HashSet::new(),
            },
            interests: HashMap::new(),
            shutdown_handles: HashMap::new(),
            probe: ProbeHandle::new(),
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
            // Request::CreateAttribute(CreateAttribute {
            //     name: "df.join/binding".to_string(),
            //     semantics: InputSemantics::Raw,
            // }),
            // Request::CreateAttribute(CreateAttribute {
            //     name: "df.union/binding".to_string(),
            //     semantics: InputSemantics::Raw,
            // }),
            // Request::CreateAttribute(CreateAttribute {
            //     name: "df.project/binding".to_string(),
            //     semantics: InputSemantics::Raw,
            // }),
            // Request::CreateAttribute(CreateAttribute {
            //     name: "df.project/variables".to_string(),
            //     semantics: InputSemantics::Raw,
            // }),
            // Request::CreateAttribute(CreateAttribute {
            //     name: "df/name".to_string(),
            //     semantics: InputSemantics::Raw,
            // }),
            // Request::CreateAttribute(CreateAttribute {
            //     name: "df.name/variables".to_string(),
            //     semantics: InputSemantics::Raw,
            // }),
            // Request::CreateAttribute(CreateAttribute {
            //     name: "df.name/plan".to_string(),
            //     semantics: InputSemantics::Raw,
            // }),
            // Request::Register(Register {
            //     publish: vec!["df.rules".to_string()],
            //     rules: vec![
            //         // [:name {:join/binding [:pattern/e :pattern/a :pattern/v]}]
            //         Rule {
            //             name: "df.rules".to_string(),
            //             plan: Plan::Pull(Pull {
            //                 paths: vec![
            //                     PullLevel {
            //                         variables: vec![],
            //                         plan: Box::new(Plan::MatchA(0, "df.join/binding".to_string(), 1)),
            //                         pull_attributes: vec!["df.pattern/e".to_string(),
            //                                               "df.pattern/a".to_string(),
            //                                               "df.pattern/v".to_string()],
            //                         path_attributes: vec!["df.join/binding".to_string()],
            //                     },
            //                     PullLevel {
            //                         variables: vec![],
            //                         plan: Box::new(Plan::MatchA(0, "df/name".to_string(), 2)),
            //                         pull_attributes: vec![],
            //                         path_attributes: vec![],
            //                     }
            //                 ]
            //             })
            //         }
            //     ],
            // }),
        ]
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
            //     self.context.register_arrangement(name, trace);
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

    /// Returns true iff the probe is behind any input handle. Mostly
    /// used as a convenience method during testing.
    pub fn is_any_outdated(&self) -> bool {
        if self.probe.less_than(self.context.internal.time()) {
            return true;
        }

        false
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

impl<Token: Hash> Server<u64, Token> {
    /// Handle a RegisterSource request.
    pub fn register_source<S: Scope<Timestamp = u64>>(
        &mut self,
        req: RegisterSource,
        scope: &mut S,
    ) -> Result<(), Error> {
        let RegisterSource { mut names, source } = req;

        if names.len() == 1 {
            let name = names.pop().unwrap();
            let datoms = source.source(scope, names.clone());

            self.context.internal.create_source(&name, None, &datoms)
        } else if names.len() > 1 {
            let datoms = source.source(scope, names.clone());

            for (name_idx, name) in names.iter().enumerate() {
                self.context
                    .internal
                    .create_source(name, Some(name_idx), &datoms)?;
            }

            Ok(())
        } else {
            Ok(())
        }
    }

    /// Handle a RegisterSink request.
    pub fn register_sink<S: Scope<Timestamp = u64>>(
        &mut self,
        req: RegisterSink,
        scope: &mut S,
    ) -> Result<(), Error> {
        let RegisterSink { name, sink } = req;

        let (input, collection) = scope.new_collection();

        sink.sink(&collection.inner)?;

        self.context.internal.sinks.insert(name, input);

        Ok(())
    }
}
