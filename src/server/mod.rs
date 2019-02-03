//! Server logic for driving the library via commands.

extern crate differential_dataflow;
extern crate timely;

use std::collections::{HashMap, HashSet};
use std::hash::Hash;

use timely::dataflow::operators::{Filter, Map};
use timely::dataflow::{ProbeHandle, Scope};

use differential_dataflow::collection::Collection;
use differential_dataflow::input::{Input, InputSession};
use differential_dataflow::trace::TraceReader;
use differential_dataflow::AsCollection;

use crate::plan::{ImplContext, Implementable};
use crate::sources::{Source, Sourceable};
use crate::Rule;
use crate::{implement, implement_neu, CollectionIndex, RelationHandle, TraceKeyHandle};
use crate::{Aid, Eid, Value};

/// Server configuration.
#[derive(Clone, Debug)]
pub struct Config {
    /// Port at which this server will listen at.
    pub port: u16,
    /// Should inputs via CLI be accepted?
    pub enable_cli: bool,
    /// Should as-of queries be possible?
    pub enable_history: bool,
    /// Should queries use the optimizer during implementation?
    pub enable_optimizer: bool,
    /// Should queries on the query graph be available?
    pub enable_meta: bool,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            port: 6262,
            enable_cli: false,
            enable_history: false,
            // enable_optimizer: false,
            enable_optimizer: false,
            enable_meta: false,
        }
    }
}

/// Transaction data. Conceptually a pair (Datom, diff) but it's kept
/// intentionally flat to be more directly compatible with Datomic.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub struct TxData(pub isize, pub Eid, pub Aid, pub Value);

/// A request expressing the arrival of inputs to one or more
/// collections. Optionally a timestamp may be specified.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub struct Transact {
    /// The timestamp at which this transaction occured.
    pub tx: Option<u64>,
    /// A sequence of additions and retractions.
    pub tx_data: Vec<TxData>,
}

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

/// A request with the intent of creating a new named, globally
/// available input that can be transacted upon.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub struct CreateAttribute {
    /// A globally unique name under which to publish data sent via
    /// this input.
    pub name: String,
}

/// Possible request types.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub enum Request {
    /// Sends a single datom.
    Datom(Eid, Aid, Value, isize, u64),
    /// Sends inputs via one or more registered handles.
    Transact(Transact),
    /// Expresses interest in a named relation.
    Interest(Interest),
    /// Registers one or more named relations.
    Register(Register),
    /// Registers an external data source.
    RegisterSource(RegisterSource),
    /// Creates a named input handle that can be `Transact`ed upon.
    CreateAttribute(CreateAttribute),
    /// Advances and flushes a named input handle.
    AdvanceInput(Option<String>, u64),
    /// Closes a named input handle.
    CloseInput(String),
}

/// Server context maintaining globally registered arrangements and
/// input handles.
pub struct Server<Token: Hash> {
    /// Server configuration.
    pub config: Config,
    /// Input handles to global arrangements.
    pub input_handles: HashMap<String, InputSession<u64, (Value, Value), isize>>,
    /// Implementation context.
    pub context: Context,
    /// A probe for the transaction id time domain.
    pub probe: ProbeHandle<u64>,
    /// Mapping from query names to interested client tokens.
    pub interests: HashMap<String, Vec<Token>>,
}

/// Implementation context.
pub struct Context {
    /// Representation of named rules.
    pub rules: HashMap<Aid, Rule>,
    /// Named relations.
    pub global_arrangements: HashMap<Aid, RelationHandle>,
    /// Forward attribute indices eid -> v.
    pub forward: HashMap<Aid, CollectionIndex<Value, Value, u64>>,
    /// Reverse attribute indices v -> eid.
    pub reverse: HashMap<Aid, CollectionIndex<Value, Value, u64>>,
    /// Set of rules known to be underconstrained.
    pub underconstrained: HashSet<Aid>,
}

impl ImplContext for Context {
    fn rule(&self, name: &str) -> Option<&Rule> {
        self.rules.get(name)
    }

    fn global_arrangement(&mut self, name: &str) -> Option<&mut RelationHandle> {
        self.global_arrangements.get_mut(name)
    }

    fn forward_index(&mut self, name: &str) -> Option<&mut CollectionIndex<Value, Value, u64>> {
        self.forward.get_mut(name)
    }

    fn reverse_index(&mut self, name: &str) -> Option<&mut CollectionIndex<Value, Value, u64>> {
        self.reverse.get_mut(name)
    }

    fn is_underconstrained(&self, name: &str) -> bool {
        // self.underconstrained.contains(name)
        true
    }
}

impl<Token: Hash> Server<Token> {
    /// Creates a new server state from a configuration.
    pub fn new(config: Config) -> Self {
        Server {
            config,
            input_handles: HashMap::new(),
            context: Context {
                rules: HashMap::new(),
                global_arrangements: HashMap::new(),
                forward: HashMap::new(),
                reverse: HashMap::new(),
                underconstrained: HashSet::new(),
            },
            probe: ProbeHandle::new(),
            interests: HashMap::new(),
        }
    }

    /// Returns commands to install built-in plans.
    pub fn builtins() -> Vec<Request> {
        vec![
            Request::CreateAttribute(CreateAttribute {
                name: "df.pattern/e".to_string(),
            }),
            Request::CreateAttribute(CreateAttribute {
                name: "df.pattern/a".to_string(),
            }),
            Request::CreateAttribute(CreateAttribute {
                name: "df.pattern/v".to_string(),
            }),
            Request::CreateAttribute(CreateAttribute {
                name: "df.join/binding".to_string(),
            }),
            Request::CreateAttribute(CreateAttribute {
                name: "df.union/binding".to_string(),
            }),
            Request::CreateAttribute(CreateAttribute {
                name: "df.project/binding".to_string(),
            }),
            Request::CreateAttribute(CreateAttribute {
                name: "df.project/symbols".to_string(),
            }),
            Request::CreateAttribute(CreateAttribute {
                name: "df/name".to_string(),
            }),
            Request::CreateAttribute(CreateAttribute {
                name: "df.name/symbols".to_string(),
            }),
            Request::CreateAttribute(CreateAttribute {
                name: "df.name/plan".to_string(),
            }),
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

    fn register_global_arrangement(&mut self, name: String, mut trace: RelationHandle) {
        // decline the capability for that trace handle to subset its
        // view of the data
        trace.distinguish_since(&[]);

        self.context.global_arrangements.insert(name, trace);
    }

    /// Returns true iff the probe is behind any input handle. Mostly
    /// used as a convenience method during testing.
    pub fn is_any_outdated(&self) -> bool {
        for handle in self.input_handles.values() {
            if self.probe.less_than(handle.time()) {
                return true;
            }
        }

        false
    }

    /// Handle a Datom request.
    pub fn datom(
        &mut self,
        owner: usize,
        worker_index: usize,
        e: Eid,
        a: Aid,
        v: Value,
        diff: isize,
        _tx: u64,
    ) {
        if owner == worker_index {
            // only the owner should actually introduce new inputs

            let handle = self
                .input_handles
                .get_mut(&a)
                .expect(&format!("Attribute {} does not exist.", a));

            handle.update((Value::Eid(e), v), diff);
        }
    }

    /// Handle a Transact request.
    pub fn transact(&mut self, req: Transact, owner: usize, worker_index: usize) {
        let Transact { tx, tx_data } = req;

        if owner == worker_index {
            // only the owner should actually introduce new inputs

            // @TODO do this smarter, e.g. grouped by handle
            for TxData(op, e, a, v) in tx_data {
                let handle = self
                    .input_handles
                    .get_mut(&a)
                    .expect(&format!("Attribute {} does not exist.", a));

                handle.update((Value::Eid(e), v), op);
            }
        }

        for handle in self.input_handles.values_mut() {
            let next_tx = match tx {
                None => handle.epoch() + 1,
                Some(tx) => tx + 1,
            };

            handle.advance_to(next_tx);
            handle.flush();
        }

        if self.config.enable_history == false {
            // if historical queries don't matter, we should advance
            // the index traces to allow them to compact

            let mut frontier = Vec::new();
            for handle in self.input_handles.values() {
                frontier.push(handle.time().clone() - 1);
            }

            let frontier_ref = &frontier;

            for trace in self.context.global_arrangements.values_mut() {
                trace.advance_by(frontier_ref);
            }
        }
    }

    /// Handles an Interest request.
    pub fn interest<S: Scope<Timestamp = u64>>(
        &mut self,
        name: &str,
        scope: &mut S,
    ) -> &mut TraceKeyHandle<Vec<Value>, u64, isize> {
        match name {
            "df.timely/operates" => {
                // use timely::logging::{BatchLogger, TimelyEvent};
                // use timely::dataflow::operators::capture::EventWriter;

                // let writer = EventWriter::new(stream);
                // let mut logger = BatchLogger::new(writer);
                // scope.log_register()
                //     .insert::<TimelyEvent,_>("timely", move |time, data| logger.publish_batch(time, data));

                // logging_stream
                //     .flat_map(|(t,_,x)| {
                //         if let Operates(event) = x {
                //             Some((event, t, 1 as isize))
                //         } else { None }
                //     })
                //     .as_collection()

                unimplemented!();
            }
            _ => {
                if self.context.global_arrangements.contains_key(name) {
                    // Rule is already implemented.
                    self.context.global_arrangement(name).unwrap()
                } else if self.config.enable_optimizer == true {
                    let rel_map = implement_neu(name, scope, &mut self.context);

                    for (name, trace) in rel_map.into_iter() {
                        self.register_global_arrangement(name, trace);
                    }

                    self.context
                        .global_arrangement(name)
                        .expect("Relation of interest wasn't actually implemented.")
                } else {
                    let rel_map = implement(name, scope, &mut self.context);

                    for (name, trace) in rel_map.into_iter() {
                        self.register_global_arrangement(name, trace);
                    }

                    self.context
                        .global_arrangement(name)
                        .expect("Relation of interest wasn't actually implemented.")
                }
            }
        }
    }

    // /// Handle an AttributeInterest request.
    // pub fn attribute_interest<S: Scope<Timestamp = u64>>
    //     (&mut self, name: &str, scope: &mut S) -> Collection<S, Vec<Value>, isize>
    // {
    //     self.attributes
    //         .get_mut(name)
    //         .expect(&format!("Could not find attribute {:?}", name))
    //         .import_named(scope, name)
    //         .as_collection(|e, v| vec![e.clone(), v.clone()])
    //         .probe_with(&mut self.probe)
    // }

    /// Handle a Register request.
    pub fn register(&mut self, req: Register) {
        let Register { rules, .. } = req;

        for rule in rules.into_iter() {
            if self.context.rules.contains_key(&rule.name) {
                // @TODO panic if hashes don't match
                // panic!("Attempted to re-register a named relation");
                continue;
            } else {
                if self.config.enable_meta == true {
                    let mut data = rule.plan.datafy();
                    let tx_data: Vec<TxData> =
                        data.drain(..).map(|(e, a, v)| TxData(1, e, a, v)).collect();

                    self.transact(Transact { tx: None, tx_data }, 0, 0);
                }

                self.context.rules.insert(rule.name.to_string(), rule);
            }
        }
    }

    /// Handle a RegisterSource request.
    pub fn register_source<S: Scope<Timestamp = u64>>(
        &mut self,
        req: RegisterSource,
        scope: &mut S,
    ) {
        let RegisterSource { mut names, source } = req;

        if names.len() == 1 {
            let name = names.pop().unwrap();
            let datoms = source.source(scope, names.clone());

            if self.context.forward.contains_key(&name) {
                panic!("Source name clashes with registered relation.");
            } else {
                let tuples = datoms.map(|(_idx, tuple)| tuple).as_collection();

                let forward = CollectionIndex::index(&name, &tuples);
                let reverse = CollectionIndex::index(&name, &tuples.map(|(e, v)| (v, e)));

                self.context.forward.insert(name.clone(), forward);
                self.context.reverse.insert(name, reverse);
            }
        } else if names.len() > 1 {
            let datoms = source.source(scope, names.clone());

            for (name_idx, name) in names.iter().enumerate() {
                if self.context.forward.contains_key(name) {
                    panic!("Source name clashes with registered relation.");
                } else {
                    let tuples = datoms
                        .filter(move |(idx, _tuple)| *idx == name_idx)
                        .map(|(_idx, tuple)| tuple)
                        .as_collection();

                    let forward = CollectionIndex::index(name, &tuples);
                    let reverse = CollectionIndex::index(name, &tuples.map(|(e, v)| (v, e)));

                    self.context.forward.insert(name.to_string(), forward);
                    self.context.reverse.insert(name.to_string(), reverse);
                }
            }
        }
    }

    /// Creates a new collection of (e,v) tuples and indexes it in
    /// various ways. Stores forward, and reverse indices, as well as
    /// the input handle in the server state.
    pub fn create_attribute<S: Scope<Timestamp = u64>>(&mut self, name: &str, scope: &mut S) {
        if self.context.forward.contains_key(name) {
            panic!("Attribute of name {} already exists.", name);
        } else {
            let (handle, tuples) = scope.new_collection::<(Value, Value), isize>();
            let forward = CollectionIndex::index(name, &tuples);
            let reverse = CollectionIndex::index(name, &tuples.map(|(e, v)| (v, e)));

            self.context.forward.insert(name.to_string(), forward);
            self.context.reverse.insert(name.to_string(), reverse);

            self.input_handles.insert(name.to_string(), handle);
        }
    }

    /// Handle an AdvanceInput request.
    pub fn advance_input(&mut self, name: Option<String>, tx: u64) {
        match name {
            None => {
                for handle in self.input_handles.values_mut() {
                    handle.advance_to(tx);
                    handle.flush();
                }
            }
            Some(name) => {
                let handle = self
                    .input_handles
                    .get_mut(&name)
                    .expect(&format!("Input {} does not exist.", name));

                handle.advance_to(tx);
                handle.flush();
            }
        }

        if self.config.enable_history == false {
            // if historical queries don't matter, we should advance
            // the index traces to allow them to compact

            let mut frontier = Vec::new();
            for handle in self.input_handles.values() {
                frontier.push(handle.time().clone() - 1);
            }

            let frontier_ref = &frontier;

            for trace in self.context.global_arrangements.values_mut() {
                trace.advance_by(frontier_ref);
            }
        }
    }

    /// Handle a CloseInput request.
    pub fn close_input(&mut self, name: String) {
        let handle = self
            .input_handles
            .remove(&name)
            .expect(&format!("Input {} does not exist.", name));

        handle.close();
    }

    /// Helper for registering, publishing, and indicating interest in
    /// a single, named query. Used for testing.
    pub fn test_single<S: Scope<Timestamp = u64>>(
        &mut self,
        scope: &mut S,
        rule: Rule,
    ) -> Collection<S, Vec<Value>, isize> {
        let interest_name = rule.name.clone();
        let publish_name = rule.name.clone();

        self.register(Register {
            rules: vec![rule],
            publish: vec![publish_name],
        });

        self.interest(&interest_name, scope)
            .import_named(scope, &interest_name)
            .as_collection(|tuple, _| tuple.clone())
            .probe_with(&mut self.probe)
    }
}
