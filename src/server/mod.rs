//! Server logic for driving the library via commands.

extern crate differential_dataflow;
extern crate timely;

use std::hash::Hash;
use std::collections::HashMap;

// use timely::dataflow::operators::Inspect;
use timely::dataflow::{Scope, ProbeHandle};

use differential_dataflow::collection::Collection;
use differential_dataflow::input::{Input, InputSession};
use differential_dataflow::operators::arrange::Arrange;
use differential_dataflow::trace::TraceReader;
use differential_dataflow::AsCollection;

use sources::{Source, Sourceable};
use plan::{Plan, Pull, PullLevel};
use {implement, Aid, Eid, RelationHandle, Attribute, Rule, Value};

/// Server configuration.
#[derive(Clone, Debug)]
pub struct Config {
    /// Port at which this server will listen at.
    pub port: u16,
    /// Should inputs via CLI be accepted?
    pub enable_cli: bool,
    /// Should as-of queries be possible?
    pub enable_history: bool,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            port: 6262,
            enable_cli: false,
            enable_history: false,
        }
    }
}

/// Transaction data. Conceptually a pair (Datom, diff) but it's kept
/// intentionally flat to be more directly compatible with Datomic.
#[derive(Serialize, Deserialize, Debug)]
pub struct TxData(pub isize, pub Eid, pub Aid, pub Value);

/// A request expressing the arrival of inputs to one or more
/// collections. Optionally a timestamp may be specified.
#[derive(Serialize, Deserialize, Debug)]
pub struct Transact {
    /// The timestamp at which this transaction occured.
    pub tx: Option<u64>,
    /// A sequence of additions and retractions.
    pub tx_data: Vec<TxData>,
}

/// A request expressing interest in receiving results published under
/// the specified name.
#[derive(Serialize, Deserialize, Debug)]
pub struct Interest {
    /// The name of a previously registered dataflow.
    pub name: String,
}

/// A request with the intent of synthesising one or more new rules
/// and optionally publishing one or more of them.
#[derive(Serialize, Deserialize, Debug)]
pub struct Register {
    /// A list of rules to synthesise in order.
    pub rules: Vec<Rule>,
    /// The names of rules that should be published.
    pub publish: Vec<String>,
}

/// A request with the intent of attaching to an external data source
/// and publishing it under a globally unique name.
#[derive(Serialize, Deserialize, Debug)]
pub struct RegisterSource {
    /// One or more globally unique names.
    pub names: Vec<String>,
    /// A source configuration.
    pub source: Source,
}

/// A request with the intent of creating a new named, globally
/// available input that can be transacted upon.
#[derive(Serialize, Deserialize, Debug)]
pub struct CreateInput {
    /// A globally unique name under which to publish data sent via
    /// this input.
    pub name: String,
}

/// Possible request types.
#[derive(Serialize, Deserialize, Debug)]
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
    CreateInput(CreateInput),
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
    pub input_handles: HashMap<String, InputSession<u64, Vec<Value>, isize>>,
    /// Named relations.
    pub global_arrangements: HashMap<String, RelationHandle>,
    /// Attributes / Base Relations.
    pub attributes: HashMap<String, Attribute>,
    /// A probe for the transaction id time domain.
    pub probe: ProbeHandle<u64>,
    /// Mapping from query names to interested client tokens.
    pub interests: HashMap<String, Vec<Token>>,
}

impl<Token: Hash> Server<Token> {
    /// Creates a new server state from a configuration.
    pub fn new(config: Config) -> Self {
        Server {
            config: config,
            input_handles: HashMap::new(),
            global_arrangements: HashMap::new(),
            attributes: HashMap::new(),
            probe: ProbeHandle::new(),
            interests: HashMap::new(),
        }
    }

    /// Returns commands to install built-in plans.
    pub fn builtins() -> Vec<Request> {
        vec![
            Request::CreateInput(CreateInput { name: "df.pattern/e".to_string() }), 
            Request::CreateInput(CreateInput { name: "df.pattern/a".to_string() }), 
            Request::CreateInput(CreateInput { name: "df.pattern/v".to_string() }), 

            Request::CreateInput(CreateInput { name: "df.join/binding".to_string() }), 

            Request::CreateInput(CreateInput { name: "df.union/binding".to_string() }), 

            Request::CreateInput(CreateInput { name: "df.project/binding".to_string() }), 
            Request::CreateInput(CreateInput { name: "df.project/symbols".to_string() }), 

            Request::CreateInput(CreateInput { name: "df/name".to_string() }), 
            Request::CreateInput(CreateInput { name: "df.name/symbols".to_string() }), 
            Request::CreateInput(CreateInput { name: "df.name/plan".to_string() }),

            Request::Register(Register {
                publish: vec!["df.rules".to_string()],
                rules: vec![
                    // [:name {:join/binding [:pattern/e :pattern/a :pattern/v]}]
                    Rule {
                        name: "df.rules".to_string(),
                        plan: Plan::Pull(Pull {
                            paths: vec![
                                PullLevel {
                                    variables: vec![],
                                    plan: Box::new(Plan::MatchA(0, "df.join/binding".to_string(), 1)),
                                    pull_attributes: vec!["df.pattern/e".to_string(),
                                                          "df.pattern/a".to_string(),
                                                          "df.pattern/v".to_string()],
                                    path_attributes: vec!["df.join/binding".to_string()],
                                },
                                PullLevel {
                                    variables: vec![],
                                    plan: Box::new(Plan::MatchA(0, "df/name".to_string(), 2)),
                                    pull_attributes: vec![],
                                    path_attributes: vec![],
                                }
                            ]
                        })
                    }
                ],
            }),
        ]
    }

    fn register_global_arrangement(&mut self, name: String, mut trace: RelationHandle) {
        // decline the capability for that trace handle to subset its
        // view of the data
        trace.distinguish_since(&[]);

        self.global_arrangements.insert(name, trace);
    }

    fn register_attribute(&mut self, name: String, attribute: Attribute) {
        self.attributes.insert(name, attribute);
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

            let handle = self.input_handles
                .get_mut(&a)
                .expect(&format!("Attribute {} does not exist.", a));

            handle.update(vec![Value::Eid(e), v], diff);
        }
    }

    /// Handle a Transact request.
    pub fn transact(&mut self, req: Transact, owner: usize, worker_index: usize) {
        let Transact { tx, tx_data } = req;

        if owner == worker_index {
            // only the owner should actually introduce new inputs

            // @TODO do this smarter, e.g. grouped by handle
            for TxData(op, e, a, v) in tx_data {
                let handle = self.input_handles
                    .get_mut(&a)
                    .expect(&format!("Attribute {} does not exist.", a));

                handle.update(vec![Value::Eid(e), v], op);
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

            for trace in self.global_arrangements.values_mut() {
                trace.advance_by(frontier_ref);
            }
        }
    }

    /// Handle an Interest request.
    pub fn interest<S: Scope<Timestamp = u64>>
        (&mut self, name: &str, scope: &mut S) -> Collection<S, Vec<Value>, isize> {

        // @TODO this should be able to assume that it will be called
        // at most once per distinct name, no matter how many clients
        // are interested

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

                panic!("not quite there yet")
            },
            _ => {
                self.global_arrangements
                    .get_mut(name)
                    .expect(&format!("Could not find relation {:?}", name))
                    .import_named(scope, name)
                    .as_collection(|tuple, _| tuple.clone())
                    .probe_with(&mut self.probe)
            }
        }
    }

    // /// Handle an AttributeInterest request.
    // pub fn attribute_interest<S: Scope<Timestamp = u64>>
    //     (&mut self, name: &str, scope: &mut S) -> Collection<S, Vec<Value>, isize> {

    //         // @TODO this should be able to assume that it will be called
    //         // at most once per distinct name, no matter how many clients
    //         // are interested

    //         self.attributes
    //             .get_mut(name)
    //             .expect(&format!("Could not find attribute {:?}", name))
    //             .import_named(scope, name)
    //             .as_collection(|e, v| vec![e.clone(), v.clone()])
    //             .probe_with(&mut self.probe)
    //     }
    
    /// Handle a Register request.
    pub fn register<S: Scope<Timestamp = u64>>(&mut self, req: Register, scope: &mut S) {
        let Register { rules, publish } = req;

        let rel_map = implement(
            rules,
            publish,
            scope,
            &mut self.global_arrangements,
            &mut self.attributes,
            &mut self.probe,
        );

        for (name, mut trace) in rel_map.into_iter() {
            if self.global_arrangements.contains_key(&name) {
                panic!("Attempted to re-register a named relation");
            } else {
                self.register_global_arrangement(name, trace);
            }
        }
    }

    /// Handle a RegisterSource request.
    pub fn register_source<S: Scope<Timestamp = u64>>(&mut self, req: RegisterSource, scope: &mut S) {
        
        let RegisterSource { mut names, source } = req;

        if names.len() == 1 {
            let name = names.pop().unwrap();
            let datoms = source.source(scope, names.clone()).as_collection();

            if self.global_arrangements.contains_key(&name) {
                panic!("Source name clashes with registered relation.");
            } else {
                let trace = datoms
                    .map(|(_idx, tuple)| (tuple,()))
                    .arrange_named(&name)
                    .trace;
                
                self.register_global_arrangement(name, trace);
            }
        } else if names.len() > 1 {
            let datoms = source.source(scope, names.clone()).as_collection();

            for (name_idx, name) in names.iter().enumerate() {
                if self.global_arrangements.contains_key(name) {
                    panic!("Source name clashes with registered relation.");
                } else {
                    let trace = datoms
                        .filter(move |(idx, _tuple)| *idx == name_idx)
                        .map(|(_idx, tuple)| (tuple,()))
                        .arrange_named(&name)
                        .trace;

                    self.register_global_arrangement(name.to_string(), trace);
                }
            }
        }
    }

    /// Handle a CreateInput request.
    pub fn create_input<S: Scope<Timestamp = u64>>(&mut self, name: &str, scope: &mut S) {
        if self.global_arrangements.contains_key(name) {
            panic!("Input name clashes with existing trace.");
        } else {
            let (handle, tuples) = scope.new_collection::<Vec<Value>, isize>();
            let trace = tuples.map(|t| (t,())).arrange_named(name).trace;

            self.register_global_arrangement(name.to_string(), trace);
            self.input_handles.insert(name.to_string(), handle);
        }
    }

    /// Handle a CreateInput request.
    pub fn create_attribute<S: Scope<Timestamp = u64>>(&mut self, name: &str, scope: &mut S) {
        if self.attributes.contains_key(name) {
            panic!("Attribute of name {} already exists.", name);
        } else {
            // @TODO use (Value,Value) inputs here
            let (handle, tuples) = scope.new_collection::<Vec<Value>, isize>();
            let collection = tuples.map(|t| (t[0].clone(),t[1].clone()));
            let attribute = Attribute::new(name, &collection);

            self.register_attribute(name.to_string(), attribute);
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
                let handle = self.input_handles
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

            for trace in self.global_arrangements.values_mut() {
                trace.advance_by(frontier_ref);
            }
        }
    }

    /// Handle a CloseInput request.
    pub fn close_input(&mut self, name: String) {
        let handle = self.input_handles
            .remove(&name)
            .expect(&format!("Input {} does not exist.", name));

        handle.close();
    }

    /// Helper for registering, publishing, and indicating interest in
    /// a single, named query. Used for testing.
    pub fn test_single<S: Scope<Timestamp = u64>>
        (&mut self, scope: &mut S, rule: Rule) -> Collection<S, Vec<Value>, isize> {
            
        let interest_name = rule.name.clone();
        let publish_name = rule.name.clone();

        self.register(Register {
            rules: vec![rule],
            publish: vec![publish_name],
        }, scope);

        self.interest(&interest_name, scope)
    }
}
