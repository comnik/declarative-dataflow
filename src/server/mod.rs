//! Server logic for driving the library via commands.

extern crate timely;
extern crate differential_dataflow;

use std::collections::{HashMap};

use timely::dataflow::{ProbeHandle};
use timely::dataflow::scopes::Child;
use timely::progress::timestamp::{RootTimestamp};
use timely::progress::nested::product::Product;
use timely::communication::Allocate;
use timely::worker::Worker;

use differential_dataflow::{AsCollection};
use differential_dataflow::collection::{Collection};
use differential_dataflow::input::{Input, InputSession};
use differential_dataflow::trace::{TraceReader};
use differential_dataflow::operators::arrange::{ArrangeBySelf};

use {QueryMap, Rule, Entity, Attribute, Value, implement};
use sources::{Source, Sourceable};

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
#[derive(Deserialize, Debug)]
pub struct TxData(pub isize, pub Entity, pub Attribute, pub Value);

/// A request expressing the arrival of inputs to one or more
/// collections. Optionally a timestamp may be specified.
#[derive(Deserialize, Debug)]
pub struct Transact {
    /// The timestamp at which this transaction occured.
    pub tx: Option<usize>,
    /// A sequence of additions and retractions.
    pub tx_data: Vec<TxData>
}

/// A request expressing interest in receiving results published under
/// the specified name.
#[derive(Deserialize, Debug)]
pub struct Interest {
    /// The name of a previously registered dataflow.
    pub name: String
}

/// A request with the intent of synthesising one or more new rules
/// and optionally publishing one or more of them.
#[derive(Deserialize, Debug)]
pub struct Register {
    /// A list of rules to synthesise in order.
    pub rules: Vec<Rule>,
    /// The names of rules that should be published.
    pub publish: Vec<String>
}

/// A request with the intent of attaching to an external data source
/// and publishing it under a globally unique name.
#[derive(Deserialize, Debug)]
pub struct RegisterSource {
    /// A globally unique name.
    pub name: String,
    /// A source configuration.
    pub source: Source
}

/// A request with the intent of creating a new named, globally
/// available input that can be transacted upon.
#[derive(Deserialize, Debug)]
pub struct CreateInput {
    /// A globally unique name under which to publish data sent via
    /// this input.
    pub name: String
}

/// Possible request types.
#[derive(Deserialize, Debug)]
pub enum Request {
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
}

/// Server context maintaining globally registered arrangements and
/// input handles.
pub struct Server {
    /// Server configuration.
    pub config: Config,
    /// Input handles to global arrangements.
    pub input_handles: HashMap<String, InputSession<usize, Vec<Value>, isize>>,
    /// Named relations.
    pub global_arrangements: QueryMap<usize, isize>,
    /// A probe for the transaction id time domain.
    pub probe: ProbeHandle<Product<RootTimestamp, usize>>,
}

impl Server {

    /// Creates a new server state from a configuration.
    pub fn new(config: Config) -> Self {
        Server {
            config: config,
            input_handles: HashMap::new(),
            global_arrangements: HashMap::new(),
            probe: ProbeHandle::new(),
        }
    }

    /// Handle a Transact request.
    pub fn transact(&mut self, req: Transact, owner: usize, worker_index: usize) {

        let Transact { tx, tx_data } = req;
        
        if owner == worker_index {

            // only the owner should actually introduce new inputs

            // @TODO do this smarter, e.g. grouped by handle
            for TxData(op, e, a, v) in tx_data {

                let handle = self.input_handles.get_mut(&a)
                    .expect(&format!("Attribute {} does not exist.", a));

                handle.update(vec![Value::Eid(e), v], op);

                let next_tx = match tx {
                    None => handle.epoch() + 1,
                    Some(tx) => tx + 1
                };

                handle.advance_to(next_tx);
            }
        }

        // @TODO do this smarter, e.g. only for handles that received inputs
        for handle in self.input_handles.values_mut() {
            handle.flush();
        }

        if self.config.enable_history == false {

            // if historical queries don't matter, we should advance
            // the index traces to allow them to compact

            let mut frontier = Vec::new();
            for handle in self.input_handles.values() {
                frontier.push(handle.time().clone());
            }

            let frontier_ref = &frontier;

            for trace in self.global_arrangements.values_mut() {
                trace.advance_by(frontier_ref);
            }
        }
    }

    /// Handle an Interest request.
    pub fn interest<'a, A: Allocate>(
        &mut self,
        req: Interest,
        scope: &mut Child<'a, Worker<A>, usize>
    ) -> Collection<Child<'a, Worker<A>, usize>, Vec<Value>, isize> {

        let Interest { name } = req;

        self.global_arrangements
            .get_mut(&name)
            .expect(&format!("Could not find relation {:?}", name))
            .import(scope)
            .as_collection(|tuple,_| tuple.clone())
            .probe_with(&mut self.probe)
    }

    /// Handle a Register request.
    pub fn register<A: Allocate>(
        &mut self,
        req: Register,
        scope: &mut Child<Worker<A>, usize>
    ) {

        let Register { rules, publish } = req;

        let rel_map = implement(rules, publish, scope, &mut self.global_arrangements, &mut self.probe);

        for (name, mut trace) in rel_map.into_iter() {
            if self.global_arrangements.contains_key(&name) {
                panic!("Attempted to re-register a named relation");
            } else {
                // decline the capability for that trace handle to subset
                // its view of the data
                trace.distinguish_since(&[]);

                self.global_arrangements.insert(name, trace);
            }
        }
    }

    /// Handle a RegisterSource request.
    pub fn register_source<A: Allocate>(
        &mut self,
        req: RegisterSource,
        scope: &mut Child<Worker<A>, usize>,
    ) {

        let RegisterSource { name, source } = req;

        let datoms = source.source(scope).as_collection();

        if self.global_arrangements.contains_key(&name) {
            panic!("Source name clashes with registered relation.");
        } else {
            let mut trace = datoms.arrange_by_self().trace;

            // decline the capability for that trace handle to subset
            // its view of the data
            trace.distinguish_since(&[]);
            
            self.global_arrangements.insert(name, trace);
        }
    }

    /// Handle a CreateInput request.
    pub fn create_input<A: Allocate>(
        &mut self,
        req: CreateInput,
        scope: &mut Child<Worker<A>, usize>,
    ) {

        let CreateInput { name } = req;
        
        if self.global_arrangements.contains_key(&name) {
            panic!("Input name clashes with existing trace.");
        } else {
            let (handle, tuples) = scope.new_collection::<Vec<Value>, isize>();
            let mut trace = tuples.arrange_by_self().trace;
            
            // decline the capability for that trace handle to subset
            // its view of the data
            trace.distinguish_since(&[]);
            
            self.global_arrangements.insert(name.clone(), trace);
            self.input_handles.insert(name, handle);
        }
    }
}
