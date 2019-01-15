//! Declarative dataflow infrastructure
//!
//! This crate contains types, traits, and logic for assembling
//! differential dataflow computations from declaratively specified
//! programs, without any additional compilation.

#![forbid(missing_docs)]

extern crate differential_dataflow;
extern crate timely;
extern crate timely_sort;
#[macro_use]
extern crate log;

#[macro_use]
extern crate abomonation_derive;
extern crate abomonation;

#[macro_use]
extern crate serde_derive;

extern crate num_rational;

use std::hash::Hash;
use std::collections::{HashMap, HashSet};

use timely::dataflow::scopes::child::{Child, Iterative};
use timely::dataflow::*;
use timely::order::Product;
use timely::progress::Timestamp;
use timely::progress::timestamp::Refines;

use differential_dataflow::{Data, Collection};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::{Arrange, TraceAgent, Arranged};
use differential_dataflow::operators::group::Threshold;
use differential_dataflow::operators::iterate::Variable;
use differential_dataflow::trace::TraceReader;
use differential_dataflow::trace::implementations::ord::{OrdKeySpine, OrdValSpine};
use differential_dataflow::trace::wrappers::enter::TraceEnter;
use differential_dataflow::trace::wrappers::enter_at::TraceEnter as TraceEnterAt;

pub use num_rational::Rational32;

pub mod timestamp;

pub mod plan;
pub use plan::{Implementable, Plan};

pub mod server;
pub mod server_impl;
pub mod sources;

/// A unique entity identifier.
#[cfg(not(feature = "uuids"))]
pub type Eid = u64;

/// A unique entity identifier.
#[cfg(feature = "uuids")]
pub type Eid = u128;

/// A unique attribute identifier.
pub type Aid = String; // u32

/// Possible data values.
///
/// This enum captures the currently supported data types, and is the least common denominator
/// for the types of records moved around.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub enum Value {
    /// An attribute identifier
    Aid(Aid),
    /// A string
    String(String),
    /// A boolean
    Bool(bool),
    /// A 64 bit signed integer
    Number(i64),
    /// A 32 bit rational
    Rational32(Rational32),
    /// An entity identifier
    Eid(Eid),
    /// Milliseconds since midnight, January 1, 1970 UTC
    Instant(u64),
    /// A 16 byte unique identifier.
    Uuid([u8; 16]),
}

/// A (tuple, time, diff) triple, as sent back to clients.
pub type Result = (Vec<Value>, u64, isize);

/// An entity, attribute, value triple.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub struct Datom(pub Eid, pub Aid, pub Value);

/// A trace of values indexed by self. 
pub type TraceKeyHandle<K, T, R> = TraceAgent<K, (), T, R, OrdKeySpine<K, T, R>>;

/// A trace of (K, V) pairs indexed by key.
pub type TraceValHandle<K, V, T, R> = TraceAgent<K, V, T, R, OrdValSpine<K, V, T, R>>;

// @TODO change this to TraceValHandle<Eid, Value> eventually
/// A handle to an arranged attribute.
pub type AttributeHandle = TraceValHandle<Value, Value, u64, isize>;

/// A handle to an arranged relation.
pub type RelationHandle = TraceKeyHandle<Vec<Value>, u64, isize>;

// A map for keeping track of collections that are being actively
// synthesized (i.e. that are not fully defined yet).
type VariableMap<G> = HashMap<String, Variable<G, Vec<Value>, isize>>;

/// Various indices over a collection of (K, V) pairs, required to
/// participate in delta-join pipelines.
pub struct CollectionIndex<K, V, T>
where
    K: Data,
    V: Data,
    T: Lattice+Data,
{
    /// A trace of type (K, ()), used to count extensions for each prefix.
    count_trace: TraceKeyHandle<K, T, isize>,

    /// A trace of type (K, V), used to propose extensions for each prefix.
    propose_trace: TraceValHandle<K, V, T, isize>,

    /// A trace of type ((K, V), ()), used to validate proposed extensions.
    validate_trace: TraceKeyHandle<(K, V), T, isize>,
}

impl<K, V, T> Clone for CollectionIndex<K, V, T>
where
    K: Data+Hash,
    V: Data+Hash,
    T: Lattice+Data+Timestamp,
{
    fn clone(&self) -> Self {
        CollectionIndex {
            count_trace: self.count_trace.clone(),
            propose_trace: self.propose_trace.clone(),
            validate_trace: self.validate_trace.clone(),
        }
    }
}

impl<K, V, T> CollectionIndex<K, V, T>
where
    K: Data+Hash,
    V: Data+Hash,
    T: Lattice+Data+Timestamp,
{
    /// Creates a named CollectionIndex from a (K, V) collection.
    pub fn index<G: Scope<Timestamp=T>>(name: &str, collection: &Collection<G, (K, V), isize>) -> Self {
        let counts = collection.map(|(k,_v)| (k,())).arrange_named(&format!("Counts({})", name)).trace;
        let propose = collection.arrange_named(&format!("Proposals({})", &name)).trace;
        let validate = collection.map(|t| (t,())).arrange_named(&format!("Validations({})", &name)).trace;

        CollectionIndex {
            count_trace: counts,
            propose_trace: propose,
            validate_trace: validate,
        }
    }

    /// Returns a LiveIndex that lives in the specified scope.
    pub fn import<G: Scope<Timestamp=T>>(
        &mut self,
        scope: &G
    ) -> LiveIndex<G, K, V,
                   TraceKeyHandle<K, T, isize>,
                   TraceValHandle<K, V, T, isize>,
                   TraceKeyHandle<(K, V), T, isize>>
    {
        LiveIndex {
            count_trace: self.count_trace.import(scope),
            propose_trace: self.propose_trace.import(scope),
            validate_trace: self.validate_trace.import(scope),
        }
    }
}

/// Attributes are the fundamental unit of data modeling in 3DF.
pub struct Attribute {
    forward: CollectionIndex<Value, Value, u64>,
    reverse: CollectionIndex<Value, Value, u64>,
}

impl Attribute {
    /// Create an Attribute from a (K, V) collection.
    pub fn new<G: Scope<Timestamp=u64>>(name: &str, collection: &Collection<G, (Value,Value), isize>) -> Self {
        let forward = collection.clone();
        let reverse = collection.map(|(e,v)| (v,e));
        
        Attribute {
            forward: CollectionIndex::index(name, &forward),
            reverse: CollectionIndex::index(name, &reverse),
        }
    }

    /// Returns a trace to the underlying (K, V) pairs.
    pub fn tuples(&self) -> TraceKeyHandle<(Value, Value), u64, isize> {
        self.forward.validate_trace.clone()
    }
}

/// CollectionIndex that was imported into a scope.
pub struct LiveIndex<G, K, V, TrCount, TrPropose, TrValidate>
where
    G: Scope,
    G::Timestamp: Lattice+Data,
    K: Data,
    V: Data,
    TrCount: TraceReader<K, (), G::Timestamp, isize>+Clone,
    TrPropose: TraceReader<K, V, G::Timestamp, isize>+Clone,
    TrValidate: TraceReader<(K,V), (), G::Timestamp, isize>+Clone,
{
    count_trace: Arranged<G, K, (), isize, TrCount>,
    propose_trace: Arranged<G, K, V, isize, TrPropose>,
    validate_trace: Arranged<G, (K, V), (), isize, TrValidate>,
}

impl<G, K, V, TrCount, TrPropose, TrValidate> Clone for LiveIndex<G, K, V, TrCount, TrPropose, TrValidate>
where
    G: Scope,
    G::Timestamp: Lattice+Data,
    K: Data,
    V: Data,
    TrCount: TraceReader<K, (), G::Timestamp, isize>+Clone,
    TrPropose: TraceReader<K, V, G::Timestamp, isize>+Clone,
    TrValidate: TraceReader<(K,V), (), G::Timestamp, isize>+Clone,
{
    fn clone(&self) -> Self {
        LiveIndex {
            count_trace: self.count_trace.clone(),
            propose_trace: self.propose_trace.clone(),
            validate_trace: self.validate_trace.clone(),
        }
    }
}

impl<G, K, V, TrCount, TrPropose, TrValidate> LiveIndex<G, K, V, TrCount, TrPropose, TrValidate>
where
    G: Scope,
    G::Timestamp: Lattice+Data,
    K: Data,
    V: Data,
    TrCount: TraceReader<K, (), G::Timestamp, isize>+Clone,
    TrPropose: TraceReader<K, V, G::Timestamp, isize>+Clone,
    TrValidate: TraceReader<(K,V), (), G::Timestamp, isize>+Clone,
{
    /// Brings the index's traces into the specified scope.
    pub fn enter<'a, TInner>(
        &self,
        child: &Child<'a, G, TInner>
    ) -> LiveIndex<Child<'a, G, TInner>, K, V,
                   TraceEnter<K, (), G::Timestamp, isize, TrCount, TInner>,
                   TraceEnter<K, V, G::Timestamp, isize, TrPropose, TInner>,
                   TraceEnter<(K,V), (), G::Timestamp, isize, TrValidate, TInner>>
    where
        TrCount::Batch: Clone,
        TrPropose::Batch: Clone,
        TrValidate::Batch: Clone,
        K: 'static,
        V: 'static,
        G::Timestamp: Clone+Default+'static,
        TInner: Refines<G::Timestamp>+Lattice+Timestamp+Clone+Default+'static,
    {
        LiveIndex {
            count_trace: self.count_trace.enter(child),
            propose_trace: self.propose_trace.enter(child),
            validate_trace: self.validate_trace.enter(child),
        }
    }

    /// Brings the index's traces into the specified scope.
    pub fn enter_at<'a, TInner, FCount, FPropose, FValidate>(
        &self,
        child: &Child<'a, G, TInner>,
        fcount: FCount,
        fpropose: FPropose,
        fvalidate: FValidate,
    ) -> LiveIndex<Child<'a, G, TInner>, K, V,
                   TraceEnterAt<K, (), G::Timestamp, isize, TrCount, TInner, FCount>,
                   TraceEnterAt<K, V, G::Timestamp, isize, TrPropose, TInner, FPropose>,
                   TraceEnterAt<(K,V), (), G::Timestamp, isize, TrValidate, TInner, FValidate>>
    where
        TrCount::Batch: Clone,
        TrPropose::Batch: Clone,
        TrValidate::Batch: Clone,
        K: 'static,
        V: 'static,
        G::Timestamp: Clone+Default+'static,
        TInner: Refines<G::Timestamp>+Lattice+Timestamp+Clone+Default+'static,
        FCount: Fn(&K, &(), &G::Timestamp)->TInner+'static,
        FPropose: Fn(&K, &V, &G::Timestamp)->TInner+'static,
        FValidate: Fn(&(K,V), &(), &G::Timestamp)->TInner+'static,
    {
        LiveIndex {
            count_trace: self.count_trace.enter_at(child, fcount),
            propose_trace: self.propose_trace.enter_at(child, fpropose),
            validate_trace: self.validate_trace.enter_at(child, fvalidate),
        }
    }
}   

/// A symbol used in a query.
type Var = u32;

/// A named relation.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Rule {
    /// The name binding the relation.
    pub name: String,
    /// The plan describing contents of the relation.
    pub plan: Plan,
}

/// A relation with identified attributes
///
/// A relation is internally a collection of records of type `Vec<Value>` each of a
/// common length, and a mapping from variable identifiers to positions in each of
/// these vectors.
trait Relation<'a, G: Scope> {
    /// List the variable identifiers.
    fn symbols(&self) -> &[Var];
    /// A collection containing all tuples.
    fn tuples(self) -> Collection<Iterative<'a, G, u64>, Vec<Value>, isize>;
    /// Returns the offset at which values for this symbol occur.
    fn offset(&self, sym: &Var) -> usize;
    /// A collection with tuples partitioned by `syms`.
    ///
    /// Variables present in `syms` are collected in order and populate a first "key"
    /// `Vec<Value>`, followed by those variables not present in `syms`.
    fn tuples_by_symbols(
        self,
        syms: &[Var],
    ) -> Collection<Iterative<'a, G, u64>, (Vec<Value>, Vec<Value>), isize>;
}

/// A collection and variable bindings.
pub struct SimpleRelation<'a, G: Scope> {
    symbols: Vec<Var>,
    tuples: Collection<Iterative<'a, G, u64>, Vec<Value>, isize>,
}

impl<'a, G: Scope> Relation<'a, G> for SimpleRelation<'a, G> {
    fn symbols(&self) -> &[Var] {
        &self.symbols
    }
    fn tuples(self) -> Collection<Iterative<'a, G, u64>, Vec<Value>, isize> {
        self.tuples
    }
    fn offset(&self, sym: &Var) -> usize {
        self.symbols().iter().position(|&x| *sym == x).unwrap()
    }

    /// Separates tuple fields by those in `syms` and those not.
    ///
    /// Each tuple is mapped to a pair `(Vec<Value>, Vec<Value>)` containing first exactly
    /// those symbols in `syms` in that order, followed by the remaining values in their
    /// original order.
    fn tuples_by_symbols(
        self,
        syms: &[Var],
    ) -> Collection<Iterative<'a, G, u64>, (Vec<Value>, Vec<Value>), isize> {
        if syms == &self.symbols()[..] {
            self.tuples().map(|x| (x, Vec::new()))
        } else if syms.is_empty() {
            self.tuples().map(|x| (Vec::new(), x))
        } else {
            let key_length = syms.len();
            let values_length = self.symbols().len() - key_length;

            let mut key_offsets: Vec<usize> = Vec::with_capacity(key_length);
            let mut value_offsets: Vec<usize> = Vec::with_capacity(values_length);
            let sym_set: HashSet<Var> = syms.iter().cloned().collect();

            // It is important to preserve the key symbols in the order
            // they were specified.
            for sym in syms.iter() {
                key_offsets.push(self.symbols().iter().position(|&v| *sym == v).unwrap());
            }

            // Values we'll just take in the order they were.
            for (idx, sym) in self.symbols().iter().enumerate() {
                if sym_set.contains(sym) == false {
                    value_offsets.push(idx);
                }
            }

            // let debug_keys: Vec<String> = key_offsets.iter().map(|x| x.to_string()).collect();
            // let debug_values: Vec<String> = value_offsets.iter().map(|x| x.to_string()).collect();
            // println!("key offsets: {:?}", debug_keys);
            // println!("value offsets: {:?}", debug_values);

            self.tuples().map(move |tuple| {
                let key: Vec<Value> = key_offsets.iter().map(|i| tuple[*i].clone()).collect();
                // @TODO second clone not really neccessary
                let values: Vec<Value> = value_offsets.iter().map(|i| tuple[*i].clone()).collect();

                (key, values)
            })
        }
    }
}

/// Takes a query plan and turns it into a differential dataflow. The
/// dataflow is extended to feed output tuples to JS clients. A probe
/// on the dataflow is returned.
pub fn implement<S: Scope<Timestamp = u64>>(
    mut rules: Vec<Rule>,
    publish: Vec<String>,
    scope: &mut S,
    global_arrangements: &mut HashMap<String, RelationHandle>,
    attributes: &mut HashMap<String, Attribute>,
    _probe: &mut ProbeHandle<u64>,
) -> HashMap<String, RelationHandle> {
    scope.iterative::<u64, _, _>(|nested| {
        let mut local_arrangements = VariableMap::new();
        let mut result_map = HashMap::new();

        // Step 0: Canonicalize, check uniqueness of bindings.
        rules.sort_by(|x, y| x.name.cmp(&y.name));
        for index in 1..rules.len() - 1 {
            if rules[index].name == rules[index - 1].name {
                panic!("Duplicate rule definitions for rule {}", rules[index].name);
            }
        }

        // Step 1: Create new recursive variables for each rule.
        for rule in rules.iter() {
            local_arrangements.insert(rule.name.clone(), Variable::new(nested, Product::new(0, 1)));
        }

        // Step 2: Create public arrangements for published relations.
        for name in publish.into_iter() {
            if let Some(relation) = local_arrangements.get(&name) {
                let trace = relation.leave()
                    // .inspect(|x| { println!("OUTPUT {:?}", x); })
                // .probe_with(probe)
                    .map(|t| (t,()))
                    .arrange_named(&name)
                    .trace;

                result_map.insert(name, trace);
            } else {
                panic!("Attempted to publish undefined name {:?}", name);
            }
        }

        // Step 3: define the executions for each rule ...
        let mut executions = Vec::with_capacity(rules.len());
        for rule in rules.iter() {
            info!("planning {:?}", rule.name);
            executions.push(
                rule.plan
                    .implement(nested, &local_arrangements, global_arrangements, attributes),
            );
        }

        // Step 4: complete named relations in a specific order (sorted by name).
        for (rule, execution) in rules.iter().zip(executions.drain(..)) {
            local_arrangements
                .remove(&rule.name)
                .expect("Rule should be in local_arrangements, but isn't")
                .set(&execution.tuples().distinct());
        }

        result_map
    })
}
