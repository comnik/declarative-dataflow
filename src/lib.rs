//! Declarative dataflow infrastructure
//!
//! This crate contains types, traits, and logic for assembling differential
//! dataflow computations from declaratively specified programs, without any
//! additional compilation.

#![forbid(missing_docs)]

extern crate timely;
extern crate differential_dataflow;

#[macro_use]
extern crate abomonation_derive;
extern crate abomonation;

#[macro_use]
extern crate serde_derive;

use std::collections::{HashMap, HashSet};

use timely::dataflow::*;
use timely::dataflow::scopes::Child;
use timely::progress::timestamp::{Timestamp, RootTimestamp};
use timely::progress::nested::product::Product;
use timely::communication::Allocate;
use timely::worker::Worker;

use differential_dataflow::collection::{Collection};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::input::{Input, InputSession};
use differential_dataflow::trace::implementations::ord::{OrdValSpine, OrdKeySpine};
use differential_dataflow::operators::arrange::{ArrangeByKey, ArrangeBySelf, TraceAgent, Arranged};
use differential_dataflow::operators::group::{Threshold};
use differential_dataflow::operators::iterate::Variable;

pub mod plan;
pub use plan::{Plan, Implementable};

pub mod sources;

//
// TYPES
//

/// A unique entity identifier.
pub type Entity = u64;
/// A unique attribute identifier.
pub type Attribute = String; // u32

/// Possible data values.
///
/// This enum captures the currently supported data types, and is the least common denominator
/// for the types of records moved around.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Abomonation, Debug, Serialize, Deserialize)]
pub enum Value {
    /// An attribute identifier
    Attribute(Attribute),
    /// A string
    String(String),
    /// A boolean
    Bool(bool),
    /// A 64 bit signed integer
    Number(i64),
    /// An entity identifier
    Eid(Entity),
    /// Milliseconds since midnight, January 1, 1970 UTC
    Instant(u64),
    /// A 16 byte unique identifier.
    Uuid([u8; 16])
}

/// An entity, attribute, value triple.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Abomonation, Debug, Serialize, Deserialize)]
pub struct Datom(pub Entity, pub Attribute, pub Value);

type TraceKeyHandle<K, T, R> = TraceAgent<K, (), T, R, OrdKeySpine<K, T, R>>;
type TraceValHandle<K, V, T, R> = TraceAgent<K, V, T, R, OrdValSpine<K, V, T, R>>;
type Arrange<G, K, V, R> = Arranged<G, K, V, R, TraceValHandle<K, V, <G as ScopeParent>::Timestamp, R>>;
type QueryMap<T, R> = HashMap<String, TraceKeyHandle<Vec<Value>, Product<RootTimestamp, T>, R>>;
type RelationMap<'a, G> = HashMap<String, Variable<'a, G, Vec<Value>, u64, isize>>;

//
// CONTEXT
//

/// Handles to maintained indices
///
/// A `DB` contains multiple indices for (entity, attribute, value) triples, by various
/// subsets of these three fields. These indices can be shared between all queries that
/// need access to the common data.
pub struct DB<T: Timestamp+Lattice> {
    /// Indexed by entity.
    pub e_av: TraceValHandle<Entity, (Attribute, Value), Product<RootTimestamp, T>, isize>,
    /// Indexed by attribute.
    pub a_ev: TraceValHandle<Attribute, (Entity, Value), Product<RootTimestamp, T>, isize>,
    /// Indexed by (entity, attribute).
    pub ea_v: TraceValHandle<(Entity, Attribute), Value, Product<RootTimestamp, T>, isize>,
    /// Indexed by (attribute, value).
    pub av_e: TraceValHandle<(Attribute, Value), Entity, Product<RootTimestamp, T>, isize>,
}

// /// Live arrangements.
// pub struct ImplContext<G: Scope + ScopeParent> where G::Timestamp : Lattice {
//     // Imported traces
//     e_av: Arrange<G, Entity, (Attribute, Value), isize>,
//     a_ev: Arrange<G, Attribute, (Entity, Value), isize>,
//     ea_v: Arrange<G, (Entity, Attribute), Value, isize>,
//     av_e: Arrange<G, (Attribute, Value), Entity, isize>,
// }

// @TODO move input_handle into server, get rid of all this and use DB
// struct directly
/// Context maintained by the query processor.
pub struct Context<T: Timestamp+Lattice> {
    /// Input handle to the collection of all Datoms in the system.
    pub input_handle: InputSession<T, Datom, isize>,
    /// Maintained indices.
    pub db: DB<T>,
    /// Named relations.
    pub queries: QueryMap<T, isize>,
}

//
// QUERY PLAN GRAMMAR
//

/// A named relation.
#[derive(Deserialize, Clone, Debug)]
pub struct Rule {
    /// The name binding the relation.
    pub name: String,
    /// The plan describing contents of the relation.
    pub plan: Plan,
}

type Var = u32;

//
// RELATIONS
//

/// A relation with identified attributes
///
/// A relation is internally a collection of records of type `Vec<Value>` each of a
/// common length, and a mapping from variable identifiers to positions in each of
/// these vectors.
trait Relation<'a, G: Scope> where G::Timestamp : Lattice {
    /// List the variable identifiers.
    fn symbols(&self) -> &[Var];
    /// A collection containing all tuples.
    fn tuples(self) -> Collection<Child<'a, G, u64>, Vec<Value>, isize>;
    /// A collection with tuples partitioned by `syms`.
    ///
    /// Variables present in `syms` are collected in order and populate a first "key"
    /// `Vec<Value>`, followed by those variables not present in `syms`.
    fn tuples_by_symbols(self, syms: &[Var]) -> Collection<Child<'a, G, u64>, (Vec<Value>, Vec<Value>), isize>;
}

/// A handle to an arranged relation.
pub type RelationHandle<T> = TraceKeyHandle<Vec<Value>, Product<RootTimestamp, T>, isize>;

/// A collection and variable bindings.
pub struct SimpleRelation<'a, G: Scope> where G::Timestamp : Lattice {
    symbols: Vec<Var>,
    tuples: Collection<Child<'a, G, u64>, Vec<Value>, isize>,
}

impl<'a, G: Scope> Relation<'a, G> for SimpleRelation<'a, G> where G::Timestamp : Lattice {
    fn symbols(&self) -> &[Var] { &self.symbols }
    fn tuples(self) -> Collection<Child<'a, G, u64>, Vec<Value>, isize> { self.tuples }

    /// Separates tuple fields by those in `syms` and those not.
    ///
    /// Each tuple is mapped to a pair `(Vec<Value>, Vec<Value>)` containing first exactly
    /// those symbols in `syms` in that order, followed by the remaining values in their
    /// original order.
    fn tuples_by_symbols(self, syms: &[Var]) -> Collection<Child<'a, G, u64>, (Vec<Value>, Vec<Value>), isize>{
        if syms == &self.symbols()[..] {
            self.tuples().map(|x| (x, Vec::new()))
        }
        else {
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

            self.tuples()
                .map(move |tuple| {
                    let key: Vec<Value> = key_offsets.iter().map(|i| tuple[*i].clone()).collect();
                    // @TODO second clone not really neccessary
                    let values: Vec<Value> = value_offsets.iter().map(|i| tuple[*i].clone()).collect();

                    (key, values)
                })
        }
    }
}

//
// QUERY PLAN IMPLEMENTATION
//

/// Takes a query plan and turns it into a differential dataflow. The
/// dataflow is extended to feed output tuples to JS clients. A probe
/// on the dataflow is returned.
pub fn implement<A: Allocate, T: Timestamp+Lattice>(
    mut rules: Vec<Rule>,
    publish: Vec<String>,
    scope: &mut Child<Worker<A>, T>,
    ctx: &mut Context<T>,
    probe: &mut ProbeHandle<Product<RootTimestamp, T>>,
) -> HashMap<String, RelationHandle<T>> {

    // let db = &mut ctx.db;
    let global_arrangements = &mut ctx.queries;

    // // @TODO Only import those we need for the query?
    // let impl_ctx: ImplContext<Child<Worker<A>, T>> = ImplContext {
    //     e_av: db.e_av.import(scope),
    //     a_ev: db.a_ev.import(scope),
    //     ea_v: db.ea_v.import(scope),
    //     av_e: db.av_e.import(scope),
    // };

    scope.scoped(|nested| {

        let mut local_arrangements = RelationMap::new();
        let mut result_map = QueryMap::new();

        // Step 0: Canonicalize, check uniqueness of bindings.
        rules.sort_by(|x,y| x.name.cmp(&y.name));
        for index in 1 .. rules.len() - 1 {
            if rules[index].name == rules[index-1].name {
                panic!("Duplicate rule definitions for rule {}", rules[index].name);
            }
        }

        // Step 1: Create new recursive variables for each rule.
        for rule in rules.iter() {
            local_arrangements.insert(rule.name.clone(), Variable::new(nested, u64::max_value(), 1));
        }

        // Step 2: Create public arrangements for published relations.
        for name in publish.into_iter() {
            if let Some(relation) = local_arrangements.get(&name) {
                let trace =
                relation
                    .leave()
                    .probe_with(probe)
                    .arrange_by_self()
                    .trace;

                result_map.insert(name, trace);
            }
            else {
                panic!("Attempted to publish undefined name {:?}", name);
            }
        }

        // Step 3: define the executions for each rule ...
        let mut executions = Vec::with_capacity(rules.len());
        for rule in rules.iter() {
            println!("Planning {:?}", rule.name);
            executions.push(rule.plan.implement(nested, &local_arrangements, global_arrangements));
        }

        // Step 4: complete named relations in a specific order (sorted by name).
        for (rule, execution) in rules.iter().zip(executions.drain(..)) {
            local_arrangements
                .remove(&rule.name)
                .expect("Rule should be in local_arrangements, but isn't")
                .set(&execution.tuples().distinct());
        }

        println!("Done");
        result_map
    })
}

/// Create a new DB instance and interactive session.
pub fn create_db<A: Allocate, T: Timestamp+Lattice>(scope: &mut Child<Worker<A>, T>) -> (InputSession<T, Datom, isize>, DB<T>) {
    let (input_handle, datoms) = scope.new_collection::<Datom, isize>();
    let db = DB {
        e_av: datoms.map(|Datom(e, a, v)| (e, (a, v))).arrange_by_key().trace,
        a_ev: datoms.map(|Datom(e, a, v)| (a, (e, v))).arrange_by_key().trace,
        ea_v: datoms.map(|Datom(e, a, v)| ((e, a), v)).arrange_by_key().trace,
        av_e: datoms.map(|Datom(e, a, v)| ((a, v), e)).arrange_by_key().trace,
    };

    (input_handle, db)
}
