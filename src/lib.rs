//! Declarative dataflow infrastructure
//!
//! This crate contains types, traits, and logic for assembling differential
//! dataflow computations from declaratively specified programs, without any
//! additional compilation.

#![forbid(missing_docs)]

extern crate differential_dataflow;
extern crate timely;
extern crate timely_sort;
#[macro_use]
extern crate log;

// #[macro_use]
// extern crate abomonation_derive;
// extern crate abomonation;

#[macro_use]
extern crate serde_derive;

extern crate num_rational;

use std::collections::{HashMap, HashSet};

use timely::dataflow::scopes::child::Iterative;
use timely::dataflow::*;
use timely::order::Product;

use differential_dataflow::collection::Collection;
use differential_dataflow::operators::arrange::{Arrange, TraceAgent};
use differential_dataflow::operators::group::Threshold;
use differential_dataflow::operators::iterate::Variable;
use differential_dataflow::trace::implementations::ord::{OrdKeySpine, OrdValSpine};

pub use num_rational::Rational32;

pub mod timestamp;

pub mod plan;
pub use plan::{Implementable, Plan};

pub mod server;
pub mod sources;

//
// TYPES
//

/// A unique entity identifier.
#[cfg(not(feature = "uuids"))]
pub type Entity = u64;

/// A unique entity identifier.
#[cfg(feature = "uuids")]
pub type Entity = u128;

/// A unique attribute identifier.
pub type Attribute = String; // u32

/// Possible data values.
///
/// This enum captures the currently supported data types, and is the least common denominator
/// for the types of records moved around.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub enum Value {
    /// An attribute identifier
    Attribute(Attribute),
    /// A string
    String(String),
    /// A boolean
    Bool(bool),
    /// A 64 bit signed integer
    Number(i64),
    /// A 32 bit rational
    Rational32(Rational32),
    /// An entity identifier
    Eid(Entity),
    /// Milliseconds since midnight, January 1, 1970 UTC
    Instant(u64),
    /// A 16 byte unique identifier.
    Uuid([u8; 16]),
}

/// An entity, attribute, value triple.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub struct Datom(pub Entity, pub Attribute, pub Value);

/// A handle to a collection trace.
pub type TraceKeyHandle<K, R> = TraceAgent<K, (), u64, R, OrdKeySpine<K, u64, R>>;
type TraceValHandle<K, V, R> = TraceAgent<K, V, u64, R, OrdValSpine<K, V, u64, R>>;
// type Arrange<G, K, V, R> = Arranged<G, K, V, R, TraceValHandle<K, V, <G as ScopeParent>::Timestamp, R>>;
/// A map from global names to registered traces.
pub type QueryMap<R> = HashMap<String, TraceKeyHandle<Vec<Value>, R>>;
type RelationMap<G> = HashMap<String, Variable<G, Vec<Value>, isize>>;

//
// CONTEXT
//

/// Handles to maintained indices
///
/// A `DB` contains multiple indices for (entity, attribute, value) triples, by various
/// subsets of these three fields. These indices can be shared between all queries that
/// need access to the common data.
pub struct DB {
    /// Indexed by entity.
    pub e_av: TraceValHandle<Entity, (Attribute, Value), isize>,
    /// Indexed by attribute.
    pub a_ev: TraceValHandle<Attribute, (Entity, Value), isize>,
    /// Indexed by (entity, attribute).
    pub ea_v: TraceValHandle<(Entity, Attribute), Value, isize>,
    /// Indexed by (attribute, value).
    pub av_e: TraceValHandle<(Attribute, Value), Entity, isize>,
}

// /// Live arrangements.
// pub struct ImplContext<G: Scope + ScopeParent> where G::Timestamp : Lattice {
//     // Imported traces
//     e_av: Arrange<G, Entity, (Attribute, Value), isize>,
//     a_ev: Arrange<G, Attribute, (Entity, Value), isize>,
//     ea_v: Arrange<G, (Entity, Attribute), Value, isize>,
//     av_e: Arrange<G, (Attribute, Value), Entity, isize>,
// }

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
trait Relation<'a, G: Scope> {
    /// List the variable identifiers.
    fn symbols(&self) -> &[Var];
    /// A collection containing all tuples.
    fn tuples(self) -> Collection<Iterative<'a, G, u64>, Vec<Value>, isize>;
    /// A collection with tuples partitioned by `syms`.
    ///
    /// Variables present in `syms` are collected in order and populate a first "key"
    /// `Vec<Value>`, followed by those variables not present in `syms`.
    fn tuples_by_symbols(
        self,
        syms: &[Var],
    ) -> Collection<Iterative<'a, G, u64>, (Vec<Value>, Vec<Value>), isize>;
}

/// A handle to an arranged relation.
pub type RelationHandle = TraceKeyHandle<Vec<Value>, isize>;

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

//
// QUERY PLAN IMPLEMENTATION
//

/// Takes a query plan and turns it into a differential dataflow. The
/// dataflow is extended to feed output tuples to JS clients. A probe
/// on the dataflow is returned.
pub fn implement<S: Scope<Timestamp = u64>>(
    mut rules: Vec<Rule>,
    publish: Vec<String>,
    scope: &mut S,
    global_arrangements: &mut QueryMap<isize>,
    _probe: &mut ProbeHandle<u64>,
) -> HashMap<String, RelationHandle> {
    scope.iterative::<u64, _, _>(|nested| {
        let mut local_arrangements = RelationMap::new();
        let mut result_map = QueryMap::new();

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
                    .implement(nested, &local_arrangements, global_arrangements),
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
