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

use std::boxed::Box;
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
use differential_dataflow::operators::join::{JoinCore};
use differential_dataflow::operators::iterate::Variable;

pub mod plan;
pub use plan::Implementable;

//
// TYPES
//

/// A unique entity identifier.
pub type Entity = u64;
/// A unique attribute identifier.
pub type Attribute = u32;

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
    pub e_av: TraceValHandle<Vec<Value>, Vec<Value>, Product<RootTimestamp, T>, isize>,
    /// Indexed by attribute.
    pub a_ev: TraceValHandle<Vec<Value>, Vec<Value>, Product<RootTimestamp, T>, isize>,
    /// Indexed by (entity, attribute).
    pub ea_v: TraceValHandle<Vec<Value>, Vec<Value>, Product<RootTimestamp, T>, isize>,
    /// Indexed by (attribute, value).
    pub av_e: TraceValHandle<Vec<Value>, Vec<Value>, Product<RootTimestamp, T>, isize>,
}

/// Live arrangements.
pub struct ImplContext<G: Scope + ScopeParent> where G::Timestamp : Lattice {
    // Imported traces
    e_av: Arrange<G, Vec<Value>, Vec<Value>, isize>,
    a_ev: Arrange<G, Vec<Value>, Vec<Value>, isize>,
    ea_v: Arrange<G, Vec<Value>, Vec<Value>, isize>,
    av_e: Arrange<G, Vec<Value>, Vec<Value>, isize>,
}

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

/// Possible query plan types.
#[derive(Deserialize, Clone, Debug)]
pub enum Plan {
    /// Projection
    Project(plan::Projection),
    /// Aggregation
    Aggregate(plan::Aggregate),
    /// Union
    Union(plan::Union),
    /// Equijoin
    Join(plan::Join),
    /// Antijoin
    Antijoin(plan::Antijoin),
    /// Negation
    Not(Box<Plan>),
    /// Filters bindings by one of the built-in predicates
    PredExpr(plan::PredExpr),
    /// Data pattern of the form [e a ?v]
    Lookup(Entity, Attribute, Var),
    /// Data pattern of the form [e ?a ?v]
    Entity(Entity, Var, Var),
    /// Data pattern of the form [?e a ?v]
    HasAttr(Var, Attribute, Var),
    /// Data pattern of the form [?e a v]
    Filter(Var, Attribute, Value),
    /// Sources data from a query-local relation
    RuleExpr(String, Vec<Var>),
    /// Sources data from a published relation
    NameExpr(String, Vec<Var>),
}

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
    fn symbols(&self) -> &Vec<Var>;
    /// A collection containing all tuples.
    fn tuples(self) -> Collection<Child<'a, G, u64>, Vec<Value>, isize>;
    /// A collection with tuples partitioned by `syms`.
    ///
    /// Variables present in `syms` are collected in order and populate a first "key"
    /// `Vec<Value>`, followed by those variables not present in `syms`.
    fn tuples_by_symbols(self, syms: Vec<Var>) -> Collection<Child<'a, G, u64>, (Vec<Value>, Vec<Value>), isize>;
}

/// A handle to an arranged relation.
pub type RelationHandle<T> = TraceKeyHandle<Vec<Value>, Product<RootTimestamp, T>, isize>;

/// A collection and variable bindings.
pub struct SimpleRelation<'a, G: Scope> where G::Timestamp : Lattice {
    symbols: Vec<Var>,
    tuples: Collection<Child<'a, G, u64>, Vec<Value>, isize>,
}

impl<'a, G: Scope> Relation<'a, G> for SimpleRelation<'a, G> where G::Timestamp : Lattice {
    fn symbols(&self) -> &Vec<Var> { &self.symbols }
    fn tuples(self) -> Collection<Child<'a, G, u64>, Vec<Value>, isize> { self.tuples }

    fn tuples_by_symbols(self, syms: Vec<Var>) -> Collection<Child<'a, G, u64>, (Vec<Value>, Vec<Value>), isize>{
        let key_length = syms.len();
        let values_length = self.symbols().len() - key_length;

        let mut key_offsets: Vec<usize> = Vec::with_capacity(key_length);
        let mut value_offsets: Vec<usize> = Vec::with_capacity(values_length);
        let sym_set: HashSet<Var> = syms.iter().cloned().collect();

        // It is important to preservere the key symbols in the order
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

//
// QUERY PLAN IMPLEMENTATION
//

/// Takes a query plan and turns it into a differential dataflow. The
/// dataflow is extended to feed output tuples to JS clients. A probe
/// on the dataflow is returned.
fn implement<A: Allocate, T: Timestamp+Lattice>(
    mut rules: Vec<Rule>,
    publish: Vec<String>,
    scope: &mut Child<Worker<A>, T>,
    ctx: &mut Context<T>,
    probe: &mut ProbeHandle<Product<RootTimestamp, T>>,
) -> HashMap<String, RelationHandle<T>> {

    let db = &mut ctx.db;
    let queries = &mut ctx.queries;

    // @TODO Only import those we need for the query?
    let impl_ctx: ImplContext<Child<Worker<A>, T>> = ImplContext {
        e_av: db.e_av.import(scope),
        a_ev: db.a_ev.import(scope),
        ea_v: db.ea_v.import(scope),
        av_e: db.av_e.import(scope),
    };

    scope.scoped(|nested| {

        let mut relation_map = RelationMap::new();
        let mut result_map = QueryMap::new();

        rules.sort_by(|x,y| x.name.cmp(&y.name));
        for index in 1 .. rules.len() - 1 {
            if rules[index].name == rules[index-1].name {
                panic!("Duplicate rule definitions for rule {}", rules[index].name);
            }
        }

        // Step 1: Create new recursive variables for each rule.
        for rule in rules.iter() {
            relation_map.insert(rule.name.clone(), Variable::new(nested, u64::max_value(), 1));
        }

        // Step 2: Create public arrangements for published relations.
        for name in publish.into_iter() {
            if let Some(relation) = relation_map.get(&name) {
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
            executions.push(rule.plan.implement(&impl_ctx, nested, &relation_map, queries));
        }

        // Step 4: complete named relations in a specific order (sorted by name).
        for (rule, execution) in rules.iter().zip(executions.drain(..)) {
            relation_map
                .remove(&rule.name)
                .expect("Rule should be in relation_map, but isn't")
                .set(&execution.tuples().distinct());
        }

        println!("Done");
        result_map
    })
}

impl<'a, 'b, A: Allocate, T: Timestamp+Lattice> Implementable<'a, 'b, A, T> for Plan {
    fn implement(
        &self,
        db: &ImplContext<Child<'a, Worker<A>, T>>,
        nested: &mut Child<'b, Child<'a, Worker<A>, T>, u64>,
        relation_map: &RelationMap<'b, Child<'a, Worker<A>, T>>,
        queries: &mut QueryMap<T, isize>
    )
    -> SimpleRelation<'b, Child<'a, Worker<A>, T>> {

        use timely::dataflow::operators::ToStream;
        use differential_dataflow::AsCollection;

        match self {
            &Plan::Project(ref projection) => projection.implement(db, nested, relation_map, queries),
            &Plan::Aggregate(ref aggregate) => aggregate.implement(db, nested, relation_map, queries),
            &Plan::Union(ref union) => union.implement(db, nested, relation_map, queries),
            // @TODO specialized join for join on single variable
            &Plan::Join(ref join) => join.implement(db, nested, relation_map, queries),
            &Plan::Antijoin(ref antijoin) => antijoin.implement(db, nested, relation_map, queries),
            &Plan::Not(ref plan) => {
                let mut rel = plan.implement(db, nested, relation_map, queries);
                SimpleRelation {
                    symbols: rel.symbols().clone(),
                    tuples: rel.tuples().negate()
                }
            },
            &Plan::PredExpr(ref pred_expr) => pred_expr.implement(db, nested, relation_map, queries),
            &Plan::Lookup(e, a, sym1) => {
                let tuple = (vec![Value::Eid(e), Value::Attribute(a)], Default::default(), 1);
                let ea_in = Some(tuple).to_stream(nested).as_collection().arrange_by_self();
                let tuples = db.ea_v.enter(nested)
                    .join_core(&ea_in, |_,tuple,_| Some(tuple.clone()));

                SimpleRelation { symbols: vec![sym1], tuples }
            },
            &Plan::Entity(e, sym1, sym2) => {
                let tuple = (vec![Value::Eid(e)], Default::default(), 1);
                let e_in = Some(tuple).to_stream(nested).as_collection().arrange_by_self();
                let tuples = db.e_av.enter(nested)
                    .join_core(&e_in, |_,tuple,_| Some(tuple.clone()));

                SimpleRelation { symbols: vec![sym1, sym2], tuples }
            },
            &Plan::HasAttr(sym1, a, sym2) => {
                let tuple = (vec![Value::Attribute(a)], Default::default(), 1);
                let a_in = Some(tuple).to_stream(nested).as_collection().arrange_by_self();
                let tuples = db.a_ev.enter(nested)
                    .join_core(&a_in, |_,tuple,_| Some(tuple.clone()));

                SimpleRelation { symbols: vec![sym1, sym2], tuples }
            },
            &Plan::Filter(sym1, a, ref v) => {
                let tuple = (vec![Value::Attribute(a), v.clone()], Default::default(), 1);
                let av_in = Some(tuple).to_stream(nested).as_collection().arrange_by_self();
                let tuples = db.av_e.enter(nested)
                    .join_core(&av_in, |_,tuple,_| Some(tuple.clone()));

                SimpleRelation { symbols: vec![sym1], tuples }
            },
            &Plan::RuleExpr(ref name, ref syms) => {
                match relation_map.get(name) {
                    None => panic!("{:?} not in relation map", name),
                    Some(named) => {
                        SimpleRelation {
                            symbols: syms.clone(),
                            tuples: named.map(|tuple| tuple.clone()),
                        }
                    }
                }
            }
            &Plan::NameExpr(ref name, ref syms) => {
                match queries.get_mut(name) {
                    None => panic!("{:?} not in query map", name),
                    Some(named) => {
                        SimpleRelation {
                            symbols: syms.clone(),
                            tuples: named.import(&nested.parent).enter(nested).as_collection(|tuple,_| tuple.clone()),
                        }
                    }
                }
            }
        }
    }
}

//
// PUBLIC API
//

/// Create a new DB instance and interactive session.
pub fn setup_db<A: Allocate, T: Timestamp+Lattice>(scope: &mut Child<Worker<A>, T>) -> (InputSession<T, Datom, isize>, DB<T>) {
    let (input_handle, datoms) = scope.new_collection::<Datom, isize>();
    let db = DB {
        e_av: datoms.map(|Datom(e, a, v)| (vec![Value::Eid(e)], vec![Value::Attribute(a), v])).arrange_by_key().trace,
        a_ev: datoms.map(|Datom(e, a, v)| (vec![Value::Attribute(a)], vec![Value::Eid(e), v])).arrange_by_key().trace,
        ea_v: datoms.map(|Datom(e, a, v)| (vec![Value::Eid(e), Value::Attribute(a)], vec![v])).arrange_by_key().trace,
        av_e: datoms.map(|Datom(e, a, v)| (vec![Value::Attribute(a), v], vec![Value::Eid(e)])).arrange_by_key().trace,
    };

    (input_handle, db)
}

/// Synthesizes a query plan and set's up the resulting collection to
/// be available via a globally unique name.
pub fn register<A: Allocate, T: Timestamp+Lattice>(
    scope: &mut Child<Worker<A>, T>,
    ctx: &mut Context<T>,
    // name: &String,
    // plan: Plan,
    rules: Vec<Rule>,
    publish: Vec<String>,
    probe: &mut ProbeHandle<Product<RootTimestamp, T>>,
) -> HashMap<String, RelationHandle<T>> {

    let result_map = implement(rules, publish, scope, ctx, probe);

    // @TODO store trace somewhere for re-use from other queries later
    // queries.insert(name.clone(), output_collection.arrange_by_self().trace);

    result_map
}
