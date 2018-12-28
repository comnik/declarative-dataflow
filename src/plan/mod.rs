//! Types and traits for implementing query plans.

use timely::communication::Allocate;
use timely::dataflow::scopes::child::{Child, Iterative};
use timely::worker::Worker;

use {Attribute, Entity, Value, Var};
use {QueryMap, Relation, RelationMap, SimpleRelation};

pub mod aggregate;
pub mod antijoin;
pub mod filter;
pub mod join;
pub mod hector;
pub mod project;
pub mod transform;
pub mod union;

pub use self::aggregate::{Aggregate, AggregationFn};
pub use self::antijoin::Antijoin;
pub use self::filter::{Filter, Predicate};
pub use self::join::Join;
pub use self::hector::{Hector, Binding};
pub use self::project::Project;
pub use self::transform::{Function, Transform};
pub use self::union::Union;

/// A type that can be implemented as a simple relation.
pub trait Implementable {
    /// Implements the type as a simple relation.
    fn implement<'a, 'b, A: Allocate>(
        &self,
        nested: &mut Iterative<'b, Child<'a, Worker<A>, u64>, u64>,
        local_arrangements: &RelationMap<Iterative<'b, Child<'a, Worker<A>, u64>, u64>>,
        global_arrangements: &mut QueryMap<isize>,
    ) -> SimpleRelation<'b, Child<'a, Worker<A>, u64>>;
}

/// Possible query plan types.
#[derive(Deserialize, Clone, Debug)]
pub enum Plan {
    /// Projection
    Project(Project<Plan>),
    /// Aggregation
    Aggregate(Aggregate<Plan>),
    /// Union
    Union(Union<Plan>),
    /// Equijoin
    Join(Join<Plan, Plan>),
    /// WCO
    Hector(Hector),
    /// Antijoin
    Antijoin(Antijoin<Plan, Plan>),
    /// Negation
    Negate(Box<Plan>),
    /// Filters bindings by one of the built-in predicates
    Filter(Filter<Plan>),
    /// Transforms a binding by a function expression
    Transform(Transform<Plan>),
    // /// Data pattern of the form [e ?a ?v]
    // MatchE(Entity, Var, Var),
    /// Data pattern of the form [?e a ?v]
    MatchA(Var, Attribute, Var),
    /// Data pattern of the form [e a ?v]
    MatchEA(Entity, Attribute, Var),
    /// Data pattern of the form [?e a v]
    MatchAV(Var, Attribute, Value),
    /// Sources data from a query-local relation
    RuleExpr(Vec<Var>, String),
    /// Sources data from a published relation
    NameExpr(Vec<Var>, String),
}

impl Implementable for Plan {
    fn implement<'a, 'b, A: Allocate>(
        &self,
        nested: &mut Iterative<'b, Child<'a, Worker<A>, u64>, u64>,
        local_arrangements: &RelationMap<Iterative<'b, Child<'a, Worker<A>, u64>, u64>>,
        global_arrangements: &mut QueryMap<isize>,
    ) -> SimpleRelation<'b, Child<'a, Worker<A>, u64>> {
        // use differential_dataflow::AsCollection;
        // use timely::dataflow::operators::ToStream;
        // use differential_dataflow::operators::arrange::ArrangeBySelf;
        // use differential_dataflow::operators::JoinCore;

        match self {
            &Plan::Project(ref projection) => {
                projection.implement(nested, local_arrangements, global_arrangements)
            }
            &Plan::Aggregate(ref aggregate) => {
                aggregate.implement(nested, local_arrangements, global_arrangements)
            }
            &Plan::Union(ref union) => {
                union.implement(nested, local_arrangements, global_arrangements)
            }
            &Plan::Join(ref join) => {
                join.implement(nested, local_arrangements, global_arrangements)
            }
            &Plan::Hector(ref hector) => {
                hector.implement(nested, local_arrangements, global_arrangements)
            }
            &Plan::Antijoin(ref antijoin) => {
                antijoin.implement(nested, local_arrangements, global_arrangements)
            }
            &Plan::Negate(ref plan) => {
                let mut rel = plan.implement(nested, local_arrangements, global_arrangements);
                SimpleRelation {
                    symbols: rel.symbols().to_vec(),
                    tuples: rel.tuples().negate(),
                }
            }
            &Plan::Filter(ref filter) => {
                filter.implement(nested, local_arrangements, global_arrangements)
            }
            &Plan::Transform(ref transform) => {
                transform.implement(nested, local_arrangements, global_arrangements)
            }
            &Plan::MatchA(sym1, ref a, sym2) => {
                let tuples = match global_arrangements.get_mut(a) {
                    None => panic!("attribute {:?} does not exist", a),
                    Some(named) => named
                        .import_named(&nested.parent, a)
                        .enter(nested)
                        .as_collection(|tuple, _| tuple.clone()),
                };

                SimpleRelation {
                    symbols: vec![sym1, sym2],
                    tuples,
                }
            }
            &Plan::MatchEA(e, ref a, sym1) => {
                let tuples = match global_arrangements.get_mut(a) {
                    None => panic!("attribute {:?} does not exist", a),
                    Some(named) => named
                        .import_named(&nested.parent, a)
                        .enter(nested)
                        .as_collection(|tuple, _| tuple.clone())
                        .filter(move |tuple| tuple[0] == Value::Eid(e))
                        .map(|tuple| vec![tuple[1].clone()]),
                };

                SimpleRelation {
                    symbols: vec![sym1],
                    tuples,
                }
            }
            &Plan::MatchAV(sym1, ref a, ref v) => {
                let tuples = match global_arrangements.get_mut(a) {
                    None => panic!("attribute {:?} does not exist", a),
                    Some(named) => {
                        let v = v.clone();
                        named
                            .import_named(&nested.parent, a)
                            .enter(nested)
                            .as_collection(|tuple, _| tuple.clone())
                            .filter(move |tuple| tuple[1] == v)
                            .map(|tuple| vec![tuple[0].clone()])
                    }
                };

                SimpleRelation {
                    symbols: vec![sym1],
                    tuples,
                }
            }
            &Plan::RuleExpr(ref syms, ref name) => match local_arrangements.get(name) {
                None => panic!("{:?} not in relation map", name),
                Some(named) => SimpleRelation {
                    symbols: syms.clone(),
                    tuples: named.map(|tuple| tuple.clone()),
                },
            },
            &Plan::NameExpr(ref syms, ref name) => match global_arrangements.get_mut(name) {
                None => panic!("{:?} not in query map", name),
                Some(named) => SimpleRelation {
                    symbols: syms.clone(),
                    tuples: named
                        .import_named(&nested.parent, name)
                        .enter(nested)
                        .as_collection(|tuple, _| tuple.clone()),
                },
            },
        }
    }
}
