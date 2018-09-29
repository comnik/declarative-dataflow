//! Types and traits for implementing query plans.

use timely::dataflow::scopes::Child;
use timely::progress::timestamp::Timestamp;
use timely::communication::Allocate;
use timely::worker::Worker;

use differential_dataflow::lattice::Lattice;

use {ImplContext, RelationMap, QueryMap, SimpleRelation, Relation};
use {Value, Var, Entity, Attribute};

pub mod filter;
pub mod aggregate;
pub mod project;
pub mod union;
pub mod join;
pub mod antijoin;

pub use self::filter::Filter;
pub use self::aggregate::Aggregate;
pub use self::project::Project;
pub use self::union::Union;
pub use self::join::Join;
pub use self::antijoin::Antijoin;

/// A type that can be implemented as a simple relation.
pub trait Implementable {
    /// Implements the type as a simple relation.
    fn implement<'a, 'b, A: Allocate, T: Timestamp+Lattice>(
        &self,
        db: &ImplContext<Child<'a, Worker<A>, T>>,
        nested: &mut Child<'b, Child<'a, Worker<A>, T>, u64>,
        relation_map: &RelationMap<'b, Child<'a, Worker<A>, T>>,
        queries: &mut QueryMap<T, isize>
    )
    -> SimpleRelation<'b, Child<'a, Worker<A>, T>>;
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
    Join(Join<Plan,Plan>),
    /// Antijoin
    Antijoin(Antijoin<Plan,Plan>),
    /// Negation
    Negate(Box<Plan>),
    /// Filters bindings by one of the built-in predicates
    Filter(Filter<Plan>),
    /// Data pattern of the form [e a ?v]
    MatchEA(Entity, Attribute, Var),
    /// Data pattern of the form [e ?a ?v]
    MatchE(Entity, Var, Var),
    /// Data pattern of the form [?e a ?v]
    MatchA(Var, Attribute, Var),
    /// Data pattern of the form [?e a v]
    MatchAV(Var, Attribute, Value),
    /// Sources data from a query-local relation
    RuleExpr(String, Vec<Var>),
    /// Sources data from a published relation
    NameExpr(String, Vec<Var>),
}


impl Implementable for Plan {
    fn implement<'a, 'b, A: Allocate, T: Timestamp+Lattice>(
        &self,
        db: &ImplContext<Child<'a, Worker<A>, T>>,
        nested: &mut Child<'b, Child<'a, Worker<A>, T>, u64>,
        relation_map: &RelationMap<'b, Child<'a, Worker<A>, T>>,
        queries: &mut QueryMap<T, isize>
    )
    -> SimpleRelation<'b, Child<'a, Worker<A>, T>> {

        use timely::dataflow::operators::ToStream;
        use differential_dataflow::AsCollection;
        use differential_dataflow::operators::arrange::ArrangeBySelf;
        use differential_dataflow::operators::JoinCore;

        match self {
            &Plan::Project(ref projection)  => projection.implement(db, nested, relation_map, queries),
            &Plan::Aggregate(ref aggregate) => aggregate.implement(db, nested, relation_map, queries),
            &Plan::Union(ref union)         => union.implement(db, nested, relation_map, queries),
            // @TODO specialized join for join on single variable
            &Plan::Join(ref join)           => join.implement(db, nested, relation_map, queries),
            &Plan::Antijoin(ref antijoin)   => antijoin.implement(db, nested, relation_map, queries),
            &Plan::Negate(ref plan)         => {
                let mut rel = plan.implement(db, nested, relation_map, queries);
                SimpleRelation {
                    symbols: rel.symbols().to_vec(),
                    tuples: rel.tuples().negate()
                }
            },
            &Plan::Filter(ref filter)       => filter.implement(db, nested, relation_map, queries),
            &Plan::MatchEA(e, a, sym1)      => {
                let tuple = ((e, a), Default::default(), 1);
                let ea_in = Some(tuple).to_stream(nested).as_collection().arrange_by_self();
                let tuples = db.ea_v.enter(nested)
                    .join_core(&ea_in, |_,tuple,()| Some(vec![tuple.clone()]));

                SimpleRelation { symbols: vec![sym1], tuples }
            },
            &Plan::MatchE(e, sym1, sym2)    => {
                let tuple = (e, Default::default(), 1);
                let e_in = Some(tuple).to_stream(nested).as_collection().arrange_by_self();
                let tuples = db.e_av.enter(nested)
                    .join_core(&e_in, |_,(a,v),()| Some(vec![Value::Attribute(a.clone()), v.clone()]));

                SimpleRelation { symbols: vec![sym1, sym2], tuples }
            },
            &Plan::MatchA(sym1, a, sym2)    => {
                let tuple = (a, Default::default(), 1);
                let a_in = Some(tuple).to_stream(nested).as_collection().arrange_by_self();
                let tuples = db.a_ev.enter(nested)
                    .join_core(&a_in, |_,(e,v),()| Some(vec![Value::Eid(e.clone()), v.clone()]));

                SimpleRelation { symbols: vec![sym1, sym2], tuples }
            },
            &Plan::MatchAV(sym1, a, ref v)  => {
                let tuple = ((a, v.clone()), Default::default(), 1);
                let av_in = Some(tuple).to_stream(nested).as_collection().arrange_by_self();
                let tuples = db.av_e.enter(nested)
                    .join_core(&av_in, |_,e,_| Some(vec![Value::Eid(e.clone())]));

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
