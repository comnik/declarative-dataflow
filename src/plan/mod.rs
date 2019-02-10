//! Types and traits for implementing query plans.

use std::ops::Deref;
use std::sync::atomic::{self, AtomicUsize};

use timely::dataflow::scopes::child::Iterative;
use timely::dataflow::Scope;

use crate::binding::{AttributeBinding, Binding, ConstantBinding};
use crate::Rule;
use crate::{Aid, Eid, Value, Var};
use crate::{CollectionIndex, CollectionRelation, Relation, RelationHandle, VariableMap};

pub mod aggregate_neu;
pub mod antijoin;
pub mod filter;
pub mod hector;
pub mod join;
pub mod project;
pub mod pull;
pub mod transform;
pub mod union;

#[cfg(feature = "set-semantics")]
pub use self::aggregate::{Aggregate, AggregationFn};
#[cfg(not(feature = "set-semantics"))]
pub use self::aggregate_neu::{Aggregate, AggregationFn};
pub use self::antijoin::Antijoin;
pub use self::filter::{Filter, Predicate};
pub use self::hector::Hector;
pub use self::join::Join;
pub use self::project::Project;
pub use self::pull::{Pull, PullLevel};
pub use self::transform::{Function, Transform};
pub use self::union::Union;

static ID: AtomicUsize = atomic::ATOMIC_USIZE_INIT;
static SYM: AtomicUsize = atomic::ATOMIC_USIZE_INIT;

/// @FIXME
pub fn next_id() -> Eid {
    ID.fetch_add(1, atomic::Ordering::SeqCst) as Eid
}

/// @FIXME
pub fn gensym() -> Var {
    SYM.fetch_sub(1, atomic::Ordering::SeqCst) as Var
}

/// A thing that can provide global state required during the
/// implementation of plans.
pub trait ImplContext {
    /// Returns the set of constraints associated with a rule.
    fn rule(&self, name: &str) -> Option<&Rule>;

    /// Returns a mutable reference to a (non-base) relation, if one
    /// is registered under the given name.
    fn global_arrangement(&mut self, name: &str) -> Option<&mut RelationHandle>;

    /// Returns a mutable reference to an attribute (a base relation)
    /// arranged from eid -> value, if one is registered under the
    /// given name.
    fn forward_index(&mut self, name: &str) -> Option<&mut CollectionIndex<Value, Value, u64>>;

    /// Returns a mutable reference to an attribute (a base relation)
    /// arranged from value -> eid, if one is registered under the
    /// given name.
    fn reverse_index(&mut self, name: &str) -> Option<&mut CollectionIndex<Value, Value, u64>>;

    /// Returns the current opinion as to whether this rule is
    /// underconstrained. Underconstrained rules cannot be safely
    /// materialized and re-used on their own (i.e. without more
    /// specific constraints).
    fn is_underconstrained(&self, name: &str) -> bool;
}

/// A type that can be implemented as a simple relation.
pub trait Implementable {
    /// Returns names of any other implementable things that need to
    /// be available before implementing this one. Attributes are not
    /// mentioned explicitley as dependencies.
    fn dependencies(&self) -> Vec<String>;

    /// Transforms an implementable into an equivalent set of bindings
    /// that can be unified by Hector.
    fn into_bindings(&self) -> Vec<Binding> {
        panic!("This plan can't be implemented via Hector.");
    }

    /// @TODO
    fn datafy(&self) -> Vec<(Eid, Aid, Value)> {
        Vec::new()
    }

    /// Implements the type as a simple relation.
    fn implement<'b, S: Scope<Timestamp = u64>, I: ImplContext>(
        &self,
        nested: &mut Iterative<'b, S, u64>,
        local_arrangements: &VariableMap<Iterative<'b, S, u64>>,
        context: &mut I,
    ) -> CollectionRelation<'b, S>;
}

/// Possible query plan types.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
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
    /// Data pattern of the form [?e a ?v]
    MatchA(Var, Aid, Var),
    /// Data pattern of the form [e a ?v]
    MatchEA(Eid, Aid, Var),
    /// Data pattern of the form [?e a v]
    MatchAV(Var, Aid, Value),
    /// Sources data from another relation.
    NameExpr(Vec<Var>, String),
    /// Pull expression
    Pull(Pull<Plan>),
    /// Single-level pull expression
    PullLevel(PullLevel<Plan>),
}

impl Plan {
    /// Returns the symbols bound by this plan.
    pub fn variables(&self) -> Vec<Var> {
        match *self {
            Plan::Project(ref projection) => projection.variables.clone(),
            Plan::Aggregate(ref aggregate) => aggregate.variables.clone(),
            Plan::Union(ref union) => union.variables.clone(),
            Plan::Join(ref join) => join.variables.clone(),
            Plan::Hector(ref hector) => hector.variables.clone(),
            Plan::Antijoin(ref antijoin) => antijoin.variables.clone(),
            Plan::Negate(ref plan) => plan.variables(),
            Plan::Filter(ref filter) => filter.variables.clone(),
            Plan::Transform(ref transform) => transform.variables.clone(),
            Plan::MatchA(e, _, v) => vec![e, v],
            Plan::MatchEA(_, _, v) => vec![v],
            Plan::MatchAV(e, _, _) => vec![e],
            Plan::NameExpr(ref variables, ref _name) => variables.clone(),
            Plan::Pull(ref pull) => pull.variables.clone(),
            Plan::PullLevel(ref path) => path.variables.clone(),
        }
    }
}

impl Implementable for Plan {
    fn dependencies(&self) -> Vec<String> {
        // @TODO provide a general fold for plans
        match *self {
            Plan::Project(ref projection) => projection.dependencies(),
            Plan::Aggregate(ref aggregate) => aggregate.dependencies(),
            Plan::Union(ref union) => union.dependencies(),
            Plan::Join(ref join) => join.dependencies(),
            Plan::Hector(ref hector) => hector.dependencies(),
            Plan::Antijoin(ref antijoin) => antijoin.dependencies(),
            Plan::Negate(ref plan) => plan.dependencies(),
            Plan::Filter(ref filter) => filter.dependencies(),
            Plan::Transform(ref transform) => transform.dependencies(),
            Plan::MatchA(_, _, _) => Vec::new(),
            Plan::MatchEA(_, _, _) => Vec::new(),
            Plan::MatchAV(_, _, _) => Vec::new(),
            Plan::NameExpr(_, ref name) => vec![name.to_string()],
            Plan::Pull(ref pull) => pull.dependencies(),
            Plan::PullLevel(ref path) => path.dependencies(),
        }
    }

    fn into_bindings(&self) -> Vec<Binding> {
        // @TODO provide a general fold for plans
        match *self {
            Plan::Project(ref projection) => projection.into_bindings(),
            Plan::Aggregate(ref aggregate) => aggregate.into_bindings(),
            Plan::Union(ref union) => union.into_bindings(),
            Plan::Join(ref join) => join.into_bindings(),
            Plan::Hector(ref hector) => hector.into_bindings(),
            Plan::Antijoin(ref antijoin) => antijoin.into_bindings(),
            Plan::Negate(ref plan) => plan.into_bindings(),
            Plan::Filter(ref filter) => filter.into_bindings(),
            Plan::Transform(ref transform) => transform.into_bindings(),
            Plan::MatchA(e, ref a, v) => vec![Binding::Attribute(AttributeBinding {
                symbols: (e, v),
                source_attribute: a.to_string(),
            })],
            Plan::MatchEA(match_e, ref a, v) => {
                let e = gensym();
                vec![
                    Binding::Attribute(AttributeBinding {
                        symbols: (e, v),
                        source_attribute: a.to_string(),
                    }),
                    Binding::Constant(ConstantBinding {
                        symbol: e,
                        value: Value::Eid(match_e),
                    }),
                ]
            }
            Plan::MatchAV(e, ref a, ref match_v) => {
                let v = gensym();
                vec![
                    Binding::Attribute(AttributeBinding {
                        symbols: (e, v),
                        source_attribute: a.to_string(),
                    }),
                    Binding::Constant(ConstantBinding {
                        symbol: v,
                        value: match_v.clone(),
                    }),
                ]
            }
            Plan::NameExpr(_, ref _name) => unimplemented!(), // @TODO hmm...
            Plan::Pull(ref pull) => pull.into_bindings(),
            Plan::PullLevel(ref path) => path.into_bindings(),
        }
    }

    fn datafy(&self) -> Vec<(Eid, Aid, Value)> {
        // @TODO provide a general fold for plans
        match *self {
            Plan::Project(ref projection) => projection.datafy(),
            Plan::Aggregate(ref aggregate) => aggregate.datafy(),
            Plan::Union(ref union) => union.datafy(),
            Plan::Join(ref join) => join.datafy(),
            Plan::Hector(ref hector) => hector.datafy(),
            Plan::Antijoin(ref antijoin) => antijoin.datafy(),
            Plan::Negate(ref plan) => plan.datafy(),
            Plan::Filter(ref filter) => filter.datafy(),
            Plan::Transform(ref transform) => transform.datafy(),
            Plan::MatchA(_e, ref a, _v) => vec![(
                next_id(),
                "df.pattern/a".to_string(),
                Value::Aid(a.to_string()),
            )],
            Plan::MatchEA(e, ref a, _) => vec![
                (next_id(), "df.pattern/e".to_string(), Value::Eid(e)),
                (
                    next_id(),
                    "df.pattern/a".to_string(),
                    Value::Aid(a.to_string()),
                ),
            ],
            Plan::MatchAV(_, ref a, ref v) => vec![
                (
                    next_id(),
                    "df.pattern/a".to_string(),
                    Value::Aid(a.to_string()),
                ),
                (next_id(), "df.pattern/v".to_string(), v.clone()),
            ],
            Plan::NameExpr(_, ref _name) => Vec::new(),
            Plan::Pull(ref pull) => pull.datafy(),
            Plan::PullLevel(ref path) => path.datafy(),
        }
    }

    fn implement<'b, S: Scope<Timestamp = u64>, I: ImplContext>(
        &self,
        nested: &mut Iterative<'b, S, u64>,
        local_arrangements: &VariableMap<Iterative<'b, S, u64>>,
        context: &mut I,
    ) -> CollectionRelation<'b, S> {
        match *self {
            Plan::Project(ref projection) => {
                projection.implement(nested, local_arrangements, context)
            }
            Plan::Aggregate(ref aggregate) => {
                aggregate.implement(nested, local_arrangements, context)
            }
            Plan::Union(ref union) => union.implement(nested, local_arrangements, context),
            Plan::Join(ref join) => join.implement(nested, local_arrangements, context),
            Plan::Hector(ref hector) => hector.implement(nested, local_arrangements, context),
            Plan::Antijoin(ref antijoin) => antijoin.implement(nested, local_arrangements, context),
            Plan::Negate(ref plan) => {
                let rel = plan.implement(nested, local_arrangements, context);
                CollectionRelation {
                    symbols: rel.symbols().to_vec(),
                    tuples: rel.tuples().negate(),
                }
            }
            Plan::Filter(ref filter) => filter.implement(nested, local_arrangements, context),
            Plan::Transform(ref transform) => {
                transform.implement(nested, local_arrangements, context)
            }
            Plan::MatchA(sym1, ref a, sym2) => {
                let tuples = match context.forward_index(a) {
                    None => panic!("attribute {:?} does not exist", a),
                    Some(index) => index
                        .validate_trace
                        .import_named(&nested.parent, a)
                        .enter(nested)
                        .as_collection(|(e, v), _| vec![e.clone(), v.clone()]),
                };

                CollectionRelation {
                    symbols: vec![sym1, sym2],
                    tuples,
                }
            }
            Plan::MatchEA(match_e, ref a, sym1) => {
                let tuples = match context.forward_index(a) {
                    None => panic!("attribute {:?} does not exist", a),
                    Some(index) => index
                        .propose_trace
                        .import_named(&nested.parent, a)
                        .enter(nested)
                        .filter(move |e, _v| *e == Value::Eid(match_e))
                        .as_collection(|_e, v| vec![v.clone()]),
                };

                CollectionRelation {
                    symbols: vec![sym1],
                    tuples,
                }
            }
            Plan::MatchAV(sym1, ref a, ref match_v) => {
                let tuples = match context.reverse_index(a) {
                    None => panic!("attribute {:?} does not exist", a),
                    Some(index) => {
                        let match_v = match_v.clone();
                        index
                            .propose_trace
                            .import_named(&nested.parent, a)
                            .enter(nested)
                            .filter(move |v, _e| *v == match_v)
                            .as_collection(|_v, e| vec![e.clone()])
                    }
                };

                CollectionRelation {
                    symbols: vec![sym1],
                    tuples,
                }
            }
            Plan::NameExpr(ref syms, ref name) => {
                if context.is_underconstrained(name) {
                    match local_arrangements.get(name) {
                        None => panic!("{:?} not in relation map", name),
                        Some(named) => CollectionRelation {
                            symbols: syms.clone(),
                            tuples: named.deref().clone(), // @TODO re-use variable directly?
                        },
                    }
                } else {
                    // If a rule is not underconstrained, we can
                    // safely re-use it. @TODO it's debatable whether
                    // we should then immediately assume that it is
                    // available as a global arrangement, but we'll do
                    // so for now.

                    match context.global_arrangement(name) {
                        None => panic!("{:?} not in query map", name),
                        Some(named) => CollectionRelation {
                            symbols: syms.clone(),
                            tuples: named
                                .import_named(&nested.parent, name)
                                .enter(nested)
                                // @TODO this destroys all the arrangement re-use
                                .as_collection(|tuple, _| tuple.clone()),
                        },
                    }
                }
            }
            Plan::Pull(ref pull) => pull.implement(nested, local_arrangements, context),
            Plan::PullLevel(ref path) => path.implement(nested, local_arrangements, context),
        }
    }
}
