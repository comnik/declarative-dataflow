//! Types and traits for implementing query plans.

use std::collections::HashSet;
use std::ops::Deref;
use std::sync::atomic::{self, AtomicUsize};

use timely::dataflow::scopes::child::Iterative;
use timely::dataflow::Scope;
use timely::progress::Timestamp;

use differential_dataflow::lattice::Lattice;

use crate::binding::{AsBinding, AttributeBinding, Binding};
use crate::domain::Domain;
use crate::timestamp::Rewind;
use crate::{AsAid, Eid, Value, Var};
use crate::{CollectionRelation, Implemented, Relation, ShutdownHandle, VariableMap};

#[cfg(feature = "set-semantics")]
pub mod aggregate;
#[cfg(not(feature = "set-semantics"))]
pub mod aggregate_neu;
pub mod antijoin;
pub mod filter;
#[cfg(feature = "graphql")]
pub mod graphql;
#[cfg(feature = "graphql")]
pub mod graphql_v2;
pub mod hector;
pub mod join;
pub mod project;
pub mod pull;
pub mod pull_v2;
pub mod transform;
pub mod union;

#[cfg(feature = "set-semantics")]
pub use self::aggregate::{Aggregate, AggregationFn};
#[cfg(not(feature = "set-semantics"))]
pub use self::aggregate_neu::{Aggregate, AggregationFn};
pub use self::antijoin::Antijoin;
pub use self::filter::{Filter, Predicate};
#[cfg(feature = "graphql")]
pub use self::graphql::GraphQl;
pub use self::hector::Hector;
pub use self::join::Join;
pub use self::project::Project;
pub use self::pull::{Pull, PullAll, PullLevel};
pub use self::transform::{Function, Transform};
pub use self::union::Union;

static SYM: AtomicUsize = AtomicUsize::new(std::usize::MAX);

/// @FIXME
pub fn gensym() -> Var {
    SYM.fetch_sub(1, atomic::Ordering::SeqCst) as Var
}

/// Description of everything a plan needs prior to synthesis.
pub struct Dependencies<A: AsAid> {
    /// NameExpr's used by this plan.
    pub names: HashSet<A>,
    /// Attributes queries in Match* expressions.
    pub attributes: HashSet<A>,
}

impl<A: AsAid> Dependencies<A> {
    /// A description representing a dependency on nothing.
    pub fn none() -> Self {
        Dependencies {
            names: HashSet::new(),
            attributes: HashSet::new(),
        }
    }

    /// A description representing a dependency on a single name.
    pub fn name(name: A) -> Self {
        let mut names = HashSet::new();
        names.insert(name);

        Dependencies {
            names,
            attributes: HashSet::new(),
        }
    }

    /// A description representing a dependency on a single attribute.
    pub fn attribute(aid: A) -> Self {
        let mut attributes = HashSet::new();
        attributes.insert(aid);

        Dependencies {
            names: HashSet::new(),
            attributes,
        }
    }
}

impl<A: AsAid> std::ops::AddAssign for Dependencies<A> {
    fn add_assign(&mut self, other: Self) {
        // Merges two dependency descriptions into one, representing
        // their union.
        self.names.extend(other.names.into_iter());
        self.attributes.extend(other.attributes.into_iter());
    }
}

impl<A: AsAid> std::ops::Add for Dependencies<A> {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        let mut merged = self;
        merged += other;
        merged
    }
}

impl<A: AsAid> std::iter::Sum<Dependencies<A>> for Dependencies<A> {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.fold(Self::none(), |sum, x| sum + x)
    }
}

/// A type that can be implemented as a simple relation.
pub trait Implementable {
    /// The type that is used to represent attributes uniquely.
    type A: AsAid;

    /// Returns names of any other implementable things that need to
    /// be available before implementing this one. Attributes are not
    /// mentioned explicitley as dependencies.
    fn dependencies(&self) -> Dependencies<Self::A>;

    /// Transforms an implementable into an equivalent set of bindings
    /// that can be unified by Hector.
    fn into_bindings(&self) -> Vec<Binding<Self::A>> {
        panic!("This plan can't be implemented via Hector.");
    }

    /// Implements the type as a simple relation.
    fn implement<'b, S>(
        &self,
        nested: &mut Iterative<'b, S, u64>,
        domain: &mut Domain<Self::A, S::Timestamp>,
        local_arrangements: &VariableMap<Self::A, Iterative<'b, S, u64>>,
    ) -> (Implemented<'b, Self::A, S>, ShutdownHandle)
    where
        S: Scope,
        S::Timestamp: Timestamp + Lattice + Rewind;
}

/// Possible query plan types.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub enum Plan<A: AsAid> {
    /// Projection
    Project(Project<Plan<A>>),
    /// Aggregation
    Aggregate(Aggregate<Plan<A>>),
    /// Union
    Union(Union<Plan<A>>),
    /// Equijoin
    Join(Join<Plan<A>, Plan<A>>),
    /// WCO
    Hector(Hector<A>),
    /// Antijoin
    Antijoin(Antijoin<Plan<A>, Plan<A>>),
    /// Negation
    Negate(Box<Plan<A>>),
    /// Filters bindings by one of the built-in predicates
    Filter(Filter<Plan<A>>),
    /// Transforms a binding by a function expression
    Transform(Transform<Plan<A>>),
    /// Data pattern of the form [?e a ?v]
    MatchA(Var, A, Var),
    /// Data pattern of the form [e a ?v]
    MatchEA(Eid, A, Var),
    /// Data pattern of the form [?e a v]
    MatchAV(Var, A, Value),
    /// Sources data from another relation.
    NameExpr(Vec<Var>, A),
    /// Pull expression
    Pull(Pull<Plan<A>>),
    /// Single-level pull expression
    PullLevel(PullLevel<A, Plan<A>>),
    /// Single-level pull expression
    PullAll(PullAll<A>),
    // /// GraphQl pull expression
    // #[cfg(feature = "graphql")]
    // GraphQl(GraphQl),
}

impl<A: AsAid> Plan<A> {
    /// Returns the variables bound by this plan.
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
            Plan::PullAll(ref path) => path.variables.clone(),
            // #[cfg(feature = "graphql")]
            // Plan::GraphQl(_) => unimplemented!(),
        }
    }
}

impl<A> Implementable for Plan<A>
where
    A: AsAid,
{
    type A = A;

    fn dependencies(&self) -> Dependencies<A> {
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
            Plan::MatchA(_, ref a, _) => Dependencies::attribute(a.clone()),
            Plan::MatchEA(_, ref a, _) => Dependencies::attribute(a.clone()),
            Plan::MatchAV(_, ref a, _) => Dependencies::attribute(a.clone()),
            Plan::NameExpr(_, ref name) => Dependencies::name(name.clone()),
            Plan::Pull(ref pull) => pull.dependencies(),
            Plan::PullLevel(ref path) => path.dependencies(),
            Plan::PullAll(ref path) => path.dependencies(),
            // #[cfg(feature = "graphql")]
            // Plan::GraphQl(ref q) => q.dependencies(),
        }
    }

    fn into_bindings(&self) -> Vec<Binding<Self::A>> {
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
            Plan::MatchA(e, ref a, v) => vec![Binding::attribute(e, a.clone(), v)],
            Plan::MatchEA(match_e, ref a, v) => {
                let e = gensym();
                vec![
                    Binding::attribute(e, a.clone(), v),
                    Binding::constant(e, Value::Eid(match_e)),
                ]
            }
            Plan::MatchAV(e, ref a, ref match_v) => {
                let v = gensym();
                vec![
                    Binding::attribute(e, a.clone(), v),
                    Binding::constant(v, match_v.clone()),
                ]
            }
            Plan::NameExpr(_, ref _name) => unimplemented!(), // @TODO hmm...
            Plan::Pull(ref pull) => pull.into_bindings(),
            Plan::PullLevel(ref path) => path.into_bindings(),
            Plan::PullAll(ref path) => path.into_bindings(),
            // #[cfg(feature = "graphql")]
            // Plan::GraphQl(ref q) => q.into_bindings(),
        }
    }

    fn implement<'b, S>(
        &self,
        nested: &mut Iterative<'b, S, u64>,
        domain: &mut Domain<A, S::Timestamp>,
        local_arrangements: &VariableMap<Self::A, Iterative<'b, S, u64>>,
    ) -> (Implemented<'b, Self::A, S>, ShutdownHandle)
    where
        S: Scope,
        S::Timestamp: Timestamp + Lattice + Rewind,
    {
        match *self {
            Plan::Project(ref projection) => {
                projection.implement(nested, domain, local_arrangements)
            }
            Plan::Aggregate(ref aggregate) => {
                aggregate.implement(nested, domain, local_arrangements)
            }
            Plan::Union(ref union) => union.implement(nested, domain, local_arrangements),
            Plan::Join(ref join) => join.implement(nested, domain, local_arrangements),
            Plan::Hector(ref hector) => hector.implement(nested, domain, local_arrangements),
            Plan::Antijoin(ref antijoin) => antijoin.implement(nested, domain, local_arrangements),
            Plan::Negate(ref plan) => {
                let (relation, mut shutdown_handle) =
                    plan.implement(nested, domain, local_arrangements);
                let variables = relation.variables();

                let tuples = {
                    let (projected, shutdown) = relation.projected(nested, domain, &variables);
                    shutdown_handle.merge_with(shutdown);

                    projected.negate()
                };

                (
                    Implemented::Collection(CollectionRelation { variables, tuples }),
                    shutdown_handle,
                )
            }
            Plan::Filter(ref filter) => filter.implement(nested, domain, local_arrangements),
            Plan::Transform(ref transform) => {
                transform.implement(nested, domain, local_arrangements)
            }
            Plan::MatchA(e, ref a, v) => {
                let binding = AttributeBinding {
                    variables: (e, v),
                    source_attribute: a.clone(),
                };

                (Implemented::Attribute(binding), ShutdownHandle::empty())
            }
            Plan::MatchEA(match_e, ref a, sym1) => {
                let (tuples, shutdown_propose) = match domain.forward_propose(a) {
                    None => panic!("attribute {:?} does not exist", a),
                    Some(propose_trace) => {
                        let (propose, shutdown_propose) = propose_trace
                            .import_frontier(&nested.parent, &format!("Propose({:?})", a));

                        let tuples = propose
                            .enter(nested)
                            .filter(move |e, _v| *e == Value::Eid(match_e))
                            .as_collection(|_e, v| vec![v.clone()]);

                        (tuples, shutdown_propose)
                    }
                };

                let relation = CollectionRelation {
                    variables: vec![sym1],
                    tuples,
                };

                (
                    Implemented::Collection(relation),
                    ShutdownHandle::from_button(shutdown_propose),
                )
            }
            Plan::MatchAV(sym1, ref a, ref match_v) => {
                let (tuples, shutdown_propose) = match domain.forward_propose(a) {
                    None => panic!("attribute {:?} does not exist", a),
                    Some(propose_trace) => {
                        let match_v = match_v.clone();
                        let (propose, shutdown_propose) = propose_trace
                            .import_frontier(&nested.parent, &format!("Propose({:?})", a));

                        let tuples = propose
                            .enter(nested)
                            .filter(move |_e, v| *v == match_v)
                            .as_collection(|e, _v| vec![e.clone()]);

                        (tuples, shutdown_propose)
                    }
                };

                let relation = CollectionRelation {
                    variables: vec![sym1],
                    tuples,
                };

                (
                    Implemented::Collection(relation),
                    ShutdownHandle::from_button(shutdown_propose),
                )
            }
            Plan::NameExpr(ref syms, ref name) => {
                match local_arrangements.get(name) {
                    None => panic!("{:?} not in relation map", name),
                    Some(named) => {
                        let relation = CollectionRelation {
                            variables: syms.clone(),
                            tuples: named.deref().clone(), // @TODO re-use variable directly?
                        };

                        (Implemented::Collection(relation), ShutdownHandle::empty())
                    }
                }
            }
            Plan::Pull(ref pull) => pull.implement(nested, domain, local_arrangements),
            Plan::PullLevel(ref path) => path.implement(nested, domain, local_arrangements),
            Plan::PullAll(ref path) => path.implement(nested, domain, local_arrangements),
            // #[cfg(feature = "graphql")]
            // Plan::GraphQl(ref query) => query.implement(nested, domain, local_arrangements),
        }
    }
}
