//! Equijoin expression plan.

use timely::dataflow::scopes::child::Iterative;
use timely::dataflow::Scope;
use timely::order::Product;
use timely::progress::Timestamp;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::{Arrange, Arranged};
use differential_dataflow::operators::JoinCore;

use crate::binding::{AsBinding, Binding};
use crate::domain::Domain;
use crate::plan::{Dependencies, Implementable};
use crate::timestamp::Rewind;
use crate::{AsAid, Value, Var};
use crate::{
    AttributeBinding, CollectionRelation, Implemented, Relation, ShutdownHandle, TraceValHandle,
    VariableMap,
};

/// A plan stage joining two source relations on the specified
/// variables. Throws if any of the join variables isn't bound by both
/// sources.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub struct Join<P1: Implementable, P2: Implementable> {
    /// TODO
    pub variables: Vec<Var>,
    /// Plan for the left input.
    pub left_plan: Box<P1>,
    /// Plan for the right input.
    pub right_plan: Box<P2>,
}

fn attribute_attribute<'b, A, S>(
    nested: &mut Iterative<'b, S, u64>,
    domain: &mut Domain<A, Value, S::Timestamp>,
    target: Var,
    left: AttributeBinding<A>,
    right: AttributeBinding<A>,
) -> (Implemented<'b, A, S>, ShutdownHandle)
where
    A: AsAid,
    S: Scope,
    S::Timestamp: Timestamp + Lattice + Rewind,
{
    let mut variables = Vec::with_capacity(3);
    variables.push(target);

    let (left_arranged, shutdown_left) = {
        let (index, shutdown_button) = if target == left.variables.0 {
            variables.push(left.variables.1);
            domain
                .forward_propose(&left.source_attribute)
                .expect("forward propose trace does not exist")
                .import_frontier(
                    &nested.parent,
                    &format!("Propose({:?})", left.source_attribute),
                )
        } else if target == left.variables.1 {
            variables.push(left.variables.0);
            domain
                .reverse_propose(&left.source_attribute)
                .expect("reverse propose trace does not exist")
                .import_frontier(
                    &nested.parent,
                    &format!("_Propose({:?})", left.source_attribute),
                )
        } else {
            panic!("Unbound target variable in Attribute<->Attribute join.");
        };

        (index.enter(nested), shutdown_button)
    };

    let (right_arranged, shutdown_right) = {
        let (index, shutdown_button) = if target == right.variables.0 {
            variables.push(right.variables.1);
            domain
                .forward_propose(&right.source_attribute)
                .expect("forward propose trace does not exist")
                .import_frontier(
                    &nested.parent,
                    &format!("Propose({:?})", right.source_attribute),
                )
        } else if target == right.variables.1 {
            variables.push(right.variables.0);
            domain
                .reverse_propose(&right.source_attribute)
                .expect("reverse propose trace does not exist")
                .import_frontier(
                    &nested.parent,
                    &format!("_Propose({:?})", right.source_attribute),
                )
        } else {
            panic!("Unbound target variable in Attribute<->Attribute join.");
        };

        (index.enter(nested), shutdown_button)
    };

    let tuples = left_arranged.join_core(&right_arranged, move |key: &Value, v1, v2| {
        let mut out = Vec::with_capacity(3);
        out.push(key.clone());
        out.push(v1.clone());
        out.push(v2.clone());

        Some(out)
    });

    let mut shutdown_handle = ShutdownHandle::from_button(shutdown_left);
    shutdown_handle.add_button(shutdown_right);

    let relation = CollectionRelation { variables, tuples };

    (Implemented::Collection(relation), shutdown_handle)
}

fn collection_collection<'b, A, S>(
    nested: &mut Iterative<'b, S, u64>,
    domain: &mut Domain<A, Value, S::Timestamp>,
    target_variables: &[Var],
    left: CollectionRelation<'b, S>,
    right: CollectionRelation<'b, S>,
) -> (Implemented<'b, A, S>, ShutdownHandle)
where
    A: AsAid,
    S: Scope,
    S::Timestamp: Timestamp + Lattice + Rewind,
{
    let mut shutdown_handle = ShutdownHandle::empty();

    let variables = target_variables
        .iter()
        .cloned()
        .chain(
            left.variables()
                .drain(..)
                .filter(|x| !target_variables.contains(x)),
        )
        .chain(
            right
                .variables()
                .drain(..)
                .filter(|x| !target_variables.contains(x)),
        )
        .collect();

    let left_arranged: Arranged<
        Iterative<'b, S, u64>,
        TraceValHandle<Vec<Value>, Vec<Value>, Product<S::Timestamp, u64>, isize>,
    > = {
        let (arranged, shutdown) = left.tuples_by_variables(nested, domain, &target_variables);
        shutdown_handle.merge_with(shutdown);
        arranged.arrange()
    };

    let right_arranged: Arranged<
        Iterative<'b, S, u64>,
        TraceValHandle<Vec<Value>, Vec<Value>, Product<S::Timestamp, u64>, isize>,
    > = {
        let (arranged, shutdown) = right.tuples_by_variables(nested, domain, &target_variables);
        shutdown_handle.merge_with(shutdown);
        arranged.arrange()
    };

    let tuples = left_arranged.join_core(&right_arranged, |key: &Vec<Value>, v1, v2| {
        Some(
            key.iter()
                .cloned()
                .chain(v1.iter().cloned())
                .chain(v2.iter().cloned())
                .collect(),
        )
    });

    let relation = CollectionRelation { variables, tuples };

    (Implemented::Collection(relation), shutdown_handle)
}

fn collection_attribute<'b, A, S>(
    nested: &mut Iterative<'b, S, u64>,
    domain: &mut Domain<A, Value, S::Timestamp>,
    target_variables: &[Var],
    left: CollectionRelation<'b, S>,
    right: AttributeBinding<A>,
) -> (Implemented<'b, A, S>, ShutdownHandle)
where
    A: AsAid,
    S: Scope,
    S::Timestamp: Timestamp + Lattice + Rewind,
{
    // @TODO specialized implementation

    let (tuples, shutdown_propose) = match domain.forward_propose(&right.source_attribute) {
        None => panic!("attribute {:?} does not exist", &right.source_attribute),
        Some(propose_trace) => {
            let (propose, shutdown_propose) = propose_trace.import_frontier(
                &nested.parent,
                &format!("Propose({:?})", right.source_attribute),
            );

            let tuples = propose
                .enter(nested)
                .as_collection(|e, v| vec![e.clone(), v.clone()]);

            (tuples, shutdown_propose)
        }
    };

    let right_collected = CollectionRelation {
        variables: vec![right.variables.0, right.variables.1],
        tuples,
    };

    let (implemented, mut shutdown_handle) =
        collection_collection(nested, domain, target_variables, left, right_collected);

    shutdown_handle.add_button(shutdown_propose);

    (implemented, shutdown_handle)
}

//             Some(var) => {
//                 assert!(*var == self.variables.1);

//                 let (index, shutdown_button) = domain
//                     .forward_validate(&self.source_attribute)
//                     .unwrap()
//                     .import_core(&scope.parent, &self.source_attribute);

//                 let frontier = index.trace.advance_frontier().to_vec();
//                 let forwarded = index.enter_at(scope, move |_, _, time| {
//                     let mut forwarded = time.clone();
//                     forwarded.advance_by(&frontier);
//                     Product::new(forwarded, 0)
//                 });

//                 (forwarded, ShutdownHandle::from_button(shutdown_button))
//             }

//             Some(var) => {
//                 assert!(*var == self.variables.0);

//                 let (index, shutdown_button) = domain
//                     .reverse_validate(&self.source_attribute)
//                     .unwrap()
//                     .import_core(&scope.parent, &self.source_attribute);

//                 let frontier = index.trace.advance_frontier().to_vec();
//                 let forwarded = index.enter_at(scope, move |_, _, time| {
//                     let mut forwarded = time.clone();
//                     forwarded.advance_by(&frontier);
//                     Product::new(forwarded, 0)
//                 });

//                 (forwarded, ShutdownHandle::from_button(shutdown_button))
//             }

impl<P1: Implementable, P2: Implementable<A = P1::A>> Implementable for Join<P1, P2> {
    type A = P1::A;

    fn dependencies(&self) -> Dependencies<Self::A> {
        self.left_plan.dependencies() + self.right_plan.dependencies()
    }

    fn into_bindings(&self) -> Vec<Binding<Self::A, Value>> {
        let mut left_bindings = self.left_plan.into_bindings();
        let mut right_bindings = self.right_plan.into_bindings();

        let mut bindings = Vec::with_capacity(left_bindings.len() + right_bindings.len());
        bindings.append(&mut left_bindings);
        bindings.append(&mut right_bindings);

        bindings
    }

    fn implement<'b, S>(
        &self,
        nested: &mut Iterative<'b, S, u64>,
        domain: &mut Domain<Self::A, Value, S::Timestamp>,
        local_arrangements: &VariableMap<Self::A, Iterative<'b, S, u64>>,
    ) -> (Implemented<'b, Self::A, S>, ShutdownHandle)
    where
        S: Scope,
        S::Timestamp: Timestamp + Lattice + Rewind,
    {
        assert!(!self.variables.is_empty());

        let (left, shutdown_left) = self.left_plan.implement(nested, domain, local_arrangements);
        let (right, shutdown_right) = self
            .right_plan
            .implement(nested, domain, local_arrangements);

        let (implemented, mut shutdown_handle) = match left {
            Implemented::Attribute(left) => {
                match right {
                    Implemented::Attribute(right) => {
                        if self.variables.len() == 1 {
                            attribute_attribute(nested, domain, self.variables[0], left, right)
                        } else if self.variables.len() == 2 {
                            unimplemented!();
                        // intersect_attributes(domain, self.variables, left, right)
                        } else {
                            panic!(
                                "Attribute<->Attribute joins can't target more than two variables."
                            );
                        }
                    }
                    Implemented::Collection(right) => {
                        collection_attribute(nested, domain, &self.variables, right, left)
                    }
                }
            }
            Implemented::Collection(left) => match right {
                Implemented::Attribute(right) => {
                    collection_attribute(nested, domain, &self.variables, left, right)
                }
                Implemented::Collection(right) => {
                    collection_collection(nested, domain, &self.variables, left, right)
                }
            },
        };

        shutdown_handle.merge_with(shutdown_left);
        shutdown_handle.merge_with(shutdown_right);

        (implemented, shutdown_handle)
    }
}
