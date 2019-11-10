//! Antijoin expression plan.

use timely::dataflow::scopes::child::Iterative;
use timely::dataflow::Scope;
use timely::progress::Timestamp;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::{Join, Threshold};

use crate::binding::{AsBinding, Binding};
use crate::domain::Domain;
use crate::plan::{Dependencies, Implementable};
use crate::timestamp::Rewind;
use crate::{CollectionRelation, Implemented, Relation, ShutdownHandle, Var, VariableMap};

/// A plan stage anti-joining both its sources on the specified
/// variables. Throws if the sources are not union-compatible, i.e. bind
/// all of the same variables in the same order.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub struct Antijoin<P1: Implementable, P2: Implementable> {
    /// TODO
    pub variables: Vec<Var>,
    /// Plan for the left input.
    pub left_plan: Box<P1>,
    /// Plan for the right input.
    pub right_plan: Box<P2>,
}

impl<P1: Implementable, P2: Implementable<A = P1::A>> Implementable for Antijoin<P1, P2> {
    type A = P1::A;

    fn dependencies(&self) -> Dependencies<Self::A> {
        self.left_plan.dependencies() + self.right_plan.dependencies()
    }

    fn into_bindings(&self) -> Vec<Binding<Self::A>> {
        unimplemented!();
        // let mut left_bindings = self.left_plan.into_bindings();
        // let mut right_bindings = self.right_plan.into_bindings();

        // let mut bindings = Vec::with_capacity(left_bindings.len() + right_bindings.len());
        // bindings.append(&mut left_bindings);
        // bindings.append(&mut right_bindings);

        // bindings
    }

    fn implement<'b, S>(
        &self,
        nested: &mut Iterative<'b, S, u64>,
        domain: &mut Domain<Self::A, S::Timestamp>,
        local_arrangements: &VariableMap<Self::A, Iterative<'b, S, u64>>,
    ) -> (Implemented<'b, Self::A, S>, ShutdownHandle)
    where
        S: Scope,
        S::Timestamp: Timestamp + Lattice + Rewind,
    {
        let mut shutdown_handle = ShutdownHandle::empty();
        let left = {
            let (left, shutdown) = self.left_plan.implement(nested, domain, local_arrangements);
            shutdown_handle.merge_with(shutdown);
            left
        };
        let right = {
            let (right, shutdown) = self
                .right_plan
                .implement(nested, domain, local_arrangements);
            shutdown_handle.merge_with(shutdown);
            right
        };

        let variables = self
            .variables
            .iter()
            .cloned()
            .chain(
                left.variables()
                    .drain(..)
                    .filter(|x| !self.variables.contains(x)),
            )
            .collect();

        let right_projected = {
            let (projected, shutdown) = right.projected(nested, domain, &self.variables);
            shutdown_handle.merge_with(shutdown);
            projected
        };

        let left_arranged = {
            let (arranged, shutdown) = left.tuples_by_variables(nested, domain, &self.variables);
            shutdown_handle.merge_with(shutdown);
            arranged
        };

        let tuples = left_arranged
            .distinct()
            .antijoin(&right_projected.distinct())
            .map(|(key, tuple)| key.iter().cloned().chain(tuple.iter().cloned()).collect());

        let relation = CollectionRelation { variables, tuples };

        (Implemented::Collection(relation), shutdown_handle)
    }
}
