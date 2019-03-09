//! Antijoin expression plan.

use timely::dataflow::scopes::child::Iterative;
use timely::dataflow::Scope;
use timely::order::TotalOrder;
use timely::progress::Timestamp;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::{Join, Threshold};

use crate::binding::Binding;
use crate::plan::{Dependencies, ImplContext, Implementable};
use crate::{CollectionRelation, Relation, ShutdownHandle, Var, VariableMap};

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

impl<P1: Implementable, P2: Implementable> Implementable for Antijoin<P1, P2> {
    fn dependencies(&self) -> Dependencies {
        Dependencies::merge(
            self.left_plan.dependencies(),
            self.right_plan.dependencies(),
        )
    }

    fn into_bindings(&self) -> Vec<Binding> {
        unimplemented!();
        // let mut left_bindings = self.left_plan.into_bindings();
        // let mut right_bindings = self.right_plan.into_bindings();

        // let mut bindings = Vec::with_capacity(left_bindings.len() + right_bindings.len());
        // bindings.append(&mut left_bindings);
        // bindings.append(&mut right_bindings);

        // bindings
    }

    fn implement<'b, T, I, S>(
        &self,
        nested: &mut Iterative<'b, S, u64>,
        local_arrangements: &VariableMap<Iterative<'b, S, u64>>,
        context: &mut I,
    ) -> (CollectionRelation<'b, S>, ShutdownHandle)
    where
        T: Timestamp + Lattice + TotalOrder,
        I: ImplContext<T>,
        S: Scope<Timestamp = T>,
    {
        let (left, shutdown_left) = self
            .left_plan
            .implement(nested, local_arrangements, context);
        let (right, shutdown_right) =
            self.right_plan
                .implement(nested, local_arrangements, context);

        let variables = self
            .variables
            .iter()
            .cloned()
            .chain(
                left.variables()
                    .iter()
                    .filter(|x| !self.variables.contains(x))
                    .cloned(),
            )
            .collect();

        let tuples = left
            .tuples_by_variables(&self.variables)
            .distinct()
            .antijoin(
                &right
                    .tuples_by_variables(&self.variables)
                    .map(|(key, _)| key)
                    .distinct(),
            )
            .map(|(key, tuple)| key.iter().cloned().chain(tuple.iter().cloned()).collect());

        let shutdown_handle = ShutdownHandle::merge(shutdown_left, shutdown_right);

        (CollectionRelation { variables, tuples }, shutdown_handle)
    }
}
