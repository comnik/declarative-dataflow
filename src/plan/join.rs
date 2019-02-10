//! Equijoin expression plan.

use timely::dataflow::scopes::child::Iterative;
use timely::dataflow::Scope;
use timely::order::TotalOrder;
use timely::progress::Timestamp;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::JoinCore;

use crate::binding::Binding;
use crate::plan::{next_id, Dependencies, ImplContext, Implementable};
use crate::{Aid, Eid, Value, Var};
use crate::{CollectionRelation, Relation, VariableMap};

/// A plan stage joining two source relations on the specified
/// symbols. Throws if any of the join symbols isn't bound by both
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

impl<P1: Implementable, P2: Implementable> Implementable for Join<P1, P2> {
    fn dependencies(&self) -> Dependencies {
        Dependencies::merge(
            self.left_plan.dependencies(),
            self.right_plan.dependencies(),
        )
    }

    fn into_bindings(&self) -> Vec<Binding> {
        let mut left_bindings = self.left_plan.into_bindings();
        let mut right_bindings = self.right_plan.into_bindings();

        let mut bindings = Vec::with_capacity(left_bindings.len() + right_bindings.len());
        bindings.append(&mut left_bindings);
        bindings.append(&mut right_bindings);

        bindings
    }

    fn datafy(&self) -> Vec<(Eid, Aid, Value)> {
        let eid = next_id();

        let mut left_data = self.left_plan.datafy();
        let mut right_data = self.right_plan.datafy();

        let mut left_eids: Vec<(Eid, Aid, Value)> = left_data
            .iter()
            .map(|(e, _, _)| (eid, "df.join/binding".to_string(), Value::Eid(*e)))
            .collect();

        let mut right_eids: Vec<(Eid, Aid, Value)> = right_data
            .iter()
            .map(|(e, _, _)| (eid, "df.join/binding".to_string(), Value::Eid(*e)))
            .collect();

        let mut data = Vec::with_capacity(
            left_data.len() + right_data.len() + left_eids.len() + right_eids.len(),
        );
        data.append(&mut left_data);
        data.append(&mut right_data);
        data.append(&mut left_eids);
        data.append(&mut right_eids);

        data
    }

    fn implement<'b, T, I, S>(
        &self,
        nested: &mut Iterative<'b, S, u64>,
        local_arrangements: &VariableMap<Iterative<'b, S, u64>>,
        context: &mut I,
    ) -> CollectionRelation<'b, S>
    where
        T: Timestamp + Lattice + TotalOrder,
        I: ImplContext<T>,
        S: Scope<Timestamp = T>,
    {
        let left = self
            .left_plan
            .implement(nested, local_arrangements, context);
        let right = self
            .right_plan
            .implement(nested, local_arrangements, context);

        let symbols = self
            .variables
            .iter()
            .cloned()
            .chain(
                left.symbols()
                    .iter()
                    .filter(|x| !self.variables.contains(x))
                    .cloned(),
            )
            .chain(
                right
                    .symbols()
                    .iter()
                    .filter(|x| !self.variables.contains(x))
                    .cloned(),
            )
            .collect();

        let tuples = left.arrange_by_symbols(&self.variables).join_core(
            &right.arrange_by_symbols(&self.variables),
            |key, v1, v2| {
                Some(
                    key.iter()
                        .cloned()
                        .chain(v1.iter().cloned())
                        .chain(v2.iter().cloned())
                        .collect(),
                )
            },
        );

        CollectionRelation { symbols, tuples }
    }
}
