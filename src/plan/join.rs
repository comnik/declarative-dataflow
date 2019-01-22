//! Equijoin expression plan.

use std::collections::HashMap;

use timely::dataflow::Scope;
use timely::dataflow::scopes::child::Iterative;

use differential_dataflow::operators::JoinCore;

use plan::{ImplContext, Implementable};
use {Relation, Binding};
use {VariableMap, CollectionRelation, Var};

/// A plan stage joining two source relations on the specified
/// symbols. Throws if any of the join symbols isn't bound by both
/// sources.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Join<P1: Implementable, P2: Implementable> {
    /// TODO
    pub variables: Vec<Var>,
    /// Plan for the left input.
    pub left_plan: Box<P1>,
    /// Plan for the right input.
    pub right_plan: Box<P2>,
}

impl<P1: Implementable, P2: Implementable> Implementable for Join<P1, P2>
{
    fn dependencies(&self) -> Vec<String> {
        let mut left_dependencies = self.left_plan.dependencies();
        let mut right_dependencies = self.right_plan.dependencies();

        let mut dependencies = Vec::with_capacity(left_dependencies.len() + right_dependencies.len());
        dependencies.append(&mut left_dependencies);
        dependencies.append(&mut right_dependencies);

        dependencies
    }

    fn into_bindings(&self) -> Vec<Binding> {
        let mut left_bindings = self.left_plan.into_bindings();
        let mut right_bindings = self.right_plan.into_bindings();

        let mut bindings = Vec::with_capacity(left_bindings.len() + right_bindings.len());
        bindings.append(&mut left_bindings);
        bindings.append(&mut right_bindings);

        bindings        
    }
    
    fn implement<'b, S: Scope<Timestamp = u64>, I: ImplContext>(
        &self,
        nested: &mut Iterative<'b, S, u64>,
        local_arrangements: &VariableMap<Iterative<'b, S, u64>>,
        context: &mut I,
    ) -> CollectionRelation<'b, S> {
        let left = self.left_plan
            .implement(nested, local_arrangements, context);
        let right = self.right_plan
            .implement(nested, local_arrangements, context);

        let symbols = self.variables
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

        let tuples = left
            .arrange_by_symbols(&self.variables)
            .join_core(
                &right.arrange_by_symbols(&self.variables),
                |key, v1, v2| {
                    Some(key.iter()
                         .cloned()
                         .chain(v1.iter().cloned())
                         .chain(v2.iter().cloned())
                         .collect())
                },
            );

        CollectionRelation { symbols, tuples }
    }
}
