//! Antijoin expression plan.

use timely::dataflow::scopes::child::Iterative;
use timely::dataflow::Scope;

use differential_dataflow::operators::Join;
use differential_dataflow::operators::Threshold;

use crate::plan::{Dependencies, ImplContext, Implementable};
use crate::{CollectionRelation, Relation, Var, VariableMap};

/// A plan stage anti-joining both its sources on the specified
/// symbols. Throws if the sources are not union-compatible, i.e. bind
/// all of the same symbols in the same order.
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

    fn implement<'b, S: Scope<Timestamp = u64>, I: ImplContext>(
        &self,
        nested: &mut Iterative<'b, S, u64>,
        local_arrangements: &VariableMap<Iterative<'b, S, u64>>,
        context: &mut I,
    ) -> CollectionRelation<'b, S> {
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
            .collect();

        let tuples = left
            .tuples_by_symbols(&self.variables)
            .distinct()
            .antijoin(
                &right
                    .tuples_by_symbols(&self.variables)
                    .map(|(key, _)| key)
                    .distinct(),
            )
            .map(|(key, tuple)| key.iter().cloned().chain(tuple.iter().cloned()).collect());

        CollectionRelation { symbols, tuples }
    }
}
