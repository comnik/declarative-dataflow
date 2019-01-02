//! Antijoin expression plan.

use timely::dataflow::Scope;
use timely::dataflow::scopes::child::Iterative;

use differential_dataflow::operators::Join;
use differential_dataflow::operators::Threshold;

use plan::Implementable;
use Relation;
use {QueryMap, RelationMap, SimpleRelation, Var};

/// A plan stage anti-joining both its sources on the specified
/// symbols. Throws if the sources are not union-compatible, i.e. bind
/// all of the same symbols in the same order.
#[derive(Deserialize, Clone, Debug)]
pub struct Antijoin<P1: Implementable, P2: Implementable> {
    /// TODO
    pub variables: Vec<Var>,
    /// Plan for the left input.
    pub left_plan: Box<P1>,
    /// Plan for the right input.
    pub right_plan: Box<P2>,
}

impl<P1: Implementable, P2: Implementable> Implementable for Antijoin<P1, P2> {
    fn implement<'b, S: Scope<Timestamp = u64>>(
        &self,
        nested: &mut Iterative<'b, S, u64>,
        local_arrangements: &RelationMap<Iterative<'b, S, u64>>,
        global_arrangements: &mut QueryMap<isize>,
    ) -> SimpleRelation<'b, S> {
        let left = self.left_plan
            .implement(nested, local_arrangements, global_arrangements);
        let right = self.right_plan
            .implement(nested, local_arrangements, global_arrangements);

        let symbols = self.variables
            .iter()
            .cloned()
            .chain(
                left.symbols()
                    .iter()
                    .filter(|x| !self.variables.contains(x))
                    .cloned(),
            )
            .collect();

        let tuples = left.tuples_by_symbols(&self.variables)
            .distinct()
            .antijoin(&right
                .tuples_by_symbols(&self.variables)
                .map(|(key, _)| key)
                .distinct())
            .map(|(key, tuple)| key.iter().cloned().chain(tuple.iter().cloned()).collect());

        SimpleRelation { symbols, tuples }
    }
}
