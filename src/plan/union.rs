//! Union expression plan.

use std::collections::HashMap;

use timely::dataflow::Scope;
use timely::dataflow::scopes::child::Iterative;

use differential_dataflow::operators::Threshold;

use plan::Implementable;
use Relation;
use {Attribute, RelationHandle, VariableMap, SimpleRelation, Var};

/// A plan stage taking the union over its sources. Frontends are
/// responsible to ensure that the sources are union-compatible
/// (i.e. bind all of the same symbols in the same order).
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Union<P: Implementable> {
    /// TODO
    pub variables: Vec<Var>,
    /// Plan for the data source.
    pub plans: Vec<P>,
}

impl<P: Implementable> Implementable for Union<P> {
    fn implement<'b, S: Scope<Timestamp = u64>>(
        &self,
        nested: &mut Iterative<'b, S, u64>,
        local_arrangements: &VariableMap<Iterative<'b, S, u64>>,
        global_arrangements: &mut HashMap<String, RelationHandle>,
        attributes: &mut HashMap<String, Attribute>,
    ) -> SimpleRelation<'b, S> {
        
        use differential_dataflow::AsCollection;
        use timely::dataflow::operators::Concatenate;

        let mut scope = nested.clone();
        let streams = self.plans.iter().map(|plan| {
            plan.implement(&mut scope, local_arrangements, global_arrangements, attributes)
                .tuples_by_symbols(&self.variables)
                .map(|(key, _vals)| key)
                .inner
        });

        let concat = nested.concatenate(streams).as_collection();

        SimpleRelation {
            symbols: self.variables.to_vec(),
            tuples: concat.distinct(),
        }
    }
}
