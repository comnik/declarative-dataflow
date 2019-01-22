//! Projection expression plan.

use std::collections::HashMap;

use timely::dataflow::Scope;
use timely::dataflow::scopes::child::Iterative;

use plan::{ImplContext, Implementable};
use {Relation, Binding};
use {VariableMap, CollectionRelation, Var};

/// A plan stage projecting its source to only the specified sequence
/// of symbols. Throws on unbound symbols. Frontends are responsible
/// for ensuring that the source binds all requested symbols.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Project<P: Implementable> {
    /// TODO
    pub variables: Vec<Var>,
    /// Plan for the data source.
    pub plan: Box<P>,
}

impl<P: Implementable> Implementable for Project<P>
{
    fn dependencies(&self) -> Vec<String> { self.plan.dependencies() }

    fn into_bindings(&self) -> Vec<Binding> { self.plan.into_bindings() }
    
    fn implement<'b, S: Scope<Timestamp = u64>, I: ImplContext>(
        &self,
        nested: &mut Iterative<'b, S, u64>,
        local_arrangements: &VariableMap<Iterative<'b, S, u64>>,
        context: &mut I,
    ) -> CollectionRelation<'b, S> {
        
        let relation = self.plan
            .implement(nested, local_arrangements, context);
        let tuples = relation
            .tuples_by_symbols(&self.variables)
            .map(|(key, _tuple)| key);

        CollectionRelation {
            symbols: self.variables.to_vec(),
            tuples,
        }
    }
}
