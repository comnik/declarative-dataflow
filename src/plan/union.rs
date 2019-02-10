//! Union expression plan.

use timely::dataflow::scopes::child::Iterative;
use timely::dataflow::Scope;
use timely::order::TotalOrder;
use timely::progress::Timestamp;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::Threshold;

use crate::binding::Binding;
use crate::plan::{Dependencies, ImplContext, Implementable};
use crate::{CollectionRelation, Relation, Var, VariableMap};

/// A plan stage taking the union over its sources. Frontends are
/// responsible to ensure that the sources are union-compatible
/// (i.e. bind all of the same symbols in the same order).
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub struct Union<P: Implementable> {
    /// TODO
    pub variables: Vec<Var>,
    /// Plan for the data source.
    pub plans: Vec<P>,
}

impl<P: Implementable> Implementable for Union<P> {
    fn dependencies(&self) -> Dependencies {
        let mut dependencies = Dependencies::none();

        for plan in self.plans.iter() {
            dependencies = Dependencies::merge(dependencies, plan.dependencies());
        }

        dependencies
    }

    fn into_bindings(&self) -> Vec<Binding> {
        let mut bindings = Vec::new();

        for plan in self.plans.iter() {
            bindings.append(&mut plan.into_bindings());
        }

        bindings
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
        use differential_dataflow::AsCollection;
        use timely::dataflow::operators::Concatenate;

        let mut scope = nested.clone();
        let streams = self.plans.iter().map(|plan| {
            plan.implement(&mut scope, local_arrangements, context)
                .tuples_by_symbols(&self.variables)
                .map(|(key, _vals)| key)
                .inner
        });

        let concat = nested.concatenate(streams).as_collection();

        CollectionRelation {
            symbols: self.variables.to_vec(),
            tuples: concat.distinct(),
        }
    }
}
