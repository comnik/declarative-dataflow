//! Projection expression plan.

use timely::dataflow::scopes::child::Iterative;
use timely::dataflow::Scope;
use timely::progress::Timestamp;

use differential_dataflow::lattice::Lattice;

use crate::binding::Binding;
use crate::domain::Domain;
use crate::plan::{Dependencies, Implementable};
use crate::timestamp::Rewind;
use crate::Var;
use crate::{CollectionRelation, Implemented, Relation, ShutdownHandle, Value, VariableMap};

/// A plan stage projecting its source to only the specified sequence
/// of variables. Throws on unbound variables. Frontends are responsible
/// for ensuring that the source binds all requested variables.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub struct Project<P: Implementable> {
    /// TODO
    pub variables: Vec<Var>,
    /// Plan for the data source.
    pub plan: Box<P>,
}

impl<P: Implementable> Implementable for Project<P> {
    type A = P::A;

    fn dependencies(&self) -> Dependencies<Self::A> {
        self.plan.dependencies()
    }

    fn into_bindings(&self) -> Vec<Binding<Self::A, Value>> {
        self.plan.into_bindings()
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
        let (relation, mut shutdown_handle) =
            self.plan.implement(nested, domain, local_arrangements);
        let tuples = {
            let (projected, shutdown) = relation.projected(nested, domain, &self.variables);
            shutdown_handle.merge_with(shutdown);

            projected
        };

        let projected = CollectionRelation {
            variables: self.variables.to_vec(),
            tuples,
        };

        (Implemented::Collection(projected), shutdown_handle)
    }
}
