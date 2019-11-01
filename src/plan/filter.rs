//! Predicate expression plan.

use timely::dataflow::scopes::child::Iterative;
use timely::dataflow::Scope;
use timely::progress::Timestamp;

use differential_dataflow::lattice::Lattice;

pub use crate::binding::{
    AsBinding, BinaryPredicate as Predicate, BinaryPredicateBinding, Binding,
};
use crate::domain::Domain;
use crate::plan::{Dependencies, Implementable};
use crate::timestamp::Rewind;
use crate::{CollectionRelation, Implemented, Relation, ShutdownHandle, Value, Var, VariableMap};

#[inline(always)]
fn lt(a: &Value, b: &Value) -> bool {
    a < b
}
#[inline(always)]
fn lte(a: &Value, b: &Value) -> bool {
    a <= b
}
#[inline(always)]
fn gt(a: &Value, b: &Value) -> bool {
    a > b
}
#[inline(always)]
fn gte(a: &Value, b: &Value) -> bool {
    a >= b
}
#[inline(always)]
fn eq(a: &Value, b: &Value) -> bool {
    a == b
}
#[inline(always)]
fn neq(a: &Value, b: &Value) -> bool {
    a != b
}

/// A plan stage filtering source tuples by the specified
/// predicate. Frontends are responsible for ensuring that the source
/// binds the argument variables.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub struct Filter<P: Implementable> {
    /// TODO
    pub variables: Vec<Var>,
    /// Logical predicate to apply.
    pub predicate: Predicate,
    /// Plan for the data source.
    pub plan: Box<P>,
    /// Constant inputs
    pub constants: Vec<Option<Value>>,
}

impl<P: Implementable> Implementable for Filter<P> {
    fn dependencies(&self) -> Dependencies {
        self.plan.dependencies()
    }

    fn into_bindings(&self) -> Vec<Binding> {
        // let mut bindings = self.plan.into_bindings();
        // let variables = self.variables.clone();

        unimplemented!();
        // bindings.push(Binding::BinaryPredicate(BinaryPredicateBinding {
        //     variables: (variables[0], variables[1]),
        //     predicate: self.predicate.clone(),
        // }));

        // bindings
    }

    fn implement<'b, S>(
        &self,
        nested: &mut Iterative<'b, S, u64>,
        domain: &mut Domain<S::Timestamp>,
        local_arrangements: &VariableMap<Iterative<'b, S, u64>>,
    ) -> (Implemented<'b, S>, ShutdownHandle)
    where
        S: Scope,
        S::Timestamp: Timestamp + Lattice + Rewind,
    {
        let (relation, mut shutdown_handle) =
            self.plan.implement(nested, domain, local_arrangements);

        let key_offsets: Vec<usize> = self
            .variables
            .iter()
            .map(|variable| relation.binds(*variable).expect("variable not found"))
            .collect();

        let binary_predicate = match self.predicate {
            Predicate::LT => lt,
            Predicate::LTE => lte,
            Predicate::GT => gt,
            Predicate::GTE => gte,
            Predicate::EQ => eq,
            Predicate::NEQ => neq,
        };

        let variables = relation.variables();
        let projected = {
            let (projected, shutdown) = relation.projected(nested, domain, &variables);
            shutdown_handle.merge_with(shutdown);
            projected
        };

        let filtered = if let Some(constant) = self.constants[0].clone() {
            CollectionRelation {
                variables,
                tuples: projected
                    .filter(move |tuple| binary_predicate(&constant, &tuple[key_offsets[0]])),
            }
        } else if let Some(constant) = self.constants[1].clone() {
            CollectionRelation {
                variables,
                tuples: projected
                    .filter(move |tuple| binary_predicate(&tuple[key_offsets[0]], &constant)),
            }
        } else {
            CollectionRelation {
                variables,
                tuples: projected.filter(move |tuple| {
                    binary_predicate(&tuple[key_offsets[0]], &tuple[key_offsets[1]])
                }),
            }
        };

        (Implemented::Collection(filtered), shutdown_handle)
    }
}
