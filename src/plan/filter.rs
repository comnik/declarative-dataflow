//! Predicate expression plan.

use timely::dataflow::scopes::child::Iterative;
use timely::dataflow::Scope;
use timely::order::TotalOrder;
use timely::progress::Timestamp;

use differential_dataflow::lattice::Lattice;

pub use crate::binding::{BinaryPredicate as Predicate, BinaryPredicateBinding, Binding};
use crate::plan::{Dependencies, ImplContext, Implementable};
use crate::{CollectionRelation, Relation, ShutdownHandle, Value, Var, VariableMap};

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
        let (relation, shutdown_handle) = self.plan.implement(nested, local_arrangements, context);

        let key_offsets: Vec<usize> = self
            .variables
            .iter()
            .map(|variable| {
                relation
                    .variables()
                    .iter()
                    .position(|&v| *variable == v)
                    .expect("Variable not found.")
            })
            .collect();

        let binary_predicate = match self.predicate {
            Predicate::LT => lt,
            Predicate::LTE => lte,
            Predicate::GT => gt,
            Predicate::GTE => gte,
            Predicate::EQ => eq,
            Predicate::NEQ => neq,
        };

        let filtered = if let Some(constant) = self.constants[0].clone() {
            CollectionRelation {
                variables: relation.variables().to_vec(),
                tuples: relation
                    .tuples()
                    .filter(move |tuple| binary_predicate(&constant, &tuple[key_offsets[0]])),
            }
        } else if let Some(constant) = self.constants[1].clone() {
            CollectionRelation {
                variables: relation.variables().to_vec(),
                tuples: relation
                    .tuples()
                    .filter(move |tuple| binary_predicate(&tuple[key_offsets[0]], &constant)),
            }
        } else {
            CollectionRelation {
                variables: relation.variables().to_vec(),
                tuples: relation.tuples().filter(move |tuple| {
                    binary_predicate(&tuple[key_offsets[0]], &tuple[key_offsets[1]])
                }),
            }
        };

        (filtered, shutdown_handle)
    }
}
