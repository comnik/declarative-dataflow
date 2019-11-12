//! Function expression plan.

use timely::dataflow::scopes::child::Iterative;
use timely::dataflow::Scope;
use timely::progress::Timestamp;

use differential_dataflow::lattice::Lattice;

use crate::binding::{AsBinding, Binding};
use crate::domain::Domain;
use crate::plan::{Dependencies, Implementable};
use crate::timestamp::Rewind;
use crate::{CollectionRelation, Implemented, Relation, ShutdownHandle, Value, Var, VariableMap};

/// Permitted functions.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub enum Function {
    /// Truncates a unix timestamp into an hourly interval
    TRUNCATE,
    /// Adds one or more numbers to the first provided
    ADD,
    /// Subtracts one or more numbers from the first provided
    SUBTRACT,
}

/// A plan stage applying a built-in function to source tuples.
/// Frontends are responsible for ensuring that the source
/// binds the argument variables and that the result is projected onto
/// the right variable.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub struct Transform<P: Implementable> {
    /// TODO
    pub variables: Vec<Var>,
    /// Variable to which the result of the transformation is bound
    pub result_variable: Var,
    /// Plan for the data source
    pub plan: Box<P>,
    /// Function to apply
    pub function: Function,
    /// Constant inputs
    pub constants: Vec<Option<Value>>,
}

impl<P: Implementable> Implementable for Transform<P> {
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
        domain: &mut Domain<Self::A, S::Timestamp>,
        local_arrangements: &VariableMap<Self::A, Iterative<'b, S, u64>>,
    ) -> (Implemented<'b, Self::A, S>, ShutdownHandle)
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

        let mut variables = relation.variables();
        variables.push(self.result_variable);

        let constants_local = self.constants.clone();

        let tuples = {
            let (tuples, shutdown) = relation.tuples(nested, domain);
            shutdown_handle.merge_with(shutdown);
            tuples
        };

        let transformed = match self.function {
            Function::TRUNCATE => CollectionRelation {
                variables,
                tuples: tuples.map(move |tuple| {
                    let mut t = match tuple[key_offsets[0]] {
                        Value::Instant(inst) => inst as u64,
                        _ => panic!("TRUNCATE can only be applied to timestamps"),
                    };
                    let default_interval = String::from(":hour");
                    let interval_param = match constants_local[1].clone() {
                        Some(Value::String(interval)) => interval,
                        None => default_interval,
                        _ => panic!("Parameter for TRUNCATE must be a string"),
                    };

                    let mod_val = match interval_param.as_ref() {
                        ":minute" => 60000,
                        ":hour" => 3_600_000,
                        ":day" => 86_400_000,
                        ":week" => 604_800_000,
                        _ => panic!("Unknown interval for TRUNCATE"),
                    };

                    t = t - (t % mod_val);
                    let mut v = tuple.clone();
                    v.push(Value::Instant(t));
                    v
                }),
            },
            Function::ADD => CollectionRelation {
                variables,
                tuples: tuples.map(move |tuple| {
                    let mut result = 0;

                    // summands (vars)
                    for offset in &key_offsets {
                        let summand = match tuple[*offset] {
                            Value::Number(s) => s as i64,
                            _ => panic!("ADD can only be applied to numbers"),
                        };

                        result += summand;
                    }

                    // summands (constants)
                    for arg in &constants_local {
                        if let Some(constant) = arg {
                            let summand = match constant {
                                Value::Number(s) => *s as i64,
                                _ => panic!("ADD can only be applied to numbers"),
                            };

                            result += summand;
                        }
                    }

                    let mut v = tuple.clone();
                    v.push(Value::Number(result));
                    v
                }),
            },
            Function::SUBTRACT => CollectionRelation {
                variables,
                tuples: tuples.map(move |tuple| {
                    // minuend is either variable or variable, depending on
                    // position in transform

                    let mut result = match constants_local[0].clone() {
                        Some(constant) => match constant {
                            Value::Number(minuend) => minuend as i64,
                            _ => panic!("SUBTRACT can only be applied to numbers"),
                        },
                        None => match tuple[key_offsets[0]] {
                            Value::Number(minuend) => minuend as i64,
                            _ => panic!("SUBTRACT can only be applied to numbers"),
                        },
                    };

                    // avoid filtering out the minuend by doubling it
                    result = result + result;

                    // subtrahends (vars)
                    for offset in &key_offsets {
                        let subtrahend = match tuple[*offset] {
                            Value::Number(s) => s as i64,
                            _ => panic!("SUBTRACT can only be applied to numbers"),
                        };

                        result -= subtrahend;
                    }

                    // subtrahends (constants)
                    for arg in &constants_local {
                        if let Some(constant) = arg {
                            let subtrahend = match constant {
                                Value::Number(s) => *s as i64,
                                _ => panic!("SUBTRACT can only be applied to numbers"),
                            };

                            result -= subtrahend;
                        }
                    }

                    let mut v = tuple.clone();
                    v.push(Value::Number(result));
                    v
                }),
            },
        };

        (Implemented::Collection(transformed), shutdown_handle)
    }
}
