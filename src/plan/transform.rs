//! Function expression plan.

use std::collections::HashMap;

use timely::dataflow::Scope;
use timely::dataflow::scopes::child::Iterative;

use plan::{ImplContext, Implementable};
use Relation;
use {VariableMap, SimpleRelation, Value, Var};

/// Permitted functions.
#[derive(Serialize, Deserialize, Clone, Debug)]
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
/// binds the argument symbols and that the result is projected onto
/// the right symbol.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Transform<P: Implementable> {
    /// TODO
    pub variables: Vec<Var>,
    /// Symbol to which the result of the transformation is bound
    pub result_sym: Var,
    /// Plan for the data source
    pub plan: Box<P>,
    /// Function to apply
    pub function: Function,
    /// Constant intputs
    pub constants: HashMap<u32, Value>,
}

impl<P: Implementable> Implementable for Transform<P>
{
    fn dependencies(&self) -> Vec<String> { self.plan.dependencies() }
    
    fn implement<'b, S: Scope<Timestamp = u64>, I: ImplContext>(
        &self,
        nested: &mut Iterative<'b, S, u64>,
        local_arrangements: &VariableMap<Iterative<'b, S, u64>>,
        context: &mut I,
    ) -> SimpleRelation<'b, S> {
        
        let rel = self
            .plan
            .implement(nested, local_arrangements, context);

        let key_offsets: Vec<usize> = self
            .variables
            .iter()
            .map(|sym| {
                rel.symbols()
                    .iter()
                    .position(|&v| *sym == v)
                    .expect("Symbol not found.")
            }).collect();

        let mut symbols = rel.symbols().to_vec().clone();
        symbols.push(self.result_sym);

        let constants_local = self.constants.clone();

        match self.function {
            Function::TRUNCATE => SimpleRelation {
                symbols: symbols,
                tuples: rel.tuples().map(move |tuple| {
                    let mut t = match tuple[key_offsets[0]] {
                        Value::Instant(inst) => inst as u64,
                        _ => panic!("TRUNCATE can only be applied to timestamps"),
                    };
                    let default_interval = String::from(":hour");
                    let interval_param = match constants_local.get(&1) {
                        Some(Value::String(interval)) => interval as &String,
                        None => &default_interval,
                        _ => panic!("Parameter for TRUNCATE must be a string"),
                    };

                    let mod_val = match interval_param.as_ref() {
                        ":minute" => 60000,
                        ":hour" => 3600000,
                        ":day" => 86400000,
                        ":week" => 604800000,
                        _ => panic!("Unknown interval for TRUNCATE"),
                    };

                    t = t - (t % mod_val);
                    let mut v = tuple.clone();
                    v.push(Value::Instant(t));
                    v
                }),
            },
            Function::ADD => SimpleRelation {
                symbols: symbols,
                tuples: rel.tuples().map(move |tuple| {
                    let mut result = 0;

                    // summands (vars)
                    for offset in &key_offsets {
                        let summand = match tuple[*offset] {
                            Value::Number(s) => s as i64,
                            _ => panic!("ADD can only be applied to numbers"),
                        };

                        result = result + summand;
                    }

                    // summands (constants)
                    for (_key, val) in &constants_local {
                        let summand = match val {
                            Value::Number(s) => *s as i64,
                            _ => panic!("ADD can only be applied to numbers"),
                        };

                        result = result + summand;
                    }

                    let mut v = tuple.clone();
                    v.push(Value::Number(result));
                    v
                }),
            },
            Function::SUBTRACT => SimpleRelation {
                symbols: symbols,
                tuples: rel.tuples().map(move |tuple| {
                    // minuend is either symbol or variable, depending on
                    // position in transform

                    let mut result = match constants_local.get(&0) {
                        Some(constant) => match constant {
                            Value::Number(minuend) => *minuend as i64,
                            _ => panic!("SUBTRACT can only be applied to numbers")
                        },
                        None => match tuple[key_offsets[0]] {
                            Value::Number(minuend) => minuend as i64,
                            _ => panic!("SUBTRACT can only be applied to numbers"),
                        }
                    };

                    // avoid filtering out the minuend by doubling it
                    result = result + result;

                    // subtrahends (vars)
                    for offset in &key_offsets {
                        let subtrahend = match tuple[*offset] {
                            Value::Number(s) => s as i64,
                            _ => panic!("SUBTRACT can only be applied to numbers"),
                        };

                        result = result - subtrahend;
                    }

                    // subtrahends (constants)
                    for (_key, val) in &constants_local {
                        let subtrahend = match val {
                            Value::Number(s) => *s as i64,
                            _ => panic!("SUBTRACT can only be applied to numbers"),
                        };

                        result = result - subtrahend;
                    }

                    let mut v = tuple.clone();
                    v.push(Value::Number(result));
                    v
                }),
            },
        }
    }
}
