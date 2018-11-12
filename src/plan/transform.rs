//! Function expression plan.
use std::collections::HashMap;

use timely::communication::Allocate;
use timely::dataflow::scopes::child::{Child, Iterative};
use timely::worker::Worker;

use plan::Implementable;
use Relation;
use {QueryMap, RelationMap, SimpleRelation, Value, Var};

/// Permitted functions.
#[derive(Deserialize, Clone, Debug)]
pub enum Function {
    /// Truncates a unix timestamp into an hourly interval
    TRUNCATE,
    /// Bind a constant to the specified symbol and add it into the result set
    BIND,
}

/// A plan stage applying a built-in function to source tuples.
/// Frontends are responsible for ensuring that the source
/// binds the argument symbols and that the result is projected onto
/// the right symbol.
#[derive(Deserialize, Clone, Debug)]
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

impl<P: Implementable> Implementable for Transform<P> {
    fn implement<'a, 'b, A: Allocate>(
        &self,
        nested: &mut Iterative<'b, Child<'a, Worker<A>, u64>, u64>,
        local_arrangements: &RelationMap<Iterative<'b, Child<'a, Worker<A>, u64>, u64>>,
        global_arrangements: &mut QueryMap<isize>,
    ) -> SimpleRelation<'b, Child<'a, Worker<A>, u64>> {
        let rel = self.plan
            .implement(nested, local_arrangements, global_arrangements);

        let key_offsets: Vec<usize> = self.variables
            .iter()
            .map(|sym| {
                rel.symbols()
                    .iter()
                    .position(|&v| *sym == v)
                    .expect("Symbol not found.")
            })
            .collect();

        let mut symbols = rel.symbols().to_vec().clone();
        symbols.push(self.result_sym);
        
        let constants_local = self.constants.clone();

        match self.function {
            Function::BIND => SimpleRelation {
                symbols: symbols,
                tuples: rel.tuples().map(move |tuple| {
                    let constant = match constants_local.get(&0) {
                        Some(constant) => constant,
                        _ => panic!("Empty parameter for BIND is not allowed"),
                    };
                    let mut v = tuple.clone();
                    v.push(constant.clone());
                    v
                }),
            },

            Function::TRUNCATE => SimpleRelation {
                symbols: symbols,
                tuples: rel.tuples().map(move |tuple| {
                    let mut t = match tuple[key_offsets[0]] {
                        Value::Number(num) => num as i64,
                        _ => panic!("TRUNCATE can only be applied to numbers"),
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
                    v.push(Value::Number(t));
                    v
                }),
            },
        }
    }
}
