//! Function expression plan.
use std::collections::HashMap;

use timely::communication::Allocate;
use timely::dataflow::scopes::child::{Child, Iterative};
use timely::progress::timestamp::Timestamp;
use timely::worker::Worker;

use differential_dataflow::lattice::Lattice;

use plan::Implementable;
use Relation;
use {QueryMap, RelationMap, SimpleRelation, Value, Var};

/// Permitted functions.
#[derive(Deserialize, Clone, Debug)]
pub enum Function {
    /// Truncates a unix timestamp into an hourly interval
    TRUNCATE,
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
            Function::TRUNCATE => SimpleRelation {
                symbols: symbols,
                tuples: rel.tuples().map(move |tuple| {
                    let mut t = match tuple[key_offsets[0]] {
                        Value::Instant(inst) => inst as u64,
                        _ => panic!("TRUNCATE can only be applied to timestamps"),
                    };
                    let default_interval = String::from("hour");
                    let interval_param = match constants_local.get(&1){  
                        Some(Value::String(interval)) => interval as &String, 
                        None => &default_interval,
                        _ => panic!("Parameter for TRUNCATE must be a string"),
                    };
                    let interval_options = vec![String::from("minute"), String::from("hour"), String::from("day"), String::from("week")];
                    let millies : Vec<u64> = vec![60000, 3600000, 86400000, 604800000];
                    let encoding : HashMap<_, _> = interval_options.iter().zip(millies.iter()).collect();

                    let mod_val = match encoding.get(&interval_param){
                        Some(val) => val,
                        None => panic!("Unknown interval for TRUNCATE") 
                    };

                    t = t - (t % *mod_val);
                    let mut v = tuple.clone();
                    v.push(Value::Instant(t));
                    v
                }),
            },
        }
    }
}
