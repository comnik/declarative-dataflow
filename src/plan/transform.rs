//! Function expression plan.

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
    /// Time interval
    INTERVAL
}

/// A plan stage applying a built-in function to source tuples.
/// Frontends are responsible for ensuring that the source
/// binds the argument symbols and that the result is projected onto
/// the right symbol.
#[derive(Deserialize, Clone, Debug)]
pub struct Transform <P: Implementable> {
    /// TODO
    pub variables: Vec<Var>,
    /// Plan for the data source.
    pub plan: Box<P>,
    /// Function to apply
    pub function: Function,
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

        let key_offsets: Vec<usize> = self
            .variables
            .iter()
            .map(|sym| {
                rel.symbols()
                    .iter()
                    .position(|&v| *sym == v)
                    .expect("Symbol not found.")
            }).collect();

        match self.function{
            Function::INTERVAL => 
                SimpleRelation {
                    symbols: rel.symbols().to_vec(),
                    tuples: rel.tuples()
                        .map(move |tuple| {
                            let mut t = match tuple[key_offsets[0]] {
                                Value::Instant(inst) => inst as u64,
                                _ => panic!("INTERVAL can only be applied to timestamps")
                            };
                            // @TODO Add parameter to control the interval
                            t = t - (t % 3600000);
                            let mut v = tuple.clone();
                            v[key_offsets[0]] = Value::Instant(t);
                            v
                        })
                }
        }
    }
}
