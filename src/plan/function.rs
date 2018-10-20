//! Function expression plan.

use timely::communication::Allocate;
use timely::dataflow::scopes::Child;
use timely::progress::timestamp::Timestamp;
use timely::worker::Worker;

use differential_dataflow::lattice::Lattice;

use plan::Implementable;
use Relation;
use {QueryMap, RelationMap, SimpleRelation, Var};

/// Permitted comparison predicates.
#[derive(Deserialize, Clone, Debug)]
pub enum FunctionExpression {
    /// Time interval
    INTERVAL,
}

/// A plan stage applying a built-in function to source tuples.
/// Frontends are responsible for ensuring that the source
/// binds the argument symbols and that the result is projected onto
/// the right symbol.
#[derive(Deserialize, Clone, Debug)]
pub struct Transformation<P: Implementable> {
    /// TODO
    pub variables: Vec<Var>,
    /// Plan for the data source.
    pub plan: Box<P>,
    /// Function to apply
    pub function_expression: FunctionExpression,
}

impl<P: Implementable> Implementable for Transformation<P> {
    fn implement<'a, 'b, A: Allocate, T: Timestamp + Lattice>(
        &self,
        nested: &mut Child<'b, Child<'a, Worker<A>, T>, u64>,
        local_arrangements: &RelationMap<'b, Child<'a, Worker<A>, T>>,
        global_arrangements: &mut QueryMap<T, isize>,
    ) -> SimpleRelation<'b, Child<'a, Worker<A>, T>> {
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

        match self.function_expression {
            FunctionExpression::INTERVAL => 
                SimpleRelation {
                    symbols: rel.symbols().to_vec(),
                    tuples: rel.tuples()
                        .map(move |tuple| {
                            let mut t = match tuple[key_offsets[0]] {
                                Value::Instant(inst) => inst as u64,
                                _ => panic!("INTERVAL can only be applied to timestamp")
                            };
                            t = t - (t %% 3600000);
                            let mut v = tuple.clone();
                            v[key_offsets[0]] = t;
                            v
                        })
                }
        }
    }
}
