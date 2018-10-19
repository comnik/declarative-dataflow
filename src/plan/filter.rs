//! Predicate expression plan.

use timely::communication::Allocate;
use timely::dataflow::scopes::child::{Child, Iterative};
use timely::worker::Worker;

use plan::Implementable;
use Relation;
use {QueryMap, RelationMap, SimpleRelation, Var};

/// Permitted comparison predicates.
#[derive(Deserialize, Clone, Debug)]
pub enum Predicate {
    /// Less than
    LT,
    /// Greater than
    GT,
    /// Less than or equal to
    LTE,
    /// Greater than or equal to
    GTE,
    /// Equal
    EQ,
    /// Not equal
    NEQ,
}

/// A plan stage filtering source tuples by the specified
/// predicate. Frontends are responsible for ensuring that the source
/// binds the argument symbols.
#[derive(Deserialize, Clone, Debug)]
pub struct Filter<P: Implementable> {
    /// TODO
    pub variables: Vec<Var>,
    /// Logical predicate to apply.
    pub predicate: Predicate,
    /// Plan for the data source.
    pub plan: Box<P>,
}

impl<P: Implementable> Implementable for Filter<P> {
    fn implement<'a, 'b, A: Allocate>(
        &self,
        nested: &mut Iterative<'b, Child<'a, Worker<A>, u64>, u64>,
        local_arrangements: &RelationMap<Iterative<'b, Child<'a, Worker<A>, u64>, u64>>,
        global_arrangements: &mut QueryMap<isize>,
    ) -> SimpleRelation<'b, Child<'a, Worker<A>, u64>> {
        let rel = self
            .plan
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

        SimpleRelation {
            symbols: rel.symbols().to_vec(),
            tuples: match self.predicate {
                Predicate::LT => rel
                    .tuples()
                    .filter(move |tuple| tuple[key_offsets[0]] < tuple[key_offsets[1]]),
                Predicate::LTE => rel
                    .tuples()
                    .filter(move |tuple| tuple[key_offsets[0]] <= tuple[key_offsets[1]]),
                Predicate::GT => rel
                    .tuples()
                    .filter(move |tuple| tuple[key_offsets[0]] > tuple[key_offsets[1]]),
                Predicate::GTE => rel
                    .tuples()
                    .filter(move |tuple| tuple[key_offsets[0]] >= tuple[key_offsets[1]]),
                Predicate::EQ => rel
                    .tuples()
                    .filter(move |tuple| tuple[key_offsets[0]] == tuple[key_offsets[1]]),
                Predicate::NEQ => rel
                    .tuples()
                    .filter(move |tuple| tuple[key_offsets[0]] != tuple[key_offsets[1]]),
            },
        }
    }
}
