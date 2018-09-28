//! Predicate expression plan.

use timely::dataflow::scopes::Child;
use timely::progress::timestamp::Timestamp;
use timely::communication::Allocate;
use timely::worker::Worker;

use differential_dataflow::lattice::Lattice;

use Relation;
use plan::Implementable;
use {ImplContext, RelationMap, QueryMap, SimpleRelation, Var};

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

/// A predicate expression plan stage.
#[derive(Deserialize, Clone, Debug)]
pub struct PredExpr<P: Implementable> {
    /// Logical predicate to apply.
    pub predicate: Predicate,
    /// TODO
    pub variables: Vec<Var>,
    /// Plan for the data source.
    pub plan: Box<P>
}

impl<P: Implementable> Implementable for PredExpr<P> {

    fn implement<'a, 'b, A: Allocate, T: Timestamp+Lattice>(
        &self,
        db: &ImplContext<Child<'a, Worker<A>, T>>,
        nested: &mut Child<'b, Child<'a, Worker<A>, T>, u64>,
        relation_map: &RelationMap<'b, Child<'a, Worker<A>, T>>,
        queries: &mut QueryMap<T, isize>
    )
    -> SimpleRelation<'b, Child<'a, Worker<A>, T>> {

        let rel = self.plan.implement(db, nested, relation_map, queries);

        let key_offsets: Vec<usize> = self.variables.iter()
            .map(|sym| rel.symbols().iter().position(|&v| *sym == v).expect("Symbol not found."))
            .collect();

        SimpleRelation {
            symbols: rel.symbols().to_vec(),
            tuples: match self.predicate {
                Predicate::LT => rel.tuples()
                    .filter(move |tuple| tuple[key_offsets[0]] < tuple[key_offsets[1]]),
                Predicate::LTE => rel.tuples()
                    .filter(move |tuple| tuple[key_offsets[0]] <= tuple[key_offsets[1]]),
                Predicate::GT => rel.tuples()
                    .filter(move |tuple| tuple[key_offsets[0]] > tuple[key_offsets[1]]),
                Predicate::GTE => rel.tuples()
                    .filter(move |tuple| tuple[key_offsets[0]] >= tuple[key_offsets[1]]),
                Predicate::EQ => rel.tuples()
                    .filter(move |tuple| tuple[key_offsets[0]] == tuple[key_offsets[1]]),
                Predicate::NEQ => rel.tuples()
                    .filter(move |tuple| tuple[key_offsets[0]] != tuple[key_offsets[1]])
            }
        }
    }
}