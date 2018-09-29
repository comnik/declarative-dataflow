//! Projection expression plan.

use timely::dataflow::scopes::Child;
use timely::progress::timestamp::Timestamp;
use timely::communication::Allocate;
use timely::worker::Worker;

use differential_dataflow::lattice::Lattice;

use Relation;
use plan::Implementable;
use {ImplContext, RelationMap, QueryMap, SimpleRelation, Var};

/// A plan stage projecting its source to only the specified sequence
/// of symbols. Throws on unbound symbols. Frontends are responsible
/// for ensuring that the source binds all requested symbols.
#[derive(Deserialize, Clone, Debug)]
pub struct Project<P: Implementable> {
    /// TODO
    pub variables: Vec<Var>,
    /// Plan for the data source.
    pub plan: Box<P>
}

impl<P: Implementable> Implementable for Project<P> {

    fn implement<'a, 'b, A: Allocate, T: Timestamp+Lattice>(
        &self,
        db: &ImplContext<Child<'a, Worker<A>, T>>,
        nested: &mut Child<'b, Child<'a, Worker<A>, T>, u64>,
        relation_map: &RelationMap<'b, Child<'a, Worker<A>, T>>,
        queries: &mut QueryMap<T, isize>
    )
    -> SimpleRelation<'b, Child<'a, Worker<A>, T>> {

        let relation = self.plan.implement(db, nested, relation_map, queries);
        let tuples = relation
            .tuples_by_symbols(&self.variables)
            .map(|(key, _tuple)| key);

        SimpleRelation { symbols: self.variables.to_vec(), tuples }

    }
}
