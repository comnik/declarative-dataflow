//! Union expression plan.

use timely::dataflow::scopes::Child;
use timely::progress::timestamp::Timestamp;
use timely::communication::Allocate;
use timely::worker::Worker;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::Threshold;

use Relation;
use plan::Implementable;
use {ImplContext, RelationMap, QueryMap, SimpleRelation, Var};

/// A plan stage taking the union over its sources. Frontends are
/// responsible to ensure that the sources are union-compatible
/// (i.e. bind all of the same symbols in the same order).
#[derive(Deserialize, Clone, Debug)]
pub struct Union<P: Implementable> {
    /// TODO
    pub variables: Vec<Var>,
    /// Plan for the data source.
    pub plans: Vec<P>
}

impl<P: Implementable> Implementable for Union<P> {

    fn implement<'a, 'b, A: Allocate, T: Timestamp+Lattice>(
        &self,
        db: &ImplContext<Child<'a, Worker<A>, T>>,
        nested: &mut Child<'b, Child<'a, Worker<A>, T>, u64>,
        relation_map: &RelationMap<'b, Child<'a, Worker<A>, T>>,
        queries: &mut QueryMap<T, isize>
    )
    -> SimpleRelation<'b, Child<'a, Worker<A>, T>> {

        use timely::dataflow::operators::Concatenate;
        use differential_dataflow::AsCollection;

        let mut scope = nested.clone();
        let streams =
        self.plans.iter().map(|plan|
            plan.implement(db, &mut scope, relation_map, queries)
                .tuples_by_symbols(&self.variables)
                .map(|(key,_vals)| key).inner
        );

        let concat = nested.concatenate(streams).as_collection();

        SimpleRelation {
            symbols: self.variables.to_vec(),
            tuples: concat.distinct()
        }
    }
}
