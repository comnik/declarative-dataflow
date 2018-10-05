//! Antijoin expression plan.

use timely::dataflow::scopes::Child;
use timely::progress::timestamp::Timestamp;
use timely::communication::Allocate;
use timely::worker::Worker;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::Join;
use differential_dataflow::operators::Threshold;

use Relation;
use plan::Implementable;
use {RelationMap, QueryMap, SimpleRelation, Var};

/// A plan stage anti-joining both its sources on the specified
/// symbols. Throws if the sources are not union-compatible, i.e. bind
/// all of the same symbols in the same order.
#[derive(Deserialize, Clone, Debug)]
pub struct Antijoin<P1: Implementable, P2: Implementable> {
    /// TODO
    pub variables: Vec<Var>,
    /// Plan for the left input.
    pub left_plan: Box<P1>,
    /// Plan for the right input.
    pub right_plan: Box<P2>,
}

impl<P1: Implementable, P2: Implementable> Implementable for Antijoin<P1,P2> {

    fn implement<'a, 'b, A: Allocate, T: Timestamp+Lattice>(
        &self,
        nested: &mut Child<'b, Child<'a, Worker<A>, T>, u64>,
        local_arrangements: &RelationMap<'b, Child<'a, Worker<A>, T>>,
        global_arrangements: &mut QueryMap<T, isize>
    )
    -> SimpleRelation<'b, Child<'a, Worker<A>, T>> {

        let left = self.left_plan.implement(nested, local_arrangements, global_arrangements);
        let right = self.right_plan.implement(nested, local_arrangements, global_arrangements);

        let symbols =
        self.variables
            .iter().cloned()
            .chain(left.symbols().iter().filter(|x| !self.variables.contains(x)).cloned())
            .collect();

        let tuples = left.tuples_by_symbols(&self.variables)
            .distinct()
            .antijoin(&right.tuples_by_symbols(&self.variables).map(|(key, _)| key).distinct())
            .map(|(key, tuple)|
                key .iter().cloned()
                    .chain(tuple.iter().cloned())
                    .collect()
            );

        SimpleRelation { symbols, tuples }
    }
}
