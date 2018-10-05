//! Equijoin expression plan.

use timely::dataflow::scopes::Child;
use timely::progress::timestamp::Timestamp;
use timely::communication::Allocate;
use timely::worker::Worker;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::Join as JoinMap;

use Relation;
use plan::Implementable;
use {RelationMap, QueryMap, SimpleRelation, Var};

/// A plan stage joining two source relations on the specified
/// symbols. Throws if any of the join symbols isn't bound by both
/// sources.
#[derive(Deserialize, Clone, Debug)]
pub struct Join<P1: Implementable, P2: Implementable> {
    /// TODO
    pub variables: Vec<Var>,
    /// Plan for the left input.
    pub left_plan: Box<P1>,
    /// Plan for the right input.
    pub right_plan: Box<P2>,
}

impl<P1: Implementable, P2: Implementable> Implementable for Join<P1,P2> {

    fn implement<'a, 'b, A: Allocate, T: Timestamp+Lattice>(
        &self,
        nested: &mut Child<'b, Child<'a, Worker<A>, T>, u64>,
        local_arrangements: &RelationMap<'b, Child<'a, Worker<A>, T>>,
        global_arrangements: &mut QueryMap<T, isize>
    )
    -> SimpleRelation<'b, Child<'a, Worker<A>, T>> {

        let left = self.left_plan.implement(nested, local_arrangements, global_arrangements);
        let right = self.right_plan.implement(nested, local_arrangements, global_arrangements);

        // useful for inspecting join inputs
        //.inspect(|&((ref key, ref values), _, _)| { println!("right {:?} {:?}", key, values) })

        let symbols =
        self.variables
            .iter().cloned()
            .chain(left.symbols().iter().filter(|x| !self.variables.contains(x)).cloned())
            .chain(right.symbols().iter().filter(|x| !self.variables.contains(x)).cloned())
            .collect();

        let tuples =
        left.tuples_by_symbols(&self.variables)
            .join_map(&right.tuples_by_symbols(&self.variables), |key, v1, v2|
                key .iter().cloned()
                    .chain(v1.iter().cloned())
                    .chain(v2.iter().cloned())
                    .collect()
            );

        // let debug_syms: Vec<String> = symbols.iter().map(|x| x.to_string()).collect();
        // println!(debug_syms);

        SimpleRelation { symbols, tuples }
    }
}
