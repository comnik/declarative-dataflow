//! WCO expression plan, integrating the following work:
//! https://github.com/frankmcsherry/differential-dataflow/tree/master/dogsdogsdogs

extern crate dogsdogsdogs;

use timely::communication::Allocate;
// use timely::dataflow::operators::Inspect;
use timely::dataflow::scopes::child::{Child, Iterative};
use timely::worker::Worker;

use differential_dataflow::operators::Threshold;

use self::dogsdogsdogs::{CollectionIndex, altneu::AltNeu};
use self::dogsdogsdogs::{ProposeExtensionMethod};

use plan::Implementable;
use Relation;
use {QueryMap, RelationMap, SimpleRelation, Var};
    
/// A plan stage joining two source relations on the specified
/// symbols. Throws if any of the join symbols isn't bound by both
/// sources.
#[derive(Deserialize, Clone, Debug)]
pub struct Hector<P: Implementable> {
    /// TODO
    pub variables: Vec<Var>,
    /// Input plans.
    pub plans: Vec<P>,
}

impl<P: Implementable> Implementable for Hector<P> {
    fn implement<'a, 'b, A: Allocate>(
        &self,
        nested: &mut Iterative<'b, Child<'a, Worker<A>, u64>, u64>,
        local_arrangements: &RelationMap<Iterative<'b, Child<'a, Worker<A>, u64>, u64>>,
        global_arrangements: &mut QueryMap<isize>,
    ) -> SimpleRelation<'b, Child<'a, Worker<A>, u64>> {
        use differential_dataflow::AsCollection;
        use timely::dataflow::operators::Concatenate;
        
        let mut scope = nested.clone();
        let streams = self.plans.iter().map(|plan| {
            plan.implement(&mut scope, local_arrangements, global_arrangements)
                .tuples_by_symbols(&self.variables)
                .map(|(key, _vals)| key)
                .inner
        });

        let concat = nested.concatenate(streams).as_collection();

        SimpleRelation {
            symbols: self.variables.to_vec(),
            tuples: concat.distinct(),
        }
    }
}
