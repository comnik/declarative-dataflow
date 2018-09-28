//! Predicate expression plan.

use timely::dataflow::scopes::Child;
use timely::progress::timestamp::Timestamp;
use timely::communication::Allocate;
use timely::worker::Worker;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::Threshold;

use Relation;
use plan::Implementable;
use super::super::{ImplContext, RelationMap, QueryMap, SimpleRelation};
use super::super::{Var, Plan};

/// A predicate expression plan stage.
#[derive(Deserialize, Clone, Debug)]
pub struct Union {
    /// TODO
    pub variables: Vec<Var>,
    /// Plan for the data source.
    pub plans: Vec<Box<Plan>>
}

impl<'a, 'b, A: Allocate, T: Timestamp+Lattice> Implementable<'a, 'b, A, T> for Union {

    fn implement(
        &self,
        db: &ImplContext<Child<'a, Worker<A>, T>>,
        nested: &mut Child<'b, Child<'a, Worker<A>, T>, u64>,
        relation_map: &RelationMap<'b, Child<'a, Worker<A>, T>>,
        queries: &mut QueryMap<T, isize>
    )
    -> SimpleRelation<'b, Child<'a, Worker<A>, T>> {

        let first = self.plans.get(0).expect("Union requires at least one plan");
        let first_rel = first.implement(db, nested, relation_map, queries);
        let mut tuples = if first_rel.symbols() == &self.variables {
            first_rel.tuples()
        } else {
            first_rel.tuples_by_symbols(self.variables.clone())
                .map(|(key, _tuple)| key)
        };

        for plan in self.plans.iter() {
            let mut rel = plan.implement(db, nested, relation_map, queries);
            let mut plan_tuples = if rel.symbols() == &self.variables {
                rel.tuples()
            } else {
                rel.tuples_by_symbols(self.variables.clone())
                    .map(|(key, _tuple)| key)
            };

            tuples = tuples.concat(&plan_tuples);
        }

        SimpleRelation {
            symbols: self.variables.to_vec(),
            tuples: tuples.distinct()
        }


    }
}