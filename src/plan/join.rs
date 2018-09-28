//! Predicate expression plan.

use timely::dataflow::scopes::Child;
use timely::progress::timestamp::Timestamp;
use timely::communication::Allocate;
use timely::worker::Worker;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::operators::JoinCore;

use Relation;
use plan::Implementable;
use super::super::{ImplContext, RelationMap, QueryMap, SimpleRelation};
use super::super::{Var, Plan};

/// A predicate expression plan stage.
#[derive(Deserialize, Clone, Debug)]
pub struct Join {
    /// Plan for the left input.
    pub left_plan: Box<Plan>,
    /// Plan for the right input.
    pub right_plan: Box<Plan>,
    /// TODO
    pub variables: Vec<Var>,
}

impl<'a, 'b, A: Allocate, T: Timestamp+Lattice> Implementable<'a, 'b, A, T> for Join {

    fn implement(
        &self,
        db: &ImplContext<Child<'a, Worker<A>, T>>,
        nested: &mut Child<'b, Child<'a, Worker<A>, T>, u64>,
        relation_map: &RelationMap<'b, Child<'a, Worker<A>, T>>,
        queries: &mut QueryMap<T, isize>
    )
    -> SimpleRelation<'b, Child<'a, Worker<A>, T>> {

        let left = self.left_plan.implement(db, nested, relation_map, queries);
        let right = self.right_plan.implement(db, nested, relation_map, queries);

        let mut left_syms: Vec<Var> = left.symbols().clone();
        left_syms.retain(|&sym| {
            match self.variables.iter().position(|&v| sym == v) {
                None => true,
                Some(_) => false
            }
        });

        let mut right_syms: Vec<Var> = right.symbols().clone();
        right_syms.retain(|&sym| {
            match self.variables.iter().position(|&v| sym == v) {
                None => true,
                Some(_) => false
            }
        });

        // useful for inspecting join inputs
        //.inspect(|&((ref key, ref values), _, _)| { println!("right {:?} {:?}", key, values) })

        let tuples = left.tuples_by_symbols(self.variables.clone())
            .arrange_by_key()
            .join_core(&right.tuples_by_symbols(self.variables.clone()).arrange_by_key(), |key, v1, v2| {
                let mut vstar = Vec::with_capacity(key.len() + v1.len() + v2.len());

                vstar.extend(key.iter().cloned());
                vstar.extend(v1.iter().cloned());
                vstar.extend(v2.iter().cloned());

                Some(vstar)
            });

        let mut symbols: Vec<Var> = Vec::with_capacity(self.variables.len() + left_syms.len() + right_syms.len());
        symbols.extend(self.variables.iter().cloned());
        symbols.append(&mut left_syms);
        symbols.append(&mut right_syms);

        // let debug_syms: Vec<String> = symbols.iter().map(|x| x.to_string()).collect();
        // println!(debug_syms);

        SimpleRelation { symbols, tuples }

    }
}