//! Equijoin expression plan.

use std::collections::HashMap;

// use timely::dataflow::operators::Inspect;
use timely::dataflow::Scope;
use timely::dataflow::scopes::child::Iterative;

use differential_dataflow::operators::Join as JoinMap;

use plan::Implementable;
use Relation;
use {RelationHandle, VariableMap, SimpleRelation, Var};

/// A plan stage joining two source relations on the specified
/// symbols. Throws if any of the join symbols isn't bound by both
/// sources.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Join<P1: Implementable, P2: Implementable> {
    /// TODO
    pub variables: Vec<Var>,
    /// Plan for the left input.
    pub left_plan: Box<P1>,
    /// Plan for the right input.
    pub right_plan: Box<P2>,
}

impl<P1: Implementable, P2: Implementable> Implementable for Join<P1, P2> {
    fn implement<'b, S: Scope<Timestamp = u64>>(
        &self,
        nested: &mut Iterative<'b, S, u64>,
        local_arrangements: &VariableMap<Iterative<'b, S, u64>>,
        global_arrangements: &mut HashMap<String, RelationHandle>,
    ) -> SimpleRelation<'b, S> {
        let left = self.left_plan
            .implement(nested, local_arrangements, global_arrangements);
        let right = self.right_plan
            .implement(nested, local_arrangements, global_arrangements);

        let symbols = self.variables
            .iter()
            .cloned()
            .chain(
                left.symbols()
                    .iter()
                    .filter(|x| !self.variables.contains(x))
                    .cloned(),
            )
            .chain(
                right
                    .symbols()
                    .iter()
                    .filter(|x| !self.variables.contains(x))
                    .cloned(),
            )
            .collect();

        let tuples = left
            .tuples_by_symbols(&self.variables)
            // .inspect(|&((ref key, ref values), _, _)| { println!("LEFT {:?} {:?}", key, values) })
            .join_map(
                &right.tuples_by_symbols(&self.variables),
                |key, v1, v2| {
                    key.iter()
                        .cloned()
                        .chain(v1.iter().cloned())
                        .chain(v2.iter().cloned())
                        .collect()
                },
            );
            // .inspect(|x| { println!("JOINED {:?}", x) })
            

        // let debug_syms: Vec<String> = symbols.iter().map(|x| x.to_string()).collect();
        // println!(debug_syms);

        SimpleRelation { symbols, tuples }
    }
}
