//! Aggregate expression plan.

use timely::dataflow::scopes::Child;
use timely::progress::timestamp::Timestamp;
use timely::communication::Allocate;
use timely::worker::Worker;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::{Group, Count};

use Relation;
use plan::Implementable;
use {RelationMap, QueryMap, SimpleRelation, Value, Var};

/// Permitted aggregation function.
#[derive(Deserialize, Clone, Debug)]
pub enum AggregationFn {
    /// Minimum
    MIN,
    /// Maximum
    MAX,
    /// Count
    COUNT,
}

/// [WIP]
/// A plan stage applying the specified aggregation function to
/// bindings for the specified symbols. Very WIP.
#[derive(Deserialize, Clone, Debug)]
pub struct Aggregate<P: Implementable> {
    /// TODO
    pub variables: Vec<Var>,
    /// Plan for the data source.
    pub plan: Box<P>,
    /// Logical predicate to apply.
    pub aggregation_fn: AggregationFn,
}

impl<P: Implementable> Implementable for Aggregate<P> {

    fn implement<'a, 'b, A: Allocate, T: Timestamp+Lattice>(
        &self,
        nested: &mut Child<'b, Child<'a, Worker<A>, T>, u64>,
        local_arrangements: &RelationMap<'b, Child<'a, Worker<A>, T>>,
        global_arrangements: &mut QueryMap<T, isize>
    )
    -> SimpleRelation<'b, Child<'a, Worker<A>, T>> {

        let relation = self.plan.implement(nested, local_arrangements, global_arrangements);
        let tuples = relation.tuples_by_symbols(&self.variables);

        match self.aggregation_fn {
            AggregationFn::MIN => {
                SimpleRelation {
                    symbols: self.variables.to_vec(),
                    tuples: tuples
                        // @TODO use rest of tuple as key
                        .map(|(ref key, ref _tuple)| ((), match key[0] {
                            Value::Number(v) => v,
                            _ => panic!("MIN can only be applied on type Number.")
                        }))
                        .group(|_key, vals, output| {
                            let mut min = vals[0].0;
                            for &(val, _) in vals.iter() {
                                if min > val { min = val; }
                            }
                            // @TODO could preserve multiplicity of smallest value here
                            output.push((*min, 1));
                        })
                        .map(|(_, min)| vec![Value::Number(min)])
                }
            },
            AggregationFn::MAX => {
                SimpleRelation {
                    symbols: self.variables.to_vec(),
                    tuples: tuples
                    // @TODO use rest of tuple as key
                        .map(|(ref key, ref _tuple)| ((), match key[0] {
                            Value::Number(v) => v,
                            _ => panic!("MAX can only be applied on type Number.")
                        }))
                        .group(|_key, vals, output| {
                            let mut max = vals[0].0;
                            for &(val, _) in vals.iter() {
                                if max < val { max = val; }
                            }
                            output.push((*max, 1));
                        })
                        .map(|(_, max)| vec![Value::Number(max)])
                }
            },
            AggregationFn::COUNT => {
                SimpleRelation {
                    symbols: self.variables.to_vec(),
                    tuples: tuples
                    // @TODO use rest of tuple as key
                        .map(|(ref _key, ref _tuple)| ())
                        .count()
                        .map(|(_, count)| vec![Value::Number(count as i64)])
                }
            }
        }
    }
}
