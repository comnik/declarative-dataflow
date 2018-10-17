//! Aggregate expression plan.

use timely::dataflow::scopes::Child;
use timely::dataflow::operators::{Map};
use timely::progress::timestamp::Timestamp;
use timely::communication::Allocate;
use timely::worker::Worker;

use differential_dataflow::AsCollection;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::{Group, Count, Consolidate, Threshold};

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
    /// Sum
    SUM,
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
    /// Relation symbols that determine the grouping.
    pub key_symbols: Vec<Var>,
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
        let tuples = relation.tuples_by_symbols(&self.key_symbols);

        match self.aggregation_fn {
            AggregationFn::MIN => {
                SimpleRelation {
                    symbols: self.variables.to_vec(),
                    tuples: tuples
                        .map(|(ref key, ref tuple)| {
                            let v = match tuple[0] {
                                Value::Number(num) => num,
                                _ => panic!("MIN can only be applied on type Number.")
                            };
                            (key.clone(), v)
                        })
                        .group(|_key, vals, output| {
                            let mut min = vals[0].0;
                            for &(val, _) in vals.iter() {
                                if min > val { min = val; }
                            }
                            // @TODO could preserve multiplicity of smallest value here
                            output.push((*min, 1));
                        })
                        .map(|(key, min)| key.iter().cloned()
                             .chain([Value::Number(min)].iter().cloned())
                             .collect())
                }
            },
            AggregationFn::MAX => {
                SimpleRelation {
                    symbols: self.variables.to_vec(),
                    tuples: tuples
                        .map(|(ref key, ref tuple)| {
                            let v = match tuple[0] {
                                Value::Number(num) => num,
                                _ => panic!("MAX can only be applied on type Number.")
                            };
                            (key.clone(), v)
                        })
                        .group(|_key, vals, output| {
                            let mut max = vals[0].0;
                            for &(val, _) in vals.iter() {
                                if max < val { max = val; }
                            }
                            // @TODO could preserve multiplicity of largest value here
                            output.push((*max, 1));
                        })
                        .map(|(key, max)| key.iter().cloned()
                             .chain([Value::Number(max)].iter().cloned())
                             .collect())
                }
            },
            AggregationFn::COUNT => {
                SimpleRelation {
                    symbols: self.variables.to_vec(),
                    tuples: tuples
                        .consolidate()
                        .distinct()
                        .group(|_key, input, output| {
                           let count = input.len();
                           output.push((count, 1))})
                        .map(|(key, count)| {
                            let c = count as i64;
                            let mut v = key.clone();
                            v.push(Value::Number(c));
                            v
                        })
                }
            },
            AggregationFn::SUM => {
                SimpleRelation {
                    symbols: self.variables.to_vec(),
                    tuples: tuples
                        .explode(|(ref key, ref tuple)| {
                            let v = match tuple[0] {
                                Value::Number(num) => num as isize,
                                _ => panic!("COUNT can only be applied to numbers")
                            };
                            // @todo clone?
                            Some((key.clone(), v))
                        })
                        .consolidate()
                        .inner
                        .map(|(data, time, delta)| (data.iter().cloned().chain([Value::Number(delta as i64)].iter().cloned()).collect(), time, delta))
                        .as_collection()
                }
            }
        }
    }
}
