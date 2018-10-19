//! Aggregate expression plan.
use std::collections::BinaryHeap;

use timely::communication::Allocate;
use timely::dataflow::operators::Map;
use timely::dataflow::scopes::child::{Child, Iterative};
use timely::worker::Worker;

use differential_dataflow::operators::{Consolidate, Group, Threshold};
use differential_dataflow::AsCollection;

use plan::Implementable;
use Relation;
use {QueryMap, RelationMap, SimpleRelation, Value, Var};

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
    /// Average
    AVG,
    ///Varianve
    VARIANCE,
    /// Standard deviation
    STDDEV,
    /// MEDIAN
    MEDIAN,
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
    fn implement<'a, 'b, A: Allocate>(
        &self,
        nested: &mut Iterative<'b, Child<'a, Worker<A>, u64>, u64>,
        local_arrangements: &RelationMap<Iterative<'b, Child<'a, Worker<A>, u64>, u64>>,
        global_arrangements: &mut QueryMap<isize>,
    ) -> SimpleRelation<'b, Child<'a, Worker<A>, u64>> {
        let relation = self
            .plan
            .implement(nested, local_arrangements, global_arrangements);
        let tuples = relation.tuples_by_symbols(&self.key_symbols);

        let prepare_unary = |(key, tuple): (Vec<Value>, Vec<Value>)| {
            let v = match tuple[0] {
                Value::Number(num) => num,
                _ => panic!("Can only be applied on type Number."),
            };
            (key.clone(), v)
        };

        match self.aggregation_fn {
            AggregationFn::MIN => {
                SimpleRelation {
                    symbols: self.variables.to_vec(),
                    tuples: tuples
                        .map(prepare_unary)
                        .group(|_key, vals, output| {
                            // @TODO vals is ordered, take the first
                            let mut min = vals[0].0;
                            for &(val, _) in vals.iter() {
                                if min > val {
                                    min = val;
                                }
                            }
                            // @TODO could preserve multiplicity of smallest value here
                            output.push((*min, 1));
                        }).map(|(key, min)| {
                            let mut v = key.clone();
                            v.push(Value::Number(min as i64));
                            v
                        }),
                }
            }
            AggregationFn::MAX => {
                SimpleRelation {
                    symbols: self.variables.to_vec(),
                    tuples: tuples
                        .map(prepare_unary)
                        .group(|_key, vals, output| {
                            // @TODO vals is sorted, just take the last
                            let mut max = vals[0].0;
                            for &(val, _) in vals.iter() {
                                if max < val {
                                    max = val;
                                }
                            }
                            // @TODO could preserve multiplicity of largest value here
                            output.push((*max, 1));
                        }).map(|(key, max)| {
                            let mut v = key.clone();
                            v.push(Value::Number(max as i64));
                            v
                        }),
                }
            }
            AggregationFn::COUNT => SimpleRelation {
                symbols: self.variables.to_vec(),
                tuples: tuples
                    .consolidate()
                    .group(|_key, input, output| {
                        let count = input.len();
                        output.push((count, 1))
                    }).map(|(key, count)| {
                        let mut v = key.clone();
                        v.push(Value::Number(count as i64));
                        v
                    }),
            },
            AggregationFn::SUM => {
                SimpleRelation {
                    symbols: self.variables.to_vec(),
                    tuples: tuples
                        .consolidate()
                        .distinct()
                        .explode(|(ref key, ref tuple)| {
                            let v = match tuple[0] {
                                Value::Number(num) => num as isize,
                                _ => panic!("COUNT can only be applied to numbers"),
                            };
                            // @todo clone?
                            Some((key.clone(), v))
                        }).consolidate()
                        .inner
                        .map(|(data, time, delta)| {
                            let mut v = data.clone();
                            v.push(Value::Number(delta as i64));
                            (v, time, delta)
                        }).as_collection(),
                }
            }
            AggregationFn::AVG => SimpleRelation {
                symbols: self.variables.to_vec(),
                tuples: tuples
                    .consolidate()
                    .map(prepare_unary)
                    .group(|_key, vals, output| {
                        let mut sum = 0;

                        for &(val, _) in vals.iter() {
                            sum += val;
                        }
                        let avg = sum / vals.len() as i64;
                        output.push((avg, 1));
                    }).map(|(key, avg)| {
                        let mut v = key.clone();
                        v.push(Value::Number(avg as i64));
                        v
                    }),
            },
            AggregationFn::VARIANCE => SimpleRelation {
                symbols: self.variables.to_vec(),
                tuples: tuples
                    .consolidate()
                    .map(prepare_unary)
                    .group(|_key, vals, output| {
                        let mut sum = 0;

                        for &(val, _) in vals.iter() {
                            sum += val;
                        }
                        let avg = sum / vals.len() as i64;

                        let mut var = 0;
                        for &(val, _) in vals.iter() {
                            var += (val - avg).pow(2);
                        }
                        var = var / vals.len() as i64;
                        output.push((var, 1));
                    }).map(|(key, var)| {
                        let mut v = key.clone();
                        v.push(Value::Number(var as i64));
                        v
                    }),
            },
            AggregationFn::STDDEV => SimpleRelation {
                symbols: self.variables.to_vec(),
                tuples: tuples
                    .consolidate()
                    .map(prepare_unary)
                    .group(|_key, vals, output| {
                        let mut sum = 0;

                        for &(val, _) in vals.iter() {
                            sum += val;
                        }
                        let avg = sum / vals.len() as i64;

                        let mut var = 0;
                        for &(val, _) in vals.iter() {
                            var += (val - avg).pow(2);
                        }
                        var = var / vals.len() as i64;
                        let std = (var as f64).sqrt() as i64;
                        output.push((std, 1));
                    }).map(|(key, std)| {
                        let mut v = key.clone();
                        v.push(Value::Number(std as i64));
                        v
                    }),
            },
            AggregationFn::MEDIAN => SimpleRelation {
                symbols: self.variables.to_vec(),
                tuples: tuples
                    .consolidate()
                    .map(prepare_unary)
                    .group(|_key, vals, output| {
                        let mut heap = BinaryHeap::new();

                        for &(val, _) in vals.iter() {
                            heap.push(*val);
                        }

                        let index = heap.len() / 2;
                        let median = heap.into_sorted_vec()[index];

                        output.push((median, 1));
                    }).map(|(key, med)| {
                        let mut v = key.clone();
                        v.push(Value::Number(med as i64));
                        v
                    }),
            },
        }
    }
}
