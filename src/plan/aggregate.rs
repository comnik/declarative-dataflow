//! Aggregate expression plan.

use timely::communication::Allocate;
use timely::dataflow::scopes::child::{Child, Iterative};
use timely::worker::Worker;

use differential_dataflow::difference::DiffPair;
use differential_dataflow::operators::{Consolidate, Count, Group, Threshold};

use plan::Implementable;
use Relation;
use {QueryMap, RelationMap, SimpleRelation, Value, Var};

use num_rational::{Ratio, Rational32};

/// Permitted aggregation function.
#[derive(Deserialize, Clone, Debug)]
pub enum AggregationFn {
    /// Minimum
    MIN,
    /// Maximum
    MAX,
    /// MEDIAN
    MEDIAN,
    /// Count
    COUNT,
    /// Sum
    SUM,
    /// Average
    AVG,
    ///Varianve
    VARIANCE,
    // /// Standard deviation
    // STDDEV,
}

fn reorder(result_vec: Vec<Value>, positions: Vec<usize>) -> Vec<Value> {
    let mut v: Vec<_> = result_vec.iter().zip(positions.iter()).collect();
    v.sort_by(|a, b| a.1.cmp(b.1));
    v.iter().map(|x| x.0.clone()).collect()
}

/// [WIP]
/// A plan stage applying the specified single aggregation function to
/// bindings for the specified symbols.
/// There are different plans for multiple and single aggregations.
/// Single aggregations are implemented more efficiently, e.g. using differentials diffpair and leveraging the order of grouped tuples.
/// All multiple aggregations currently are done inside one grouped context, which does not allow the former and
/// thus is less performant.
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
    /// Aggregation symbol
    pub aggregation_symbol: Vec<Var>,
}

impl<P: Implementable> Implementable for Aggregate<P> {
    fn implement<'a, 'b, A: Allocate>(
        &self,
        nested: &mut Iterative<'b, Child<'a, Worker<A>, u64>, u64>,
        local_arrangements: &RelationMap<Iterative<'b, Child<'a, Worker<A>, u64>, u64>>,
        global_arrangements: &mut QueryMap<isize>,
    ) -> SimpleRelation<'b, Child<'a, Worker<A>, u64>> {
        let relation = self.plan
            .implement(nested, local_arrangements, global_arrangements);
        let tuples = relation.tuples_by_symbols(&self.key_symbols);

        let prepare_unary = |(key, tuple): (Vec<Value>, Vec<Value>)| {
            let v = match tuple[0] {
                Value::Number(num) => num,
                _ => panic!("Can only be applied on type Number."),
            };
            (key.clone(), v)
        };

        let mut result_position = self.key_symbols.clone();
        result_position.append(&mut self.aggregation_symbol.clone());

        let mut variables = self.variables.clone();

        let mut sym_position = Vec::new();

        for sym in result_position.iter() {
            let i = variables.iter().position(|&v| *sym == v).unwrap();
            sym_position.push(i);
            variables[i] = 0;
        }

        match self.aggregation_fn {
            AggregationFn::MIN => SimpleRelation {
                symbols: self.variables.to_vec(),
                tuples: tuples
                    .map(prepare_unary)
                    .group(|_key, vals, output| {
                        let min = vals[0].0;
                        output.push((*min, 1));
                    })
                    .map(move |(key, min)| {
                        let mut v = key.clone();
                        v.push(Value::Number(min as i64));
                        reorder(v, sym_position.clone())
                    }),
            },
            AggregationFn::MAX => SimpleRelation {
                symbols: self.variables.to_vec(),
                tuples: tuples
                    .map(prepare_unary)
                    .group(|_key, vals, output| {
                        let max = vals[vals.len() - 1].0;
                        output.push((*max, 1));
                    })
                    .map(move |(key, max)| {
                        let mut v = key.clone();
                        v.push(Value::Number(max as i64));
                        reorder(v, sym_position.clone())
                    }),
            },
            AggregationFn::MEDIAN => SimpleRelation {
                symbols: self.variables.to_vec(),
                tuples: tuples
                    .map(prepare_unary)
                    .group(|_key, vals, output| {
                        let median = vals[vals.len() / 2].0;
                        output.push((*median, 1));
                    })
                    .map(move |(key, med)| {
                        let mut v = key.clone();
                        v.push(Value::Number(med as i64));
                        reorder(v, sym_position.clone())
                    }),
            },
            AggregationFn::COUNT => SimpleRelation {
                symbols: self.variables.to_vec(),
                tuples: tuples
                    .group(|_key, input, output| output.push((input.len(), 1)))
                    .map(move |(key, count)| {
                        let mut v = key.clone();
                        v.push(Value::Number(count as i64));
                        reorder(v, sym_position.clone())
                    }),
            },
            AggregationFn::SUM => SimpleRelation {
                symbols: self.variables.to_vec(),
                tuples: tuples
                    .consolidate()
                    .distinct()
                    .explode(|(key, tuple)| {
                        let v = match tuple[0] {
                            Value::Number(num) => num as isize,
                            _ => panic!("SUM can only be applied to numbers"),
                        };
                        Some((key, v))
                    })
                    .count()
                    .map(move |(key, count)| {
                        let mut v = key.clone();
                        v.push(Value::Number(count as i64));
                        reorder(v, sym_position.clone())
                    }),
            },
            AggregationFn::AVG => SimpleRelation {
                symbols: self.variables.to_vec(),
                tuples: tuples
                    .consolidate()
                    .distinct()
                    .explode(|(key, tuple)| {
                        let v = match tuple[0] {
                            Value::Number(num) => num as isize,
                            _ => panic!("AVG can only be applied to numbers"),
                        };
                        Some((key, DiffPair::new(v, 1)))
                    })
                    .count()
                    .map(move |(key, diff_pair)| {
                        let mut v = key.clone();
                        v.push(Value::Rational32(Ratio::new(
                            diff_pair.element1 as i32,
                            diff_pair.element2 as i32,
                        )));
                        reorder(v, sym_position.clone())
                    }),
            },
            AggregationFn::VARIANCE => SimpleRelation {
                symbols: self.variables.to_vec(),
                tuples: tuples
                    .consolidate()
                    .distinct()
                    .explode(|(key, tuple)| {
                        let v = match tuple[0] {
                            Value::Number(num) => num as isize,
                            _ => panic!("VAR can only be applied to numbers"),
                        };
                        Some((key, DiffPair::new(DiffPair::new(v * v, v), 1)))
                    })
                    .count()
                    .map(move |(key, diff_pair)| {
                        let mut v = key.clone();
                        let sum_square = diff_pair.element1.element1 as i32;
                        let sum = diff_pair.element1.element2 as i32;
                        let c = diff_pair.element2 as i32;
                        v.push(Value::Rational32(
                            Rational32::new(sum_square, c) - Rational32::new(sum, c).pow(2),
                        ));
                        reorder(v, sym_position.clone())
                    }),
            },
            // AggregationFn::STDDEV => SimpleRelation {
            //     symbols: self.variables.to_vec(),
            //     tuples: tuples
            //         .consolidate()
            //         .distinct()
            //         .explode(|(key, tuple)| {
            //             let v = match tuple[0] {
            //                 Value::Number(num) => num as isize,
            //                 _ => panic!("STDDEV can only be applied to numbers"),
            //             };
            //             Some((key, DiffPair::new(DiffPair::new(v*v, v), 1)))})
            //         .count()
            //         .map(|(key, diff_pair)| {
            //             let mut v = key.clone();
            //             let sum_square = diff_pair.element1.element1 as f64;
            //             let sum = diff_pair.element1.element2 as f64;
            //             let c = diff_pair.element2 as f64;
            //
            //             v.push(Value::Rational32(Rational32::from_float((sum_square/c - (sum/c).powi(2)).sqrt()).unwrap()));
            //             v
            //         }),
            // },
        }
    }
}

/// [WIP]
/// A plan stage applying multiple aggregations to
/// bindings for the specified symbols. Very WIP.
#[derive(Deserialize, Clone, Debug)]
pub struct AggregateMulti<P: Implementable> {
    /// TODO
    pub variables: Vec<Var>,
    /// Plan for the data source.
    pub plan: Box<P>,
    /// Logical predicate to apply.
    pub aggregation_fns: Vec<AggregationFn>,
    /// Relation symbols that determine the grouping.
    pub key_symbols: Vec<Var>,
    /// Aggregation symbols
    pub aggregation_symbols: Vec<Var>,
}

impl<P: Implementable> Implementable for AggregateMulti<P> {
    fn implement<'a, 'b, A: Allocate>(
        &self,
        nested: &mut Iterative<'b, Child<'a, Worker<A>, u64>, u64>,
        local_arrangements: &RelationMap<Iterative<'b, Child<'a, Worker<A>, u64>, u64>>,
        global_arrangements: &mut QueryMap<isize>,
    ) -> SimpleRelation<'b, Child<'a, Worker<A>, u64>> {
        let relation = self.plan
            .implement(nested, local_arrangements, global_arrangements);

        let mut value_offsets = Vec::new();
        let mut seen = Vec::new();

        for sym in self.aggregation_symbols.iter() {
            if !seen.contains(&sym) {
                seen.push(&sym);
                value_offsets.push(seen.iter().position(|&v| sym == v).unwrap());
            } else {
                value_offsets.push(seen.iter().position(|&v| sym == v).unwrap());
            }
        }

        let mut result_position = self.key_symbols.clone();
        result_position.append(&mut self.aggregation_symbols.clone());

        let mut variables = self.variables.clone();

        let mut sym_position = Vec::new();

        for sym in result_position.iter() {
            let i = variables.iter().position(|&v| *sym == v).unwrap();
            sym_position.push(i);
            variables[i] = 0;
        }

        let tuples = relation.tuples_by_symbols(&self.key_symbols);

        let aggregation_fns = self.aggregation_fns.clone();

        SimpleRelation {
            symbols: self.variables.to_vec(),
            tuples: tuples
                .group(move |_key, vals, output| {
                    let mut results = Vec::new();
                    for (i, agg_fn) in aggregation_fns.iter().enumerate() {
                        let mut values: Vec<Value> =
                            vals.iter().map(|x| x.0[value_offsets[i]].clone()).collect();
                        values.sort();
                        values.dedup();
                        match agg_fn {
                            AggregationFn::MIN => {
                                let min = values[0].clone();
                                results.push(min);
                            }
                            AggregationFn::MAX => {
                                let max = values[values.len() - 1].clone();
                                results.push(max);
                            }
                            AggregationFn::MEDIAN => {
                                let median = values[values.len() / 2].clone();
                                results.push(median);
                            }
                            AggregationFn::COUNT => {
                                results.push(Value::Number(values.len() as i64));
                            }
                            AggregationFn::SUM => {
                                let mut sum = 0;
                                for value in values.iter() {
                                    let val = match value {
                                        Value::Number(num) => num,
                                        _ => panic!("SUM can only be applied to type Number!"),
                                    };
                                    sum += val;
                                }
                                results.push(Value::Number(sum as i64));
                            }
                            AggregationFn::AVG => {
                                let mut sum = 0;
                                for value in values.iter() {
                                    let val = match value {
                                        Value::Number(num) => num,
                                        _ => panic!("AVG can only be applied to type Number!"),
                                    };
                                    sum += val;
                                }
                                results.push(Value::Rational32(Ratio::new(
                                    sum as i32,
                                    values.len() as i32,
                                )));
                            } 
                            AggregationFn::VARIANCE => {
                                let mut sum = 0;
                                for value in values.iter() {
                                    let val = match value {
                                        Value::Number(num) => num,
                                        _ => panic!("VARIANCE can only be applied to type Number!"),
                                    };
                                    sum += val;
                                }
                                let avg = Ratio::new(sum as i32, vals.len() as i32);
                                let mut var = Ratio::new(0, 1);
                                for value in values.iter() {
                                    let val = match value {
                                        Value::Number(num) => num,
                                        _ => panic!("VARIANCE can only be applied to type Number!"),
                                    };
                                    var += (Ratio::new(*val as i32, 1) - avg).pow(2);
                                }
                                results.push(Value::Rational32(var / vals.len() as i32));
                                }
                        }
                    }
                    output.push((results, 1));
                })
                .map(move |(key, vals)| {
                    let mut v = key.clone();
                    v.append(&mut vals.clone());
                    reorder(v, sym_position.clone())
                }),
        }
    }
}
