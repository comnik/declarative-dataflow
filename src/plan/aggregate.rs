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
                    .map(|(key, diff_pair)| {
                        let mut v = key.clone();
                        v.push(Value::Rational32(Ratio::new(
                            diff_pair.element1 as i32,
                            diff_pair.element2 as i32,
                        )));
                        v
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
                    .map(|(key, diff_pair)| {
                        let mut v = key.clone();
                        let sum_square = diff_pair.element1.element1 as i32;
                        let sum = diff_pair.element1.element2 as i32;
                        let c = diff_pair.element2 as i32;
                        v.push(Value::Rational32(
                            Rational32::new(sum_square, c) - Rational32::new(sum, c).pow(2),
                        ));
                        v
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
