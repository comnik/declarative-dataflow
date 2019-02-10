//! Aggregate expression plan.

use timely::dataflow::scopes::child::Iterative;
use timely::dataflow::Scope;

use differential_dataflow::difference::DiffPair;
use differential_dataflow::operators::Join as JoinMap;
use differential_dataflow::operators::{Count, Group};

use crate::binding::Binding;
use crate::plan::{Dependencies, ImplContext, Implementable};
use crate::{CollectionRelation, Relation, Value, Var, VariableMap};

use num_rational::{Ratio, Rational32};

/// Permitted aggregation function.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
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

/// [WIP] A plan stage applying the specified aggregation functions to
/// bindings for the specified symbols. Given multiple aggregations
/// we iterate and n-1 joins are applied to the results.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub struct Aggregate<P: Implementable> {
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
    /// With symbols
    pub with_symbols: Vec<Var>,
}

impl<P: Implementable> Implementable for Aggregate<P> {
    fn dependencies(&self) -> Dependencies {
        self.plan.dependencies()
    }

    fn into_bindings(&self) -> Vec<Binding> {
        self.plan.into_bindings()
    }

    fn implement<'b, S: Scope<Timestamp = u64>, I: ImplContext>(
        &self,
        nested: &mut Iterative<'b, S, u64>,
        local_arrangements: &VariableMap<Iterative<'b, S, u64>>,
        context: &mut I,
    ) -> CollectionRelation<'b, S> {
        let relation = self.plan.implement(nested, local_arrangements, context);

        // We split the incoming tuples into their (key, value) parts.
        let tuples = relation.tuples_by_symbols(&self.key_symbols);

        // For each aggregation function that is to be applied, we
        // need to determine the index (into the value part of each
        // tuple) at which its argument is to be found.

        let mut value_offsets = Vec::new();
        let mut seen = Vec::new();

        for sym in self.aggregation_symbols.iter() {
            if !seen.contains(&sym) {
                seen.push(&sym);
                value_offsets.push(seen.len() - 1);
            } else {
                value_offsets.push(seen.iter().position(|&v| sym == v).unwrap());
            }
        }

        // Users can specify weird find clauses like [:find ?key1 (min ?v1) ?key2]
        // and we would like to avoid an extra projection. Thus, we pre-compute
        // the correct output offset for each aggregation.

        let mut variables = self.variables.clone();
        let mut output_offsets = Vec::new();

        for sym in self.aggregation_symbols.iter() {
            let output_index = variables.iter().position(|&v| *sym == v).unwrap();
            output_offsets.push(output_index);

            variables[output_index] = 0;
        }

        let mut collections = Vec::new();

        // We iterate over all aggregations and keep track of the
        // resulting collections, s.t. they can be joined afterwards.
        for (i, aggregation_fn) in self.aggregation_fns.iter().enumerate() {
            let value_offset = value_offsets[i];
            let with_length = self.with_symbols.len();

            // Access the right value for the given iteration loop and extend possible with-values.
            let prepare_unary = move |(key, tuple): (Vec<Value>, Vec<Value>)| {
                let value = &tuple[value_offset];
                let mut v = vec![value.clone()];

                // With-symbols are always the last elements in the
                // value part of each tuple, given they are specified.
                // We append these, s.t. we consolidate correctly.
                if with_length > 0 {
                    v.extend(tuple.iter().rev().take(with_length).cloned());
                }

                (key, v)
            };

            match aggregation_fn {
                AggregationFn::MIN => {
                    let tuples = tuples.map(prepare_unary).group(|_key, vals, output| {
                        let min = &vals[0].0[0];
                        output.push((vec![min.clone()], 1));
                    });
                    collections.push(tuples);
                }
                AggregationFn::MAX => {
                    let tuples = tuples.map(prepare_unary).group(|_key, vals, output| {
                        let max = &vals[vals.len() - 1].0[0];
                        output.push((vec![max.clone()], 1));
                    });
                    collections.push(tuples);
                }
                AggregationFn::MEDIAN => {
                    let tuples = tuples.map(prepare_unary).group(|_key, vals, output| {
                        let median = &vals[vals.len() / 2].0[0];
                        output.push((vec![median.clone()], 1));
                    });
                    collections.push(tuples);
                }
                AggregationFn::COUNT => {
                    let tuples = tuples.map(prepare_unary).group(|_key, input, output| {
                        let mut total_count = 0;
                        for (_, count) in input.iter() {
                            total_count += count;
                        }

                        output.push((vec![Value::Number(total_count as i64)], 1))
                    });
                    collections.push(tuples);
                }
                AggregationFn::SUM => {
                    let tuples = tuples
                        .map(prepare_unary)
                        .explode(|(key, val)| {
                            let v = match val[0] {
                                Value::Number(num) => num,
                                _ => panic!("SUM can only be applied on type Number."),
                            };
                            Some((key, v as isize))
                        })
                        .count()
                        .map(move |(key, count)| (key, vec![Value::Number(count as i64)]));
                    collections.push(tuples);
                }
                AggregationFn::AVG => {
                    let tuples = tuples
                        .map(prepare_unary)
                        .explode(move |(key, val)| {
                            let v = match val[0] {
                                Value::Number(num) => num,
                                _ => panic!("AVG can only be applied on type Number."),
                            };
                            Some((key, DiffPair::new(v as isize, 1)))
                        })
                        .count()
                        .map(move |(key, diff_pair)| {
                            (
                                key,
                                vec![Value::Rational32(Ratio::new(
                                    diff_pair.element1 as i32,
                                    diff_pair.element2 as i32,
                                ))],
                            )
                        });
                    collections.push(tuples);
                }
                AggregationFn::VARIANCE => {
                    let tuples = tuples
                        .map(prepare_unary)
                        .explode(move |(key, val)| {
                            let v = match val[0] {
                                Value::Number(num) => num,
                                _ => panic!("VARIANCE can only be applied on type Number."),
                            };
                            Some((
                                key,
                                DiffPair::new(
                                    DiffPair::new(v as isize * v as isize, v as isize),
                                    1,
                                ),
                            ))
                        })
                        .count()
                        .map(move |(key, diff_pair)| {
                            let sum_square = diff_pair.element1.element1 as i32;
                            let sum = diff_pair.element1.element2 as i32;
                            let c = diff_pair.element2 as i32;
                            (
                                key,
                                vec![Value::Rational32(
                                    Rational32::new(sum_square, c) - Rational32::new(sum, c).pow(2),
                                )],
                            )
                        });
                    collections.push(tuples);
                }
            };
        }

        if collections.len() == 1 {
            let output_index = output_offsets[0];
            CollectionRelation {
                symbols: self.variables.to_vec(),
                tuples: collections[0].map(move |(key, val)| {
                    let mut k = key.clone();
                    let v = val[0].clone();
                    k.insert(output_index, v);
                    k
                }),
            }
        } else {
            // @TODO replace this with a join application
            let left = collections.remove(0);
            let tuples = collections.iter().fold(left, |coll, next| {
                coll.join_map(&next, |key, v1, v2| {
                    let mut val = v1.clone();
                    val.append(&mut v2.clone());
                    (key.clone(), val)
                })
            });

            CollectionRelation {
                symbols: self.variables.to_vec(),
                tuples: tuples.map(move |(key, vals)| {
                    let mut v = key.clone();
                    for (i, val) in vals.iter().enumerate() {
                        v.insert(output_offsets[i], val.clone())
                    }
                    v
                }),
            }
        }
    }
}
