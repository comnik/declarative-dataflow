//! Aggregate expression plan.

use timely::communication::Allocate;
use timely::dataflow::scopes::child::{Child, Iterative};
use timely::worker::Worker;

use differential_dataflow::difference::DiffPair;
use differential_dataflow::operators::{Consolidate, Count, Group, Threshold};
use differential_dataflow::operators::Join as JoinMap;

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

/// [WIP]
/// A plan stage applying the specified aggregation functions to
/// bindings for the specified symbols.
/// Given multiple aggregations we iterate and n-1 joins are applied to the results.
#[derive(Deserialize, Clone, Debug)]
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
}

impl<P: Implementable> Implementable for Aggregate<P> {
    fn implement<'a, 'b, A: Allocate>(
        &self,
        nested: &mut Iterative<'b, Child<'a, Worker<A>, u64>, u64>,
        local_arrangements: &RelationMap<Iterative<'b, Child<'a, Worker<A>, u64>, u64>>,
        global_arrangements: &mut QueryMap<isize>,
    ) -> SimpleRelation<'b, Child<'a, Worker<A>, u64>> {
        
        let relation = self.plan.implement(nested, local_arrangements, global_arrangements);

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
        let mut output_positions = Vec::new();
        
        for sym in self.aggregation_symbols.iter() {
            let output_index = variables.iter().position(|&v| *sym == v).unwrap();
            output_positions.push(output_index);

            variables[output_index] = 0;
        }

        let mut collections = Vec::new();

        // We iterate over all aggregations and keep track of the
        // resulting collections, s.t. they can be joined afterwards.
        for (i, aggregation_fn) in self.aggregation_fns.iter().enumerate() {
            let value_offset = value_offsets[i];

            match aggregation_fn {
                AggregationFn::MIN =>  {
                    let tuples = tuples
                        .map(move |(key, tuple)| {
                            let v = match tuple[value_offset] {
                                Value::Number(num) => num,
                                _ => panic!("MIN can only be applied on type Number."),
                            };
                            (key, v)
                        })
                        .group(|_key, vals, output| {
                            let min = vals[0].0;
                            output.push((*min, 1));
                        })
                        .map(move |(key, min)| {
                            (key, vec![Value::Number(min as i64)])
                        });
                    collections.push(tuples);
                },
                AggregationFn::MAX => {
                    let tuples = tuples
                        .map(move |(key, tuple)| {
                            let v = match tuple[value_offset] {
                                Value::Number(num) => num,
                                _ => panic!("MAX can only be applied on type Number."),
                            };
                            (key, v)
                        })
                        .group(|_key, vals, output| {
                            let max = vals[vals.len() - 1].0;
                            output.push((*max, 1));
                        })
                        .map(move |(key, max)| {
                            (key, vec![Value::Number(max as i64)])
                        });
                    collections.push(tuples);
                },
                AggregationFn::MEDIAN =>  {
                    let tuples = tuples
                        .map(move |(key, tuple)| {
                            let v = match tuple[value_offset] {
                                Value::Number(num) => num,
                                _ => panic!("MEDIAN can only be applied on type Number."),
                            };
                            (key, v)
                        })
                        .group(|_key, vals, output| {
                            let median = vals[vals.len() / 2].0;
                            output.push((*median, 1));
                        })
                        .map(move |(key, med)| {
                            (key, vec![Value::Number(med as i64)])
                        });
                    collections.push(tuples);
                },
                AggregationFn::COUNT =>  {
                    let tuples = tuples
                        .map(move |(key, tuple)| {
                            let v = tuple[value_offset].clone() ;
                            (key, v)
                        })
                        .group(|_key, input, output| output.push((input.len(), 1)))
                        .map(move |(key, count)| {
                            (key, vec![Value::Number(count as i64)])
                        });
                    collections.push(tuples);
                },
                AggregationFn::SUM =>  {
                    let tuples = tuples
                        .map(move |(key, tuple)| {
                            let v = match tuple[value_offset] {
                                Value::Number(num) => num,
                                _ => panic!("MEDIAN can only be applied on type Number."),
                            };
                            (key, v)
                        })
                        .consolidate()
                        .distinct()
                        .explode(|(key, val)| {
                            Some((key, val as isize))
                        })
                        .count()
                        .map(move |(key, count)| {
                            (key, vec![Value::Number(count as i64)])
                        });
                    collections.push(tuples);
                },
                AggregationFn::AVG =>  {
                    let tuples = tuples
                        .map(move |(key, tuple)| {
                            let v = match tuple[value_offset] {
                                Value::Number(num) => num,
                                _ => panic!("MEDIAN can only be applied on type Number."),
                            };
                            (key, v)
                        })
                        .consolidate()
                        .distinct()
                        .explode(move |(key, val)| {
                            Some((key, DiffPair::new(val as isize, 1)))
                        })
                        .count()
                        .map(move |(key, diff_pair)| {
                            (key, vec![Value::Rational32(Ratio::new(
                                diff_pair.element1 as i32,
                                diff_pair.element2 as i32))])
                        });
                    collections.push(tuples);
                },
                AggregationFn::VARIANCE => {
                    let tuples = tuples
                        .map(move |(key, tuple)| {
                            let v = match tuple[value_offset] {
                                Value::Number(num) => num,
                                _ => panic!("MEDIAN can only be applied on type Number."),
                            };
                            (key, v)
                        })
                        .consolidate()
                        .distinct()
                        .explode(move |(key, val)| {
                            Some((key, DiffPair::new(DiffPair::new(val as isize * val as isize, val as isize), 1)))
                        })
                        .count()
                        .map(move |(key, diff_pair)| {
                            let sum_square = diff_pair.element1.element1 as i32;
                            let sum = diff_pair.element1.element2 as i32;
                            let c = diff_pair.element2 as i32;
                            (key, vec![Value::Rational32(
                                Rational32::new(sum_square, c) - Rational32::new(sum, c).pow(2))])
                        });
                    collections.push(tuples);
                },
            };
        }

        if collections.len() == 1 {
            let output_position = output_positions[0];
            SimpleRelation{
                symbols: self.variables.to_vec(),
                tuples: collections[0]
                    .map(move |(key, val)| {
                        let mut k = key.clone();
                        let v = val[0].clone();
                        k.insert(output_position, v);
                        k
                    })
            }
        }
        else {
            // @TODO replace this with a join application
            let left = collections.remove(0);
            let tuples = collections.iter()
                .fold(left, |coll, next| coll.join_map(&next, |key, v1, v2| {
                    let mut val = v1.clone();
                    val.append(&mut v2.clone());
                    (key.clone(), val)
                }));
            
            SimpleRelation {
                symbols: self.variables.to_vec(),
                tuples: tuples.map(move |(key, vals)|{
                    let mut v = key.clone();
                    for (i, val) in vals.iter().enumerate(){
                        v.insert(output_positions[i], val.clone())
                    }
                    v
                })
            }
        }
    }
}
