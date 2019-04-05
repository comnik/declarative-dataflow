//! Aggregate expression plan using Diffvector and Monoids

use differential_dataflow::difference::Monoid;
use std::ops::AddAssign;

use timely::dataflow::scopes::child::Iterative;
use timely::dataflow::Scope;
use timely::order::TotalOrder;
use timely::progress::Timestamp;

use differential_dataflow::difference::DiffVector;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::{Count};

use crate::binding::Binding;
use crate::plan::{Dependencies, ImplContext, Implementable};
use crate::{CollectionRelation, Relation, ShutdownHandle, Value, Var, VariableMap};

/// Permitted aggregation function.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub enum AggregationFn {
    /// Count
    COUNT,
    /// Sum
    SUM,
}

#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
struct Max {
    /// Associated value
    pub value: u32,
}

impl<'a> AddAssign<&'a Self> for Max {
    fn add_assign(&mut self, rhs: &Max) {
        *self = Max {
            value: std::cmp::max(self.value, rhs.value),
        }
    }
}

impl Monoid for Max {
    fn zero() -> Max {
        Max { value: 0 }
    }
}

#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
enum Diff {
    Maximum(Max),
    Sum(i64),
}

// impl<'a> AddAssign<&'a Self> for Diff {
//     fn add_assign(&mut self, rhs: &Self) {
//         match self {
//             Diff::Maximum(max) => max.add_assign(rhs),
//             Diff::Sum(sum) => sum.add_assign(rhs),
//         }
//     }
// }

// impl Monoid for Diff {
//     fn zero() {
//         match self {
//             Diff::Maximum(max) => max.zero(),
//             Diff::Sum(sum) => 0,
//         }
//     }
// }

/// [WIP] A plan stage applying the specified aggregation functions to
/// bindings for the specified variables. Given multiple aggregations
/// we iterate and n-1 joins are applied to the results.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub struct Aggregate<P: Implementable> {
    /// TODO
    pub variables: Vec<Var>,
    /// Plan for the data source.
    pub plan: Box<P>,
    /// Logical predicate to apply.
    pub aggregation_fns: Vec<AggregationFn>,
    /// Relation variables that determine the grouping.
    pub key_variables: Vec<Var>,
    /// Aggregation variables
    pub aggregation_variables: Vec<Var>,
    /// With variables
    pub with_variables: Vec<Var>,
}

impl<P: Implementable> Implementable for Aggregate<P> {
    fn dependencies(&self) -> Dependencies {
        self.plan.dependencies()
    }

    fn into_bindings(&self) -> Vec<Binding> {
        self.plan.into_bindings()
    }

    fn implement<'b, T, I, S>(
        &self,
        nested: &mut Iterative<'b, S, u64>,
        local_arrangements: &VariableMap<Iterative<'b, S, u64>>,
        context: &mut I,
    ) -> (CollectionRelation<'b, S>, ShutdownHandle)
    where
        T: Timestamp + Lattice + TotalOrder,
        I: ImplContext<T>,
        S: Scope<Timestamp = T>,
    {
        let (relation, shutdown_handle) = self.plan.implement(nested, local_arrangements, context);

        // We split the incoming tuples into their (key, value) parts.
        let tuples = relation.tuples_by_variables(&self.key_variables);

        // For each aggregation function that is to be applied, we
        // need to determine the index (into the value part of each
        // tuple) at which its argument is to be found.

        let mut value_offsets = Vec::new();
        let mut seen = Vec::new();

        for variable in self.aggregation_variables.iter() {
            if !seen.contains(&variable) {
                seen.push(&variable);
                value_offsets.push(seen.len() - 1);
            } else {
                value_offsets.push(seen.iter().position(|&v| variable == v).unwrap());
            }
        }

        // Users can specify weird find clauses like [:find ?key1 (min ?v1) ?key2]
        // and we would like to avoid an extra projection. Thus, we pre-compute
        // the correct output offset for each aggregation.

        let mut variables = self.variables.clone();
        let mut output_offsets = Vec::new();

        for variable in self.aggregation_variables.iter() {
            let output_index = variables.iter().position(|&v| *variable == v).unwrap();
            output_offsets.push(output_index);

            variables[output_index] = 0;
        }
        let agg_fns = self.aggregation_fns.clone();
        (
            CollectionRelation {
                variables: self.variables.to_vec(),
                tuples: tuples
                    .explode(move |(key, values)| {
                        let mut v = Vec::with_capacity(agg_fns.len());
                        for (agg, index) in agg_fns.iter().zip(value_offsets.clone()) {
                            v.push(match agg {
                                AggregationFn::COUNT => match values[index] {
                                    _ => 1 as isize,
                                },
                                AggregationFn::SUM => match values[index] {
                                    Value::Number(val) => val as isize,
                                    _ => panic!("Cannot `sum` non numbers"),
                                },
                            })
                        }
                        Some((key, DiffVector::new(v))
                    })
                    .count()
                    .map(|(key, vals)| {
                        let mut v = key.clone();
                        let mut diffs: Vec<Value> =
                            vals.into_iter().map(|x| Value::Number(x as i64)).collect();
                        v.append(&mut diffs);
                        v
                    })
                    ,
            },
            shutdown_handle,
        )
    }
}
