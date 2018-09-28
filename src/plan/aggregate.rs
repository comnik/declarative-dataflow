//! Predicate expression plan.

use timely::dataflow::scopes::Child;
use timely::progress::timestamp::Timestamp;
use timely::communication::Allocate;
use timely::worker::Worker;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::{Group, Count};

use Relation;
use plan::Implementable;
use super::super::{ImplContext, RelationMap, QueryMap, SimpleRelation};
use super::super::{Value, Var, Plan};

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

/// A predicate expression plan stage.
#[derive(Deserialize, Clone, Debug)]
pub struct Aggregate {
    /// Logical predicate to apply.
    pub aggregation_fn: AggregationFn,
    /// TODO
    pub variables: Vec<Var>,
    /// Plan for the data source.
    pub plan: Box<Plan>
}

impl<'a, 'b, A: Allocate, T: Timestamp+Lattice> Implementable<'a, 'b, A, T> for Aggregate {

    fn implement(
        &self,
        db: &ImplContext<Child<'a, Worker<A>, T>>,
        nested: &mut Child<'b, Child<'a, Worker<A>, T>, u64>,
        relation_map: &RelationMap<'b, Child<'a, Worker<A>, T>>,
        queries: &mut QueryMap<T, isize>
    )
    -> SimpleRelation<'b, Child<'a, Worker<A>, T>> {

        let relation = self.plan.implement(db, nested, relation_map, queries);
        let tuples = relation.tuples_by_symbols(self.variables.clone());

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