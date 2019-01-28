//! Pull expression plan, but without nesting.

use timely::dataflow::operators::Concatenate;
use timely::dataflow::scopes::child::Iterative;
use timely::dataflow::Scope;
use timely::order::Product;
use timely::progress::Timestamp;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::AsCollection;

use crate::binding::AsBinding;
use crate::plan::{Dependencies, ImplContext, Implementable};
use crate::{Aid, Value, Var};
use crate::{CollectionRelation, Implemented, Relation, ShutdownHandle, VariableMap};

/// A plan stage for extracting all matching [e a v] tuples for a
/// given set of attributes and an input relation specifying entities.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub struct PullLevel<P: Implementable> {
    /// TODO
    pub variables: Vec<Var>,
    /// Plan for the input relation.
    pub plan: Box<P>,
    /// Eid variable.
    pub pull_variable: Var,
    /// Attributes to pull for the input entities.
    pub pull_attributes: Vec<Aid>,
    /// Attribute names to distinguish plans of the same
    /// length. Useful to feed into a nested hash-map directly.
    pub path_attributes: Vec<Aid>,
}

/// A plan stage for pull queries split into individual paths. So
/// `[:parent/name {:parent/child [:child/name]}]` would be
/// represented as:
///
/// (?parent)                      <- [:parent/name] | no constraints
/// (?parent :parent/child ?child) <- [:child/name]  | [?parent :parent/child ?child]
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub struct Pull<P: Implementable> {
    /// TODO
    pub variables: Vec<Var>,
    /// Individual paths to pull.
    pub paths: Vec<PullLevel<P>>,
}

fn interleave(values: &[Value], constants: &[Aid]) -> Vec<Value> {
    if values.is_empty() || constants.is_empty() {
        values.to_owned()
    } else {
        let size: usize = values.len() + constants.len();
        // + 2, because we know there'll be a and v coming...
        let mut result: Vec<Value> = Vec::with_capacity(size + 2);

        let mut next_value = 0;
        let mut next_const = 0;

        for i in 0..size {
            if i % 2 == 0 {
                // on even indices we interleave an attribute
                let a = constants[next_const].clone();
                result.push(Value::Aid(a));
                next_const = next_const + 1;
            } else {
                // on odd indices we take from the result tuple
                result.push(values[next_value].clone());
                next_value = next_value + 1;
            }
        }

        result
    }
}

impl<P: Implementable> Implementable for PullLevel<P> {
    fn dependencies(&self) -> Dependencies {
        Dependencies::none()
    }

    fn implement<'b, T, I, S>(
        &self,
        nested: &mut Iterative<'b, S, u64>,
        local_arrangements: &VariableMap<Iterative<'b, S, u64>>,
        context: &mut I,
    ) -> (Implemented<'b, S>, ShutdownHandle)
    where
        T: Timestamp + Lattice,
        I: ImplContext<T>,
        S: Scope<Timestamp = T>,
    {
        use differential_dataflow::operators::arrange::{Arrange, Arranged, TraceAgent};
        use differential_dataflow::operators::JoinCore;
        use differential_dataflow::trace::implementations::ord::OrdValSpine;
        use differential_dataflow::trace::TraceReader;

        let (input, mut shutdown_handle) = self.plan.implement(nested, local_arrangements, context);

        if self.pull_attributes.is_empty() {
            if self.path_attributes.is_empty() {
                // nothing to pull
                (input, shutdown_handle)
            } else {
                let path_attributes = self.path_attributes.clone();
                let tuples = {
                    let (tuples, shutdown) = input.tuples(nested, context);
                    shutdown_handle.merge_with(shutdown);

                    tuples.map(move |tuple| interleave(&tuple, &path_attributes))
                };

                (
                    Implemented::Collection(CollectionRelation {
                        variables: self.variables.to_vec(),
                        tuples,
                    }),
                    shutdown_handle,
                )
            }
        } else {
            // Arrange input entities by eid.
            let e_offset = input
                .binds(self.pull_variable)
                .expect("input relation doesn't bind pull_variable");

            let paths = {
                let (tuples, shutdown) = input.tuples(nested, context);
                shutdown_handle.merge_with(shutdown);
                tuples
            };

            let e_path: Arranged<
                Iterative<S, u64>,
                TraceAgent<OrdValSpine<Value, Vec<Value>, Product<T, u64>, isize>>,
            > = paths.map(move |t| (t[e_offset].clone(), t)).arrange();

            let mut shutdown_handle = shutdown_handle;
            let streams = self.pull_attributes.iter().map(|a| {
                let e_v = match context.forward_index(a) {
                    None => panic!("attribute {:?} does not exist", a),
                    Some(index) => {
                        let frontier: Vec<T> = index.propose_trace.advance_frontier().to_vec();
                        let (arranged, shutdown_propose) =
                            index.propose_trace.import_core(&nested.parent, a);

                        let e_v = arranged.enter_at(nested, move |_, _, time| {
                            let mut forwarded = time.clone();
                            forwarded.advance_by(&frontier);
                            Product::new(forwarded, 0)
                        });

                        shutdown_handle.add_button(shutdown_propose);

                        e_v
                    }
                };

                let attribute = Value::Aid(a.clone());
                let path_attributes: Vec<Aid> = self.path_attributes.clone();

                e_path
                    .join_core(&e_v, move |_e, path: &Vec<Value>, v: &Value| {
                        // Each result tuple must hold the interleaved
                        // path, the attribute, and the value,
                        // i.e. [?p "parent/child" ?c ?a ?v]
                        let mut result = interleave(path, &path_attributes);
                        result.push(attribute.clone());
                        result.push(v.clone());

                        Some(result)
                    })
                    .inner
            });

            let tuples = nested.concatenate(streams).as_collection();

            let relation = CollectionRelation {
                variables: vec![], // @TODO
                tuples,
            };

            (Implemented::Collection(relation), shutdown_handle)
        }
    }
}

impl<P: Implementable> Implementable for Pull<P> {
    fn dependencies(&self) -> Dependencies {
        Dependencies::none()
    }

    fn implement<'b, T, I, S>(
        &self,
        nested: &mut Iterative<'b, S, u64>,
        local_arrangements: &VariableMap<Iterative<'b, S, u64>>,
        context: &mut I,
    ) -> (Implemented<'b, S>, ShutdownHandle)
    where
        T: Timestamp + Lattice,
        I: ImplContext<T>,
        S: Scope<Timestamp = T>,
    {
        let mut scope = nested.clone();
        let mut shutdown_handle = ShutdownHandle::empty();

        let streams = self.paths.iter().map(|path| {
            let relation = {
                let (relation, shutdown) = path.implement(&mut scope, local_arrangements, context);
                shutdown_handle.merge_with(shutdown);
                relation
            };

            let tuples = {
                let (tuples, shutdown) = relation.tuples(&mut scope, context);
                shutdown_handle.merge_with(shutdown);
                tuples
            };

            tuples.inner
        });

        let tuples = nested.concatenate(streams).as_collection();

        let relation = CollectionRelation {
            variables: self.variables.to_vec(),
            tuples,
        };

        (Implemented::Collection(relation), shutdown_handle)
        }
    }
}
