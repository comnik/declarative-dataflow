//! Pull expression plan, but without nesting.

use std::collections::HashMap;

use timely::dataflow::scopes::child::Iterative;
use timely::dataflow::{Scope, Stream};
use timely::order::Product;
use timely::progress::Timestamp;

use differential_dataflow::lattice::Lattice;

use crate::binding::AsBinding;
use crate::plan::{Dependencies, ImplContext, Implementable, Plan};
use crate::{Aid, Value, Var};
use crate::{Relation, ShutdownHandle, VariableMap};

/// A sequence of attributes that uniquely identify a nesting level in
/// a Pull query.
pub type PathId = Vec<Aid>;

/// A plan stage for extracting all matching [e a v] tuples for a
/// given set of attributes and an input relation specifying entities.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub struct PullLevel<P: Implementable> {
    /// Plan for the input relation.
    pub plan: Box<P>,
    /// Eid variable.
    pub pull_variable: Var,
    /// Attributes to pull for the input entities.
    pub pull_attributes: Vec<Aid>,
    /// Attribute names to distinguish plans of the same
    /// length. Useful to feed into a nested hash-map directly.
    pub path_attributes: Vec<Aid>,
    /// @TODO
    pub cardinality_many: bool,
}

impl<P: Implementable> PullLevel<P> {
    /// See Implementable::dependencies, as PullLevel v2 can't
    /// implement Implementable directly.
    fn dependencies(&self) -> Dependencies {
        let mut dependencies = self.plan.dependencies();

        for attribute in &self.pull_attributes {
            let attribute_dependencies = Dependencies::attribute(&attribute);
            dependencies = Dependencies::merge(dependencies, attribute_dependencies);
        }

        dependencies
    }

    /// See Implementable::implement, as PullLevel v2 can't implement
    /// Implementable directly.
    fn implement<'b, T, I, S>(
        &self,
        nested: &mut Iterative<'b, S, u64>,
        local_arrangements: &VariableMap<Iterative<'b, S, u64>>,
        context: &mut I,
    ) -> (
        HashMap<PathId, Stream<S, (Vec<Value>, S::Timestamp, isize)>>,
        ShutdownHandle,
    )
    where
        T: Timestamp + Lattice,
        I: ImplContext<T>,
        S: Scope<Timestamp = T>,
    {
        use differential_dataflow::operators::arrange::{Arrange, Arranged, TraceAgent};
        use differential_dataflow::operators::JoinCore;
        use differential_dataflow::trace::implementations::ord::OrdValSpine;
        use differential_dataflow::trace::TraceReader;

        assert_eq!(self.pull_attributes.is_empty(), false);

        let (input, mut shutdown_handle) = self.plan.implement(nested, local_arrangements, context);

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
        let path_streams = self
            .pull_attributes
            .iter()
            .map(|a| {
                let e_v = match context.forward_propose(a) {
                    None => panic!("attribute {:?} does not exist", a),
                    Some(propose_trace) => {
                        let frontier: Vec<T> = propose_trace.advance_frontier().to_vec();
                        let (arranged, shutdown_propose) =
                            propose_trace.import_core(&nested.parent, a);

                        let e_v = arranged.enter_at(nested, move |_, _, time| {
                            let mut forwarded = time.clone();
                            forwarded.advance_by(&frontier);
                            Product::new(forwarded, 0)
                        });

                        shutdown_handle.add_button(shutdown_propose);

                        e_v
                    }
                };

                let path_id: Vec<Aid> = {
                    assert_eq!(self.path_attributes.is_empty(), false);

                    let mut path_attributes = self.path_attributes.clone();
                    path_attributes.push(a.clone());
                    path_attributes
                };

                let path_stream = e_path
                    .join_core(&e_v, move |_e, path: &Vec<Value>, v: &Value| {
                        let mut result = path.clone();
                        result.push(v.clone());

                        Some(result)
                    })
                    .leave()
                    .inner;

                (path_id, path_stream)
            })
            .collect::<HashMap<_, _>>();

        (path_streams, shutdown_handle)
    }
}

/// A plan stage for extracting all tuples for a given set of
/// attributes.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub struct PullAll {
    /// Attributes to pull for the input entities.
    pub pull_attributes: Vec<Aid>,
}

impl PullAll {
    /// See Implementable::dependencies, as PullAll v2 can't implement
    /// Implementable directly.
    fn dependencies(&self) -> Dependencies {
        let mut dependencies = Dependencies::none();

        for attribute in &self.pull_attributes {
            let attribute_dependencies = Dependencies::attribute(&attribute);
            dependencies = Dependencies::merge(dependencies, attribute_dependencies);
        }

        dependencies
    }

    /// See Implementable::implement, as PullAll v2 can't implement
    /// Implementable directly.
    fn implement<'b, T, I, S>(
        &self,
        nested: &mut Iterative<'b, S, u64>,
        _local_arrangements: &VariableMap<Iterative<'b, S, u64>>,
        context: &mut I,
    ) -> (
        HashMap<PathId, Stream<S, (Vec<Value>, S::Timestamp, isize)>>,
        ShutdownHandle,
    )
    where
        T: Timestamp + Lattice,
        I: ImplContext<T>,
        S: Scope<Timestamp = T>,
    {
        use differential_dataflow::trace::TraceReader;

        assert!(!self.pull_attributes.is_empty());

        let mut shutdown_handle = ShutdownHandle::empty();

        let path_streams = self
            .pull_attributes
            .iter()
            .map(|a| {
                let e_v = match context.forward_propose(a) {
                    None => panic!("attribute {:?} does not exist", a),
                    Some(propose_trace) => {
                        let frontier: Vec<T> = propose_trace.advance_frontier().to_vec();
                        let (arranged, shutdown_propose) =
                            propose_trace.import_core(&nested.parent, a);

                        let e_v = arranged.enter_at(nested, move |_, _, time| {
                            let mut forwarded = time.clone();
                            forwarded.advance_by(&frontier);
                            Product::new(forwarded, 0)
                        });

                        shutdown_handle.add_button(shutdown_propose);

                        e_v
                    }
                };

                let path_stream = e_v
                    .as_collection(|e, v| vec![e.clone(), v.clone()])
                    .leave()
                    .inner;

                (vec![a.to_string()], path_stream)
            })
            .collect::<HashMap<_, _>>();

        (path_streams, shutdown_handle)
    }
}

/// @TODO
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub enum Pull {
    /// @TODO
    All(PullAll),
    /// @TODO
    Level(PullLevel<Plan>),
}

impl Pull {
    /// See Implementable::dependencies, as Pull v2 can't implement
    /// Implementable directly.
    pub fn dependencies(&self) -> Dependencies {
        match self {
            Pull::All(ref pull) => pull.dependencies(),
            Pull::Level(ref pull) => pull.dependencies(),
        }
    }

    /// See Implementable::implement, as Pull v2 can't implement
    /// Implementable directly.
    pub fn implement<'b, T, I, S>(
        &self,
        nested: &mut Iterative<'b, S, u64>,
        local_arrangements: &VariableMap<Iterative<'b, S, u64>>,
        context: &mut I,
    ) -> (
        HashMap<PathId, Stream<S, (Vec<Value>, S::Timestamp, isize)>>,
        ShutdownHandle,
    )
    where
        T: Timestamp + Lattice,
        I: ImplContext<T>,
        S: Scope<Timestamp = T>,
    {
        match self {
            Pull::All(ref pull) => pull.implement(nested, local_arrangements, context),
            Pull::Level(ref pull) => pull.implement(nested, local_arrangements, context),
        }
    }
}
