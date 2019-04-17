//! WCO expression plan, integrating the following work:
//! https://github.com/frankmcsherry/differential-dataflow/tree/master/dogsdogsdogs

use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::Hash;
use std::rc::Rc;

use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::operators::{Concatenate, Operator, Partition};
use timely::dataflow::scopes::child::Iterative;
use timely::dataflow::Scope;
use timely::order::{Product, TotalOrder};
use timely::progress::Timestamp;
use timely::worker::AsWorker;
use timely::PartialOrder;

use timely_sort::Unsigned;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::{Consolidate, Count, Threshold};
use differential_dataflow::trace::{BatchReader, Cursor, TraceReader};
use differential_dataflow::{AsCollection, Collection, Data, Hashable};

use crate::binding::{AsBinding, BinaryPredicate, Binding};
use crate::binding::{BinaryPredicateBinding, ConstantBinding};
use crate::logging::DeclarativeEvent;
use crate::plan::{Dependencies, ImplContext, Implementable};
use crate::timestamp::altneu::AltNeu;
use crate::{Aid, Value, Var};
use crate::{CollectionRelation, Implemented, LiveIndex, ShutdownHandle, VariableMap};

type Extender<'a, S, P, V> = Box<(dyn PrefixExtender<S, Prefix = P, Extension = V> + 'a)>;

/// A type capable of extending a stream of prefixes. Implementors of
/// `PrefixExtension` provide types and methods for extending a
/// differential dataflow collection, via the three methods `count`,
/// `propose`, and `validate`.
trait PrefixExtender<G: Scope> {
    /// The required type of prefix to extend.
    type Prefix;
    /// The type to be produced as extension.
    type Extension;
    /// Annotates prefixes with the number of extensions the relation would propose.
    fn count(
        &mut self,
        prefixes: &Collection<G, (Self::Prefix, usize, usize)>,
        index: usize,
    ) -> Option<Collection<G, (Self::Prefix, usize, usize)>>;
    /// Extends each prefix with corresponding extensions.
    fn propose(
        &mut self,
        prefixes: &Collection<G, Self::Prefix>,
    ) -> Collection<G, (Self::Prefix, Self::Extension)>;
    /// Restricts proposed extensions by those the extender would have proposed.
    fn validate(
        &mut self,
        extensions: &Collection<G, (Self::Prefix, Self::Extension)>,
    ) -> Collection<G, (Self::Prefix, Self::Extension)>;
}

trait IntoExtender<'a, S, V>
where
    S: Scope,
    S::Timestamp: Timestamp + Lattice,
    V: Data + Hash,
{
    fn into_extender<P: Data + IndexNode<V>, B: AsBinding + std::fmt::Debug>(
        &self,
        prefix: &B,
    ) -> Vec<Extender<'a, S, P, V>>;
}

impl<'a, S> IntoExtender<'a, S, Value> for ConstantBinding
where
    S: Scope,
    S::Timestamp: Timestamp + Lattice,
{
    fn into_extender<P: Data + IndexNode<Value>, B: AsBinding + std::fmt::Debug>(
        &self,
        _prefix: &B,
    ) -> Vec<Extender<'a, S, P, Value>> {
        vec![Box::new(ConstantExtender {
            phantom: std::marker::PhantomData,
            value: self.value.clone(),
        })]
    }
}

impl<'a, S, V> IntoExtender<'a, S, V> for BinaryPredicateBinding
where
    S: Scope,
    S::Timestamp: Timestamp + Lattice,
    V: Data + Hash,
{
    fn into_extender<P: Data + IndexNode<V>, B: AsBinding + std::fmt::Debug>(
        &self,
        prefix: &B,
    ) -> Vec<Extender<'a, S, P, V>> {
        match direction(prefix, self.variables) {
            Err(_msg) => {
                // We won't panic here, this just means the predicate's variables
                // aren't sufficiently bound by the prefixes yet.
                vec![]
            }
            Ok(direction) => vec![Box::new(BinaryPredicateExtender {
                phantom: std::marker::PhantomData,
                predicate: self.predicate.clone(),
                direction,
            })],
        }
    }
}

//
// OPERATOR
//

/// A plan stage joining two source relations on the specified
/// variables. Throws if any of the join variables isn't bound by both
/// sources.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub struct Hector {
    /// Variables to bind.
    pub variables: Vec<Var>,
    /// Bindings to join.
    pub bindings: Vec<Binding>,
}

enum Direction {
    Forward(usize),
    Reverse(usize),
}

fn direction<P>(prefix: &P, extender_variables: (Var, Var)) -> Result<Direction, &'static str>
where
    P: AsBinding + std::fmt::Debug,
{
    match AsBinding::binds(prefix, extender_variables.0) {
        None => match AsBinding::binds(prefix, extender_variables.1) {
            None => {
                error!(
                    "Neither extender variable {:?} bound by prefix {:?}.",
                    extender_variables, prefix
                );
                Err("Neither extender variable bound by prefix.")
            }
            Some(offset) => Ok(Direction::Reverse(offset)),
        },
        Some(offset) => {
            match AsBinding::binds(prefix, extender_variables.1) {
                Some(_) => Err("Both extender variables already bound by prefix."),
                None => {
                    // Prefix binds the first extender variable, but not
                    // the second. Can use forward index.
                    Ok(Direction::Forward(offset))
                }
            }
        }
    }
}

/// Bindings can be in conflict with the source binding of a given
/// delta pipeline. We need to identify them and handle them as
/// special cases, because we always have to start from prefixes of
/// size two.
pub fn source_conflicts(source_index: usize, bindings: &[Binding]) -> Vec<&Binding> {
    match bindings[source_index] {
        Binding::Attribute(ref source) => {
            let prefix_0 = vec![source.variables.0];
            let prefix_1 = vec![source.variables.1];

            bindings
                .iter()
                .enumerate()
                .flat_map(|(index, binding)| {
                    if index == source_index {
                        None
                    } else if binding.can_extend(&prefix_0, source.variables.1)
                        || binding.can_extend(&prefix_1, source.variables.0)
                    {
                        Some(binding)
                    } else {
                        None
                    }
                })
                .collect()
        }
        _ => panic!("Source must be an AttributeBinding."),
    }
}

/// Orders the variables s.t. each has at least one binding from
/// itself to a prior variable. `source_binding` indicates the binding
/// from which we will source the prefixes in the resulting delta
/// pipeline. Returns the chosen variable order and the corresponding
/// binding order.
///
/// (adapted from github.com/frankmcsherry/dataflow-join/src/motif.rs)
pub fn plan_order(source_index: usize, bindings: &[Binding]) -> (Vec<Var>, Vec<Binding>) {
    let mut variables = bindings
        .iter()
        .flat_map(AsBinding::variables)
        .collect::<Vec<Var>>();
    variables.sort();
    variables.dedup();

    // Determine an order on the attributes. The order may not
    // introduce a binding until one of its consituents is already
    // bound by the prefix. These constraints are captured via the
    // `AsBinding::ready_to_extend` method. The order may otherwise be
    // arbitrary, for example selecting the most constrained attribute
    // first. Presently, we just pick attributes arbitrarily.

    let mut prefix: Vec<Var> = Vec::with_capacity(variables.len());
    match bindings[source_index] {
        Binding::Attribute(ref source) => {
            prefix.push(source.variables.0);
            prefix.push(source.variables.1);
        }
        _ => panic!("Source binding must be an attribute."),
    }

    let candidates_for = |bindings: &[Binding], target: Var| {
        bindings
            .iter()
            .enumerate()
            .flat_map(move |(index, other)| {
                if index == source_index {
                    // Ignore the source binding itself.
                    None
                } else if other.binds(target).is_some() {
                    Some(other.clone())
                } else {
                    // Some bindings might not even talk about the target
                    // variable.
                    None
                }
            })
            .collect::<Vec<Binding>>()
    };

    let mut ordered_bindings = Vec::new();
    let mut candidates: Vec<Binding> = prefix
        .iter()
        .flat_map(|x| candidates_for(&bindings, *x))
        .collect();

    loop {
        debug!("Candidates: {:?}", candidates);

        let mut waiting_candidates = Vec::new();

        candidates.sort();
        candidates.dedup();

        for candidate in candidates.drain(..) {
            match candidate.ready_to_extend(&prefix) {
                None => {
                    waiting_candidates.push(candidate);
                }
                Some(target) => {
                    if AsBinding::binds(&prefix, target).is_none() {
                        prefix.push(target);
                        for new_candidate in candidates_for(&bindings, target) {
                            if candidate != new_candidate {
                                waiting_candidates.push(new_candidate);
                            }
                        }
                    }

                    ordered_bindings.push(candidate);
                }
            }
        }

        if waiting_candidates.is_empty() {
            break;
        }

        for candidate in waiting_candidates.drain(..) {
            candidates.push(candidate);
        }

        if prefix.len() == variables.len() {
            break;
        }
    }

    debug!("Candidates: {:?}", candidates);

    for candidate in candidates.drain(..) {
        ordered_bindings.push(candidate);
    }

    (prefix, ordered_bindings)
}

trait IndexNode<V> {
    fn index(&self, index: usize) -> V;
}

impl IndexNode<Value> for (Value, Value) {
    #[inline(always)]
    fn index(&self, index: usize) -> Value {
        assert!(index <= 1);
        if index == 0 {
            self.0.clone()
        } else {
            self.1.clone()
        }
    }
}

impl IndexNode<Value> for Vec<Value> {
    #[inline(always)]
    fn index(&self, index: usize) -> Value {
        self[index].clone()
    }
}

impl Hector {
    // @TODO pass single binding as argument?
    // @TODO make these static and take variables as well?

    fn implement_single_binding<'b, T, I, S>(
        &self,
        nested: &mut Iterative<'b, S, u64>,
        _local_arrangements: &VariableMap<Iterative<'b, S, u64>>,
        context: &mut I,
    ) -> (Implemented<'b, S>, ShutdownHandle)
    where
        T: Timestamp + Lattice + TotalOrder,
        I: ImplContext<T>,
        S: Scope<Timestamp = T>,
    {
        // With only a single binding given, we don't want to do
        // anything fancy (provided the binding is sourceable).

        match self.bindings.first().unwrap() {
            Binding::Attribute(binding) => match context.forward_index(&binding.source_attribute) {
                None => panic!("Unknown attribute {}", &binding.source_attribute),
                Some(index) => {
                    let frontier: Vec<T> = index.validate_trace.advance_frontier().to_vec();
                    let (validate, shutdown_validate) = index
                        .validate_trace
                        .import_core(&nested.parent, &format!("Validate({})", index.name));

                    let prefix = binding.variables();
                    let target_variables = self.variables.clone();
                    let tuples = validate
                        .enter_at(nested, move |_, _, time| {
                            let mut forwarded = time.clone();
                            forwarded.advance_by(&frontier);
                            Product::new(forwarded, 0)
                        })
                        .as_collection(move |tuple, ()| {
                            target_variables
                                .iter()
                                .flat_map(|x| {
                                    Some(tuple.index(AsBinding::binds(&prefix, *x).unwrap()))
                                })
                                .collect()
                        });

                    let relation = CollectionRelation {
                        variables: self.variables.clone(),
                        tuples,
                    };

                    (
                        Implemented::Collection(relation),
                        ShutdownHandle::from_button(shutdown_validate),
                    )
                }
            },
            _ => {
                panic!("Passed a single, non-sourceable binding.");
            }
        }
    }

    // fn two_way<'b, T, I, S>(
    //     nested: &mut Iterative<'b, S, u64>,
    //     _local_arrangements: &VariableMap<Iterative<'b, S, u64>>,
    //     context: &mut I,
    //     left: Binding,
    //     right: Binding,
    // ) -> (Implemented<'b, S>, ShutdownHandle)
    // where
    //     T: Timestamp + Lattice,
    //     I: ImplContext<T>,
    //     S: Scope<Timestamp = T>,
    // {
    //     let (source, right) = match left {
    //         Binding::Attribute(source) => (source, right),
    //         _ => match right {
    //             Binding::Attribute(source) => (source, left),
    //             _ => panic!("At least one binding must be sourceable for Hector::two_way."),
    //         }
    //     };

    //     match right {
    //         Binding::Constant(constant_binding) => {
    //             let match_v = constant_binding.value;
    //             let offset = source.binds(constant_binding.variable)
    //                 .unwrap_or_else(|| panic!("Source doesn't bind constant binding {:?}", constant_binding));

    //             match offset {
    //                 0 => {
    //                     // [?a :edge ?b] (constant ?a x) <=> [x :edge ?b]

    //                     let (index, shutdown) = context.forward_index(&source.source_attribute)
    //                         .unwrap_or_else(|| panic!("No forward index found for attribute {}", &source.source_attribute))
    //                         .propose_trace
    //                         .import_core(&nested.parent, &source.source_attribute);

    //                     let frontier: Vec<T> = index.trace.advance_frontier().to_vec();

    //                     index
    //                         .filter(move |e, _v| *e == match_v)
    //                         .enter_at(&nested, move |_, _, time| {
    //                             let mut forwarded = time.clone(); forwarded.advance_by(&frontier);
    //                             Product::new(forwarded, Default::default())
    //                         })
    //                 }
    //                 1 => {
    //                     // [?a :edge ?b] (constant ?b x) <=> [?a :edge x]

    //                     let (index, shutdown) = context.reverse_index(&source.source_attribute)
    //                         .unwrap_or_else(|| panic!("No reverse index found for attribute {}", &source.source_attribute))
    //                         .propose_trace
    //                         .import_core(&nested.parent, &source.source_attribute);

    //                     let frontier: Vec<T> = index.trace.advance_frontier().to_vec();

    //                     index
    //                         .filter(move |e, _v| *e == match_v)
    //                         .enter_at(&nested, move |_, _, time| {
    //                             let mut forwarded = time.clone(); forwarded.advance_by(&frontier);
    //                             Product::new(forwarded, Default::default())
    //                         })
    //                 }
    //                 other => panic!("Unexpected offset {}", other),
    //             }
    //         }
    //         _ => unimplemented!(),
    //     }
    // }
}

impl Implementable for Hector {
    fn dependencies(&self) -> Dependencies {
        let attributes = self
            .bindings
            .iter()
            .flat_map(|binding| {
                if let Binding::Attribute(binding) = binding {
                    Some(binding.source_attribute.clone())
                } else {
                    None
                }
            })
            .collect::<HashSet<Aid>>();

        Dependencies {
            names: HashSet::new(),
            attributes,
        }
    }

    fn into_bindings(&self) -> Vec<Binding> {
        self.bindings.clone()
    }

    fn implement<'b, T, I, S>(
        &self,
        nested: &mut Iterative<'b, S, u64>,
        _local_arrangements: &VariableMap<Iterative<'b, S, u64>>,
        context: &mut I,
    ) -> (Implemented<'b, S>, ShutdownHandle)
    where
        T: Timestamp + Lattice + TotalOrder,
        I: ImplContext<T>,
        S: Scope<Timestamp = T>,
    {
        if self.bindings.is_empty() {
            panic!("No bindings passed.");
        } else if self.variables.is_empty() {
            panic!("No variables requested.");
        } else if self.bindings.len() == 1 {
            self.implement_single_binding(nested, _local_arrangements, context)
        // } else if self.bindings.len() == 2 {
        //     Hector::two_way(nested, _local_arrangements, context, self.bindings[0].clone(), self.bindings[1].clone())
        } else {
            // In order to avoid delta pipelines looking at each
            // other's data in naughty ways, we need to run them all
            // inside a scope with lexicographic times.

            let (joined, shutdown_handle) = nested.scoped::<AltNeu<Product<T,u64>>, _, _>("AltNeu", |inner| {

                let scope = inner.clone();

                // We cache aggressively, to avoid importing and
                // wrapping things more than once.

                let mut shutdown_handle = ShutdownHandle::empty();
                let mut forward_cache = HashMap::new();
                let mut reverse_cache = HashMap::new();

                // Attempt to acquire a logger for tuple counts.
                let logger = {
                    let register = scope.parent.log_register();
                    register.get::<DeclarativeEvent>("declarative")
                };

                // For each AttributeBinding (only AttributeBindings
                // actually experience change), we construct a delta query
                // driven by changes to that binding.

                let changes = self.bindings.iter().enumerate()
                    .flat_map(|(idx, delta_binding)| match delta_binding {
                        Binding::Attribute(delta_binding) => {

                            // We need to determine an order on the attributes
                            // that ensures that each is bound by preceeding
                            // attributes. For now, we will take the requested order.

                            // @TODO use binding order returned here?
                            // might be problematic to ensure ordering is maintained?
                            let (variables, _) = plan_order(idx, &self.bindings);

                            let mut prefix = Vec::with_capacity(variables.len());

                            debug!("Source {:?}", delta_binding);

                            // We would like to avoid starting with single-variable
                            // (or even empty) prefixes, because the dataflow-y nature
                            // of this implementation means we will always be starting
                            // from attributes (which correspond to two-variable prefixes).
                            // 
                            // But to get away with that we need to check for single-variable
                            // bindings in conflict with the source binding.

                            let mut source_conflicts = source_conflicts(idx, &self.bindings);

                            let index = forward_cache
                                .entry(delta_binding.source_attribute.to_string())
                                .or_insert_with(|| {
                                    let (arranged, shutdown) =
                                        context.forward_index(&delta_binding.source_attribute)
                                        .expect("forward_index doesn't exist")
                                        .import(&scope.parent.parent);

                                    shutdown_handle.merge_with(shutdown);

                                    arranged
                                });
                            let frontier: Vec<T> = index.propose.trace.advance_frontier().to_vec();

                            let mut source = if !source_conflicts.is_empty() {
                                // @TODO there can be more than one conflict
                                assert_eq!(source_conflicts.len(), 1);

                                // @TODO Not just constant bindings can cause issues here!

                                let conflict = source_conflicts.pop().unwrap();
                                // for conflict in source_conflicts.drain(..) {
                                    match conflict {
                                        Binding::Constant(constant_binding) => {
                                            prefix.push(constant_binding.variable);

                                            let match_v = constant_binding.value.clone();

                                            // Guaranteed to intersect with offset zero at this point.
                                            match direction(&prefix, delta_binding.variables).unwrap() {
                                                Direction::Forward(_) => {
                                                    prefix.push(delta_binding.variables.1);

                                                    index
                                                        .propose
                                                        .filter(move |e, _v| *e == match_v)
                                                        .enter_at(&scope.parent, move |_, _, time| {
                                                            let mut forwarded = time.clone(); forwarded.advance_by(&frontier);
                                                            Product::new(forwarded, Default::default())
                                                        })
                                                        .enter(&scope)
                                                        .as_collection(|e,v| vec![e.clone(), v.clone()])
                                                }
                                                Direction::Reverse(_) => {
                                                    prefix.push(delta_binding.variables.0);

                                                    index
                                                        .propose
                                                        .filter(move |_e, v| *v == match_v)
                                                        .enter_at(&scope.parent, move |_, _, time| {
                                                            let mut forwarded = time.clone(); forwarded.advance_by(&frontier);
                                                            Product::new(forwarded, Default::default())
                                                        })
                                                        .enter(&scope)
                                                        .as_collection(|v,e| vec![e.clone(), v.clone()])
                                                }
                                            }
                                        }
                                        _ => panic!("Can't resolve conflicts on {:?} bindings", conflict),
                                    // }
                                }
                            } else {
                                prefix.push(delta_binding.variables.0);
                                prefix.push(delta_binding.variables.1);

                                index
                                    .validate
                                    .enter_at(&scope.parent, move |_, _, time| {
                                        let mut forwarded = time.clone();
                                        forwarded.advance_by(&frontier);
                                        Product::new(forwarded, Default::default())
                                    })
                                    .enter(&scope)
                                    .as_collection(|(e,v),()| vec![e.clone(), v.clone()])
                            };

                            for target in variables.iter() {
                                match AsBinding::binds(&prefix, *target) {
                                    Some(_) => { /* already bound */ continue },
                                    None => {
                                        debug!("Extending {:?} to {:?}", prefix, target);

                                        let mut extenders: Vec<Extender<'_, _, Vec<Value>, _>> = vec![];

                                        // Handling AntijoinBinding's requires dealing with recursion,
                                        // because they wrap another binding. We don't actually want to wrap
                                        // all of the below inside of a recursive function, because passing
                                        // all these nested scopes and caches around leads to a world of lifetimes pain.
                                        //
                                        // Therefore we make our own little queue of bindings and process them iteratively.

                                        let mut bindings: VecDeque<(usize, Binding)> = VecDeque::new();

                                        for (idx, binding) in self.bindings.iter().cloned().enumerate() {
                                            if let Binding::Not(antijoin_binding) = binding {
                                                bindings.push_back((idx, (*antijoin_binding.binding).clone()));
                                                bindings.push_back((idx, Binding::Not(antijoin_binding)));
                                            } else {
                                                bindings.push_back((idx, binding));
                                            }
                                        }

                                        while let Some((other_idx, other)) = bindings.pop_front() {

                                            // We need to distinguish between conflicting relations
                                            // that appear before the current one in the sequence (< idx),
                                            // and those that appear afterwards.

                                            // Ignore the current delta source itself.
                                            if other_idx == idx { continue; }

                                            // Ignore any binding not talking about the target variable.
                                            if other.binds(*target).is_none() { continue; }

                                            // Ignore any binding that isn't ready to extend, either
                                            // because it doesn't even talk about the target variable, or
                                            // because none of its dependent variables are bound by the prefix
                                            // yet (relevant for attributes).
                                            if !other.can_extend(&prefix, *target) {
                                                debug!("{:?} can't extend", other);
                                                continue;
                                            }

                                            let is_neu = other_idx >= idx;

                                            debug!("\t...using {:?}", other);

                                            match other {
                                                Binding::Not(_other) => {
                                                    // Due to the way we enqueued the bindings above, we can now
                                                    // rely on the internal exteneder being available as the last
                                                    // extender on the stack.
                                                    let internal_extender = extenders.pop().expect("No internal extender available on stack.");

                                                    extenders.push(
                                                        Box::new(AntijoinExtender {
                                                            phantom: std::marker::PhantomData,
                                                            extender: internal_extender,
                                                        })
                                                    );
                                                }
                                                Binding::Constant(other) => {
                                                    extenders.append(&mut other.into_extender(&prefix));
                                                }
                                                Binding::BinaryPredicate(other) => {
                                                    extenders.append(&mut other.into_extender(&prefix));
                                                }
                                                Binding::Attribute(other) => {
                                                    match direction(&prefix, other.variables) {
                                                        Err(msg) => panic!(msg),
                                                        Ok(direction) => match direction {
                                                            Direction::Forward(offset) => {
                                                                let index = forward_cache.entry(other.source_attribute.to_string())
                                                                    .or_insert_with(|| {
                                                                        let (arranged, shutdown) =
                                                                            context.forward_index(&other.source_attribute)
                                                                            .expect("forward index doesn't exist")
                                                                            .import(&scope.parent.parent);

                                                                        shutdown_handle.merge_with(shutdown);

                                                                        arranged
                                                                    });
                                                                let frontier: Vec<T> = index.propose.trace.advance_frontier().to_vec();

                                                                let (frontier1, frontier2, frontier3) = (frontier.clone(), frontier.clone(), frontier);
                                                                let (neu1, neu2, neu3) = (is_neu, is_neu, is_neu);

                                                                let forward = index
                                                                // .enter(&scope.parent)
                                                                    .enter_at(
                                                                        &scope.parent,
                                                                        move |_, _, t| { let mut forwarded = t.clone(); forwarded.advance_by(&frontier1); Product::new(forwarded, 0) },
                                                                        move |_, _, t| { let mut forwarded = t.clone(); forwarded.advance_by(&frontier2); Product::new(forwarded, 0) },
                                                                        move |_, _, t| { let mut forwarded = t.clone(); forwarded.advance_by(&frontier3); Product::new(forwarded, 0) },
                                                                    )
                                                                    .enter_at(
                                                                        &scope,
                                                                        move |_,_,t| AltNeu { time: t.clone(), neu: neu1 },
                                                                        move |_,_,t| AltNeu { time: t.clone(), neu: neu2 },
                                                                        move |_,_,t| AltNeu { time: t.clone(), neu: neu3 },
                                                                    );

                                                                extenders.push(
                                                                    Box::new(CollectionExtender {
                                                                        phantom: std::marker::PhantomData,
                                                                        indices: forward,
                                                                        key_selector: Rc::new(move |prefix: &Vec<Value>| prefix.index(offset)),
                                                                        fallback: other.default,
                                                                    })
                                                                );
                                                            },
                                                            Direction::Reverse(offset) => {
                                                                let index = reverse_cache.entry(other.source_attribute.to_string())
                                                                    .or_insert_with(|| {
                                                                        let (arranged, shutdown) =
                                                                            context.reverse_index(&other.source_attribute).unwrap()
                                                                            .import(&scope.parent.parent);

                                                                        shutdown_handle.merge_with(shutdown);

                                                                        arranged
                                                                    });
                                                                let frontier: Vec<T> = index.propose.trace.advance_frontier().to_vec();

                                                                let (frontier1, frontier2, frontier3) = (frontier.clone(), frontier.clone(), frontier);
                                                                let (neu1, neu2, neu3) = (is_neu, is_neu, is_neu);

                                                                let reverse = index
                                                                // .enter(&scope.parent)
                                                                    .enter_at(
                                                                        &scope.parent,
                                                                        move |_, _, t| { let mut forwarded = t.clone(); forwarded.advance_by(&frontier1); Product::new(forwarded, 0) },
                                                                        move |_, _, t| { let mut forwarded = t.clone(); forwarded.advance_by(&frontier2); Product::new(forwarded, 0) },
                                                                        move |_, _, t| { let mut forwarded = t.clone(); forwarded.advance_by(&frontier3); Product::new(forwarded, 0) },
                                                                    )
                                                                    .enter_at(
                                                                        &scope,
                                                                        move |_,_,t| AltNeu { time: t.clone(), neu: neu1 },
                                                                        move |_,_,t| AltNeu { time: t.clone(), neu: neu2 },
                                                                        move |_,_,t| AltNeu { time: t.clone(), neu: neu3 },
                                                                    );

                                                                extenders.push(
                                                                    Box::new(CollectionExtender {
                                                                        phantom: std::marker::PhantomData,
                                                                        indices: reverse,
                                                                        key_selector: Rc::new(move |prefix: &Vec<Value>| prefix.index(offset)),
                                                                        fallback: other.default,
                                                                    })
                                                                );
                                                            },
                                                        }
                                                    }
                                                }
                                            }
                                        }

                                        prefix.push(*target);

                                        // @TODO impl ProposeExtensionMethod for Arranged
                                        let extended = source.extend(&mut extenders[..]);

                                        if let Some(_) = &logger {
                                            let worker_index = scope.index();
                                            let source_attribute = delta_binding.source_attribute.to_string();
                                            extended
                                                // .inspect(move |x| { println!("{} extended: {:?}", source_attribute, x); })
                                                .map(|_| ())
                                                .consolidate()
                                                .count()
                                                .map(move |(_, count)| (Value::Eid(worker_index as u64), Value::Number(count as i64)))
                                                .leave()
                                                .leave()
                                                .inspect(move |x| { println!("{}: {:?}", source_attribute, x); });
                                        }

                                        source = extended
                                            .map(|(tuple,v)| {
                                                let mut out = Vec::with_capacity(tuple.len() + 1);
                                                out.append(&mut tuple.clone());
                                                out.push(v);

                                                out
                                            })
                                    }
                                }
                            }

                            if self.variables == prefix {
                                Some(source.inner)
                            } else {
                                let target_variables = self.variables.clone();

                                Some(source
                                     .map(move |tuple| {
                                         target_variables.iter()
                                             .flat_map(|x| Some(tuple.index(AsBinding::binds(&prefix, *x).unwrap())))
                                             .collect()
                                     })
                                     .inner)
                            }
                        }
                        _ => None
                    });

                (inner.concatenate(changes).as_collection().leave(), shutdown_handle)
            });

            let relation = CollectionRelation {
                variables: self.variables.clone(),
                tuples: joined.distinct(),
            };

            (Implemented::Collection(relation), shutdown_handle)
        }
    }
}

//
// GENERIC IMPLEMENTATION
//

trait ProposeExtensionMethod<'a, S: Scope, P: Data + Ord> {
    fn extend<E: Data + Ord>(
        &self,
        extenders: &mut [Extender<'a, S, P, E>],
    ) -> Collection<S, (P, E)>;
}

impl<'a, S: Scope, P: Data + Ord> ProposeExtensionMethod<'a, S, P> for Collection<S, P> {
    fn extend<E: Data + Ord>(
        &self,
        extenders: &mut [Extender<'a, S, P, E>],
    ) -> Collection<S, (P, E)> {
        if extenders.is_empty() {
            // @TODO don't panic
            panic!("No extenders specified.");
        } else if extenders.len() == 1 {
            extenders[0].propose(&self.clone())
        } else {
            let mut counts = self.map(|p| (p, 1 << 31, 0));
            for (index, extender) in extenders.iter_mut().enumerate() {
                if let Some(new_counts) = extender.count(&counts, index) {
                    counts = new_counts;
                }
            }

            let parts = counts
                .inner
                .partition(extenders.len() as u64, |((p, _, i), t, d)| {
                    (i as u64, (p, t, d))
                });

            let mut results = Vec::new();
            for (index, nominations) in parts.into_iter().enumerate() {
                let mut extensions = extenders[index].propose(&nominations.as_collection());
                for other in (0..extenders.len()).filter(|&x| x != index) {
                    extensions = extenders[other].validate(&extensions);
                }

                results.push(extensions.inner); // save extensions
            }

            self.scope().concatenate(results).as_collection()
        }
    }
}

struct ConstantExtender<P, V>
where
    V: Data + Hash,
{
    phantom: std::marker::PhantomData<P>,
    value: V,
}

impl<'a, S, V, P> PrefixExtender<S> for ConstantExtender<P, V>
where
    S: Scope,
    S::Timestamp: Lattice + Data,
    V: Data + Hash,
    P: Data,
{
    type Prefix = P;
    type Extension = V;

    fn count(
        &mut self,
        prefixes: &Collection<S, (P, usize, usize)>,
        index: usize,
    ) -> Option<Collection<S, (P, usize, usize)>> {
        Some(prefixes.map(move |(prefix, old_count, old_index)| {
            if 1 < old_count {
                (prefix.clone(), 1, index)
            } else {
                (prefix.clone(), old_count, old_index)
            }
        }))
    }

    fn propose(&mut self, prefixes: &Collection<S, P>) -> Collection<S, (P, V)> {
        let value = self.value.clone();
        prefixes.map(move |prefix| (prefix.clone(), value.clone()))
    }

    fn validate(&mut self, extensions: &Collection<S, (P, V)>) -> Collection<S, (P, V)> {
        let target = self.value.clone();
        extensions.filter(move |(_prefix, extension)| *extension == target)
    }
}

struct BinaryPredicateExtender<P, V>
where
    V: Data + Hash,
{
    phantom: std::marker::PhantomData<(P, V)>,
    predicate: BinaryPredicate,
    direction: Direction,
}

impl<'a, S, V, P> PrefixExtender<S> for BinaryPredicateExtender<P, V>
where
    S: Scope,
    S::Timestamp: Lattice + Data,
    V: Data + Hash,
    P: Data + IndexNode<V>,
{
    type Prefix = P;
    type Extension = V;

    fn count(
        &mut self,
        _prefixes: &Collection<S, (P, usize, usize)>,
        _index: usize,
    ) -> Option<Collection<S, (P, usize, usize)>> {
        None
    }

    fn propose(&mut self, prefixes: &Collection<S, P>) -> Collection<S, (P, V)> {
        prefixes.map(|_prefix| panic!("BinaryPredicateExtender should never be asked to propose."))
    }

    fn validate(&mut self, extensions: &Collection<S, (P, V)>) -> Collection<S, (P, V)> {
        use self::BinaryPredicate::{EQ, GT, GTE, LT, LTE, NEQ};
        match self.direction {
            Direction::Reverse(offset) => {
                match self.predicate {
                    LT => extensions
                        .filter(move |(prefix, extension)| *extension > prefix.index(offset)),
                    LTE => extensions
                        .filter(move |(prefix, extension)| *extension >= prefix.index(offset)),
                    GT => extensions
                        .filter(move |(prefix, extension)| *extension < prefix.index(offset)),
                    GTE => extensions
                        .filter(move |(prefix, extension)| *extension <= prefix.index(offset)),
                    EQ => extensions
                        .filter(move |(prefix, extension)| *extension == prefix.index(offset)),
                    NEQ => extensions
                        .filter(move |(prefix, extension)| *extension != prefix.index(offset)),
                }
            }
            Direction::Forward(offset) => {
                match self.predicate {
                    LT => extensions
                        .filter(move |(prefix, extension)| *extension < prefix.index(offset)),
                    LTE => extensions
                        .filter(move |(prefix, extension)| *extension <= prefix.index(offset)),
                    GT => extensions
                        .filter(move |(prefix, extension)| *extension > prefix.index(offset)),
                    GTE => extensions
                        .filter(move |(prefix, extension)| *extension >= prefix.index(offset)),
                    EQ => extensions
                        .filter(move |(prefix, extension)| *extension == prefix.index(offset)),
                    NEQ => extensions
                        .filter(move |(prefix, extension)| *extension != prefix.index(offset)),
                }
            }
        }
    }
}

struct CollectionExtender<S, K, V, P, F, TrCount, TrPropose, TrValidate>
where
    S: Scope,
    S::Timestamp: Lattice + Data,
    K: Data,
    V: Data,
    F: Fn(&P) -> K,
    TrCount: TraceReader<Key = K, Val = (), Time = S::Timestamp, R = isize> + Clone + 'static,
    TrCount::Batch: BatchReader<TrCount::Key, TrCount::Val, S::Timestamp, TrCount::R> + 'static,
    TrCount::Cursor: Cursor<TrCount::Key, TrCount::Val, S::Timestamp, TrCount::R> + 'static,
    TrPropose: TraceReader<Key = K, Val = V, Time = S::Timestamp, R = isize> + Clone + 'static,
    TrPropose::Batch:
        BatchReader<TrPropose::Key, TrPropose::Val, S::Timestamp, TrPropose::R> + 'static,
    TrPropose::Cursor: Cursor<TrPropose::Key, TrPropose::Val, S::Timestamp, TrPropose::R> + 'static,
    TrValidate:
        TraceReader<Key = (K, V), Val = (), Time = S::Timestamp, R = isize> + Clone + 'static,
    TrValidate::Batch:
        BatchReader<TrValidate::Key, TrValidate::Val, S::Timestamp, TrValidate::R> + 'static,
    TrValidate::Cursor:
        Cursor<TrValidate::Key, TrValidate::Val, S::Timestamp, TrValidate::R> + 'static,
{
    phantom: std::marker::PhantomData<P>,
    indices: LiveIndex<S, K, V, TrCount, TrPropose, TrValidate>,
    key_selector: Rc<F>,
    fallback: Option<V>,
}

impl<'a, S, K, V, P, F, TrCount, TrPropose, TrValidate> PrefixExtender<S>
    for CollectionExtender<S, K, V, P, F, TrCount, TrPropose, TrValidate>
where
    S: Scope,
    S::Timestamp: Lattice + Data,
    K: Data + Hash,
    V: Data + Hash,
    P: Data,
    F: Fn(&P) -> K + 'static,
    TrCount: TraceReader<Key = K, Val = (), Time = S::Timestamp, R = isize> + Clone + 'static,
    TrCount::Batch: BatchReader<TrCount::Key, TrCount::Val, S::Timestamp, TrCount::R> + 'static,
    TrCount::Cursor: Cursor<TrCount::Key, TrCount::Val, S::Timestamp, TrCount::R> + 'static,
    TrPropose: TraceReader<Key = K, Val = V, Time = S::Timestamp, R = isize> + Clone + 'static,
    TrPropose::Batch:
        BatchReader<TrPropose::Key, TrPropose::Val, S::Timestamp, TrPropose::R> + 'static,
    TrPropose::Cursor: Cursor<TrPropose::Key, TrPropose::Val, S::Timestamp, TrPropose::R> + 'static,
    TrValidate:
        TraceReader<Key = (K, V), Val = (), Time = S::Timestamp, R = isize> + Clone + 'static,
    TrValidate::Batch:
        BatchReader<TrValidate::Key, TrValidate::Val, S::Timestamp, TrValidate::R> + 'static,
    TrValidate::Cursor:
        Cursor<TrValidate::Key, TrValidate::Val, S::Timestamp, TrValidate::R> + 'static,
{
    type Prefix = P;
    type Extension = V;

    fn count(
        &mut self,
        prefixes: &Collection<S, (P, usize, usize)>,
        index: usize,
    ) -> Option<Collection<S, (P, usize, usize)>> {
        // This method takes a stream of `(prefix, time, diff)`
        // changes, and we want to produce the corresponding stream of
        // `((prefix, count), time, diff)` changes, just by looking up
        // `count` in `count_trace`. We are just doing a stream of
        // changes and a stream of look-ups, no consolidation or any
        // funny business like that. We *could* organize the input
        // differences by key and save some time, or we could skip
        // that.

        let counts = &self.indices.count;
        let mut counts_trace = Some(counts.trace.clone());

        let mut stash = HashMap::new();
        let logic1 = self.key_selector.clone();
        let logic2 = self.key_selector.clone();

        let exchange = Exchange::new(move |update: &((P, usize, usize), S::Timestamp, isize)| {
            logic1(&(update.0).0).hashed().as_u64()
        });

        let mut buffer1 = Vec::new();
        let mut buffer2 = Vec::new();

        let fallback = self.fallback.clone();

        // TODO: This should be a custom operator with no connection from the second input to the output.
        Some(
            prefixes
                .inner
                .binary_frontier(&counts.stream, exchange, Pipeline, "Count", move |_, _| {
                    move |input1, input2, output| {
                        // drain the first input, stashing requests.
                        input1.for_each(|capability, data| {
                            data.swap(&mut buffer1);
                            stash
                                .entry(capability.retain())
                                .or_insert_with(Vec::new)
                                .extend(buffer1.drain(..))
                        });

                        // advance the `distinguish_since` frontier to allow all merges.
                        input2.for_each(|_, batches| {
                            batches.swap(&mut buffer2);
                            for batch in buffer2.drain(..) {
                                if let Some(ref mut trace) = counts_trace {
                                    trace.distinguish_since(batch.upper());
                                }
                            }
                        });

                        if let Some(ref mut trace) = counts_trace {
                            for (capability, prefixes) in stash.iter_mut() {
                                // defer requests at incomplete times.
                                // NOTE: not all updates may be at complete times, but if this test fails then none of them are.
                                if !input2.frontier.less_equal(capability.time()) {
                                    let mut session = output.session(capability);

                                    // sort requests for in-order cursor traversal. could consolidate?
                                    prefixes
                                        .sort_by(|x, y| logic2(&(x.0).0).cmp(&logic2(&(y.0).0)));

                                    let (mut cursor, storage) = trace.cursor();

                                    for &mut (
                                        (ref prefix, old_count, old_index),
                                        ref time,
                                        ref mut diff,
                                    ) in prefixes.iter_mut()
                                    {
                                        if !input2.frontier.less_equal(time) {
                                            let key = logic2(prefix);
                                            cursor.seek_key(&storage, &key);
                                            if cursor.get_key(&storage) == Some(&key) {
                                                let mut count = 0;
                                                cursor.map_times(&storage, |t, d| {
                                                    if t.less_equal(time) {
                                                        count += d;
                                                    }
                                                });
                                                // assert!(count >= 0);
                                                let count = count as usize;
                                                if count > 0 {
                                                    if count < old_count {
                                                        session.give((
                                                            (prefix.clone(), count, index),
                                                            time.clone(),
                                                            *diff,
                                                        ));
                                                    } else {
                                                        session.give((
                                                            (prefix.clone(), old_count, old_index),
                                                            time.clone(),
                                                            *diff,
                                                        ));
                                                    }
                                                }
                                            } else if let Some(value) = &fallback {
                                                if old_count > 1 {
                                                    session.give((
                                                        (prefix.clone(), 1, index),
                                                        time.clone(),
                                                        *diff,
                                                    ));
                                                } else {
                                                    session.give((
                                                        (prefix.clone(), old_count, old_index),
                                                        time.clone(),
                                                        *diff,
                                                    ));
                                                }
                                            }
                                            *diff = 0;
                                        }
                                    }

                                    prefixes.retain(|ptd| ptd.2 != 0);
                                }
                            }
                        }

                        // drop fully processed capabilities.
                        stash.retain(|_, prefixes| !prefixes.is_empty());

                        // advance the consolidation frontier (TODO: wierd lexicographic times!)
                        if let Some(trace) = counts_trace.as_mut() {
                            trace.advance_by(&input1.frontier().frontier());
                        }

                        if input1.frontier().is_empty() && stash.is_empty() {
                            counts_trace = None;
                        }
                    }
                })
                .as_collection(),
        )
    }

    fn propose(&mut self, prefixes: &Collection<S, P>) -> Collection<S, (P, V)> {
        let propose = &self.indices.propose;
        let mut propose_trace = Some(propose.trace.clone());

        let mut stash = HashMap::new();
        let logic1 = self.key_selector.clone();
        let logic2 = self.key_selector.clone();

        let mut buffer1 = Vec::new();
        let mut buffer2 = Vec::new();

        let exchange = Exchange::new(move |update: &(P, S::Timestamp, isize)| {
            logic1(&update.0).hashed().as_u64()
        });

        let fallback = self.fallback.clone();

        prefixes
            .inner
            .binary_frontier(
                &propose.stream,
                exchange,
                Pipeline,
                "Propose",
                move |_, _| {
                    move |input1, input2, output| {
                        // drain the first input, stashing requests.
                        input1.for_each(|capability, data| {
                            data.swap(&mut buffer1);
                            stash
                                .entry(capability.retain())
                                .or_insert_with(Vec::new)
                                .extend(buffer1.drain(..))
                        });

                        // advance the `distinguish_since` frontier to allow all merges.
                        input2.for_each(|_, batches| {
                            batches.swap(&mut buffer2);
                            for batch in buffer2.drain(..) {
                                if let Some(ref mut trace) = propose_trace {
                                    trace.distinguish_since(batch.upper());
                                }
                            }
                        });

                        if let Some(ref mut trace) = propose_trace {
                            for (capability, prefixes) in stash.iter_mut() {
                                // defer requests at incomplete times.
                                // NOTE: not all updates may be at complete times, but if this test fails then none of them are.
                                if !input2.frontier.less_equal(capability.time()) {
                                    let mut session = output.session(capability);

                                    // sort requests for in-order cursor traversal. could consolidate?
                                    prefixes.sort_by(|x, y| logic2(&x.0).cmp(&logic2(&y.0)));

                                    let (mut cursor, storage) = trace.cursor();

                                    for &mut (ref prefix, ref time, ref mut diff) in
                                        prefixes.iter_mut()
                                    {
                                        if !input2.frontier.less_equal(time) {
                                            let key = logic2(prefix);
                                            cursor.seek_key(&storage, &key);
                                            if cursor.get_key(&storage) == Some(&key) {
                                                while let Some(value) = cursor.get_val(&storage) {
                                                    let mut count = 0;
                                                    cursor.map_times(&storage, |t, d| {
                                                        if t.less_equal(time) {
                                                            count += d;
                                                        }
                                                    });
                                                    // assert!(count >= 0);
                                                    if count > 0 {
                                                        session.give((
                                                            (prefix.clone(), value.clone()),
                                                            time.clone(),
                                                            *diff,
                                                        ));
                                                    }
                                                    cursor.step_val(&storage);
                                                }
                                                cursor.rewind_vals(&storage);
                                            } else if let Some(value) = &fallback {
                                                session.give((
                                                    (prefix.clone(), value.clone()),
                                                    time.clone(),
                                                    *diff,
                                                ));
                                            }
                                            *diff = 0;
                                        }
                                    }

                                    prefixes.retain(|ptd| ptd.2 != 0);
                                }
                            }
                        }

                        // drop fully processed capabilities.
                        stash.retain(|_, prefixes| !prefixes.is_empty());

                        // advance the consolidation frontier (TODO: wierd lexicographic times!)
                        if let Some(trace) = propose_trace.as_mut() {
                            trace.advance_by(&input1.frontier().frontier());
                        }

                        if input1.frontier().is_empty() && stash.is_empty() {
                            propose_trace = None;
                        }
                    }
                },
            )
            .as_collection()
    }

    fn validate(&mut self, extensions: &Collection<S, (P, V)>) -> Collection<S, (P, V)> {
        // This method takes a stream of `(prefix, time, diff)` changes, and we want to produce the corresponding
        // stream of `((prefix, count), time, diff)` changes, just by looking up `count` in `count_trace`. We are
        // just doing a stream of changes and a stream of look-ups, no consolidation or any funny business like
        // that. We *could* organize the input differences by key and save some time, or we could skip that.

        let validate = &self.indices.validate;
        let mut validate_trace = Some(validate.trace.clone());

        let mut stash = HashMap::new();
        let logic1 = self.key_selector.clone();
        let logic2 = self.key_selector.clone();

        let mut buffer1 = Vec::new();
        let mut buffer2 = Vec::new();

        let exchange = Exchange::new(move |update: &((P, V), S::Timestamp, isize)| {
            (logic1(&(update.0).0).clone(), ((update.0).1).clone())
                .hashed()
                .as_u64()
        });

        let fallback = self.fallback.clone();

        extensions
            .inner
            .binary_frontier(
                &validate.stream,
                exchange,
                Pipeline,
                "Validate",
                move |_, _| {
                    move |input1, input2, output| {
                        // drain the first input, stashing requests.
                        input1.for_each(|capability, data| {
                            data.swap(&mut buffer1);
                            stash
                                .entry(capability.retain())
                                .or_insert_with(Vec::new)
                                .extend(buffer1.drain(..))
                        });

                        // advance the `distinguish_since` frontier to allow all merges.
                        input2.for_each(|_, batches| {
                            batches.swap(&mut buffer2);
                            for batch in buffer2.drain(..) {
                                if let Some(ref mut trace) = validate_trace {
                                    trace.distinguish_since(batch.upper());
                                }
                            }
                        });

                        if let Some(ref mut trace) = validate_trace {
                            for (capability, prefixes) in stash.iter_mut() {
                                // defer requests at incomplete times.
                                // NOTE: not all updates may be at complete times, but if this test fails then none of them are.
                                if !input2.frontier.less_equal(capability.time()) {
                                    let mut session = output.session(capability);

                                    // sort requests for in-order cursor traversal. could consolidate?
                                    prefixes.sort_by(|x, y| {
                                        (logic2(&(x.0).0), &((x.0).1))
                                            .cmp(&(logic2(&(y.0).0), &((y.0).1)))
                                    });

                                    let (mut cursor, storage) = trace.cursor();

                                    for &mut (ref prefix, ref time, ref mut diff) in
                                        prefixes.iter_mut()
                                    {
                                        if !input2.frontier.less_equal(time) {
                                            let key = (logic2(&prefix.0), (prefix.1).clone());
                                            cursor.seek_key(&storage, &key);
                                            if cursor.get_key(&storage) == Some(&key) {
                                                let mut count = 0;
                                                cursor.map_times(&storage, |t, d| {
                                                    if t.less_equal(time) {
                                                        count += d;
                                                    }
                                                });
                                                // assert!(count >= 0);
                                                if count > 0 {
                                                    session.give((
                                                        prefix.clone(),
                                                        time.clone(),
                                                        *diff,
                                                    ));
                                                }
                                            } else if let Some(value) = &fallback {
                                                if *value == prefix.1 {
                                                    session.give((
                                                        prefix.clone(),
                                                        time.clone(),
                                                        *diff,
                                                    ));
                                                }
                                            }
                                            *diff = 0;
                                        }
                                    }

                                    prefixes.retain(|ptd| ptd.2 != 0);
                                }
                            }
                        }

                        // drop fully processed capabilities.
                        stash.retain(|_, prefixes| !prefixes.is_empty());

                        // advance the consolidation frontier (TODO: wierd lexicographic times!)
                        if let Some(trace) = validate_trace.as_mut() {
                            trace.advance_by(&input1.frontier().frontier());
                        }

                        if input1.frontier().is_empty() && stash.is_empty() {
                            validate_trace = None;
                        }
                    }
                },
            )
            .as_collection()
    }
}

struct AntijoinExtender<'a, S, V, P>
where
    S: Scope,
    S::Timestamp: Lattice + Data,
    V: Data,
{
    phantom: std::marker::PhantomData<P>,
    extender: Extender<'a, S, P, V>,
}

impl<'a, S, V, P> PrefixExtender<S> for AntijoinExtender<'a, S, V, P>
where
    S: Scope,
    S::Timestamp: Lattice + Data,
    V: Data + Hash,
    P: Data,
{
    type Prefix = P;
    type Extension = V;

    fn count(
        &mut self,
        _prefixes: &Collection<S, (P, usize, usize)>,
        _index: usize,
    ) -> Option<Collection<S, (P, usize, usize)>> {
        None
    }

    fn propose(&mut self, prefixes: &Collection<S, P>) -> Collection<S, (P, V)> {
        prefixes.map(|_prefix| panic!("AntijoinExtender should never be asked to propose."))
    }

    fn validate(&mut self, extensions: &Collection<S, (P, V)>) -> Collection<S, (P, V)> {
        extensions.concat(&self.extender.validate(extensions).negate())
    }
}
