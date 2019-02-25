//! WCO expression plan, integrating the following work:
//! https://github.com/frankmcsherry/differential-dataflow/tree/master/dogsdogsdogs

use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::rc::Rc;

use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::operators::Concatenate;
use timely::dataflow::operators::Operator;
use timely::dataflow::operators::Partition;
use timely::dataflow::scopes::child::Iterative;
use timely::dataflow::Scope;
use timely::order::{Product, TotalOrder};
use timely::progress::Timestamp;
use timely::PartialOrder;

use timely_sort::Unsigned;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::Threshold;
use differential_dataflow::trace::{BatchReader, Cursor, TraceReader};
use differential_dataflow::{AsCollection, Collection, Data, Hashable};

use crate::binding::{AsBinding, BinaryPredicate, Binding};
use crate::binding::{BinaryPredicateBinding, ConstantBinding};
use crate::plan::{Dependencies, ImplContext, Implementable};
use crate::timestamp::altneu::AltNeu;
use crate::{CollectionRelation, LiveIndex, VariableMap};
use crate::{Value, Var};

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
        prefix_symbols: &B,
    ) -> Vec<Extender<'a, S, P, V>>;
}

impl<'a, S> IntoExtender<'a, S, Value> for ConstantBinding
where
    S: Scope,
    S::Timestamp: Timestamp + Lattice,
{
    fn into_extender<P: Data + IndexNode<Value>, B: AsBinding + std::fmt::Debug>(
        &self,
        _prefix_symbols: &B,
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
        prefix_symbols: &B,
    ) -> Vec<Extender<'a, S, P, V>> {
        match direction(prefix_symbols, self.symbols) {
            Err(_msg) => {
                // We won't panic here, this just means the predicate's symbols
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
/// symbols. Throws if any of the join symbols isn't bound by both
/// sources.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub struct Hector {
    /// Symbols to bind.
    pub variables: Vec<Var>,
    /// Bindings to join.
    pub bindings: Vec<Binding>,
}

enum Direction {
    Forward(usize),
    Reverse(usize),
}

fn direction<P>(prefix_symbols: &P, extender_symbols: (Var, Var)) -> Result<Direction, &'static str>
where
    P: AsBinding + std::fmt::Debug,
{
    match AsBinding::binds(prefix_symbols, extender_symbols.0) {
        None => match AsBinding::binds(prefix_symbols, extender_symbols.1) {
            None => {
                println!(
                    "Neither extender symbol {:?} bound by prefix {:?}.",
                    extender_symbols, prefix_symbols
                );
                Err("Neither extender symbol bound by prefix.")
            }
            Some(offset) => Ok(Direction::Reverse(offset)),
        },
        Some(offset) => {
            match AsBinding::binds(prefix_symbols, extender_symbols.1) {
                Some(_) => Err("Both extender symbols already bound by prefix."),
                None => {
                    // Prefix binds the first extender symbol, but not
                    // the second. Can use forward index.
                    Ok(Direction::Forward(offset))
                }
            }
        }
    }
}

trait IndexNode<V> {
    fn index(&self, index: usize) -> V;
}

impl IndexNode<Value> for Vec<Value> {
    #[inline(always)]
    fn index(&self, index: usize) -> Value {
        self[index].clone()
    }
}

impl Implementable for Hector {
    fn dependencies(&self) -> Dependencies {
        Dependencies::none()
    }

    fn into_bindings(&self) -> Vec<Binding> {
        self.bindings.clone()
    }

    fn implement<'b, T, I, S>(
        &self,
        nested: &mut Iterative<'b, S, u64>,
        _local_arrangements: &VariableMap<Iterative<'b, S, u64>>,
        context: &mut I,
    ) -> CollectionRelation<'b, S>
    where
        T: Timestamp + Lattice + TotalOrder,
        I: ImplContext<T>,
        S: Scope<Timestamp = T>,
    {
        if self.bindings.is_empty() {
            panic!("No bindings passed.");
        } else if self.variables.is_empty() {
            panic!("No symbols requested.");
        } else if self.bindings.len() == 1 {
            // With only a single binding given, we don't want to do
            // anything fancy (provided the binding is sourceable).

            match self.bindings.first().unwrap() {
                Binding::Attribute(binding) => {
                    match context.forward_index(&binding.source_attribute) {
                        None => panic!("Unknown attribute {}", &binding.source_attribute),
                        Some(index) => {
                            let frontier: Vec<T> = index
                                .validate_trace
                                .advance_frontier()
                                .iter()
                                .cloned()
                                .collect();
                            let tuples = index
                                .validate_trace
                                .import(&nested.parent)
                                // .enter(&nested)
                                .enter_at(nested, move |_, _, time| {
                                    let mut forwarded = time.clone();
                                    forwarded.advance_by(&frontier);
                                    Product::new(forwarded, 0)
                                })
                                .as_collection(|(e, v), ()| vec![e.clone(), v.clone()]);

                            CollectionRelation {
                                symbols: vec![binding.symbols.0, binding.symbols.1],
                                tuples,
                            }
                        }
                    }
                }
                _ => {
                    panic!("Passed a single, non-sourceable binding.");
                }
            }
        } else {
            // In order to avoid delta pipelines looking at each
            // other's data in naughty ways, we need to run them all
            // inside a scope with lexicographic times.

            let joined = nested.scoped::<AltNeu<Product<T,u64>>, _, _>("AltNeu", |inner| {

                let scope = inner.clone();

                // @TODO
                // We need to determine an order on the attributes
                // that ensures that each is bound by preceeding
                // attributes. For now, we will take the requested order.

                // We cache aggressively, to avoid importing and
                // wrapping things more than once.

                let mut forward_cache = HashMap::new();
                let mut reverse_cache = HashMap::new();

                // For each AttributeBinding (only AttributeBindings
                // actually experience change), we construct a delta query
                // driven by changes to that binding.

                // @TODO only do it for distinct attributes
                let changes = self.bindings.iter().enumerate()
                    .flat_map(|(idx, delta_binding)| match delta_binding {
                        Binding::Attribute(delta_binding) => {

                            let mut prefix_symbols = Vec::with_capacity(self.variables.len());

                            // We would like to avoid starting with single-symbol
                            // (or even empty) prefixes, because the dataflow-y nature
                            // of this implementation means we will always be starting
                            // from attributes (which correspond to two-symbol prefixes).
                            // 
                            // But to get away with that we need to check for single-symbol
                            // bindings in conflict with the source binding.

                            // @TODO Not just constant bindings can cause issues here!

                            let mut source_conflicts: Vec<&Binding> = self.bindings.iter()
                                .filter(|other| {
                                    match other {
                                        Binding::Attribute(_) => false,
                                        Binding::BinaryPredicate(_) => false,
                                        _ => {
                                            other.binds(delta_binding.symbols.0).is_some()
                                                || other.binds(delta_binding.symbols.1).is_some()
                                        }
                                    }
                                })
                                .collect();

                            let mut source = if !source_conflicts.is_empty() {
                                // @TODO there can be more than one conflict
                                // for conflict in source_conflicts.drain(..) {
                                let conflict = source_conflicts.pop().unwrap();
                                    match conflict {
                                        Binding::Constant(constant_binding) => {
                                            prefix_symbols.push(constant_binding.symbol);

                                            let match_v = constant_binding.value.clone();

                                            // Guaranteed to intersect with offset zero at this point.
                                            match direction(&prefix_symbols, delta_binding.symbols).unwrap() {
                                                Direction::Forward(_) => {
                                                    prefix_symbols.push(delta_binding.symbols.1);

                                                    let index = forward_cache
                                                        .entry(delta_binding.source_attribute.to_string())
                                                        .or_insert_with(|| {
                                                            context.forward_index(&delta_binding.source_attribute).unwrap()
                                                                .import(&scope.parent.parent)
                                                        });
                                                    let frontier: Vec<T> = index.propose.trace.advance_frontier().iter().cloned().collect();

                                                    index
                                                        .propose
                                                        .filter(move |e,_v| *e == match_v)
                                                        // .enter(&scope.parent)
                                                        .enter_at(&scope.parent, move |_, _, time| {
                                                            let mut forwarded = time.clone(); forwarded.advance_by(&frontier);
                                                            Product::new(forwarded, Default::default())
                                                        })
                                                        .enter(&scope)
                                                        .as_collection(|e,v| vec![e.clone(), v.clone()])
                                                }
                                                Direction::Reverse(_) => {
                                                    prefix_symbols.push(delta_binding.symbols.0);

                                                    let index = reverse_cache
                                                        .entry(delta_binding.source_attribute.to_string())
                                                        .or_insert_with(|| {
                                                            context.reverse_index(&delta_binding.source_attribute).unwrap()
                                                                .import(&scope.parent.parent)
                                                        });
                                                    let frontier: Vec<T> = index.propose.trace.advance_frontier().iter().cloned().collect();

                                                    index
                                                        .propose
                                                        .filter(move |v,_e| *v == match_v)
                                                    // .enter(&scope.parent)
                                                        .enter_at(&scope.parent, move |_, _, time| {
                                                            let mut forwarded = time.clone(); forwarded.advance_by(&frontier);
                                                            Product::new(forwarded, Default::default())
                                                        })
                                                        .enter(&scope)
                                                        .as_collection(|v,e| vec![v.clone(), e.clone()])
                                                }
                                            }
                                        }
                                        _ => panic!("Can't resolve conflicts on {:?} bindings", conflict),
                                    }
                                // }
                            } else {
                                prefix_symbols.push(delta_binding.symbols.0);
                                prefix_symbols.push(delta_binding.symbols.1);

                                let index = forward_cache
                                    .entry(delta_binding.source_attribute.to_string())
                                    .or_insert_with(|| {
                                        context.forward_index(&delta_binding.source_attribute).unwrap()
                                            .import(&scope.parent.parent)
                                    });
                                let frontier: Vec<T> = index.validate.trace.advance_frontier().iter().cloned().collect();

                                index
                                    .validate
                                    // .enter(&scope.parent)
                                    .enter_at(&scope.parent, move |_, _, time| {
                                        let mut forwarded = time.clone();
                                        forwarded.advance_by(&frontier);
                                        Product::new(forwarded, Default::default())
                                    })
                                    .enter(&scope)
                                    .as_collection(|(e,v),()| vec![e.clone(), v.clone()])
                            };

                            for target in self.variables.iter() {
                                match AsBinding::binds(&prefix_symbols, *target) {
                                    Some(_) => { /* already bound */ continue },
                                    None => {
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

                                            // Ignore any binding not talking about the target symbol.
                                            if other.binds(*target).is_none() { continue; }

                                            let is_neu = other_idx >= idx;

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
                                                    extenders.append(&mut other.into_extender(&prefix_symbols));
                                                }
                                                Binding::BinaryPredicate(other) => {
                                                    extenders.append(&mut other.into_extender(&prefix_symbols));
                                                }
                                                Binding::Attribute(other) => {
                                                    match direction(&prefix_symbols, other.symbols) {
                                                        Err(msg) => panic!(msg),
                                                        Ok(direction) => match direction {
                                                            Direction::Forward(offset) => {
                                                                let index = forward_cache.entry(other.source_attribute.to_string())
                                                                    .or_insert_with(|| {
                                                                        context.forward_index(&other.source_attribute).unwrap()
                                                                            .import(&scope.parent.parent)
                                                                    });
                                                                let frontier: Vec<T> = index.propose.trace.advance_frontier().iter().cloned().collect();

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
                                                                        context.reverse_index(&other.source_attribute).unwrap()
                                                                            .import(&scope.parent.parent)
                                                                    });
                                                                let frontier: Vec<T> = index.propose.trace.advance_frontier().iter().cloned().collect();

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

                                        prefix_symbols.push(*target);

                                        // @TODO impl ProposeExtensionMethod for Arranged
                                        source = source
                                            .extend(&mut extenders[..])
                                            .map(|(tuple,v)| {
                                                let mut out = Vec::with_capacity(tuple.len() + 1);
                                                out.append(&mut tuple.clone());
                                                out.push(v);

                                                out
                                            })
                                    }
                                }
                            }

                            if self.variables == prefix_symbols {
                                Some(source.inner)
                            } else {
                                let target_variables = self.variables.clone();
                                Some(source
                                     .map(move |tuple| {
                                         target_variables.iter()
                                             .map(|x| tuple.index(AsBinding::binds(&prefix_symbols, *x).unwrap()))
                                             .collect()
                                     })
                                     .inner)
                            }
                        }
                        _ => None
                    });

                inner.concatenate(changes).as_collection().leave()
            });

            CollectionRelation {
                symbols: self.variables.clone(),
                tuples: joined.distinct(),
            }
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
    TrCount: TraceReader<K, (), S::Timestamp, isize> + Clone + 'static,
    TrPropose: TraceReader<K, V, S::Timestamp, isize> + Clone + 'static,
    TrValidate: TraceReader<(K, V), (), S::Timestamp, isize> + Clone + 'static,
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
    TrCount: TraceReader<K, (), S::Timestamp, isize> + Clone + 'static,
    TrPropose: TraceReader<K, V, S::Timestamp, isize> + Clone + 'static,
    TrValidate: TraceReader<(K, V), (), S::Timestamp, isize> + Clone + 'static,
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
