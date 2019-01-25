//! WCO expression plan, integrating the following work:
//! https://github.com/frankmcsherry/differential-dataflow/tree/master/dogsdogsdogs

use std::rc::Rc;
use std::collections::HashMap;
use std::hash::Hash;

use timely::PartialOrder;
use timely::dataflow::{Scope, ScopeParent};
use timely::dataflow::channels::pact::{Pipeline, Exchange};
use timely::dataflow::operators::Operator;
use timely::progress::Timestamp;
use timely::order::Product;
use timely::dataflow::operators::Partition;
use timely::dataflow::operators::Concatenate;
// use timely::dataflow::operators::Inspect;
use timely::dataflow::scopes::child::{Child, Iterative};

use timely_sort::Unsigned;

use differential_dataflow::{Data, Collection, AsCollection, Hashable};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::Threshold;
use differential_dataflow::trace::{Cursor, TraceReader, BatchReader};

use timestamp::altneu::AltNeu;
use plan::{ImplContext, Implementable};
use binding::Binding;
use {VariableMap, CollectionRelation, Value, Var, LiveIndex};

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
    fn count(&mut self, &Collection<G, (Self::Prefix, usize, usize)>, usize) -> Collection<G, (Self::Prefix, usize, usize)>;
    /// Extends each prefix with corresponding extensions.
    fn propose(&mut self, &Collection<G, Self::Prefix>) -> Collection<G, (Self::Prefix, Self::Extension)>;
    /// Restricts proposed extensions by those the extender would have proposed.
    fn validate(&mut self, &Collection<G, (Self::Prefix, Self::Extension)>) -> Collection<G, (Self::Prefix, Self::Extension)>;
}

// The only thing we know how to make an extender out of (at the
// moment) is a collection. This could be generalized to any type that
// can return something implementing PrefixExtender.

trait IntoExtender<'a, S, K, V, TrCount, TrPropose, TrValidate>
where
    S: Scope+ScopeParent,
    K: Data+Hash,
    V: Data+Hash,
    S::Timestamp: Lattice+Data+Timestamp,
    TrCount: TraceReader<K, (), AltNeu<S::Timestamp>, isize>+Clone,
    TrPropose: TraceReader<K, V, AltNeu<S::Timestamp>, isize>+Clone,
    TrValidate: TraceReader<(K,V), (), AltNeu<S::Timestamp>, isize>+Clone,
{
    fn extender_using<P, F: Fn(&P)->K>(
        &self,
        logic: F
    ) -> CollectionExtender<'a, S, K, V, P, F, TrCount, TrPropose, TrValidate>;
}

impl<'a, S, K, V, TrCount, TrPropose, TrValidate> IntoExtender<'a, S, K, V, TrCount, TrPropose, TrValidate>
    for LiveIndex<Child<'a, S, AltNeu<S::Timestamp>>, K, V, TrCount, TrPropose, TrValidate>
where
    S: Scope+ScopeParent,
    K: Data+Hash,
    V: Data+Hash,
    S::Timestamp: Lattice+Data+Timestamp,
    TrCount: TraceReader<K, (), AltNeu<S::Timestamp>, isize>+Clone,
    TrPropose: TraceReader<K, V, AltNeu<S::Timestamp>, isize>+Clone,
    TrValidate: TraceReader<(K,V), (), AltNeu<S::Timestamp>, isize>+Clone,
{    
    fn extender_using<P, F: Fn(&P)->K>(
        &self,
        logic: F
    ) -> CollectionExtender<'a, S, K, V, P, F, TrCount, TrPropose, TrValidate>
    {
        CollectionExtender {
            phantom: std::marker::PhantomData,
            indices: self.clone(),
            key_selector: Rc::new(logic),
        }
    }
}

//
// OPERATOR
//

/// A plan stage joining two source relations on the specified
/// symbols. Throws if any of the join symbols isn't bound by both
/// sources.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Hector {
    /// @TODO
    pub variables: Vec<Var>,
    /// Bindings to join.
    pub bindings: Vec<Binding>,
}

enum Direction { Forward(usize), Reverse(usize), }

fn direction(prefix_symbols: (Var,Var), extender_symbols: (Var,Var)) -> Result<Direction, &'static str> {
    if prefix_symbols == extender_symbols {
        Err("Attempt to intersect attribute with itself")
    } else if prefix_symbols.0 == extender_symbols.0 {
        // forward select_e
        Ok(Direction::Forward(0))
    } else if prefix_symbols.0 == extender_symbols.1 {
        // forward select_v
        Ok(Direction::Forward(1))
    } else if prefix_symbols.1 == extender_symbols.0 {
        // reverse select_e
        Ok(Direction::Reverse(0))
    } else if prefix_symbols.1 == extender_symbols.1 {
        // reverse select_v
        Ok(Direction::Reverse(1))
    } else {
        Err("Requested variable not bound by Attribute")
    }
}

fn select_e((e,_v): &(Value,Value)) -> Value { e.clone() }
fn select_v((_e,v): &(Value,Value)) -> Value { v.clone() }

impl Implementable for Hector
{
    fn dependencies(&self) -> Vec<String> { Vec::new() }

    fn into_bindings(&self) -> Vec<Binding> { self.bindings.clone() }
    
    fn implement<'b, S: Scope<Timestamp = u64>, I: ImplContext>(
        &self,
        nested: &mut Iterative<'b, S, u64>,
        _local_arrangements: &VariableMap<Iterative<'b, S, u64>>,
        context: &mut I,
    ) -> CollectionRelation<'b, S> {

        let joined = nested.scoped::<AltNeu<Product<u64,u64>>, _, _>("AltNeu", |inner| {

            let scope = inner.clone();

            // Avoid importing things more than once.
            let mut forward_import = HashMap::new();
            let mut forward_alt = HashMap::new();
            let mut forward_neu = HashMap::new();
            let mut reverse_import = HashMap::new();
            let mut reverse_alt = HashMap::new();
            let mut reverse_neu = HashMap::new();

            // For each AttributeBinding (only AttributeBindings
            // actually experience change), we construct a delta query
            // driven by changes to that binding.

            let changes = self.bindings.iter().enumerate()
                .flat_map(|(idx, delta_binding)| match delta_binding {
                    Binding::Attribute(delta_binding) => {
                        
                        let mut extenders: Vec<Box<dyn PrefixExtender<Child<'_, Iterative<'b, S, u64>, AltNeu<Product<u64, u64>>>, Prefix=(Value, Value), Extension=_>>> = vec![];

                        for (other_idx, other) in self.bindings.iter().enumerate() {

                            // We need to distinguish between conflicting relations
                            // that appear before the current one in the sequence (< idx),
                            // and those that appear afterwards.

                            if other_idx == idx {
                                continue
                            }

                            let (is_neu, forward_cache, reverse_cache) = if other_idx < idx {
                                (false, &mut forward_alt, &mut reverse_alt)
                            } else {
                                (true, &mut forward_neu, &mut reverse_neu)
                            };

                            match other {
                                Binding::Constant(other) => {
                                    extenders.push(Box::new(ConstantExtender {
                                        phantom: std::marker::PhantomData,
                                        value: other.value.clone(),
                                    }));
                                }
                                Binding::Attribute(other) => {
                                    match direction(other.symbols, delta_binding.symbols) {
                                        Err(msg) => panic!(msg),
                                        Ok(direction) => match direction {
                                            Direction::Forward(offset) => {
                                                let forward = forward_cache.entry(&other.source_attribute)
                                                    .or_insert_with(|| {
                                                        let imported = forward_import.entry(&other.source_attribute)
                                                            .or_insert_with(|| {
                                                                context.forward_index(&other.source_attribute).unwrap()
                                                                    .import(&scope.parent.parent)
                                                                    .enter(&scope.parent)
                                                            });

                                                        let neu1 = is_neu.clone();
                                                        let neu2 = is_neu.clone();
                                                        let neu3 = is_neu.clone();
                                                        
                                                        imported.enter_at(
                                                            &scope,
                                                            move |_,_,t| AltNeu { time: t.clone(), neu: neu1 },
                                                            move |_,_,t| AltNeu { time: t.clone(), neu: neu2 },
                                                            move |_,_,t| AltNeu { time: t.clone(), neu: neu3 },
                                                        )
                                                    });

                                                if offset == 0 {
                                                    extenders.push(Box::new(forward.extender_using(select_e)));
                                                } else {
                                                    extenders.push(Box::new(forward.extender_using(select_v)));
                                                }
                                            }
                                            Direction::Reverse(offset) => {
                                                let reverse = reverse_cache.entry(&other.source_attribute)
                                                    .or_insert_with(|| {
                                                        let imported = reverse_import.entry(&other.source_attribute)
                                                            .or_insert_with(|| {
                                                                context.reverse_index(&other.source_attribute).unwrap()
                                                                    .import(&scope.parent.parent)
                                                                    .enter(&scope.parent)
                                                            });

                                                        let neu1 = is_neu.clone();
                                                        let neu2 = is_neu.clone();
                                                        let neu3 = is_neu.clone();
                                                        
                                                        imported.enter_at(
                                                            &scope,
                                                            move |_,_,t| AltNeu { time: t.clone(), neu: neu1 },
                                                            move |_,_,t| AltNeu { time: t.clone(), neu: neu2 },
                                                            move |_,_,t| AltNeu { time: t.clone(), neu: neu3 },
                                                        )
                                                    });

                                                if offset == 0 {
                                                    extenders.push(Box::new(reverse.extender_using(select_e)));
                                                } else {
                                                    extenders.push(Box::new(reverse.extender_using(select_v)));
                                                }
                                            }
                                        }
                                    }
                                }
                            }                
                        }
                        
                        // @TODO impl ProposeExtensionMethod for Arranged
                        let output = forward_import.entry(&delta_binding.source_attribute)
                            .or_insert_with(|| {
                                context.forward_index(&delta_binding.source_attribute).unwrap()
                                    .import(&scope.parent.parent)
                                    .enter(&scope.parent)
                            })
                            .validate_trace
                            .enter(&scope)
                            .as_collection(|tuple,()| tuple.clone())
                            .extend(&mut extenders[..])
                            .map(|((e,v1),v2)| {
                                // @TODO project correctly

                                //
                                
                                vec![e,v1,v2]
                            });

                        Some(output.inner)
                    }
                    _ => None
                });

            inner.concatenate(changes).as_collection().leave()
        });

        CollectionRelation {
            symbols: vec![],
            tuples: joined.distinct(),
        }
    }
}

//
// GENERIC IMPLEMENTATION
//

trait ProposeExtensionMethod<'a, S: Scope+ScopeParent, P: Data+Ord> {
    fn propose_using<PE: PrefixExtender<Child<'a, S, AltNeu<S::Timestamp>>, Prefix=P>>
        (&self, extender: &mut PE) -> Collection<Child<'a, S, AltNeu<S::Timestamp>>, (P, PE::Extension)>;
    
    fn extend<E: Data+Ord>(
        &self,
        extenders: &mut [Box<(dyn PrefixExtender<Child<'a, S, AltNeu<S::Timestamp>>,Prefix=P,Extension=E> + 'a)>]
    ) -> Collection<Child<'a, S, AltNeu<S::Timestamp>>, (P, E)>;
}

impl<'a, S: Scope+ScopeParent, P: Data+Ord> ProposeExtensionMethod<'a, S, P>
    for Collection<Child<'a, S, AltNeu<S::Timestamp>>, P>
{
    fn propose_using<PE: PrefixExtender<Child<'a, S, AltNeu<S::Timestamp>>, Prefix=P>>
        (&self, extender: &mut PE) -> Collection<Child<'a, S, AltNeu<S::Timestamp>>, (P, PE::Extension)>
    {
        extender.propose(self)
    }
    
    fn extend<E: Data+Ord>(
        &self,
        extenders: &mut [Box<(dyn PrefixExtender<Child<'a, S, AltNeu<S::Timestamp>>, Prefix=P, Extension=E> + 'a)>]
    ) -> Collection<Child<'a, S, AltNeu<S::Timestamp>>, (P, E)>
    {
        if extenders.len() == 1 {
            extenders[0].propose(&self.clone())
        }
        else {
            let mut counts = self.map(|p| (p, 1 << 31, 0));
            for (index,extender) in extenders.iter_mut().enumerate() {
                counts = extender.count(&counts, index);
            }

            let parts = counts.inner.partition(extenders.len() as u64, |((p, _, i),t,d)| (i as u64, (p,t,d)));

            let mut results = Vec::new();
            for (index, nominations) in parts.into_iter().enumerate() {
                let mut extensions = extenders[index].propose(&nominations.as_collection());
                for other in (0..extenders.len()).filter(|&x| x != index) {
                    extensions = extenders[other].validate(&extensions);
                }

                results.push(extensions.inner);    // save extensions
            }

            self.scope().concatenate(results).as_collection()
        }
    }
}

struct ConstantExtender<P, V>
where
    V: Data+Hash
{
    phantom: std::marker::PhantomData<P>,
    value: V,
}

impl<'a, S, V, P> PrefixExtender<Child<'a, S, AltNeu<S::Timestamp>>>
    for ConstantExtender<P, V>
where
    S: Scope+ScopeParent,
    S::Timestamp: Lattice+Data,
    V: Data+Hash,
    P: Data,
{
    type Prefix = P;
    type Extension = V;

    fn count(
        &mut self,
        prefixes: &Collection<Child<'a, S, AltNeu<S::Timestamp>>, (P, usize, usize)>,
        index: usize
    ) -> Collection<Child<'a, S, AltNeu<S::Timestamp>>, (P, usize, usize)>
    {
        prefixes.map(move |(prefix, old_count, old_index)| {
            if 1 < old_count {
                (prefix.clone(), 1, index)
            } else {
                (prefix.clone(), old_count, old_index)
            }
        })
    }

    fn propose(
        &mut self,
        prefixes: &Collection<Child<'a, S, AltNeu<S::Timestamp>>, P>
    ) -> Collection<Child<'a, S, AltNeu<S::Timestamp>>, (P, V)>
    {
        let value = self.value.clone();
        prefixes.map(move |prefix| (prefix.clone(), value.clone()))
    }

    fn validate(
        &mut self,
        extensions: &Collection<Child<'a, S, AltNeu<S::Timestamp>>, (P, V)>
    ) -> Collection<Child<'a, S, AltNeu<S::Timestamp>>, (P, V)>
    {
        let target = self.value.clone();
        extensions
            .filter(move |(_prefix, extension)| *extension == target)
    }
}

struct CollectionExtender<'a, S, K, V, P, F, TrCount, TrPropose, TrValidate>
where
    S: Scope+ScopeParent,
    S::Timestamp: Lattice+Data,
    K: Data,
    V: Data,
    F: Fn(&P)->K,
    TrCount: TraceReader<K, (), AltNeu<S::Timestamp>, isize>+Clone+'static,
    TrPropose: TraceReader<K, V, AltNeu<S::Timestamp>, isize>+Clone+'static,
    TrValidate: TraceReader<(K,V), (), AltNeu<S::Timestamp>, isize>+Clone+'static,
{
    phantom: std::marker::PhantomData<P>,
    indices: LiveIndex<Child<'a, S, AltNeu<S::Timestamp>>, K, V, TrCount, TrPropose, TrValidate>,
    key_selector: Rc<F>,
}

impl<'a, S, K, V, P, F, TrCount, TrPropose, TrValidate> PrefixExtender<Child<'a, S, AltNeu<S::Timestamp>>>
    for CollectionExtender<'a, S, K, V, P, F, TrCount, TrPropose, TrValidate>
where
    S: Scope+ScopeParent,
    S::Timestamp: Lattice+Data,
    K: Data+Hash,
    V: Data+Hash,
    P: Data,
    F: Fn(&P)->K+'static,
    TrCount: TraceReader<K, (), AltNeu<S::Timestamp>, isize>+Clone+'static,
    TrPropose: TraceReader<K, V, AltNeu<S::Timestamp>, isize>+Clone+'static,
    TrValidate: TraceReader<(K,V), (), AltNeu<S::Timestamp>, isize>+Clone+'static,
{

    type Prefix = P;
    type Extension = V;

    fn count(
        &mut self,
        prefixes: &Collection<Child<'a, S, AltNeu<S::Timestamp>>, (P, usize, usize)>,
        index: usize
    ) -> Collection<Child<'a, S, AltNeu<S::Timestamp>>, (P, usize, usize)>
    {
        // This method takes a stream of `(prefix, time, diff)`
        // changes, and we want to produce the corresponding stream of
        // `((prefix, count), time, diff)` changes, just by looking up
        // `count` in `count_trace`. We are just doing a stream of
        // changes and a stream of look-ups, no consolidation or any
        // funny business like that. We *could* organize the input
        // differences by key and save some time, or we could skip
        // that.

        let counts = &self.indices.count_trace;
        let mut counts_trace = Some(counts.trace.clone());

        let mut stash = HashMap::new();
        let logic1 = self.key_selector.clone();
        let logic2 = self.key_selector.clone();

        let exchange = Exchange::new(move |update: &((P,usize,usize),AltNeu<S::Timestamp>,isize)| logic1(&(update.0).0).hashed().as_u64());

        let mut buffer1 = Vec::new();
        let mut buffer2 = Vec::new();

        // TODO: This should be a custom operator with no connection from the second input to the output.
        prefixes.inner.binary_frontier(&counts.stream, exchange, Pipeline, "Count", move |_,_| move |input1, input2, output| {

            // drain the first input, stashing requests.
            input1.for_each(|capability, data| {
                data.swap(&mut buffer1);
                stash.entry(capability.retain())
                     .or_insert(Vec::new())
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
                        prefixes.sort_by(|x,y| logic2(&(x.0).0).cmp(&logic2(&(y.0).0)));

                        let (mut cursor, storage) = trace.cursor();

                        for &mut ((ref prefix, old_count, old_index), ref time, ref mut diff) in prefixes.iter_mut() {
                            if !input2.frontier.less_equal(time) {
                                let key = logic2(prefix);
                                cursor.seek_key(&storage, &key);
                                if cursor.get_key(&storage) == Some(&key) {
                                    let mut count = 0;
                                    cursor.map_times(&storage, |t, d| if t.less_equal(time) { count += d; });
                                    // assert!(count >= 0);
                                    let count = count as usize;
                                    if count > 0 {
                                        if count < old_count {
                                            session.give(((prefix.clone(), count, index), time.clone(), diff.clone()));
                                        }
                                        else {
                                            session.give(((prefix.clone(), old_count, old_index), time.clone(), diff.clone()));
                                        }
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
            stash.retain(|_,prefixes| !prefixes.is_empty());

            // advance the consolidation frontier (TODO: wierd lexicographic times!)
            counts_trace.as_mut().map(|trace| trace.advance_by(&input1.frontier().frontier()));

            if input1.frontier().is_empty() && stash.is_empty() {
                counts_trace = None;
            }

        }).as_collection()
    }

    fn propose(
        &mut self,
        prefixes: &Collection<Child<'a, S, AltNeu<S::Timestamp>>, P>
    ) -> Collection<Child<'a, S, AltNeu<S::Timestamp>>, (P, V)>
    {
        let propose = &self.indices.propose_trace;
        let mut propose_trace = Some(propose.trace.clone());

        let mut stash = HashMap::new();
        let logic1 = self.key_selector.clone();
        let logic2 = self.key_selector.clone();

        let mut buffer1 = Vec::new();
        let mut buffer2 = Vec::new();

        let exchange = Exchange::new(move |update: &(P,AltNeu<S::Timestamp>,isize)| logic1(&update.0).hashed().as_u64());

        prefixes.inner.binary_frontier(&propose.stream, exchange, Pipeline, "Propose", move |_,_| move |input1, input2, output| {

            // drain the first input, stashing requests.
            input1.for_each(|capability, data| {
                data.swap(&mut buffer1);
                stash.entry(capability.retain())
                     .or_insert(Vec::new())
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
                        prefixes.sort_by(|x,y| logic2(&x.0).cmp(&logic2(&y.0)));

                        let (mut cursor, storage) = trace.cursor();

                        for &mut (ref prefix, ref time, ref mut diff) in prefixes.iter_mut() {
                            if !input2.frontier.less_equal(time) {
                                let key = logic2(prefix);
                                cursor.seek_key(&storage, &key);
                                if cursor.get_key(&storage) == Some(&key) {
                                    while let Some(value) = cursor.get_val(&storage) {
                                        let mut count = 0;
                                        cursor.map_times(&storage, |t, d| if t.less_equal(time) { count += d; });
                                        // assert!(count >= 0);
                                        if count > 0 {
                                            session.give(((prefix.clone(), value.clone()), time.clone(), diff.clone()));
                                        }
                                        cursor.step_val(&storage);
                                    }
                                    cursor.rewind_vals(&storage);
                                }
                                *diff = 0;
                            }
                        }

                        prefixes.retain(|ptd| ptd.2 != 0);
                    }
                }
            }

            // drop fully processed capabilities.
            stash.retain(|_,prefixes| !prefixes.is_empty());

            // advance the consolidation frontier (TODO: wierd lexicographic times!)
            propose_trace.as_mut().map(|trace| trace.advance_by(&input1.frontier().frontier()));

            if input1.frontier().is_empty() && stash.is_empty() {
                propose_trace = None;
            }

        }).as_collection()
    }

    fn validate(
        &mut self,
        extensions: &Collection<Child<'a, S, AltNeu<S::Timestamp>>, (P, V)>
    ) -> Collection<Child<'a, S, AltNeu<S::Timestamp>>, (P, V)> {

        // This method takes a stream of `(prefix, time, diff)` changes, and we want to produce the corresponding
        // stream of `((prefix, count), time, diff)` changes, just by looking up `count` in `count_trace`. We are
        // just doing a stream of changes and a stream of look-ups, no consolidation or any funny business like
        // that. We *could* organize the input differences by key and save some time, or we could skip that.

        let validate = &self.indices.validate_trace;
        let mut validate_trace = Some(validate.trace.clone());

        let mut stash = HashMap::new();
        let logic1 = self.key_selector.clone();
        let logic2 = self.key_selector.clone();

        let mut buffer1 = Vec::new();
        let mut buffer2 = Vec::new();

        let exchange = Exchange::new(move |update: &((P,V),AltNeu<S::Timestamp>,isize)|
            (logic1(&(update.0).0).clone(), ((update.0).1).clone()).hashed().as_u64()
        );

        extensions.inner.binary_frontier(&validate.stream, exchange, Pipeline, "Validate", move |_,_| move |input1, input2, output| {

            // drain the first input, stashing requests.
            input1.for_each(|capability, data| {
                data.swap(&mut buffer1);
                stash.entry(capability.retain())
                     .or_insert(Vec::new())
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
                        prefixes.sort_by(|x,y| (logic2(&(x.0).0), &((x.0).1)).cmp(&(logic2(&(y.0).0), &((y.0).1))));

                        let (mut cursor, storage) = trace.cursor();

                        for &mut (ref prefix, ref time, ref mut diff) in prefixes.iter_mut() {
                            if !input2.frontier.less_equal(time) {
                                let key = (logic2(&prefix.0), (prefix.1).clone());
                                cursor.seek_key(&storage, &key);
                                if cursor.get_key(&storage) == Some(&key) {
                                    let mut count = 0;
                                    cursor.map_times(&storage, |t, d| if t.less_equal(time) { count += d; });
                                    // assert!(count >= 0);
                                    if count > 0 {
                                        session.give((prefix.clone(), time.clone(), diff.clone()));
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
            stash.retain(|_,prefixes| !prefixes.is_empty());

            // advance the consolidation frontier (TODO: wierd lexicographic times!)
            validate_trace.as_mut().map(|trace| trace.advance_by(&input1.frontier().frontier()));

            if input1.frontier().is_empty() && stash.is_empty() {
                validate_trace = None;
            }

        }).as_collection()

    }

}
