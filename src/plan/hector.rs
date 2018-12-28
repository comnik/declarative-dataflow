//! WCO expression plan, integrating the following work:
//! https://github.com/frankmcsherry/differential-dataflow/tree/master/dogsdogsdogs

use std::rc::Rc;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::hash::Hash;

use timely::PartialOrder;
use timely::dataflow::Scope;
use timely::dataflow::channels::pact::{Pipeline, Exchange};
use timely::dataflow::operators::Operator;
use timely::progress::Timestamp;
use timely::order::Product;
use timely::dataflow::operators::Partition;
use timely::dataflow::operators::Concatenate;
use timely::communication::Allocate;
// use timely::dataflow::operators::Inspect;
use timely::dataflow::scopes::child::{Child, Iterative};
use timely::worker::Worker;

use timely_sort::Unsigned;

use differential_dataflow::{Data, Collection, AsCollection, Hashable};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::Threshold;
use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::operators::arrange::Arrange;
use differential_dataflow::trace::{Cursor, TraceReader, BatchReader};
use differential_dataflow::trace::implementations::spine_fueled::Spine;
use differential_dataflow::trace::implementations::ord::{OrdValBatch, OrdKeyBatch};

use timestamp::altneu::AltNeu;
use plan::Implementable;
use {QueryMap, RelationMap, SimpleRelation, Value, Var};

//
// OPERATOR
//

/// A plan stage joining two source relations on the specified
/// symbols. Throws if any of the join symbols isn't bound by both
/// sources.
#[derive(Deserialize, Clone, Debug)]
pub struct Hector {
    /// Bindings to join.
    pub bindings: Vec<Binding>,
}

/// Describes symbols whose possible values are given by a global
/// arrangement.
#[derive(Deserialize, Clone, Debug)]
pub struct Binding {
    /// The symbols this binding talks about.
    pub symbols: (Var,Var),
    /// The name of a globally registered arrangement.
    pub source_name: String,
}

struct Attribute {
    alt_forward: CollectionIndex<Value, Value, AltNeu<Product<u64,u64>>>,
    neu_forward: CollectionIndex<Value, Value, AltNeu<Product<u64,u64>>>,
    alt_reverse: CollectionIndex<Value, Value, AltNeu<Product<u64,u64>>>,
    neu_reverse: CollectionIndex<Value, Value, AltNeu<Product<u64,u64>>>,
}

impl Attribute {
    pub fn new<G: Scope<Timestamp=AltNeu<Product<u64,u64>>>> (name: &str, collection: &Collection<G, (Value,Value), isize>) -> Self {
        let forward = collection.clone();
        let reverse = collection.map(|(e,v)| (v,e));
        
        Attribute {
            alt_forward: CollectionIndex::index(name, &forward),
            neu_forward: CollectionIndex::index(name, &forward.delay(|time| AltNeu::neu(time.time.clone()))),
            alt_reverse: CollectionIndex::index(name, &reverse),
            neu_reverse: CollectionIndex::index(name, &reverse.delay(|time| AltNeu::neu(time.time.clone()))),
        }
    }
}

fn select_e((e,_v): &(Value,Value)) -> Value { e.clone() }
fn select_v((_e,v): &(Value,Value)) -> Value { v.clone() }

impl Implementable for Hector {
    fn implement<'a, 'b, A: Allocate>(
        &self,
        nested: &mut Iterative<'b, Child<'a, Worker<A>, u64>, u64>,
        _local_arrangements: &RelationMap<Iterative<'b, Child<'a, Worker<A>, u64>, u64>>,
        global_arrangements: &mut QueryMap<isize>,
    ) -> SimpleRelation<'b, Child<'a, Worker<A>, u64>> {

        let nested_copy = nested.clone();

        let joined = nested.scoped::<AltNeu<Product<u64,u64>>, _, _>("AltNeu", |inner| {

            let mut scope = inner.clone();
            
            // First we prepare the collections that back the
            // attributes, avoiding duplication.
           
            let mut collections = HashMap::new();

            for binding in self.bindings.iter() {
                match global_arrangements.get_mut(&binding.source_name) {
                    None => panic!("{:?} not in query map", binding.source_name),
                    Some(named) => {
                        match collections.entry(binding.source_name.clone()) {
                            Entry::Occupied(x) => { x.into_mut(); },
                            Entry::Vacant(x) => {
                                let collection = named
                                    .import_named(&nested_copy.parent, &binding.source_name)
                                    .enter(&nested_copy)
                                    .enter(&mut scope)
                                    .as_collection(|tuple, _| (tuple[0].clone(), tuple[1].clone()));

                                x.insert(collection);
                            }
                        };
                    }
                }
            }

            // For every collection, we create the corresponding
            // attribute, which will keep track of all required
            // indices on the backing collections.
            //
            // @TODO move these into a global store

            let mut attributes = HashMap::new();
            for (name, collection) in collections.iter() {
                attributes.insert(name.clone(), Attribute::new(&name, collection));
            }
            
            // For each binding, we construct a delta query driven by
            // changes to that binding.

            let changes = self.bindings.iter().enumerate().map(|(idx, delta_binding)| {

                println!("IDX {:?}", idx);
                
                let mut extenders: Vec<Box<dyn PrefixExtender<Child<'_, Child<'_, Child<'_, Worker<A>, u64>, Product<u64, u64>>, AltNeu<Product<u64, u64>>>, Prefix=(Value, Value), Extension=_>>> = vec![];
                
                // @TODO reverse if necessary
                
                if idx > 0 {
                    // Conflicting relations that appear before the
                    // current one in the sequence (< idx)

                    for preceeding in self.bindings.iter().take(idx) {

                        let attribute = attributes.get(&preceeding.source_name).unwrap();
                        
                        if preceeding.symbols == delta_binding.symbols {
                            panic!("Attempt to intersect attribute with itself");
                        } else if preceeding.symbols.0 == delta_binding.symbols.0 {
                            println!("alt forward select_e");
                            extenders.push(Box::new(attribute.alt_forward.extend_using(select_e)));
                        } else if preceeding.symbols.0 == delta_binding.symbols.1 {
                            println!("alt forward select_v");
                            extenders.push(Box::new(attribute.alt_forward.extend_using(select_v)));
                        } else if preceeding.symbols.1 == delta_binding.symbols.0 {
                            println!("alt reverse select_e");
                            extenders.push(Box::new(attribute.alt_reverse.extend_using(select_e)));
                        } else if preceeding.symbols.1 == delta_binding.symbols.1 {
                            println!("alt reverse select_v");
                            extenders.push(Box::new(attribute.alt_reverse.extend_using(select_v)));
                        } else {
                            panic!("Requested variable not bound by Attribute")
                        }
                    }
                }

                if idx < self.bindings.len() {
                    // Conflicting relations that appear after the
                    // current one in the sequence (> idx)

                    for succeeding in self.bindings.iter().skip(idx + 1) {

                        let attribute = attributes.get(&succeeding.source_name).unwrap();
                        
                        if succeeding.symbols == delta_binding.symbols {
                            panic!("Attempt to intersect attribute with itself");
                        } else if succeeding.symbols.0 == delta_binding.symbols.0 {
                            println!("neu forward select_e");
                            extenders.push(Box::new(attribute.neu_forward.extend_using(select_e)));
                        } else if succeeding.symbols.0 == delta_binding.symbols.1 {
                            println!("neu forward select_v");
                            extenders.push(Box::new(attribute.neu_forward.extend_using(select_v)));
                        } else if succeeding.symbols.1 == delta_binding.symbols.0 {
                            println!("neu reverse select_e");
                            extenders.push(Box::new(attribute.neu_reverse.extend_using(select_e)));
                        } else if succeeding.symbols.1 == delta_binding.symbols.1 {
                            println!("neu reverse select_v");
                            extenders.push(Box::new(attribute.neu_reverse.extend_using(select_v)));
                        } else {
                            panic!("Requested variable not bound by Attribute")
                        }
                    }
                }
                
                // @TODO project correctly
                collections.get_mut(&delta_binding.source_name)
                    .unwrap()
                    .extend(&mut extenders[..])
                    .map(|((e,v1),v2)| vec![e,v1,v2])
                    .inner
            });

            inner.concatenate(changes).as_collection().leave()
        });

        SimpleRelation {
            symbols: vec![],
            tuples: joined.distinct(),
        }
    }
}

//
// GENERIC IMPLEMENTATION
//

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

trait ProposeExtensionMethod<G: Scope, P: Data+Ord> {
    fn propose_using<PE: PrefixExtender<G, Prefix=P>>(&self, extender: &mut PE) -> Collection<G, (P, PE::Extension)>;
    fn extend<E: Data+Ord>(&self, extenders: &mut [Box<dyn PrefixExtender<G,Prefix=P,Extension=E>>]) -> Collection<G, (P, E)>;
}

impl<G: Scope, P: Data+Ord> ProposeExtensionMethod<G, P> for Collection<G, P> {
    fn propose_using<PE: PrefixExtender<G, Prefix=P>>(&self, extender: &mut PE) -> Collection<G, (P, PE::Extension)> {
        extender.propose(self)
    }
    fn extend<E: Data+Ord>(&self, extenders: &mut [Box<dyn PrefixExtender<G,Prefix=P,Extension=E>>]) -> Collection<G, (P, E)>
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

trait ValidateExtensionMethod<G: Scope, P, E> {
    fn validate_using<PE: PrefixExtender<G, Prefix=P, Extension=E>>(&self, extender: &mut PE) -> Collection<G, (P, E)>;
}

impl<G: Scope, P, E> ValidateExtensionMethod<G, P, E> for Collection<G, (P, E)> {
    fn validate_using<PE: PrefixExtender<G, Prefix=P, Extension=E>>(&self, extender: &mut PE) -> Collection<G, (P, E)> {
        extender.validate(self)
    }
}

//
// SPECIFIC IMPLEMENTATION
//

// These are all defined here so that users can be assured a common layout.
type TraceValSpine<K,V,T,R> = Spine<K, V, T, R, Rc<OrdValBatch<K,V,T,R>>>;
type TraceValHandle<K,V,T,R> = TraceAgent<K, V, T, R, TraceValSpine<K,V,T,R>>;
type TraceKeySpine<K,T,R> = Spine<K, (), T, R, Rc<OrdKeyBatch<K,T,R>>>;
type TraceKeyHandle<K,T,R> = TraceAgent<K, (), T, R, TraceKeySpine<K,T,R>>;

struct CollectionIndex<K, V, T>
where
    K: Data,
    V: Data,
    T: Lattice+Data,
{
    /// A trace of type (K, ()), used to count extensions for each prefix.
    count_trace: TraceKeyHandle<K, T, isize>,

    /// A trace of type (K, V), used to propose extensions for each prefix.
    propose_trace: TraceValHandle<K, V, T, isize>,

    /// A trace of type ((K, V), ()), used to validate proposed extensions.
    validate_trace: TraceKeyHandle<(K, V), T, isize>,
}

impl<K, V, T> Clone for CollectionIndex<K, V, T>
where
    K: Data+Hash,
    V: Data+Hash,
    T: Lattice+Data+Timestamp,
{
    fn clone(&self) -> Self {
        CollectionIndex {
            count_trace: self.count_trace.clone(),
            propose_trace: self.propose_trace.clone(),
            validate_trace: self.validate_trace.clone(),
        }
    }
}

impl<K, V, T> CollectionIndex<K, V, T>
where
    K: Data+Hash,
    V: Data+Hash,
    T: Lattice+Data+Timestamp,
{    
    pub fn index<G: Scope<Timestamp=T>>(name: &str, collection: &Collection<G, (K, V), isize>) -> Self {
        let counts = collection.map(|(k,_v)| (k,())).arrange_named(&format!("Counts({})", name)).trace;
        let propose = collection.arrange_named(&format!("Proposals({})", &name)).trace;
        let validate = collection.map(|t| (t,())).arrange_named(&format!("Validations({})", &name)).trace;

        CollectionIndex {
            count_trace: counts,
            propose_trace: propose,
            validate_trace: validate,
        }
    }

    pub fn extend_using<P, F: Fn(&P)->K>(&self, logic: F) -> CollectionExtender<K, V, T, P, F> {
        CollectionExtender {
            phantom: std::marker::PhantomData,
            indices: self.clone(),
            key_selector: Rc::new(logic),
        }
    }
}

struct CollectionExtender<K, V, T, P, F>
where
    K: Data,
    V: Data,
    T: Lattice+Data,
    F: Fn(&P)->K,
{
    phantom: std::marker::PhantomData<P>,
    indices: CollectionIndex<K, V, T>,
    key_selector: Rc<F>,
}

impl<G, K, V, P, F> PrefixExtender<G> for CollectionExtender<K, V, G::Timestamp, P, F>
where
    G: Scope,
    K: Data+Hash,
    V: Data+Hash,
    P: Data,
    G::Timestamp: Lattice+Data,
    F: Fn(&P)->K+'static,
{

    type Prefix = P;
    type Extension = V;

    fn count(&mut self, prefixes: &Collection<G, (P, usize, usize)>, index: usize) -> Collection<G, (P, usize, usize)> {

        // This method takes a stream of `(prefix, time, diff)` changes, and we want to produce the corresponding
        // stream of `((prefix, count), time, diff)` changes, just by looking up `count` in `count_trace`. We are
        // just doing a stream of changes and a stream of look-ups, no consolidation or any funny business like
        // that. We *could* organize the input differences by key and save some time, or we could skip that.

        let counts = self.indices.count_trace.import(&prefixes.scope());
        let mut counts_trace = Some(counts.trace.clone());

        let mut stash = HashMap::new();
        let logic1 = self.key_selector.clone();
        let logic2 = self.key_selector.clone();

        let exchange = Exchange::new(move |update: &((P,usize,usize),G::Timestamp,isize)| logic1(&(update.0).0).hashed().as_u64());

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

    fn propose(&mut self, prefixes: &Collection<G, P>) -> Collection<G, (P, V)> {

        // This method takes a stream of `(prefix, time, diff)` changes, and we want to produce the corresponding
        // stream of `((prefix, count), time, diff)` changes, just by looking up `count` in `count_trace`. We are
        // just doing a stream of changes and a stream of look-ups, no consolidation or any funny business like
        // that. We *could* organize the input differences by key and save some time, or we could skip that.

        let propose = self.indices.propose_trace.import(&prefixes.scope());
        let mut propose_trace = Some(propose.trace.clone());

        let mut stash = HashMap::new();
        let logic1 = self.key_selector.clone();
        let logic2 = self.key_selector.clone();

        let mut buffer1 = Vec::new();
        let mut buffer2 = Vec::new();

        let exchange = Exchange::new(move |update: &(P,G::Timestamp,isize)| logic1(&update.0).hashed().as_u64());

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

    fn validate(&mut self, extensions: &Collection<G, (P, V)>) -> Collection<G, (P, V)> {


        // This method takes a stream of `(prefix, time, diff)` changes, and we want to produce the corresponding
        // stream of `((prefix, count), time, diff)` changes, just by looking up `count` in `count_trace`. We are
        // just doing a stream of changes and a stream of look-ups, no consolidation or any funny business like
        // that. We *could* organize the input differences by key and save some time, or we could skip that.

        let validate = self.indices.validate_trace.import(&extensions.scope());
        let mut validate_trace = Some(validate.trace.clone());

        let mut stash = HashMap::new();
        let logic1 = self.key_selector.clone();
        let logic2 = self.key_selector.clone();

        let mut buffer1 = Vec::new();
        let mut buffer2 = Vec::new();

        let exchange = Exchange::new(move |update: &((P,V),G::Timestamp,isize)|
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
