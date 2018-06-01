extern crate timely;
extern crate differential_dataflow;
    
#[macro_use]
extern crate abomonation_derive;
extern crate abomonation;

#[macro_use]
extern crate serde_derive;

use std::string::String;
use std::boxed::Box;
use std::ops::Deref;
use std::collections::{HashMap, HashSet};

use timely::dataflow::*;
use timely::dataflow::scopes::{Root, Child};
use timely::progress::Timestamp;
use timely::progress::timestamp::RootTimestamp;
use timely::progress::nested::product::Product;
use timely::dataflow::operators::probe::{Handle};

use differential_dataflow::collection::{Collection};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::input::{Input, InputSession};
use differential_dataflow::trace::implementations::ord::{OrdValSpine, OrdKeySpine};
use differential_dataflow::operators::arrange::{ArrangeByKey, ArrangeBySelf, TraceAgent, Arranged};
use differential_dataflow::operators::group::Threshold;
use differential_dataflow::operators::join::{JoinCore};
use differential_dataflow::operators::iterate::Variable;

//
// TYPES
//

pub type Entity = u64;
pub type Attribute = u32;

#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Abomonation, Debug, Serialize, Deserialize)]
pub enum Value {
    Eid(Entity),
    Attribute(Attribute),
    Number(i64),
    String(String),
}

#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Abomonation, Debug, Serialize, Deserialize)]
pub struct Datom(pub Entity, pub Attribute, pub Value);

#[derive(Deserialize, Debug)]
pub struct TxData(pub isize, pub Entity, pub Attribute, pub Value);

#[derive(Serialize, Debug)]
pub struct Out(Vec<Value>, isize);

type ProbeHandle<T> = Handle<Product<RootTimestamp, T>>;
type TraceKeyHandle<K, T, R> = TraceAgent<K, (), T, R, OrdKeySpine<K, T, R>>;
type TraceValHandle<K, V, T, R> = TraceAgent<K, V, T, R, OrdValSpine<K, V, T, R>>;
type Arrange<G: Scope, K, V, R> = Arranged<G, K, V, R, TraceValHandle<K, V, G::Timestamp, R>>;
type ArrangeSelf<G: Scope, K, R> = Arranged<G, K, (), R, TraceKeyHandle<K, G::Timestamp, R>>;
type InputMap<G: Scope> = HashMap<(Option<Entity>, Option<Attribute>, Option<Value>), ArrangeSelf<G, Vec<Value>, isize>>;
type QueryMap<T, R> = HashMap<String, TraceKeyHandle<Vec<Value>, Product<RootTimestamp, T>, R>>;
type RelationMap<'a, G> = HashMap<String, NamedRelation<'a, G>>;

//
// CONTEXT
//

pub struct DB<T: Timestamp+Lattice> {
    e_av: TraceValHandle<Vec<Value>, Vec<Value>, Product<RootTimestamp, T>, isize>,
    a_ev: TraceValHandle<Vec<Value>, Vec<Value>, Product<RootTimestamp, T>, isize>,
    ea_v: TraceValHandle<Vec<Value>, Vec<Value>, Product<RootTimestamp, T>, isize>,
    av_e: TraceValHandle<Vec<Value>, Vec<Value>, Product<RootTimestamp, T>, isize>,
}

struct ImplContext<G: Scope + ScopeParent> where G::Timestamp : Lattice {
    // Imported traces
    e_av: Arrange<G, Vec<Value>, Vec<Value>, isize>,
    a_ev: Arrange<G, Vec<Value>, Vec<Value>, isize>,
    ea_v: Arrange<G, Vec<Value>, Vec<Value>, isize>,
    av_e: Arrange<G, Vec<Value>, Vec<Value>, isize>,

    // Parameter inputs
    input_map: InputMap<G>,
    
    // Collection variables for recursion
    // variable_map: RelationMap<'a, G>,
}

pub struct Context<T: Timestamp+Lattice> {
    pub input_handle: InputSession<T, Datom, isize>,
    pub db: DB<T>,
    pub probes: Vec<ProbeHandle<T>>,
    pub queries: QueryMap<T, isize>,
}

//
// QUERY PLAN GRAMMAR
//

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Plan {
    Project(Box<Plan>, Vec<Var>),
    Union(Box<Plan>, Box<Plan>, Vec<Var>),
    Join(Box<Plan>, Box<Plan>, Var),
    Not(Box<Plan>),
    Lookup(Entity, Attribute, Var),
    Entity(Entity, Var, Var),
    HasAttr(Var, Attribute, Var),
    Filter(Var, Attribute, Value),
    Recur(Vec<Var>),
}

type Var = u32;

//
// RELATIONS
//

trait Relation<'a, G: Scope> where G::Timestamp : Lattice {
    fn symbols(&self) -> &Vec<Var>;
    fn tuples(self) -> Collection<Child<'a, G, u64>, Vec<Value>, isize>;
    fn tuples_by_symbols(self, syms: Vec<Var>) -> Collection<Child<'a, G, u64>, (Vec<Value>, Vec<Value>), isize>;
}

pub struct RelationHandles<T: Timestamp+Lattice> {
    pub input: InputSession<T, Vec<Value>, isize>,
    pub trace: TraceKeyHandle<Vec<Value>, Product<RootTimestamp, T>, isize>,
}

struct SimpleRelation<'a, G: Scope> where G::Timestamp : Lattice {
    symbols: Vec<Var>,
    tuples: Collection<Child<'a, G, u64>, Vec<Value>, isize>,
}

impl<'a, G: Scope> Relation<'a, G> for SimpleRelation<'a, G> where G::Timestamp : Lattice {
    fn symbols(&self) -> &Vec<Var> { &self.symbols }
    fn tuples(self) -> Collection<Child<'a, G, u64>, Vec<Value>, isize> { self.tuples }

    fn tuples_by_symbols(self, syms: Vec<Var>) -> Collection<Child<'a, G, u64>, (Vec<Value>, Vec<Value>), isize>{
        let key_length = syms.len();
        let values_length = self.symbols().len() - key_length;
        
        let mut key_offsets: Vec<usize> = Vec::with_capacity(key_length);
        let mut value_offsets: Vec<usize> = Vec::with_capacity(values_length);
        let sym_set: HashSet<Var> = syms.iter().cloned().collect();

        // It is important to preservere the key symbols in the order
        // they were specified.
        for sym in syms.iter() {
            key_offsets.push(self.symbols().iter().position(|&v| *sym == v).unwrap());
        }

        // Values we'll just take in the order they were.
        for (idx, sym) in self.symbols().iter().enumerate() {
            if sym_set.contains(sym) == false {
                value_offsets.push(idx);
            }
        }

        // let debug_keys: Vec<String> = key_offsets.iter().map(|x| x.to_string()).collect();
        // let debug_values: Vec<String> = value_offsets.iter().map(|x| x.to_string()).collect();
        // println!("key offsets: {:?}", debug_keys);
        // println!("value offsets: {:?}", debug_values);
        
        self.tuples()
            .map(move |tuple| {
                let key: Vec<Value> = key_offsets.iter().map(|i| tuple[*i].clone()).collect();
                // @TODO second clone not really neccessary
                let values: Vec<Value> = value_offsets.iter().map(|i| tuple[*i].clone()).collect();
                
                (key, values)
            })
    }
}

struct NamedRelation<'a, G: Scope> where G::Timestamp : Lattice {
    variable: Option<Variable<'a, G, Vec<Value>, isize>>,
    tuples: Collection<Child<'a, G, u64>, Vec<Value>, isize>,
}

impl<'a, G: Scope> NamedRelation<'a, G> where G::Timestamp : Lattice {
    pub fn from (source: &Collection<Child<'a, G, u64>, Vec<Value>, isize>) -> Self {
        let variable = Variable::from(source.clone());
        NamedRelation {
            variable: Some(variable),
            tuples: source.clone(),
        }
    }
    pub fn add_execution(&mut self, execution: &Collection<Child<'a, G, u64>, Vec<Value>, isize>) {
        self.tuples = self.tuples.concat(execution);
    }
}

impl<'a, G: Scope> Drop for NamedRelation<'a, G> where G::Timestamp : Lattice {
    fn drop(&mut self) {
        if let Some(variable) = self.variable.take() {
            variable.set(&self.tuples.distinct());
        }
    }
}

//
// QUERY PLAN IMPLEMENTATION
//

/// Takes a query plan and turns it into a differential dataflow. The
/// dataflow is extended to feed output tuples to JS clients. A probe
/// on the dataflow is returned.
fn implement<A: timely::Allocate, T: Timestamp+Lattice> (name: &String, plan: Plan, scope: &mut Child<Root<A>, T>, ctx: &mut Context<T>) -> HashMap<String, RelationHandles<T>> {
        
    let db = &mut ctx.db;
    let queries = &mut ctx.queries;
        
    // @TODO Only import those we need for the query?
    let impl_ctx: ImplContext<Child<Root<A>, T>> = ImplContext {
        e_av: db.e_av.import(scope),
        a_ev: db.a_ev.import(scope),
        ea_v: db.ea_v.import(scope),
        av_e: db.av_e.import(scope),

        input_map: create_inputs(&plan, scope)
    };

    let (source_handle, source) = scope.new_collection();
    
    scope.scoped(|nested| {

        let mut relation_map = HashMap::new();
        let mut result_map = HashMap::new();
        
        let output_relation = NamedRelation::from(&source.enter(nested));
        let output_trace = output_relation.variable.as_ref().unwrap().leave().arrange_by_self().trace;
        result_map.insert(name.clone(), RelationHandles { input: source_handle, trace: output_trace });
        
        relation_map.insert("self".to_string(), output_relation);

        let execution = implement_plan(&plan, &impl_ctx, nested, &relation_map, queries);
        
        relation_map
            .get_mut(&"self".to_string()).unwrap() // @TODO try without to_string
            .add_execution(&execution.tuples());

        result_map
    })
}

fn create_inputs<'a, A: timely::Allocate, T: Timestamp+Lattice>
(plan: &Plan, scope: &mut Child<'a, Root<A>, T>) -> InputMap<Child<'a, Root<A>, T>> {

    match plan {
        &Plan::Project(ref plan, _) => { create_inputs(plan.deref(), scope) },
        &Plan::Union(ref left_plan, ref right_plan, _) => {
            let mut left_inputs = create_inputs(left_plan.deref(), scope);
            let mut right_inputs = create_inputs(right_plan.deref(), scope);
            
            for (k, v) in right_inputs.drain() {
                left_inputs.insert(k, v);
            }

            left_inputs
        },
        &Plan::Join(ref left_plan, ref right_plan, _) => {
            let mut left_inputs = create_inputs(left_plan.deref(), scope);
            let mut right_inputs = create_inputs(right_plan.deref(), scope);
            
            for (k, v) in right_inputs.drain() {
                left_inputs.insert(k, v);
            }

            left_inputs
        },
        &Plan::Not(ref plan) => { create_inputs(plan.deref(), scope) },
        &Plan::Lookup(e, a, _) => {
            let mut inputs = HashMap::new();
            inputs.insert((Some(e), Some(a), None), scope.new_collection_from(vec![vec![Value::Eid(e), Value::Attribute(a)]]).1.arrange_by_self());
            inputs
        },
        &Plan::Entity(e, _, _) => {
            let mut inputs = HashMap::new();
            inputs.insert((Some(e), None, None), scope.new_collection_from(vec![vec![Value::Eid(e)]]).1.arrange_by_self());
            inputs
        },
        &Plan::HasAttr(_, a, _) => {
            let mut inputs = HashMap::new();
            inputs.insert((None, Some(a), None), scope.new_collection_from(vec![vec![Value::Attribute(a)]]).1.arrange_by_self());
            inputs
        },
        &Plan::Filter(_, a, ref v) => {
            let mut inputs = HashMap::new();
            inputs.insert((None, Some(a), Some(v.clone())), scope.new_collection_from(vec![vec![Value::Attribute(a), v.clone()]]).1.arrange_by_self());
            inputs
        },
        &Plan::Recur(_) => { HashMap::new() }
    }
}

fn implement_plan<'a, 'b, A: timely::Allocate, T: Timestamp+Lattice>
    (plan: &Plan,
     db: &ImplContext<Child<'a, Root<A>, T>>,
     nested: &mut Child<'b, Child<'a, Root<A>, T>, u64>,
     relation_map: &RelationMap<'b, Child<'a, Root<A>, T>>,
     queries: &QueryMap<T, isize>) -> SimpleRelation<'b, Child<'a, Root<A>, T>> {
        
    match plan {
        &Plan::Project(ref plan, ref symbols) => {
            let mut relation = implement_plan(plan.deref(), db, nested, relation_map, queries);
            let tuples = relation
                .tuples_by_symbols(symbols.clone())
                .map(|(key, _tuple)| key);

            // @TODO distinct? or just before negation?
            SimpleRelation { symbols: symbols.to_vec(), tuples }
        },
        &Plan::Union(ref left_plan, ref right_plan, ref symbols) => {
            // @TODO can just concat more than two + a single distinct
            // @TODO or move out distinct, except for negation
            let mut left = implement_plan(left_plan.deref(), db, nested, relation_map, queries);
            let mut right = implement_plan(right_plan.deref(), db, nested, relation_map, queries);

            let mut left_tuples;
            if left.symbols() == symbols {
               left_tuples = left.tuples();
            } else {
                left_tuples = left
                    .tuples_by_symbols(symbols.clone())
                    .map(|(key, _tuple)| key);                
            }

            let mut right_tuples;
            if right.symbols() == symbols {
                right_tuples = right.tuples();
            } else {
                right_tuples = right
                    .tuples_by_symbols(symbols.clone())
                    .map(|(key, _tuple)| key);
            }

            SimpleRelation {
                symbols: symbols.to_vec(),
                tuples: left_tuples.concat(&right_tuples).distinct()
            }
        },
        &Plan::Join(ref left_plan, ref right_plan, join_var) => {
            let mut left = implement_plan(left_plan.deref(), db, nested, relation_map, queries);
            let mut right = implement_plan(right_plan.deref(), db, nested, relation_map, queries);

            let mut join_vars = vec![join_var];

            let mut left_syms: Vec<Var> = left.symbols().clone();
            left_syms.retain(|&sym| sym != join_var);
            
            let mut right_syms: Vec<Var> = right.symbols().clone();
            right_syms.retain(|&sym| sym != join_var);

            // useful for inspecting join inputs
            //.inspect(|&((ref key, ref values), _, _)| { println!("right {:?} {:?}", key, values) })
            
            let tuples = left.tuples_by_symbols(join_vars.clone())
                .arrange_by_key()
                .join_core(&right.tuples_by_symbols(join_vars.clone()).arrange_by_key(), |key, v1, v2| {
                    // @TODO can haz array here?
                    // @TODO avoid allocation, if capacity available in v1
                    let mut vstar = Vec::with_capacity(key.len() + v1.len() + v2.len());

                    vstar.extend(key.iter().cloned());
                    vstar.extend(v1.iter().cloned());
                    vstar.extend(v2.iter().cloned());
                    
                    Some(vstar)                    
                });

            let mut symbols: Vec<Var> = Vec::with_capacity(left_syms.len() + right_syms.len());
            symbols.append(&mut join_vars);
            symbols.append(&mut left_syms);
            symbols.append(&mut right_syms);

            // let debug_syms: Vec<String> = symbols.iter().map(|x| x.to_string()).collect();
            // println!(debug_syms);

            SimpleRelation { symbols, tuples }
        },
        &Plan::Not(ref plan) => {
            // implement_negation(plan.deref(), db)
            
            let mut rel = implement_plan(plan.deref(), db, nested, relation_map, queries);
            SimpleRelation {
                symbols: rel.symbols().clone(),
                tuples: rel.tuples().negate()
            }
        },
        &Plan::Lookup(e, a, sym1) => {
            // let ea_in = scope.new_collection_from(vec![(e, a)]).1.enter(nested).arrange_by_self();
            let ea_in = db.input_map.get(&(Some(e), Some(a), None)).unwrap().enter(nested);
            let tuples = db.ea_v.enter(nested)
                .join_core(&ea_in, |_, tuple, _| { Some(tuple.clone()) });
            
            SimpleRelation { symbols: vec![sym1], tuples }
        },
        &Plan::Entity(e, sym1, sym2) => {
            // let e_in = scope.new_collection_from(vec![e]).1.enter(nested).arrange_by_self();
            let e_in = db.input_map.get(&(Some(e), None, None)).unwrap().enter(nested);
            let tuples = db.e_av.enter(nested)
                .join_core(&e_in, |_, tuple, _| { Some(tuple.clone()) });
            
            SimpleRelation { symbols: vec![sym1, sym2], tuples }
        },
        &Plan::HasAttr(sym1, a, sym2) => {
            // let a_in = scope.new_collection_from(vec![a]).1.enter(nested).arrange_by_self();
            let a_in = db.input_map.get(&(None, Some(a), None)).unwrap().enter(nested);
            let tuples = db.a_ev.enter(nested)
                .join_core(&a_in, |_, tuple, _| { Some(tuple.clone()) });
            
            SimpleRelation { symbols: vec![sym1, sym2], tuples }
        },
        &Plan::Filter(sym1, a, ref v) => {
            // let av_in = scope.new_collection_from(vec![(a, v.clone())]).1.enter(nested).arrange_by_self();
            let av_in = db.input_map.get(&(None, Some(a), Some(v.clone()))).unwrap().enter(nested);
            let tuples = db.av_e.enter(nested)
                .join_core(&av_in, |_, tuple, _| { Some(tuple.clone()) });
            
            SimpleRelation { symbols: vec![sym1], tuples }
        }
        &Plan::Recur(ref syms) => {
            match relation_map.get(&"self".to_string()) {
                None => panic!("'self' not in relation map"),
                Some(named) => {
                    SimpleRelation {
                        symbols: syms.clone(),
                        tuples: named.variable.as_ref().unwrap().deref().map(|tuple| tuple.clone()),
                    }
                }
            }
        }
    }
}

// fn implement_negation<'a>(plan: &Plan, db: &mut DB, scope: &mut Scope<'a>) -> SimpleRelation<'a> {
//     match plan {
//         &Plan::Lookup(e, a, sym1) => {
//             let ea_in = scope.new_collection_from(vec![(e, a)]).1;
//             let tuples = db.ea_v.import(scope)
//                 .antijoin(&ea_in)
//                 .distinct()
//                 .map(|(_, v)| { vec![v] });
            
//             SimpleRelation { symbols: vec![sym1], tuples }
//         },
//         &Plan::Entity(e, sym1, sym2) => {
//             let e_in = scope.new_collection_from(vec![e]).1;
//             let tuples = db.e_av.import(scope)
//                 .antijoin(&e_in)
//                 .distinct()
//                 .map(|(_, (a, v))| { vec![Value::Attribute(a), v] });
            
//             SimpleRelation { symbols: vec![sym1, sym2], tuples }
//         },
//         &Plan::HasAttr(sym1, a, sym2) => {
//             let a_in = scope.new_collection_from(vec![a]).1;
//             let tuples = db.a_ev.import(scope)
//                 .antijoin(&a_in)
//                 .distinct()
//                 .map(|(_, (e, v))| { vec![Value::Eid(e), v] });
            
//             SimpleRelation { symbols: vec![sym1, sym2], tuples }
//         },
//         &Plan::Filter(sym1, a, ref v) => {
//             let av_in = scope.new_collection_from(vec![(a, v.clone())]).1;
//             let tuples = db.av_e.import(scope)
//                 .antijoin(&av_in)
//                 .distinct()
//                 .map(|(_, e)| { vec![Value::Eid(e)] });
                
//             SimpleRelation { symbols: vec![sym1], tuples }
//         },
//         _ => panic!("Negation not supported for this plan.")
//     }
// }

//
// PUBLIC API
//

pub fn setup_db<A: timely::Allocate, T: Timestamp+Lattice> (scope: &mut Child<Root<A>, T>) -> (InputSession<T, Datom, isize>, DB<T>) {
    let (input_handle, datoms) = scope.new_collection::<Datom, isize>();
    let db = DB {
        e_av: datoms.map(|Datom(e, a, v)| (vec![Value::Eid(e)], vec![Value::Attribute(a), v])).arrange_by_key().trace,
        a_ev: datoms.map(|Datom(e, a, v)| (vec![Value::Attribute(a)], vec![Value::Eid(e), v])).arrange_by_key().trace,
        ea_v: datoms.map(|Datom(e, a, v)| (vec![Value::Eid(e), Value::Attribute(a)], vec![v])).arrange_by_key().trace,
        av_e: datoms.map(|Datom(e, a, v)| (vec![Value::Attribute(a), v], vec![Value::Eid(e)])).arrange_by_key().trace,
    };

    (input_handle, db)
}

pub fn register<A: timely::Allocate, T: Timestamp+Lattice>
(scope: &mut Child<Root<A>, T>, ctx: &mut Context<T>, name: String, plan: Plan) -> HashMap<String, RelationHandles<T>> {
    
    let mut result_map = implement(&name, plan, scope, ctx);

    // @TODO store trace somewhere for re-use from other queries later
    // queries.insert(name.clone(), output_collection.arrange_by_self().trace);
    
    result_map
}

// @TODO this is probably only neccessary in the WASM interface
//
// pub fn transact<A: Allocate>(ctx: &mut Context<A, usize>, tx: usize, d: Vec<TxData>) -> bool {
//     for TxData(op, e, a, v) in d {
//         ctx.input_handle.update(Datom(e, a, v), op);
//     }
//     ctx.input_handle.advance_to(tx + 1);
//     ctx.input_handle.flush();

//     for probe in &mut ctx.probes {
//         while probe.less_than(ctx.input_handle.time()) {
//             ctx.root.step();
//         }
//     }

//     true
// }

