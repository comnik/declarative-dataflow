//! Pull expression plan, but without nesting.

use timely::dataflow::Scope;
use timely::dataflow::scopes::child::Iterative;

use plan::Implementable;
use Relation;
use {QueryMap, RelationMap, SimpleRelation, Var, Attribute, Value};

/// A plan stage for extracting all matching [e a v] tuples for a
/// given set of attributes and an input relation specifying entities.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PullLevel<P: Implementable> {
    /// TODO
    pub variables: Vec<Var>,
    /// Plan for the input relation.
    pub plan: Box<P>,
    /// Attributes to pull for the input entities.
    pub attributes: Vec<Attribute>,
}

impl<P: Implementable> Implementable for PullLevel<P> {
    fn implement<'b, S: Scope<Timestamp = u64>>(
        &self,
        nested: &mut Iterative<'b, S, u64>,
        local_arrangements: &RelationMap<Iterative<'b, S, u64>>,
        global_arrangements: &mut QueryMap<isize>,
    ) -> SimpleRelation<'b, S> {

        use timely::order::Product;
        use timely::dataflow::operators::Concatenate;
        
        use differential_dataflow::AsCollection;
        use differential_dataflow::operators::JoinCore;
        use differential_dataflow::operators::arrange::{Arrange, Arranged, TraceAgent};
        use differential_dataflow::trace::implementations::ord::OrdValSpine;

        // @TODO use CollectionIndex
        
        let input = self.plan
            .implement(nested, local_arrangements, global_arrangements);

        // Arrange input entities by eid.
        let paths = input.tuples();
        let e_path: Arranged<Iterative<S, u64>, Value, Vec<Value>, isize,
                             TraceAgent<Value, Vec<Value>, Product<u64,u64>, isize,
                                        OrdValSpine<Value, Vec<Value>, Product<u64, u64>, isize>>> = paths
            .map(|t| (t.last().unwrap().clone(), t))
            .arrange();
        
        let streams = self.attributes.iter().map(|a| {
            let e_v: Arranged<Iterative<S, u64>, Value, Value, isize,
                              TraceAgent<Value, Value, Product<u64,u64>, isize,
                                         OrdValSpine<Value, Value, Product<u64, u64>, isize>>> = match global_arrangements.get_mut(a) {
                None => panic!("attribute {:?} does not exist", a),
                Some(named) => named
                    .import_named(&nested.parent, a)
                    .enter(nested)
                    .as_collection(|tuple, _| (tuple[0].clone(), tuple[1].clone()))
                    .arrange(),
            };

            let attribute = Value::Attribute(a.clone());
                
            e_path
                .join_core(&e_v, move |_e, path: &Vec<Value>, v: &Value| {
                    let mut result: Vec<Value> = path.clone();
                    result.push(attribute.clone());
                    result.push(v.clone());
                    
                    Some(result)
                })
                .inner
        });

        let concat = nested.concatenate(streams).as_collection(); 
        
        SimpleRelation {
            symbols: vec![], // @TODO
            tuples: concat,
        }
    }
}
