//! Pull expression plan, but without nesting.

use timely::dataflow::Scope;
use timely::dataflow::scopes::child::Iterative;

use plan::Implementable;
use Relation;
use {QueryMap, RelationMap, SimpleRelation, Var, Entity, Attribute, Value};

/// A plan stage for extracting all matching [e a v] tuples for a
/// given set of attributes and an input relation specifying entities.
#[derive(Deserialize, Clone, Debug)]
pub struct PullLevel<P: Implementable> {
    /// TODO
    pub variables: Vec<Var>,
    /// Plan for the input relation.
    pub plan: Box<P>,
    /// Symbol bound to eids in the input relation.
    pub eid_sym: Var,
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
        
        use differential_dataflow::{Collection, AsCollection};
        use differential_dataflow::operators::JoinCore;
        use differential_dataflow::operators::arrange::{Arrange, Arranged, TraceAgent};
        use differential_dataflow::trace::implementations::ord::OrdValSpine;

        // @TODO use CollectionIndex
        
        let input = self.plan
            .implement(nested, local_arrangements, global_arrangements);

        let e = self.eid_sym;
        let e_offset = input.offset(&e);

        // Arrange input entities by eid.
        let eav: Collection<Iterative<S, u64>, Vec<Value>, isize> = input.tuples();
        let eids: Arranged<Iterative<S, u64>, Entity, (), isize, TraceAgent<Entity, (), Product<u64,u64>, isize, OrdValSpine<Entity, (), Product<u64, u64>, isize>>> = eav
            .map(move |t: Vec<Value>| {
                match t[e_offset] {
                    Value::Eid(eid) => (eid, ()),
                    _ => panic!("expected an eid")
                }
            })
            .arrange();
        
        let streams = self.attributes.iter().map(|a| {
            let e_v: Arranged<Iterative<S, u64>, Entity, Value, isize, TraceAgent<Entity, Value, Product<u64,u64>, isize, OrdValSpine<Entity, Value, Product<u64, u64>, isize>>> = match global_arrangements.get_mut(a) {
                None => panic!("attribute {:?} does not exist", a),
                Some(named) => named
                    .import_named(&nested.parent, a)
                    .enter(nested)
                    .as_collection(|tuple, _| {
                        match tuple[0] {
                            Value::Eid(eid) => (eid, tuple[1].clone()),
                            _ => panic!("expected an eid"),
                        }
                    })
                    .arrange(),
            };

            let attribute = Value::Attribute(a.clone());
                
            eids
                .join_core(&e_v, move |e: &Entity, _, v: &Value| {
                    Some(vec![Value::Eid(e.clone()), attribute.clone(), v.clone()])
                })
                .inner
        });

        let concat = nested.concatenate(streams).as_collection(); 
        
        SimpleRelation {
            symbols: vec![],
            tuples: concat,
        }
    }
}
