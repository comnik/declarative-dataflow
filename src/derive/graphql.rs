//! GraphQL derivations.

use std::collections::HashMap;

use timely::dataflow::operators::Exchange;
use timely::dataflow::scopes::child::Iterative;
use timely::dataflow::{Scope, Stream};
use timely::order::Product;
use timely::progress::Timestamp;

use differential_dataflow::hashable::Hashable;
use differential_dataflow::lattice::Lattice;

use graphql_parser::parse_query;
use graphql_parser::query::{Definition, Document, OperationDefinition, Selection, SelectionSet};
use graphql_parser::query::{Name, Value as GqValue};

use crate::binding::{AsBinding, Binding};
use crate::plan::{gensym, Dependencies, Implementable};
use crate::plan::{Hector, Plan};
use crate::timestamp::Rewind;
use crate::{Aid, Value, Var};
use crate::{Relation, ShutdownHandle, VariableMap};

use crate::domain::{AsSingletonDomain, Domain};

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
    fn implement<'b, S>(
        &self,
        nested: &mut Iterative<'b, S, u64>,
        domain: &mut Domain<Aid, S::Timestamp>,
        local_arrangements: &VariableMap<Iterative<'b, S, u64>>,
    ) -> (
        HashMap<PathId, Stream<S, ((Value, Value), S::Timestamp, isize)>>,
        ShutdownHandle,
    )
    where
        S: Scope,
        S::Timestamp: Timestamp + Lattice + Rewind,
    {
        use differential_dataflow::operators::arrange::{Arrange, Arranged, TraceAgent};
        use differential_dataflow::operators::JoinCore;
        use differential_dataflow::trace::implementations::ord::OrdValSpine;
        use differential_dataflow::trace::TraceReader;

        assert_eq!(self.pull_attributes.is_empty(), false);

        let (input, mut shutdown_handle) = self.plan.implement(nested, domain, local_arrangements);

        // Arrange input entities by eid.
        let e_offset = input
            .binds(self.pull_variable)
            .expect("input relation doesn't bind pull_variable");

        let paths = {
            let (tuples, shutdown) = input.tuples(nested, domain);
            shutdown_handle.merge_with(shutdown);
            tuples
        };

        let e_path: Arranged<
            Iterative<S, u64>,
            TraceAgent<OrdValSpine<Value, Vec<Value>, Product<S::Timestamp, u64>, isize>>,
        > = paths.map(move |t| (t[e_offset].clone(), t)).arrange();

        let mut shutdown_handle = shutdown_handle;
        let path_streams = self
            .pull_attributes
            .iter()
            .map(|a| {
                let e_v = match domain.forward_propose(a) {
                    None => panic!("attribute {:?} does not exist", a),
                    Some(propose_trace) => {
                        let frontier: Vec<S::Timestamp> = propose_trace.advance_frontier().to_vec();
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
                    .join_core(&e_v, move |e, _path: &Vec<Value>, v: &Value| {
                        Some((e.clone(), v.clone()))
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
    fn implement<'b, S>(
        &self,
        nested: &mut Iterative<'b, S, u64>,
        domain: &mut Domain<Aid, S::Timestamp>,
        _local_arrangements: &VariableMap<Iterative<'b, S, u64>>,
    ) -> (
        HashMap<PathId, Stream<S, ((Value, Value), S::Timestamp, isize)>>,
        ShutdownHandle,
    )
    where
        S: Scope,
        S::Timestamp: Timestamp + Lattice + Rewind,
    {
        use differential_dataflow::trace::TraceReader;

        assert!(!self.pull_attributes.is_empty());

        let mut shutdown_handle = ShutdownHandle::empty();

        let path_streams = self
            .pull_attributes
            .iter()
            .map(|a| {
                let e_v = match domain.forward_propose(a) {
                    None => panic!("attribute {:?} does not exist", a),
                    Some(propose_trace) => {
                        let frontier: Vec<S::Timestamp> = propose_trace.advance_frontier().to_vec();
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
                    .as_collection(|e, v| (e.clone(), v.clone()))
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
    pub fn implement<'b, S>(
        &self,
        nested: &mut Iterative<'b, S, u64>,
        domain: &mut Domain<Aid, S::Timestamp>,
        local_arrangements: &VariableMap<Iterative<'b, S, u64>>,
    ) -> (
        HashMap<PathId, Stream<S, ((Value, Value), S::Timestamp, isize)>>,
        ShutdownHandle,
    )
    where
        S: Scope,
        S::Timestamp: Timestamp + Lattice + Rewind,
    {
        match self {
            Pull::All(ref pull) => pull.implement(nested, domain, local_arrangements),
            Pull::Level(ref pull) => pull.implement(nested, domain, local_arrangements),
        }
    }
}

/// A domain transform expressed as a GraphQL query, e.g. `{ Heroes {
/// name age weight } }`.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub struct GraphQl {
    /// String representation of GraphQL query
    pub query: String,
    /// Cached paths
    paths: Vec<Pull>,
    /// Required attributes to filter entities by
    required_aids: Vec<Aid>,
}

impl GraphQl {
    /// Creates a new GraphQl instance by parsing the AST obtained
    /// from the provided query.
    pub fn new(query: String) -> Self {
        let ast = parse_query(&query).expect("graphQL ast parsing failed");
        let empty_plan = Hector {
            variables: vec![0],
            bindings: vec![],
        };

        GraphQl {
            query,
            paths: ast.into_paths(empty_plan),
            required_aids: vec![],
        }
    }

    /// Creates a new GraphQl starting from the specified root plan.
    pub fn with_plan(root_plan: Plan, query: String) -> Self {
        let ast = parse_query(&query).expect("graphQL ast parsing failed");
        let paths = ast.into_paths(Hector {
            variables: root_plan.variables(),
            bindings: root_plan.into_bindings(),
        });

        GraphQl {
            query,
            paths,
            required_aids: vec![],
        }
    }

    /// Creates a new GraphQl that filters top-level entities down to
    /// only those with all of the required Aids present.
    pub fn with_required_aids(query: String, required_aids: Vec<Aid>) -> Self {
        let mut query = GraphQl::new(query);
        query.required_aids = required_aids;
        query
    }
}

trait IntoPaths {
    fn into_paths(&self, root_plan: Hector) -> Vec<Pull>;
}

impl IntoPaths for Document {
    /// Transforms the provided GraphQL query AST into corresponding pull
    /// paths. The structure of a typical parsed ast looks like this:
    ///
    /// ```
    /// Document {
    ///   definitions: [
    ///     Operation(SelectionSet(SelectionSet {
    ///       items: [
    ///         Field(Field {
    ///           name: ...,
    ///           selection_set: SelectionSet(...}
    ///         }),
    ///         ...
    ///       ]
    ///     }))
    ///   ]
    /// }
    /// ```
    fn into_paths(&self, root_plan: Hector) -> Vec<Pull> {
        self.definitions
            .iter()
            .flat_map(|definition| definition.into_paths(root_plan.clone()))
            .collect()
    }
}

impl IntoPaths for Definition {
    fn into_paths(&self, root_plan: Hector) -> Vec<Pull> {
        match self {
            Definition::Operation(operation) => operation.into_paths(root_plan),
            Definition::Fragment(_) => unimplemented!(),
        }
    }
}

impl IntoPaths for OperationDefinition {
    fn into_paths(&self, root_plan: Hector) -> Vec<Pull> {
        use OperationDefinition::{Query, SelectionSet};

        match self {
            Query(_) => unimplemented!(),
            SelectionSet(selection_set) => {
                selection_set_to_paths(&selection_set, root_plan, &[], &[])
            }
            _ => unimplemented!(),
        }
    }
}

/// Gathers the fields that we want to pull at a specific level. These
/// only include fields that do not refer to nested entities.
fn pull_attributes(selection_set: &SelectionSet) -> Vec<Aid> {
    selection_set
        .items
        .iter()
        .flat_map(|item| match item {
            Selection::Field(field) => {
                if field.selection_set.items.is_empty() {
                    Some(field.name.to_string())
                } else {
                    None
                }
            }
            _ => unimplemented!(),
        })
        .collect::<Vec<Aid>>()
}

/// Takes a GraphQL `SelectionSet` and recursively transforms it into
/// `PullLevel`s.
///
/// A `SelectionSet` consists of multiple items. We're interested in
/// items of type `Field`, which might contain a nested `SelectionSet`
/// themselves. We iterate through each field and construct (1) a
/// parent path, which describes how to traverse to the current
/// nesting level ("vertical"), and (2) pull attributes, which
/// describe the attributes pulled at the current nesting level
/// ("horizontal"); only attributes at the lowest nesting level can be
/// part of a `PullLevel`'s `pull_attributes`.
fn selection_set_to_paths(
    selection_set: &SelectionSet,
    mut plan: Hector,
    arguments: &[(Name, GqValue)],
    parent_path: &[String],
) -> Vec<Pull> {
    // We must first construct the correct plan for this level,
    // starting from that for the parent level. We do this even if no
    // attributes are actually pulled at this level. In that case we
    // will not synthesize this plan, but it still is required in
    // order to pass all necessary bindings to nested levels.

    // For any level after the first, we must introduce a binding
    // linking the parent level to the current one.
    if !parent_path.is_empty() {
        let parent = *plan.variables.last().expect("plan has no variables");

        let this = plan.variables.len() as Var;
        let aid = parent_path.last().unwrap();

        plan.variables.push(this);
        plan.bindings.push(Binding::attribute(parent, aid, this));
    }

    let this = *plan.variables.last().expect("plan has no variables");

    // Then we must introduce additional bindings for any arguments.
    for (aid, v) in arguments.iter() {
        // This variable is only relevant for tying the two clauses
        // together, we do not want to include it into the output
        // projection.
        let vsym = gensym();

        plan.bindings.push(Binding::attribute(this, aid, vsym));
        plan.bindings
            .push(Binding::constant(vsym, v.clone().into()));
    }

    // We will first gather the attributes that need to be retrieved
    // at this level. These are the fields that do not refer to a
    // nested entity. This is the easy part.
    let pull_attributes = pull_attributes(selection_set);

    // Now we process nested levels.
    let nested_levels = selection_set
        .items
        .iter()
        .flat_map(|item| match item {
            Selection::Field(field) => {
                if !field.selection_set.items.is_empty() {
                    let mut parent_path = parent_path.to_vec();
                    parent_path.push(field.name.to_string());

                    selection_set_to_paths(
                        &field.selection_set,
                        plan.clone(),
                        &field.arguments,
                        &parent_path,
                    )
                } else {
                    vec![]
                }
            }
            _ => unimplemented!(),
        })
        .collect::<Vec<Pull>>();

    let mut levels = nested_levels;

    // Here we don't actually want to include the current plan, if
    // we're not interested in any attributes at this level.
    if !pull_attributes.is_empty() {
        if plan.bindings.is_empty() {
            levels.push(Pull::All(PullAll { pull_attributes }));
        } else {
            levels.push(Pull::Level(PullLevel {
                pull_attributes,
                path_attributes: parent_path.to_vec(),
                pull_variable: this,
                plan: Box::new(Plan::Hector(plan)),
                cardinality_many: false,
            }));
        }
    }

    levels
}

impl GraphQl {
    /// See Implementable::dependencies, as GraphQl v2 can't implement
    /// Implementable directly.
    pub fn dependencies(&self) -> Dependencies {
        let mut dependencies = Dependencies::none();

        for path in self.paths.iter() {
            dependencies = Dependencies::merge(dependencies, path.dependencies());
        }

        dependencies
    }

    /// @TODO
    pub fn derive<'b, S>(
        &self,
        nested: &mut Iterative<'b, S, u64>,
        domain: &mut Domain<Aid, S::Timestamp>,
        namespace: &str,
    ) -> Domain<Aid, S::Timestamp>
    where
        S: Scope,
        S::Timestamp: Timestamp + Lattice + Rewind,
    {
        let mut out_domain = Domain::new_from("", domain);
        let dummy = HashMap::new();

        for path in self.paths.iter() {
            let (streams, shutdown) = path.implement(nested, domain, &dummy);
            // @TODO Domain's should keep track of shutdown handles
            std::mem::forget(shutdown);

            for (path_id, stream) in streams.into_iter() {
                let path = stream
                    .exchange(|((e, _v), _t, _diff)| e.clone().hashed())
                    .as_singleton_domain(format!("{}/{}", namespace, path_id.last().unwrap()))
                    .into();

                out_domain += path;
            }
        }

        out_domain
    }
}
