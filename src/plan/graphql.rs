//! GraphQL expression plan.

use timely::dataflow::scopes::child::Iterative;
use timely::dataflow::Scope;
use timely::progress::Timestamp;

use differential_dataflow::lattice::Lattice;

use graphql_parser::parse_query;
use graphql_parser::query::{Definition, Document, OperationDefinition, Selection, SelectionSet};
use graphql_parser::query::{Name, Value};

use crate::binding::Binding;
use crate::domain::Domain;
use crate::plan::{gensym, Dependencies, Implementable};
use crate::plan::{Hector, Plan, Pull, PullAll, PullLevel};
use crate::timestamp::Rewind;
use crate::{AsAid, Var};
use crate::{Implemented, ShutdownHandle, VariableMap};

/// A plan for GraphQL queries, e.g. `{ Heroes { name age weight } }`.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub struct GraphQl<A: AsAid> {
    /// String representation of GraphQL query
    pub query: String,
    /// Cached paths
    paths: Vec<Plan<A>>,
}

impl<A: AsAid + From<String>> GraphQl<A> {
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
        }
    }

    /// Creates a new GraphQl starting from the specified root plan.
    pub fn with_plan(root_plan: Plan<A>, query: String) -> Self {
        let ast = parse_query(&query).expect("graphQL ast parsing failed");
        let paths = ast.into_paths(Hector {
            variables: root_plan.variables(),
            bindings: root_plan.into_bindings(),
        });

        GraphQl { query, paths }
    }
}

trait IntoPaths {
    fn into_paths<A: AsAid + From<String>>(&self, root_plan: Hector<A>) -> Vec<Plan<A>>;
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
    fn into_paths<A: AsAid + From<String>>(&self, root_plan: Hector<A>) -> Vec<Plan<A>> {
        self.definitions
            .iter()
            .flat_map(|definition| definition.into_paths(root_plan.clone()))
            .collect()
    }
}

impl IntoPaths for Definition {
    fn into_paths<A: AsAid + From<String>>(&self, root_plan: Hector<A>) -> Vec<Plan<A>> {
        match self {
            Definition::Operation(operation) => operation.into_paths(root_plan),
            Definition::Fragment(_) => unimplemented!(),
        }
    }
}

impl IntoPaths for OperationDefinition {
    fn into_paths<A: AsAid + From<String>>(&self, root_plan: Hector<A>) -> Vec<Plan<A>> {
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
fn pull_attributes<A: AsAid + From<String>>(selection_set: &SelectionSet) -> Vec<A> {
    selection_set
        .items
        .iter()
        .flat_map(|item| match item {
            Selection::Field(field) => {
                if field.selection_set.items.is_empty() {
                    Some(A::from(field.name.to_string()))
                } else {
                    None
                }
            }
            _ => unimplemented!(),
        })
        .collect::<Vec<A>>()
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
fn selection_set_to_paths<A: AsAid + From<String>>(
    selection_set: &SelectionSet,
    mut plan: Hector<A>,
    arguments: &[(Name, Value)],
    parent_path: &[A],
) -> Vec<Plan<A>> {
    // We must first construct the correct plan for this level,
    // starting from that for the parent level. We do this even if no
    // attributes are actually pulled at this level. In that case we
    // will not synthesize this plan, but it still is required in
    // order to pass all necessary bindings to nested levels.

    // For any level after the first, we must introduce a binding
    // linking the parent level to the current one.
    if !parent_path.is_empty() {
        let parent = *plan.variables.last().unwrap();
        let this = plan.variables.len() as Var;
        let aid = parent_path.last().unwrap();

        plan.variables.push(this);
        plan.bindings
            .push(Binding::attribute(parent, A::from(aid.to_string()), this));
    }

    let this = *plan.variables.last().unwrap();

    // Then we must introduce additional bindings for any arguments.
    for (aid, v) in arguments.iter() {
        // This variable is only relevant for tying the two clauses
        // together, we do not want to include it into the output
        // projection.
        let vsym = gensym();

        plan.bindings
            .push(Binding::attribute(this, A::from(aid.to_string()), vsym));
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
                    parent_path.push(A::from(field.name.to_string()));

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
        .collect::<Vec<Plan<A>>>();

    let mut levels = nested_levels;

    // Here we don't actually want to include the current plan, if
    // we're not interested in any attributes at this level.
    if !pull_attributes.is_empty() {
        if plan.bindings.is_empty() {
            levels.push(Plan::PullAll(PullAll {
                variables: vec![],
                pull_attributes,
            }));
        } else {
            levels.push(Plan::PullLevel(PullLevel {
                pull_attributes,
                path_attributes: parent_path.to_vec(),
                pull_variable: this,
                variables: vec![],
                plan: Box::new(Plan::Hector(plan)),
                cardinality_many: false,
            }));
        }
    }

    levels
}

impl<A: AsAid + From<String>> Implementable for GraphQl<A> {
    type A = A;

    fn dependencies(&self) -> Dependencies<Self::A> {
        self.paths.iter().map(|path| path.dependencies()).sum()
    }

    fn implement<'b, S>(
        &self,
        nested: &mut Iterative<'b, S, u64>,
        domain: &mut Domain<Self::A, S::Timestamp>,
        local_arrangements: &VariableMap<Self::A, Iterative<'b, S, u64>>,
    ) -> (Implemented<'b, Self::A, S>, ShutdownHandle)
    where
        S: Scope,
        S::Timestamp: Timestamp + Lattice + Rewind,
    {
        let parsed = Pull {
            variables: vec![],
            paths: self.paths.clone(),
        };

        parsed.implement(nested, domain, local_arrangements)
    }
}

impl std::convert::From<Value> for crate::Value {
    fn from(v: Value) -> crate::Value {
        match v {
            Value::Int(v) => crate::Value::Number(v.as_i64().expect("failed to convert to i64")),
            Value::String(v) => crate::Value::String(v),
            Value::Boolean(v) => crate::Value::Bool(v),
            _ => unimplemented!(),
        }
    }
}
