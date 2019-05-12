//! GraphQL expression plan.

use timely::dataflow::scopes::child::Iterative;
use timely::dataflow::Scope;
use timely::progress::Timestamp;

use differential_dataflow::lattice::Lattice;

use graphql_parser::parse_query;
use graphql_parser::query::{Definition, Document, OperationDefinition, Selection, SelectionSet};
use graphql_parser::query::{Name, Value};

use crate::binding::Binding;
use crate::plan::{gensym, Dependencies, ImplContext, Implementable};
use crate::plan::{Hector, Plan, Pull, PullLevel};
use crate::{Aid, Var};
use crate::{Implemented, ShutdownHandle, VariableMap};

/// A plan for GraphQL queries, e.g. `{ Heroes { name age weight } }`.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub struct GraphQl {
    /// String representation of GraphQL query
    pub query: String,
    /// Cached paths
    paths: Vec<PullLevel<Plan>>,
}

impl GraphQl {
    /// Creates a new GraphQL instance by parsing the AST obtained
    /// from the provided query.
    pub fn new(query: String) -> Self {
        let ast = parse_query(&query).expect("graphQL ast parsing failed");

        GraphQl {
            query,
            paths: ast.into_paths(),
        }
    }
}

trait IntoPaths {
    fn into_paths(&self) -> Vec<PullLevel<Plan>>;
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
    fn into_paths(&self) -> Vec<PullLevel<Plan>> {
        self.definitions
            .iter()
            .flat_map(|definition| definition.into_paths())
            .collect()
    }
}

impl IntoPaths for Definition {
    fn into_paths(&self) -> Vec<PullLevel<Plan>> {
        match self {
            Definition::Operation(operation) => operation.into_paths(),
            Definition::Fragment(_) => unimplemented!(),
        }
    }
}

impl IntoPaths for OperationDefinition {
    fn into_paths(&self) -> Vec<PullLevel<Plan>> {
        use OperationDefinition::{Query, SelectionSet};

        match self {
            Query(_) => unimplemented!(),
            SelectionSet(selection_set) => {
                let empty_plan = Hector {
                    variables: vec![0],
                    bindings: vec![],
                };
                selection_set_to_paths(&selection_set, empty_plan, &vec![], &vec![])
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
    arguments: &Vec<(Name, Value)>,
    parent_path: &Vec<String>,
) -> Vec<PullLevel<Plan>> {
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
        let aid = parent_path.last().clone().unwrap();

        plan.variables.push(this);
        plan.bindings.push(Binding::attribute(parent, aid, this));
    }

    let this = *plan.variables.last().unwrap();

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
        .collect::<Vec<PullLevel<Plan>>>();

    let mut levels = nested_levels;

    // Here we don't actually want to include the current plan, if
    // we're not interested in any attributes at this level.
    if !pull_attributes.is_empty() {
        levels.push(PullLevel {
            pull_attributes,
            path_attributes: parent_path.to_vec(),
            pull_variable: this,
            variables: vec![],
            plan: Box::new(Plan::Hector(plan)),
        });
    }

    levels
}

impl Implementable for GraphQl {
    fn dependencies(&self) -> Dependencies {
        let mut dependencies = Dependencies::none();

        for path in self.paths.iter() {
            dependencies = Dependencies::merge(dependencies, path.dependencies());
        }

        dependencies
    }

    fn implement<'b, T, I, S>(
        &self,
        nested: &mut Iterative<'b, S, u64>,
        local_arrangements: &VariableMap<Iterative<'b, S, u64>>,
        context: &mut I,
    ) -> (Implemented<'b, S>, ShutdownHandle)
    where
        T: Timestamp + Lattice,
        I: ImplContext<T>,
        S: Scope<Timestamp = T>,
    {
        let parsed = Pull {
            variables: vec![],
            paths: self.paths.clone(),
        };

        parsed.implement(nested, local_arrangements, context)
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
