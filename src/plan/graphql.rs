//! GraphQL expression plan.

use timely::dataflow::scopes::child::Iterative;
use timely::dataflow::Scope;
use timely::progress::Timestamp;

use differential_dataflow::lattice::Lattice;

use graphql_parser::parse_query;
use graphql_parser::query::{Definition, Document, OperationDefinition, Selection, SelectionSet};

use crate::plan::{Dependencies, ImplContext, Implementable};
use crate::plan::{Plan, Pull, PullLevel};
use crate::{Implemented, ShutdownHandle, VariableMap};

/// A plan for GraphQL queries, e.g. `{ Heroes { name age weight } }`
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub struct GraphQl {
    /// String representation of GraphQL query
    pub query: String,
    /// Cached paths
    pub paths: Vec<PullLevel<Plan>>,
}

impl GraphQl {
    /// Creates a new GraphQL instance by parsing the ast obtained from the provided query
    pub fn new(query: String) -> Self {
        let q = query.clone();
        GraphQl {
            query,
            paths: ast_to_paths(parse_query(&q).expect("graphQL ast parsing failed")),
        }
    }
}

/// Takes a GraphQL `SelectionSet` and recursively transforms it into `PullLevel`s.
///
/// A `SelectionSet` consists of multiple items. We're interested in items of type
/// `Field`, which might contain a nested `SelectionSet` themselves. We iterate through
/// each field and construct (1) a parent path, which describes how to traverse to the
/// current nesting level ("vertical"), and (2) pull attributes, which describe the
/// attributes pulled at the current nesting level ("horizontal"); only attributes at
/// the lowest nesting level can be part of a `PullLevel`'s `pull_attributes`.
fn selection_set_to_paths(
    selection_set: &SelectionSet,
    parent_path: &Vec<String>,
    at_root: bool,
) -> Vec<PullLevel<Plan>> {
    let mut result = vec![];
    let mut pull_attributes = vec![];
    let variables = vec![];

    // 1. Collect Fields and potentially recur.
    for item in &selection_set.items {
        match item {
            Selection::Field(field) => {
                // at lowest nesting level
                if field.selection_set.items.is_empty() {
                    // "horizontal" path
                    pull_attributes.push(field.name.to_string());
                }

                // "vertical" path to this nesting level
                let mut new_parent_path = parent_path.to_vec();
                new_parent_path.push(field.name.to_string());

                // recur
                result.extend(selection_set_to_paths(
                    &field.selection_set,
                    &new_parent_path,
                    parent_path.is_empty(),
                ));
            }
            _ => unimplemented!(),
        }
    }

    // 2. Construct the current level's `PullLevel`.
    if !pull_attributes.is_empty() && !parent_path.is_empty() {
        let plan = if at_root {
            // If we're looking at the root of the GraphQL query, we need to push
            // the pulled IDs into the v position.
            Plan::NameExpr(vec![0, 1], parent_path.last().unwrap().to_string())
        } else {
            // Otherwise, we transform into a simple [?e a ?v] clause.
            Plan::MatchA(0, parent_path.last().unwrap().to_string(), 1)
        };

        let pull_level = PullLevel {
            pull_attributes,
            path_attributes: parent_path.to_vec(),
            pull_variable: 1,
            variables,
            plan: Box::new(plan),
        };
        result.push(pull_level);
    }

    result
}

/// Inbound direction: Parse the provided GraphQL query ast into
/// pull paths, which will then be used downstream to query data.
///
/// The structure of a typical parsed ast looks like this:
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
fn ast_to_paths(ast: Document) -> Vec<PullLevel<Plan>> {
    let mut result = vec![];
    for definition in &ast.definitions {
        match definition {
            Definition::Operation(operation_definition) => match operation_definition {
                OperationDefinition::Query(_) => unimplemented!(),
                OperationDefinition::SelectionSet(selection_set) => {
                    result.extend(selection_set_to_paths(selection_set, &vec![], true))
                }
                _ => unimplemented!(),
            },
            Definition::Fragment(_) => unimplemented!(),
        };
    }

    result
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
        let ast = parse_query(&self.query).expect("graphQL ast parsing failed");
        let parsed = Pull {
            variables: vec![],
            paths: ast_to_paths(ast),
        };

        parsed.implement(nested, local_arrangements, context)
    }
}
