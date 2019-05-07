//! GraphQL expression plan.

use timely::dataflow::scopes::child::Iterative;
use timely::dataflow::Scope;
use timely::progress::Timestamp;

use differential_dataflow::lattice::Lattice;

use graphql_parser::parse_query;
use graphql_parser::query::{Definition, Document, OperationDefinition, Selection, SelectionSet};

use crate::plan::{Dependencies, ImplContext, Implementable};
use crate::plan::{Plan, Pull, PullLevel};
use crate::Aid;
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
            SelectionSet(selection_set) => selection_set_to_paths(&selection_set, &vec![], true),
            _ => unimplemented!(),
        }
    }
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
    parent_path: &Vec<String>,
    at_root: bool,
) -> Vec<PullLevel<Plan>> {
    // We will first gather the attributes that need to be retrieved
    // at this level. These are the fields that do not refer to a
    // nested entity.
    let pull_attributes = selection_set
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
        .collect::<Vec<Aid>>();

    // We then gather any nested levels that need to be transformed
    // into their own plans.
    let nested_levels = selection_set
        .items
        .iter()
        .flat_map(|item| match item {
            Selection::Field(field) => {
                if !field.selection_set.items.is_empty() {
                    let mut parent_path = parent_path.to_vec();
                    parent_path.push(field.name.to_string());

                    selection_set_to_paths(&field.selection_set, &parent_path, false)
                } else {
                    vec![]
                }
            }
            _ => unimplemented!(),
        })
        .collect::<Vec<PullLevel<Plan>>>();

    let mut levels = nested_levels;

    // Finally we construct a plan stage for our current level.
    if !pull_attributes.is_empty() && !parent_path.is_empty() {
        let current_level = if parent_path.len() == 1 {
            // @TODO
            // The root selection of the GraphQL query is currently
            // assumed to be defined by a named, binary query of the
            // form (root _ ?e).
            PullLevel {
                pull_attributes,
                path_attributes: parent_path.to_vec(),
                pull_variable: 1,
                variables: vec![],
                plan: Box::new(Plan::NameExpr(
                    vec![0, 1],
                    parent_path.last().unwrap().to_string(),
                )),
            }
        } else {
            // We select children via a [?e a ?v] clause.
            PullLevel {
                pull_attributes,
                path_attributes: parent_path.to_vec(),
                pull_variable: 1,
                variables: vec![],
                plan: Box::new(Plan::MatchA(0, parent_path.last().unwrap().to_string(), 1)),
            }
        };

        levels.push(current_level);
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
