//! GraphQL expression plan.

use graphql_parser::parse_query;
use graphql_parser::query::{Definition, Selection, SelectionSet, OperationDefinition, Document};

use crate::plan::{Plan, ImplContext, Implementable, PullLevel};

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

fn selection_set_to_paths(
    selection_set: &SelectionSet,
    parent_path: &Vec<String>,
    at_root: bool,
) -> Vec<PullLevel<Plan>> {
    let mut result = vec![];
    let mut pull_attributes = vec![];
    let variables = vec![];

    for item in &selection_set.items {
        match item {
            Selection::Field(field) => {
                if field.selection_set.items.is_empty() {
                    pull_attributes.push(field.name.to_string());
                }

                let mut new_parent_path = parent_path.to_vec();
                new_parent_path.push(field.name.to_string());

                result.extend(selection_set_to_paths(
                    &field.selection_set,
                    &new_parent_path,
                    parent_path.is_empty(),
                ));
            }
            _ => unimplemented!(),
        }
    }

    // parent_path handles root path case
    if !pull_attributes.is_empty() && !parent_path.is_empty() {
        // for root, we expect a NameExpr that puts the pulled IDs in the v position
        let plan;
        if at_root {
            plan = Box::new(Plan::NameExpr(
                vec![0, 1],
                parent_path.last().unwrap().to_string(),
            ));
        } else {
            plan = Box::new(Plan::MatchA(0, parent_path.last().unwrap().to_string(), 1));
        }

        let pull_level = PullLevel {
            pull_attributes,
            path_attributes: parent_path.to_vec(),
            variables,
            plan,
        };
        result.push(pull_level);
    }

    result
}

/// converts an ast to paths
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
    ) -> (CollectionRelation<'b, S>, ShutdownHandle)
    where
        T: Timestamp + Lattice + TotalOrder,
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