//! GraphQL expression plan.

use graphql_parser::parse_query;
use graphql_parser::query::{Definition, Selection, SelectionSet, OperationDefinition, Document};

use crate::plan::{Plan, ImplContext, Implementable, PullLevel};

/// A plan for GraphQL queries, e.g. `{ Heroes { name age weight } }`.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct GraphQl {
    /// String representation of a GraphQL query.
    pub query: String,
}

fn selection_set_to_paths(
    selection_set: &SelectionSet,
    parent_path: &Vec<String>
) -> Vec<PullLevel<Plan>>
{    
    let mut result = vec![];
    let mut pull_attributes = vec![];
    let variables = vec![];

    for item in &selection_set.items {
        match item {
            Selection::Field(field) => {
                pull_attributes.push(field.name.to_string());
                let mut new_parent_path = parent_path.to_vec();
                new_parent_path.push(field.name.to_string());
                result.extend(selection_set_to_paths(&field.selection_set, &new_parent_path));
            },
            _ => unimplemented!()
        }
    }

    // parent_path handles root path case
    if !pull_attributes.is_empty() && !parent_path.is_empty() {
        let pull_level = PullLevel {
            pull_attributes,
            path_attributes: parent_path.to_vec(),
            variables,
            plan: Box::new(Plan::MatchA(0, parent_path.last().unwrap().to_string(), 1))
        };
        result.push(pull_level);
    }

    result
}

/// Converts a GraphQL AST to pull paths.
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
fn ast_to_paths (ast: Document) -> Vec<PullLevel<Plan>> {
    let mut result = vec![];
    for definition in &ast.definitions {
        match definition {
            Definition::Operation(operation_definition) => {
                match operation_definition {
                    OperationDefinition::Query(query) => unimplemented!(),
                    OperationDefinition::SelectionSet(selection_set) => result.extend(selection_set_to_paths(selection_set, &vec![])),
                    _ => unimplemented!()
                }
            },
            Definition::Fragment(fragment_definition) => unimplemented!()
        };
    }

    result
}

impl Implementable for GraphQl {
    fn implement<'b, S: Scope<Timestamp = u64>>(
        &self,
        nested: &mut Iterative<'b, S, u64>,
        local_arrangements: &RelationMap<Iterative<'b, S, u64>>,
        global_arrangements: &mut QueryMap<isize>,
    ) -> SimpleRelation<'b, S> {
        let ast = parse_query(&self.query).expect("graphQL ast parsing failed");
        let parsed = Pull { paths: ast_to_paths(ast) };

        parsed.implement(nested, local_arrangements, global_arrangements)
    }
}
