//! GraphQL expression plan.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::Operator;
use timely::dataflow::operators::{Delay, Exchange};
use timely::dataflow::scopes::child::Iterative;
use timely::dataflow::{Scope, Stream};
use timely::progress::Timestamp;

use differential_dataflow::hashable::Hashable;
use differential_dataflow::lattice::Lattice;

use graphql_parser::parse_query;
use graphql_parser::query::{Definition, Document, OperationDefinition, Selection, SelectionSet};
use graphql_parser::query::{Name, Value as GqValue};

use serde_json::Map;
use serde_json::Value as JValue;

use crate::binding::Binding;
use crate::plan::pull_v2::{PathId, Pull, PullAll, PullLevel};
use crate::plan::{gensym, Dependencies, ImplContext, Implementable};
use crate::plan::{Hector, Plan};
use crate::{Aid, Output, Value, Var};
use crate::{ShutdownHandle, VariableMap};

/// A plan for GraphQL queries, e.g. `{ Heroes { name age weight } }`.
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
        let parent = *plan.variables.last().unwrap();
        let this = plan.variables.len() as Var;
        let aid = parent_path.last().unwrap();

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
    pub fn dependencies(&self) -> Dependencies {
        let mut dependencies = Dependencies::none();

        for path in self.paths.iter() {
            dependencies = Dependencies::merge(dependencies, path.dependencies());
        }

        dependencies
    }

    pub fn implement<'b, T, I, S>(
        &self,
        nested: &mut Iterative<'b, S, u64>,
        context: &mut I,
    ) -> (Stream<S, Output>, ShutdownHandle)
    where
        T: Timestamp + Lattice,
        I: ImplContext<T>,
        S: Scope<Timestamp = T>,
        S::Timestamp: std::convert::Into<crate::timestamp::Time>,
    {
        use timely::dataflow::operators::Concatenate;

        let states = Rc::new(RefCell::new(JValue::Object(Map::new())));

        let dummy = VariableMap::new();

        let mut paths = self
            .paths
            .iter()
            .flat_map(|path| {
                let (streams, shutdown) = path.implement(nested, &dummy, context);
                std::mem::forget(shutdown);
                streams
            })
            .collect::<HashMap<PathId, _>>();

        let streams = paths.drain().map(|(path_id, stream)| {
            let states = states.clone();
            let mut buffer = HashMap::new();
            let mut vector = Vec::new();

            stream
                .exchange(|(path, _t, _diff)| path[0].clone().hashed())
                .delay(|(_path, t, _diff), _cap| t.clone())
                .unary_notify(
                    Pipeline,
                    "Changes",
                    vec![],
                    move |input, output, notificator| {
                        input.for_each(|cap, data| {
                            data.swap(&mut vector);
                            buffer
                                .entry(cap.time().clone())
                                .or_insert_with(Vec::new)
                                .extend(vector.drain(..));

                            notificator.notify_at(cap.retain());
                        });

                        let mut states = states.borrow_mut();

                        notificator.for_each(|cap, _, _| {
                            if let Some(mut paths_at_time) = buffer.remove(cap.time()) {
                                let mut changes = Vec::<String>::new();

                                for (mut path, t, diff) in paths_at_time.drain(..) {
                                    let value = JValue::from(path.pop().unwrap());
                                    let pointer = interleave(path, &path_id[..]);

                                    *pointer_mut(&mut states, &pointer) = value;

                                    changes.push(pointer[0].clone());
                                }

                                output.session(&cap).give_iterator(changes.drain(..));
                            }
                        });
                    },
                )
        });

        let mut change_keys = HashMap::new();
        let mut excise_keys = Vec::new();
        let mut vector = Vec::new();

        let required_aids = self.required_aids.clone();

        let snapshots = nested.parent.concatenate(streams).unary_notify(
            Pipeline,
            "GraphQl",
            vec![],
            move |input, output, notificator| {
                input.for_each(|cap, data| {
                    data.swap(&mut vector);
                    change_keys
                        .entry(cap.time().clone())
                        .or_insert_with(HashSet::new)
                        .extend(vector.drain(..));

                    notificator.notify_at(cap.retain());
                });

                {
                    let mut states = states.borrow_mut();

                    for (key, snapshot) in states.as_object().unwrap().iter() {
                        for required_aid in required_aids.iter() {
                            if !snapshot.as_object().unwrap().contains_key(required_aid) {
                                excise_keys.push(key.clone());
                            }
                        }
                    }

                    let states_map = states.as_object_mut().unwrap();
                    for key in excise_keys.drain(..) {
                        states_map.remove(&key);
                    }
                }

                {
                    let states = states.borrow();

                    notificator.for_each(|cap, _, _| {
                        if let Some(mut keys) = change_keys.remove(cap.time()) {
                            let t = cap.time().clone();

                            let snapshots = keys.drain().flat_map(|key| {
                                if let Some(snapshot) = states.get(key) {
                                    Some(Output::Json(
                                        "test".to_string(),
                                        snapshot.clone(),
                                        t.clone().into(),
                                        1,
                                    ))
                                } else {
                                    None
                                }
                            });

                            output.session(&cap).give_iterator(snapshots);
                        }
                    });
                }
            },
        );

        (snapshots, ShutdownHandle::empty())
    }
}

fn interleave(mut values: Vec<Value>, constants: &[Aid]) -> Vec<String> {
    if values.is_empty() {
        values
            .drain(..)
            .map(|v| JValue::from(v).as_str().unwrap().to_string())
            .collect()
    } else if constants.is_empty() {
        values
            .drain(..)
            .map(|v| JValue::from(v).as_str().unwrap().to_string())
            .collect()
    } else {
        let size: usize = values.len() + constants.len();
        // + 2, because we know there'll be a and v coming...
        let mut result: Vec<String> = Vec::with_capacity(size + 2);

        let mut next_const = 0;

        let mut values = values.drain(..).rev().collect::<Vec<Value>>();

        for i in 0..size {
            if i % 2 == 0 {
                // on even indices we take from the result tuple
                let v: Value = values.pop().unwrap();
                result.push(JValue::from(v).as_str().unwrap().to_string());
            } else {
                // on odd indices we interleave an attribute
                let a = constants[next_const].to_string();
                result.push(a);
                next_const += 1;
            }
        }

        result
    }
}

fn pointer_mut<'a>(v: &'a mut JValue, tokens: &[String]) -> &'a mut JValue {
    if tokens.is_empty() {
        v
    } else {
        let mut target = v;

        for token in tokens {
            // borrow checker gets confused about `target` being
            // mutably borrowed too many times because of the loop
            // this once-per-loop binding makes the scope clearer and
            // circumvents the error
            let target_once = target;

            target = match *target_once {
                JValue::Object(ref mut map) => {
                    if !map.contains_key(token) {
                        map.insert(token.to_string(), JValue::Object(Map::new()));
                    }

                    map.get_mut(token).unwrap()
                }
                // JValue::Array(ref mut list) => {
                //     parse_index(&token).and_then(move |x| list.get_mut(x))
                // }
                _ => panic!("!!!"),
            };
        }

        target
    }
}

fn parse_index(s: &str) -> Option<usize> {
    if s.starts_with('+') || (s.starts_with('0') && s.len() != 1) {
        return None;
    }
    s.parse().ok()
}
