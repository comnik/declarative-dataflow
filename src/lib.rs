//! Declarative dataflow infrastructure
//!
//! This crate contains types, traits, and logic for assembling
//! differential dataflow computations from declaratively specified
//! programs, without any additional compilation.

#![forbid(missing_docs)]

#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;

pub mod binding;
pub mod domain;
pub mod logging;
pub mod operators;
pub mod plan;
pub mod server;
pub mod sinks;
pub mod sources;
pub mod timestamp;

use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Duration;

use timely::dataflow::operators::CapabilitySet;
use timely::dataflow::scopes::child::Iterative;
use timely::dataflow::*;
use timely::order::Product;
use timely::progress::Timestamp;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::{ShutdownButton, TraceAgent};
use differential_dataflow::operators::iterate::Variable;
#[cfg(not(feature = "set-semantics"))]
use differential_dataflow::operators::Consolidate;
#[cfg(feature = "set-semantics")]
use differential_dataflow::operators::Threshold;
use differential_dataflow::trace::implementations::ord::{OrdKeySpine, OrdValSpine};
use differential_dataflow::trace::TraceReader;
use differential_dataflow::{Collection, ExchangeData};

#[cfg(feature = "uuid")]
pub use uuid::Uuid;

pub use num_rational::Rational32;

pub use binding::{AsBinding, AttributeBinding, Binding};
pub use plan::{Hector, ImplContext, Implementable, Plan};
pub use timestamp::{Rewind, Time};

/// A unique entity identifier.
pub type Eid = u64;

/// A unique attribute identifier.
pub type Aid = String; // u32

/// Possible data values.
///
/// This enum captures the currently supported data types, and is the
/// least common denominator for the types of records moved around.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub enum Value {
    /// An attribute identifier
    Aid(Aid),
    /// A string
    String(String),
    /// A boolean
    Bool(bool),
    /// A 64 bit signed integer
    Number(i64),
    /// A 32 bit rational
    Rational32(Rational32),
    /// An entity identifier
    Eid(Eid),
    /// Milliseconds since midnight, January 1, 1970 UTC
    Instant(u64),
    /// A 16 byte unique identifier.
    #[cfg(feature = "uuid")]
    Uuid(Uuid),
    /// A fixed-precision real number.
    #[cfg(feature = "real")]
    Real(fixed::types::I16F16),
}

impl Value {
    /// Helper to create an Aid value from a string representation.
    pub fn aid(v: &str) -> Self {
        Value::Aid(v.to_string())
    }

    /// Helper to create a UUID value from a string representation.
    #[cfg(feature = "uuid")]
    pub fn uuid_str(v: &str) -> Self {
        let uuid = Uuid::parse_str(v).expect("failed to parse UUID");
        Value::Uuid(uuid)
    }
}

impl std::convert::From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::String(v.to_string())
    }
}

#[cfg(feature = "real")]
impl std::convert::From<f64> for Value {
    fn from(v: f64) -> Self {
        let real =
            fixed::types::I16F16::checked_from_float(v).expect("failed to convert to I16F16");

        Value::Real(real)
    }
}

#[cfg(feature = "serde_json")]
impl std::convert::From<Value> for serde_json::Value {
    fn from(v: Value) -> Self {
        match v {
            Value::Aid(v) => serde_json::Value::String(v),
            Value::String(v) => serde_json::Value::String(v),
            Value::Bool(v) => serde_json::Value::Bool(v),
            Value::Number(v) => serde_json::Value::Number(serde_json::Number::from(v)),
            _ => unimplemented!(),
        }
    }
}

impl std::convert::From<Value> for Eid {
    fn from(v: Value) -> Eid {
        if let Value::Eid(eid) = v {
            eid
        } else {
            panic!("Value {:?} can't be converted to Eid", v);
        }
    }
}

/// A client-facing, non-exceptional error.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Error {
    /// Error category.
    #[serde(rename = "df.error/category")]
    pub category: String,
    /// Free-frorm description.
    #[serde(rename = "df.error/message")]
    pub message: String,
}

impl Error {
    /// Fix client bug.
    pub fn incorrect<E: std::string::ToString>(error: E) -> Error {
        Error {
            category: "df.error.category/incorrect".to_string(),
            message: error.to_string(),
        }
    }

    /// Fix client noun.
    pub fn not_found<E: std::string::ToString>(error: E) -> Error {
        Error {
            category: "df.error.category/not-found".to_string(),
            message: error.to_string(),
        }
    }

    /// Coordinate with worker.
    pub fn conflict<E: std::string::ToString>(error: E) -> Error {
        Error {
            category: "df.error.category/conflict".to_string(),
            message: error.to_string(),
        }
    }

    /// Fix worker bug.
    pub fn fault<E: std::string::ToString>(error: E) -> Error {
        Error {
            category: "df.error.category/fault".to_string(),
            message: error.to_string(),
        }
    }

    /// Fix client verb.
    pub fn unsupported<E: std::string::ToString>(error: E) -> Error {
        Error {
            category: "df.error.category/unsupported".to_string(),
            message: error.to_string(),
        }
    }
}

/// Transaction data.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub struct TxData(pub isize, pub Value, pub Aid, pub Value, pub Option<Time>);

impl TxData {
    /// Creates TxData representing the addition of a single fact.
    pub fn add(e: Eid, a: &str, v: Value) -> Self {
        TxData(1, Value::Eid(e), a.to_string(), v, None)
    }

    /// Creates TxData representing the addition of a single fact at a
    /// specific point in time.
    pub fn add_at(e: Eid, a: &str, v: Value, t: Time) -> Self {
        TxData(1, Value::Eid(e), a.to_string(), v, Some(t))
    }

    /// Creates TxData representing the retraction of a single fact.
    pub fn retract(e: Eid, a: &str, v: Value) -> Self {
        TxData(-1, Value::Eid(e), a.to_string(), v, None)
    }

    /// Creates TxData representing the retraction of a single fact at
    /// a specific point in time.
    pub fn retract_at(e: Eid, a: &str, v: Value, t: Time) -> Self {
        TxData(-1, Value::Eid(e), a.to_string(), v, Some(t))
    }
}

/// A (tuple, time, diff) triple, as sent back to clients.
pub type ResultDiff<T> = (Vec<Value>, T, isize);

/// A worker-local client connection identifier.
pub type Client = usize;

/// Anything that can be returned to clients.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Output {
    /// A batch of (tuple, time, diff) triples as returned by Datalog
    /// queries.
    QueryDiff(String, Vec<ResultDiff<Time>>),
    /// A JSON object, e.g. as returned by GraphQL queries.
    #[cfg(feature = "serde_json")]
    Json(String, serde_json::Value, Time, isize),
    /// A message forwarded to a specific client.
    #[cfg(feature = "serde_json")]
    Message(Client, serde_json::Value),
    /// An error forwarded to a specific client.
    Error(Client, Error, server::TxId),
}

/// A trace of values indexed by self.
pub type TraceKeyHandle<K, T, R> = TraceAgent<OrdKeySpine<K, T, R>>;

/// A trace of (K, V) pairs indexed by key.
pub type TraceValHandle<K, V, T, R> = TraceAgent<OrdValSpine<K, V, T, R>>;

/// A handle to an arranged relation.
pub type RelationHandle<T> = TraceKeyHandle<Vec<Value>, T, isize>;

// A map for keeping track of collections that are being actively
// synthesized (i.e. that are not fully defined yet).
type VariableMap<G> = HashMap<String, Variable<G, Vec<Value>, isize>>;

trait Shutdownable {
    fn press(&mut self);
}

impl<T> Shutdownable for ShutdownButton<T> {
    #[inline(always)]
    fn press(&mut self) {
        self.press();
    }
}

/// A wrapper around a vector of ShutdownButton's. Ensures they will
/// be pressed on dropping the handle.
pub struct ShutdownHandle {
    shutdown_buttons: Vec<Box<dyn Shutdownable>>,
}

impl Drop for ShutdownHandle {
    fn drop(&mut self) {
        for mut button in self.shutdown_buttons.drain(..) {
            trace!("pressing shutdown button");
            button.press();
        }
    }
}

impl ShutdownHandle {
    /// Returns an empty shutdown handle.
    pub fn empty() -> Self {
        ShutdownHandle {
            shutdown_buttons: Vec::new(),
        }
    }

    /// Wraps a single shutdown button into a shutdown handle.
    pub fn from_button<T: Timestamp>(button: ShutdownButton<CapabilitySet<T>>) -> Self {
        ShutdownHandle {
            shutdown_buttons: vec![Box::new(button)],
        }
    }

    /// Adds another shutdown button to this handle. This button will
    /// then also be pressed, whenever the handle is shut down or
    /// dropped.
    pub fn add_button<T: Timestamp>(&mut self, button: ShutdownButton<CapabilitySet<T>>) {
        self.shutdown_buttons.push(Box::new(button));
    }

    /// Combines the buttons of another handle into self.
    pub fn merge_with(&mut self, mut other: Self) {
        self.shutdown_buttons.append(&mut other.shutdown_buttons);
    }

    /// Combines two shutdown handles into a single one, which will
    /// control both.
    pub fn merge(mut left: Self, mut right: Self) -> Self {
        let mut shutdown_buttons =
            Vec::with_capacity(left.shutdown_buttons.len() + right.shutdown_buttons.len());
        shutdown_buttons.append(&mut left.shutdown_buttons);
        shutdown_buttons.append(&mut right.shutdown_buttons);

        ShutdownHandle { shutdown_buttons }
    }
}

/// Attribute indices can have various operations applied to them,
/// based on their semantics.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub enum InputSemantics {
    /// No special semantics enforced. Source is responsible for
    /// everything.
    Raw,
    /// Only a single value per eid is allowed at any given timestamp.
    CardinalityOne,
    /// Multiple different values for any given eid are allowed, but
    /// (e,v) pairs are enforced to be distinct.
    CardinalityMany,
    // /// @TODO
    // CAS,
}

/// Per-attribute semantics.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub struct AttributeConfig {
    /// Modifiers to apply on attribute inputs, such as keeping only
    /// the most recent value per eid, or compare-and-swap.
    pub input_semantics: InputSemantics,
    /// How close indexed traces should follow the computation
    /// frontier.
    pub trace_slack: Option<Time>,
    /// Will this attribute require reverse indices?
    pub enable_reverse: bool,
    /// Will this attribute be used in worst-case optimal queries?
    pub enable_wco: bool,
    /// Does this attribute care about its respective time
    /// dimension? Timeless attributes do not have an
    /// influence on the overall progress in the system.
    pub timeless: bool,
}

impl Default for AttributeConfig {
    fn default() -> Self {
        AttributeConfig {
            input_semantics: InputSemantics::Raw,
            trace_slack: None,
            enable_reverse: false,
            enable_wco: false,
            timeless: false,
        }
    }
}

impl AttributeConfig {
    /// Shortcut to specifying an attribute that will live in some
    /// transaction time domain and always compact up to the
    /// computation frontier.
    pub fn tx_time(input_semantics: InputSemantics) -> Self {
        AttributeConfig {
            input_semantics,
            // @TODO It's not super clear yet, whether this can be
            // 0. There might be an off-by-one error hidden somewhere,
            // s.t. traces advance to t+1 when we're still accepting
            // inputs for t+1.
            trace_slack: Some(Time::TxId(1)),
            ..Default::default()
        }
    }

    /// Shortcut to specifying an attribute that will live in some
    /// real-time domain and always compact up to the computation
    /// frontier.
    pub fn real_time(input_semantics: InputSemantics) -> Self {
        AttributeConfig {
            input_semantics,
            trace_slack: Some(Time::Real(Duration::from_secs(0))),
            ..Default::default()
        }
    }

    /// Shortcut to specifying an attribute that will live in an
    /// arbitrary time domain and never compact its trace.
    pub fn uncompacted(input_semantics: InputSemantics) -> Self {
        AttributeConfig {
            input_semantics,
            trace_slack: None,
            ..Default::default()
        }
    }
}

/// Per-relation semantics.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug)]
pub struct RelationConfig {
    /// How close the arranged trace should follow the computation
    /// frontier.
    pub trace_slack: Option<Time>,
}

/// A variable used in a query.
type Var = u32;

/// A named relation.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub struct Rule {
    /// The name identifying the relation.
    pub name: String,
    /// The plan describing contents of the relation.
    pub plan: Plan,
}

/// A relation between a set of variables.
///
/// Relations can be backed by a collection of records of type
/// `Vec<Value>`, each of a common length (with offsets corresponding
/// to the variable offsets), or by an existing arrangement.
trait Relation<'a, G, I>: AsBinding
where
    G: Scope,
    G::Timestamp: Lattice + ExchangeData,
    I: ImplContext<G::Timestamp>,
{
    /// A collection containing all tuples.
    fn tuples(
        self,
        nested: &mut Iterative<'a, G, u64>,
        context: &mut I,
    ) -> (
        Collection<Iterative<'a, G, u64>, Vec<Value>, isize>,
        ShutdownHandle,
    );

    /// A collection containing all tuples projected onto the
    /// specified variables.
    fn projected(
        self,
        nested: &mut Iterative<'a, G, u64>,
        context: &mut I,
        target_variables: &[Var],
    ) -> (
        Collection<Iterative<'a, G, u64>, Vec<Value>, isize>,
        ShutdownHandle,
    );

    /// A collection with tuples partitioned by `variables`.
    ///
    /// Each tuple is mapped to a pair `(Vec<Value>, Vec<Value>)`
    /// containing first exactly those variables in `variables` in that
    /// order, followed by the remaining values in their original
    /// order.
    fn tuples_by_variables(
        self,
        nested: &mut Iterative<'a, G, u64>,
        context: &mut I,
        variables: &[Var],
    ) -> (
        Collection<Iterative<'a, G, u64>, (Vec<Value>, Vec<Value>), isize>,
        ShutdownHandle,
    );
}

/// A collection and variable bindings.
pub struct CollectionRelation<'a, G: Scope> {
    variables: Vec<Var>,
    tuples: Collection<Iterative<'a, G, u64>, Vec<Value>, isize>,
}

impl<'a, G: Scope> AsBinding for CollectionRelation<'a, G>
where
    G::Timestamp: Lattice + ExchangeData,
{
    fn variables(&self) -> Vec<Var> {
        self.variables.clone()
    }

    fn binds(&self, variable: Var) -> Option<usize> {
        self.variables.binds(variable)
    }

    fn ready_to_extend(&self, _prefix: &AsBinding) -> Option<Var> {
        unimplemented!();
    }

    fn required_to_extend(&self, _prefix: &AsBinding, _target: Var) -> Option<Option<Var>> {
        unimplemented!();
    }
}

impl<'a, G, I> Relation<'a, G, I> for CollectionRelation<'a, G>
where
    G: Scope,
    G::Timestamp: Lattice + ExchangeData,
    I: ImplContext<G::Timestamp>,
{
    fn tuples(
        self,
        _nested: &mut Iterative<'a, G, u64>,
        _context: &mut I,
    ) -> (
        Collection<Iterative<'a, G, u64>, Vec<Value>, isize>,
        ShutdownHandle,
    ) {
        (self.tuples, ShutdownHandle::empty())
    }

    fn projected(
        self,
        _nested: &mut Iterative<'a, G, u64>,
        _context: &mut I,
        target_variables: &[Var],
    ) -> (
        Collection<Iterative<'a, G, u64>, Vec<Value>, isize>,
        ShutdownHandle,
    ) {
        if self.variables() == target_variables {
            (self.tuples, ShutdownHandle::empty())
        } else {
            let relation_variables = self.variables();
            let target_variables = target_variables.to_vec();

            let tuples = self.tuples.map(move |tuple| {
                target_variables
                    .iter()
                    .map(|x| {
                        let idx = relation_variables.binds(*x).unwrap();
                        tuple[idx].clone()
                    })
                    .collect()
            });

            (tuples, ShutdownHandle::empty())
        }
    }

    fn tuples_by_variables(
        self,
        _nested: &mut Iterative<'a, G, u64>,
        _context: &mut I,
        variables: &[Var],
    ) -> (
        Collection<Iterative<'a, G, u64>, (Vec<Value>, Vec<Value>), isize>,
        ShutdownHandle,
    ) {
        if variables == &self.variables()[..] {
            (
                self.tuples.map(|x| (x, Vec::new())),
                ShutdownHandle::empty(),
            )
        } else if variables.is_empty() {
            (
                self.tuples.map(|x| (Vec::new(), x)),
                ShutdownHandle::empty(),
            )
        } else {
            let key_length = variables.len();
            let values_length = self.variables().len() - key_length;

            let mut key_offsets: Vec<usize> = Vec::with_capacity(key_length);
            let mut value_offsets: Vec<usize> = Vec::with_capacity(values_length);
            let variable_set: HashSet<Var> = variables.iter().cloned().collect();

            // It is important to preserve the key variables in the order
            // they were specified.
            for variable in variables.iter() {
                key_offsets.push(self.binds(*variable).unwrap());
            }

            // Values we'll just take in the order they were.
            for (idx, variable) in self.variables().iter().enumerate() {
                if !variable_set.contains(variable) {
                    value_offsets.push(idx);
                }
            }

            let arranged = self.tuples.map(move |tuple| {
                let key: Vec<Value> = key_offsets.iter().map(|i| tuple[*i].clone()).collect();
                // @TODO second clone not really neccessary
                let values: Vec<Value> = value_offsets
                    .iter()
                    .map(move |i| tuple[*i].clone())
                    .collect();

                (key, values)
            });

            (arranged, ShutdownHandle::empty())
        }
    }
}

impl<'a, G, I> Relation<'a, G, I> for AttributeBinding
where
    G: Scope,
    G::Timestamp: Lattice + ExchangeData,
    I: ImplContext<G::Timestamp>,
{
    fn tuples(
        self,
        nested: &mut Iterative<'a, G, u64>,
        context: &mut I,
    ) -> (
        Collection<Iterative<'a, G, u64>, Vec<Value>, isize>,
        ShutdownHandle,
    ) {
        let variables = self.variables();
        self.projected(nested, context, &variables)
    }

    fn projected(
        self,
        nested: &mut Iterative<'a, G, u64>,
        context: &mut I,
        target_variables: &[Var],
    ) -> (
        Collection<Iterative<'a, G, u64>, Vec<Value>, isize>,
        ShutdownHandle,
    ) {
        match context.forward_propose(&self.source_attribute) {
            None => panic!("attribute {:?} does not exist", self.source_attribute),
            Some(propose_trace) => {
                let frontier = propose_trace.advance_frontier().to_vec();
                let (propose, shutdown_propose) =
                    propose_trace.import_core(&nested.parent, &self.source_attribute);

                let tuples = propose.enter_at(nested, move |_, _, time| {
                    let mut forwarded = time.clone();
                    forwarded.advance_by(&frontier);
                    Product::new(forwarded, 0)
                });

                let (e, v) = self.variables;
                let projected = if target_variables == [e, v] {
                    tuples.as_collection(|e, v| vec![e.clone(), v.clone()])
                } else if target_variables == [v, e] {
                    tuples.as_collection(|e, v| vec![v.clone(), e.clone()])
                } else if target_variables == [e] {
                    tuples.as_collection(|e, _v| vec![e.clone()])
                } else if target_variables == [v] {
                    tuples.as_collection(|_e, v| vec![v.clone()])
                } else {
                    panic!("invalid projection")
                };

                (projected, ShutdownHandle::from_button(shutdown_propose))
            }
        }
    }

    fn tuples_by_variables(
        self,
        nested: &mut Iterative<'a, G, u64>,
        context: &mut I,
        variables: &[Var],
    ) -> (
        Collection<Iterative<'a, G, u64>, (Vec<Value>, Vec<Value>), isize>,
        ShutdownHandle,
    ) {
        match context.forward_propose(&self.source_attribute) {
            None => panic!("attribute {:?} does not exist", self.source_attribute),
            Some(propose_trace) => {
                let frontier = propose_trace.advance_frontier().to_vec();
                let (propose, shutdown_propose) =
                    propose_trace.import_core(&nested.parent, &self.source_attribute);

                let tuples = propose.enter_at(nested, move |_, _, time| {
                    let mut forwarded = time.clone();
                    forwarded.advance_by(&frontier);
                    Product::new(forwarded, 0)
                });

                let (e, v) = self.variables;
                let arranged = if variables == [e, v] {
                    tuples.as_collection(|e, v| (vec![e.clone(), v.clone()], vec![]))
                } else if variables == [v, e] {
                    tuples.as_collection(|e, v| (vec![v.clone(), e.clone()], vec![]))
                } else if variables == [e] {
                    tuples.as_collection(|e, v| (vec![e.clone()], vec![v.clone()]))
                } else if variables == [v] {
                    tuples.as_collection(|e, v| (vec![v.clone()], vec![e.clone()]))
                } else {
                    panic!("invalid projection")
                };

                (arranged, ShutdownHandle::from_button(shutdown_propose))
            }
        }
    }
}

/// @TODO
pub enum Implemented<'a, G>
where
    G: Scope,
    G::Timestamp: Lattice + ExchangeData,
{
    /// A relation backed by an attribute.
    Attribute(AttributeBinding),
    /// A relation backed by a Differential collection.
    Collection(CollectionRelation<'a, G>),
    // Arranged(ArrangedRelation<'a, G>)
}

impl<'a, G: Scope> AsBinding for Implemented<'a, G>
where
    G::Timestamp: Lattice + ExchangeData,
{
    fn variables(&self) -> Vec<Var> {
        match self {
            Implemented::Attribute(attribute_binding) => attribute_binding.variables(),
            Implemented::Collection(relation) => relation.variables(),
        }
    }

    fn binds(&self, variable: Var) -> Option<usize> {
        match self {
            Implemented::Attribute(attribute_binding) => attribute_binding.binds(variable),
            Implemented::Collection(relation) => relation.binds(variable),
        }
    }

    fn ready_to_extend(&self, prefix: &AsBinding) -> Option<Var> {
        match self {
            Implemented::Attribute(attribute_binding) => attribute_binding.ready_to_extend(prefix),
            Implemented::Collection(relation) => relation.ready_to_extend(prefix),
        }
    }

    fn required_to_extend(&self, prefix: &AsBinding, target: Var) -> Option<Option<Var>> {
        match self {
            Implemented::Attribute(attribute_binding) => {
                attribute_binding.required_to_extend(prefix, target)
            }
            Implemented::Collection(relation) => relation.required_to_extend(prefix, target),
        }
    }
}

impl<'a, G, I> Relation<'a, G, I> for Implemented<'a, G>
where
    G: Scope,
    G::Timestamp: Lattice + ExchangeData,
    I: ImplContext<G::Timestamp>,
{
    fn tuples(
        self,
        nested: &mut Iterative<'a, G, u64>,
        context: &mut I,
    ) -> (
        Collection<Iterative<'a, G, u64>, Vec<Value>, isize>,
        ShutdownHandle,
    ) {
        match self {
            Implemented::Attribute(attribute_binding) => attribute_binding.tuples(nested, context),
            Implemented::Collection(relation) => relation.tuples(nested, context),
        }
    }

    fn projected(
        self,
        nested: &mut Iterative<'a, G, u64>,
        context: &mut I,
        target_variables: &[Var],
    ) -> (
        Collection<Iterative<'a, G, u64>, Vec<Value>, isize>,
        ShutdownHandle,
    ) {
        match self {
            Implemented::Attribute(attribute_binding) => {
                attribute_binding.projected(nested, context, target_variables)
            }
            Implemented::Collection(relation) => {
                relation.projected(nested, context, target_variables)
            }
        }
    }

    fn tuples_by_variables(
        self,
        nested: &mut Iterative<'a, G, u64>,
        context: &mut I,
        variables: &[Var],
    ) -> (
        Collection<Iterative<'a, G, u64>, (Vec<Value>, Vec<Value>), isize>,
        ShutdownHandle,
    ) {
        match self {
            Implemented::Attribute(attribute_binding) => {
                attribute_binding.tuples_by_variables(nested, context, variables)
            }
            Implemented::Collection(relation) => {
                relation.tuples_by_variables(nested, context, variables)
            }
        }
    }
}

// /// A arrangement and variable bindings.
// struct ArrangedRelation<'a, G: Scope>
// where
//     G::Timestamp: Lattice+ExchangeData
// {
//     variables: Vec<Var>,
//     tuples: Arranged<Iterative<'a, G, u64>, Vec<Value>, Vec<Value>, isize,
//                      TraceValHandle<Vec<Value>, Vec<Value>, Product<G::Timestamp,u64>, isize>>,
// }

/// Helper function to create a query plan. The resulting query will
/// provide values for the requested target variables, under the
/// constraints expressed by the bindings provided.
pub fn q(target_variables: Vec<Var>, bindings: Vec<Binding>) -> Plan {
    Plan::Hector(Hector {
        variables: target_variables,
        bindings,
    })
}

/// Returns a deduplicates list of all rules used in the definition of
/// the specified names. Includes the specified names.
pub fn collect_dependencies<T, I>(context: &I, names: &[&str]) -> Result<Vec<Rule>, Error>
where
    T: Timestamp + Lattice,
    I: ImplContext<T>,
{
    let mut seen = HashSet::new();
    let mut rules = Vec::new();
    let mut queue = VecDeque::new();

    for name in names {
        match context.rule(name) {
            None => {
                return Err(Error::not_found(format!("Unknown rule {}.", name)));
            }
            Some(rule) => {
                seen.insert(name.to_string());
                queue.push_back(rule.clone());
            }
        }
    }

    while let Some(next) = queue.pop_front() {
        let dependencies = next.plan.dependencies();
        for dep_name in dependencies.names.iter() {
            if !seen.contains(dep_name) {
                match context.rule(dep_name) {
                    None => {
                        return Err(Error::not_found(format!("Unknown rule {}", dep_name)));
                    }
                    Some(rule) => {
                        seen.insert(dep_name.to_string());
                        queue.push_back(rule.clone());
                    }
                }
            }
        }

        // Ensure all required attributes exist.
        for aid in dependencies.attributes.iter() {
            if !context.has_attribute(aid) {
                return Err(Error::not_found(format!(
                    "Rule depends on unknown attribute {}",
                    aid
                )));
            }
        }

        rules.push(next);
    }

    Ok(rules)
}

/// Takes a query plan and turns it into a differential dataflow.
pub fn implement<T, I, S>(
    name: &str,
    scope: &mut S,
    context: &mut I,
) -> Result<
    (
        HashMap<String, Collection<S, Vec<Value>, isize>>,
        ShutdownHandle,
    ),
    Error,
>
where
    T: Timestamp + Lattice + Default,
    I: ImplContext<T>,
    S: Scope<Timestamp = T>,
{
    scope.iterative::<u64, _, _>(|nested| {
        let publish = vec![name];
        let mut rules = collect_dependencies(&*context, &publish[..])?;

        let mut local_arrangements = VariableMap::new();
        let mut result_map = HashMap::new();

        // Step 0: Canonicalize, check uniqueness of bindings.
        if rules.is_empty() {
            return Err(Error::not_found(format!(
                "Couldn't find any rules for name {}.",
                name
            )));
        }

        rules.sort_by(|x, y| x.name.cmp(&y.name));
        for index in 1..rules.len() - 1 {
            if rules[index].name == rules[index - 1].name {
                return Err(Error::conflict(format!(
                    "Duplicate rule definitions for rule {}",
                    rules[index].name
                )));
            }
        }

        // Step 1: Create new recursive variables for each rule.
        for rule in rules.iter() {
            if context.is_underconstrained(&rule.name) {
                local_arrangements.insert(
                    rule.name.clone(),
                    Variable::new(nested, Product::new(Default::default(), 1)),
                );
            }
        }

        // Step 2: Create public arrangements for published relations.
        for name in publish.into_iter() {
            if let Some(relation) = local_arrangements.get(name) {
                result_map.insert(name.to_string(), relation.leave());
            } else {
                return Err(Error::not_found(format!(
                    "Attempted to publish undefined name {}.",
                    name
                )));
            }
        }

        // Step 3: Define the executions for each rule.
        let mut executions = Vec::with_capacity(rules.len());
        let mut shutdown_handle = ShutdownHandle::empty();
        for rule in rules.iter() {
            info!("planning {:?}", rule.name);
            let (relation, shutdown) = rule.plan.implement(nested, &local_arrangements, context);

            executions.push(relation);
            shutdown_handle.merge_with(shutdown);
        }

        // Step 4: Complete named relations in a specific order (sorted by name).
        for (rule, execution) in rules.iter().zip(executions.drain(..)) {
            match local_arrangements.remove(&rule.name) {
                None => {
                    return Err(Error::not_found(format!(
                        "Rule {} should be in local arrangements, but isn't.",
                        &rule.name
                    )));
                }
                Some(variable) => {
                    let (tuples, shutdown) = execution.tuples(nested, context);
                    shutdown_handle.merge_with(shutdown);

                    #[cfg(feature = "set-semantics")]
                    variable.set(&tuples.distinct());

                    #[cfg(not(feature = "set-semantics"))]
                    variable.set(&tuples.consolidate());
                }
            }
        }

        Ok((result_map, shutdown_handle))
    })
}

/// @TODO
pub fn implement_neu<T, I, S>(
    name: &str,
    scope: &mut S,
    context: &mut I,
) -> Result<
    (
        HashMap<String, Collection<S, Vec<Value>, isize>>,
        ShutdownHandle,
    ),
    Error,
>
where
    T: Timestamp + Lattice + Default,
    I: ImplContext<T>,
    S: Scope<Timestamp = T>,
{
    scope.iterative::<u64, _, _>(move |nested| {
        let publish = vec![name];
        let mut rules = collect_dependencies(&*context, &publish[..])?;

        let mut local_arrangements = VariableMap::new();
        let mut result_map = HashMap::new();

        // Step 0: Canonicalize, check uniqueness of bindings.
        if rules.is_empty() {
            return Err(Error::not_found(format!(
                "Couldn't find any rules for name {}.",
                name
            )));
        }

        rules.sort_by(|x, y| x.name.cmp(&y.name));
        for index in 1..rules.len() - 1 {
            if rules[index].name == rules[index - 1].name {
                return Err(Error::conflict(format!(
                    "Duplicate rule definitions for rule {}",
                    rules[index].name
                )));
            }
        }

        // @TODO at this point we need to know about...
        // @TODO ... which rules require recursion (and thus need wrapping in a Variable)
        // @TODO ... which rules are supposed to be re-used
        // @TODO ... which rules are supposed to be re-synthesized
        //
        // but based entirely on control data written to the server by something external
        // (for the old implement it could just be a decision based on whether the rule has a namespace)

        // Step 1: Create new recursive variables for each rule.
        for name in publish.iter() {
            if context.is_underconstrained(name) {
                local_arrangements.insert(
                    name.to_string(),
                    Variable::new(nested, Product::new(Default::default(), 1)),
                );
            }
        }

        // Step 2: Create public arrangements for published relations.
        for name in publish.into_iter() {
            if let Some(relation) = local_arrangements.get(name) {
                result_map.insert(name.to_string(), relation.leave());
            } else {
                return Err(Error::not_found(format!(
                    "Attempted to publish undefined name {}.",
                    name
                )));
            }
        }

        // Step 3: Define the executions for each rule.
        let mut executions = Vec::with_capacity(rules.len());
        let mut shutdown_handle = ShutdownHandle::empty();
        for rule in rules.iter() {
            info!("neu_planning {:?}", rule.name);

            let plan = q(rule.plan.variables(), rule.plan.into_bindings());

            let (relation, shutdown) = plan.implement(nested, &local_arrangements, context);

            executions.push(relation);
            shutdown_handle.merge_with(shutdown);
        }

        // Step 4: Complete named relations in a specific order (sorted by name).
        for (rule, execution) in rules.iter().zip(executions.drain(..)) {
            match local_arrangements.remove(&rule.name) {
                None => {
                    return Err(Error::not_found(format!(
                        "Rule {} should be in local arrangements, but isn't.",
                        &rule.name
                    )));
                }
                Some(variable) => {
                    let (tuples, shutdown) = execution.tuples(nested, context);
                    shutdown_handle.merge_with(shutdown);

                    #[cfg(feature = "set-semantics")]
                    variable.set(&tuples.distinct());

                    #[cfg(not(feature = "set-semantics"))]
                    variable.set(&tuples.consolidate());
                }
            }
        }

        Ok((result_map, shutdown_handle))
    })
}
