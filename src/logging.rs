//! Loggers and logging events for declarative dataflow.

/// Logger for differential dataflow events.
pub type Logger = ::timely::logging::Logger<DeclarativeEvent>;

/// Possible different declarative events.
#[derive(Debug, Clone, Serialize, Ord, PartialOrd, Eq, PartialEq)]
pub enum DeclarativeEvent {
    /// Tuples materialized during a join.
    JoinTuples(JoinTuplesEvent),
}

/// Tuples materialized during a join.
#[derive(Debug, Clone, Serialize, Ord, PartialOrd, Eq, PartialEq)]
pub struct JoinTuplesEvent {
    /// How many tuples.
    pub cardinality: i64,
}

impl From<JoinTuplesEvent> for DeclarativeEvent {
    fn from(e: JoinTuplesEvent) -> Self {
        DeclarativeEvent::JoinTuples(e)
    }
}
