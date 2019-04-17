//! Types and operators to work with external data sources.

use std::cell::RefCell;
use std::rc::{Rc, Weak};
use std::time::{Duration, Instant};

use timely::dataflow::operators::capture::event::link::EventLink;
use timely::dataflow::{Scope, Stream};
use timely::logging::TimelyEvent;
use timely::order::TotalOrder;
use timely::progress::Timestamp;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::logging::DifferentialEvent;

use crate::server::scheduler::Scheduler;
use crate::AttributeConfig;
use crate::{Aid, Value};

#[cfg(feature = "csv-source")]
pub mod csv_file;
// pub mod declarative_logging;
pub mod differential_logging;
// pub mod json_file;
pub mod timely_logging;

#[cfg(feature = "csv-source")]
pub use self::csv_file::CsvFile;
// pub use self::json_file::JsonFile;

/// A struct encapsulating any state required to create sources.
pub struct SourcingContext {
    /// The logical start of the computation, used by sources to
    /// compute their relative progress.
    pub t0: Instant,
    /// A weak handle to a scheduler, used by sources to defer their
    /// next activation when polling.
    pub scheduler: Weak<RefCell<Scheduler>>,
    /// A weak handle to a Timely event link.
    pub timely_events: Rc<EventLink<Duration, (Duration, usize, TimelyEvent)>>,
    /// A weak handle to Differential event link.
    pub differential_events: Rc<EventLink<Duration, (Duration, usize, DifferentialEvent)>>,
}

/// An external data source that can provide Datoms.
pub trait Sourceable<S>
where
    S: Scope,
    S::Timestamp: Timestamp + Lattice + TotalOrder,
{
    /// Conjures from thin air (or from wherever the source lives) one
    /// or more timely streams feeding directly into attributes.
    fn source(
        &self,
        scope: &mut S,
        context: SourcingContext,
    ) -> Vec<(
        Aid,
        AttributeConfig,
        Stream<S, ((Value, Value), S::Timestamp, isize)>,
    )>;
}

/// Supported external data sources.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub enum Source {
    /// Timely logging streams
    TimelyLogging(timely_logging::TimelyLogging),
    /// Differential logging streams
    DifferentialLogging(differential_logging::DifferentialLogging),
    // /// Declarative logging streams
    // DeclarativeLogging(declarative_logging::DeclarativeLogging),
    /// CSV files
    #[cfg(feature = "csv-source")]
    CsvFile(CsvFile),
    // /// Files containing json objects
    // JsonFile(JsonFile),
}

#[cfg(feature = "real-time")]
impl<S: Scope<Timestamp = Duration>> Sourceable<S> for Source {
    fn source(
        &self,
        scope: &mut S,
        context: SourcingContext,
    ) -> Vec<(
        Aid,
        AttributeConfig,
        Stream<S, ((Value, Value), Duration, isize)>,
    )> {
        match *self {
            Source::TimelyLogging(ref source) => source.source(scope, context),
            Source::DifferentialLogging(ref source) => source.source(scope, context),
            // Source::DeclarativeLogging(ref source) => source.source(scope, context),
            #[cfg(feature = "csv-source")]
            Source::CsvFile(ref source) => source.source(scope, context),
            _ => unimplemented!(),
        }
    }
}

#[cfg(not(feature = "real-time"))]
impl<S: Scope<Timestamp = u64>> Sourceable<S> for Source {
    fn source(
        &self,
        _scope: &mut S,
        context: SourcingContext,
    ) -> Vec<(
        Aid,
        AttributeConfig,
        Stream<S, ((Value, Value), u64, isize)>,
    )> {
        match *self {
            // Source::TimelyLogging(ref source) => source.source(scope, context),
            // Source::DifferentialLogging(ref source) => source.source(scope, context),
            // #[cfg(feature = "csv-source")]
            // Source::CsvFile(ref source) => source.source(scope, context),
            // Source::JsonFile(ref source) => source.source(scope, context),
            _ => unimplemented!(),
        }
    }
}
