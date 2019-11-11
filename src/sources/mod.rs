//! Types and operators to work with external data sources.

use std::cell::RefCell;
use std::rc::{Rc, Weak};
use std::time::{Duration, Instant};

use timely::dataflow::operators::capture::event::link::EventLink;
use timely::dataflow::{ProbeHandle, Scope, Stream};
use timely::logging::TimelyEvent;
use timely::progress::Timestamp;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::logging::DifferentialEvent;

use crate::scheduling::Scheduler;
use crate::AttributeConfig;
use crate::{AsAid, Value};

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
pub struct SourcingContext<T: Timestamp> {
    /// The logical start of the computation, used by sources to
    /// compute their relative progress.
    pub t0: Instant,
    /// A handle to the timely probe of the domain this source is created in.
    pub domain_probe: ProbeHandle<T>,
    /// A weak handle to a scheduler, used by sources to defer their
    /// next activation when polling.
    pub scheduler: Weak<RefCell<Scheduler<T>>>,
    /// A weak handle to a Timely event link.
    pub timely_events: Rc<EventLink<Duration, (Duration, usize, TimelyEvent)>>,
    /// A weak handle to Differential event link.
    pub differential_events: Rc<EventLink<Duration, (Duration, usize, DifferentialEvent)>>,
}

/// An external data source that can provide Datoms.
pub trait Sourceable<A, S>
where
    A: AsAid,
    S: Scope,
    S::Timestamp: Timestamp + Lattice,
{
    /// Conjures from thin air (or from wherever the source lives) one
    /// or more timely streams feeding directly into attributes.
    fn source(
        &self,
        scope: &mut S,
        context: SourcingContext<S::Timestamp>,
    ) -> Vec<(
        A,
        AttributeConfig,
        Stream<S, ((Value, Value), S::Timestamp, isize)>,
    )>;
}

/// Supported external data sources.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub enum Source<A: AsAid + From<&'static str>> {
    /// Timely logging streams
    TimelyLogging(timely_logging::TimelyLogging<A>),
    /// Differential logging streams
    DifferentialLogging(differential_logging::DifferentialLogging<A>),
    // /// Declarative logging streams
    // DeclarativeLogging(declarative_logging::DeclarativeLogging),
    /// CSV files
    #[cfg(feature = "csv-source")]
    CsvFile(CsvFile<A>),
    // /// Files containing json objects
    // JsonFile(JsonFile<A>),
}

#[cfg(feature = "real-time")]
impl<A, S> Sourceable<A, S> for Source<A>
where
    A: AsAid + From<&'static str>,
    S: Scope<Timestamp = Duration>,
{
    fn source(
        &self,
        scope: &mut S,
        context: SourcingContext<S::Timestamp>,
    ) -> Vec<(
        A,
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
impl<A, S> Sourceable<A, S> for Source<A>
where
    A: AsAid + From<&'static str>,
    S: Scope,
    S::Timestamp: Timestamp + Lattice,
{
    fn source(
        &self,
        _scope: &mut S,
        _context: SourcingContext<S::Timestamp>,
    ) -> Vec<(
        A,
        AttributeConfig,
        Stream<S, ((Value, Value), S::Timestamp, isize)>,
    )> {
        unimplemented!();
    }
}
