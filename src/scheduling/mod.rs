//! Various schedulers to help with flow control and efficient polling
//! of source systems.

use timely::dataflow::ProbeHandle;
use timely::progress::Timestamp;

pub mod frontier_scheduler;
pub use frontier_scheduler::FrontierScheduler;

pub mod realtime_scheduler;
pub use realtime_scheduler::Event as SchedulingEvent;
pub use realtime_scheduler::RealtimeScheduler;

/// Common scheduler behaviour.
pub trait AsScheduler {
    /// Returns true whenever an activator is queued and ready to be
    /// scheduled.
    fn has_pending(&self) -> bool;
}

/// A unified scheduler abstracting access to timer-triggered, and
/// frontier-triggered scheduling strategies.
pub struct Scheduler<T: Timestamp> {
    /// A frontier-triggered scheduler.
    pub frontier: FrontierScheduler<T>,
    /// A timer-triggered scheduler.
    pub realtime: RealtimeScheduler,
}

impl<T: Timestamp> From<ProbeHandle<T>> for Scheduler<T> {
    fn from(probe: ProbeHandle<T>) -> Self {
        Self {
            frontier: FrontierScheduler::from(probe),
            realtime: RealtimeScheduler::new(),
        }
    }
}

impl<T: Timestamp> AsScheduler for Scheduler<T> {
    fn has_pending(&self) -> bool {
        self.frontier.has_pending() || self.realtime.has_pending()
    }
}
