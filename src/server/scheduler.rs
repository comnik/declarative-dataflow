//! Timer-based management of operator activators.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::rc::Weak;
use std::time::{Duration, Instant};

use timely::scheduling::Activator;

/// A scheduler allows polling sources to defer triggering their
/// activators, in case they do not have work available. This reduces
/// time spent polling infrequently updated sources and allows us to
/// (optionally) block for input from within the event loop without
/// unnecessarily delaying sources that have run out of fuel during
/// the current step.
#[derive(Default)]
pub struct Scheduler {
    activator_queue: BinaryHeap<TimedActivator>,
}

impl Scheduler {
    /// Creates a new, empty scheduler.
    pub fn new() -> Self {
        Scheduler {
            activator_queue: BinaryHeap::new(),
        }
    }

    /// Returns true whenever an activator is queued and ready to be
    /// scheduled.
    pub fn has_pending(&self) -> bool {
        if let Some(ref timed_activator) = self.activator_queue.peek() {
            timed_activator.is_ready()
        } else {
            false
        }
    }

    /// Returns the duration until the next activation in
    /// `activator_queue` from `now()`. If no activations are present
    /// returns None.
    pub fn until_next(&self) -> Option<Duration> {
        if let Some(ref timed_activator) = self.activator_queue.peek() {
            Some(timed_activator.until_ready())
        } else {
            None
        }
    }

    /// Schedule activation at the specified instant. No hard
    /// guarantees on when the activator will actually be triggered.
    pub fn schedule_at(&mut self, at: Instant, activator: Weak<Activator>) {
        self.activator_queue.push(TimedActivator {
            at,
            activator,
            event: None,
        });
    }

    /// Schedule activation now. No hard guarantees on when the
    /// activator will actually be triggered.
    pub fn schedule_now(&mut self, activator: Weak<Activator>) {
        self.schedule_at(Instant::now(), activator);
    }

    /// Schedule activation after the specified duration. No hard
    /// guarantees on when the activator will actually be triggered.
    pub fn schedule_after(&mut self, after: Duration, activator: Weak<Activator>) {
        self.schedule_at(Instant::now() + after, activator);
    }

    /// Schedule an event at the specified instant. No hard guarantees
    /// on when the activator will actually be triggered.
    pub fn event_at(&mut self, at: Instant, event: Event) {
        self.activator_queue.push(TimedActivator {
            at,
            activator: Weak::new(),
            event: Some(event),
        });
    }

    /// Schedule an event after the specified duration. No hard
    /// guarantees on when the activator will actually be triggered.
    pub fn event_after(&mut self, after: Duration, event: Event) {
        self.event_at(Instant::now() + after, event);
    }
}

impl Iterator for Scheduler {
    type Item = TimedActivator;
    fn next(&mut self) -> Option<TimedActivator> {
        if self.has_pending() {
            Some(self.activator_queue.pop().unwrap())
        } else {
            None
        }
    }
}

/// A set of things that we might want to schedule.
#[derive(PartialEq, Eq, Debug)]
pub enum Event {
    /// A domain tick.
    Tick,
}

/// A thing that can be scheduled at an instant. Scheduling this
/// activator might result in an `Event`.
pub struct TimedActivator {
    at: Instant,
    activator: Weak<Activator>,
    event: Option<Event>,
}

impl TimedActivator {
    fn is_ready(&self) -> bool {
        Instant::now() >= self.at
    }

    fn until_ready(&self) -> Duration {
        let now = Instant::now();
        if self.at > now {
            self.at.duration_since(now)
        } else {
            Duration::from_millis(0)
        }
    }

    /// Trigger the activation, potentially resulting in an event.
    pub fn schedule(self) -> Option<Event> {
        if let Some(activator) = self.activator.upgrade() {
            activator.activate();
        }

        self.event
    }
}

// We want the activator_queue to act like a min-heap.
impl Ord for TimedActivator {
    fn cmp(&self, other: &TimedActivator) -> Ordering {
        other.at.cmp(&self.at)
    }
}

impl PartialOrd for TimedActivator {
    fn partial_cmp(&self, other: &TimedActivator) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for TimedActivator {
    fn eq(&self, other: &TimedActivator) -> bool {
        self.at.eq(&other.at)
    }
}

impl Eq for TimedActivator {}
