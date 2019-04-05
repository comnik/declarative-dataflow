//! Timer-based management of operator activators.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::time::{Duration, Instant};

use timely::scheduling::activate::Activator;

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
            Instant::now() >= timed_activator.at
        } else {
            false
        }
    }

    /// Schedule activation at the specified instant. No hard
    /// guarantees on when the activator will actually be triggered.
    pub fn schedule_at(&mut self, at: Instant, activator: Activator) {
        self.activator_queue.push(TimedActivator { at, activator });
    }

    /// Schedule activation after the specified duration. No hard
    /// guarantees on when the activator will actually be triggered.
    pub fn schedule_after(&mut self, after: Duration, activator: Activator) {
        self.activator_queue.push(TimedActivator {
            at: Instant::now() + after,
            activator,
        });
    }
}

impl Iterator for Scheduler {
    type Item = Activator;
    fn next(&mut self) -> Option<Activator> {
        if self.has_pending() {
            Some(self.activator_queue.pop().unwrap().activator)
        } else {
            None
        }
    }
}

struct TimedActivator {
    pub at: Instant,
    pub activator: Activator,
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
