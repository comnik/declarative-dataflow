//! Frontier-triggered scheduling of operator activators.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::rc::Weak;

use timely::dataflow::ProbeHandle;
use timely::progress::{frontier::AntichainRef, Timestamp};
use timely::scheduling::Activator;

use crate::scheduling::AsScheduler;

/// A scheduler allows polling sources to defer triggering their
/// activators, in case they do not have work available. This reduces
/// time spent polling infrequently updated sources and allows us to
/// (optionally) block for input from within the event loop without
/// unnecessarily delaying sources that have run out of fuel during
/// the current step.
pub struct FrontierScheduler<T: Timestamp> {
    probe: ProbeHandle<T>,
    activator_queue: BinaryHeap<FrontieredActivator<T>>,
}

impl<T: Timestamp> From<ProbeHandle<T>> for FrontierScheduler<T> {
    /// Creates a new, empty scheduler from the given probe handle.
    fn from(probe: ProbeHandle<T>) -> Self {
        FrontierScheduler {
            probe,
            activator_queue: BinaryHeap::new(),
        }
    }
}

impl<T: Timestamp> AsScheduler for FrontierScheduler<T> {
    fn has_pending(&self) -> bool {
        if let Some(ref timed_activator) = self.activator_queue.peek() {
            self.probe
                .with_frontier(|frontier| timed_activator.is_ready(frontier))
        } else {
            false
        }
    }
}

impl<T: Timestamp> FrontierScheduler<T> {
    /// Schedule activation once the computational frontier has
    /// progressed past a point in time. No hard guarantees on when
    /// the activator will actually be triggered.
    pub fn schedule_at(&mut self, at: T, activator: Weak<Activator>) {
        self.activator_queue
            .push(FrontieredActivator { at, activator });
    }
}

impl<T: Timestamp> Iterator for FrontierScheduler<T> {
    type Item = FrontieredActivator<T>;
    fn next(&mut self) -> Option<FrontieredActivator<T>> {
        if self.has_pending() {
            Some(self.activator_queue.pop().unwrap())
        } else {
            None
        }
    }
}

/// A thing that can be scheduled at an instant.
pub struct FrontieredActivator<T> {
    at: T,
    activator: Weak<Activator>,
}

impl<T: Timestamp> FrontieredActivator<T> {
    fn is_ready(&self, frontier: AntichainRef<T>) -> bool {
        !frontier.less_than(&self.at)
    }

    /// Trigger the activation.
    pub fn schedule(self) {
        if let Some(activator) = self.activator.upgrade() {
            activator.activate();
        }
    }
}

// We want the activator_queue to act like a min-heap.
impl<T: Timestamp> Ord for FrontieredActivator<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        other.at.cmp(&self.at)
    }
}

impl<T: Timestamp> PartialOrd for FrontieredActivator<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: Timestamp> PartialEq for FrontieredActivator<T> {
    fn eq(&self, other: &Self) -> bool {
        self.at.eq(&other.at)
    }
}

impl<T: Timestamp> Eq for FrontieredActivator<T> {}
