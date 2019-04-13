//! Input sessions for unordered collection updates.
//!
//! Although users can directly manipulate timely dataflow streams as collection inputs,
//! the `UnorderedSession` type can make this more efficient and less error-prone. Specifically,
//! the type batches up updates with their logical times and ships them with coarsened
//! timely dataflow capabilities, exposing more concurrency to the operator implementations
//! than are evident from the logical times, which appear to execute in sequence.

use timely::dataflow::operators::unordered_input::{ActivateCapability, UnorderedHandle};
use timely::progress::Timestamp;

use differential_dataflow::difference::Monoid;
use differential_dataflow::Data;

/// An input session wrapping a single timely dataflow capability.
///
/// Each timely dataflow message has a corresponding capability, which is a logical time in the
/// timely dataflow system. Differential dataflow updates can happen at a much higher rate than
/// timely dataflow's progress tracking infrastructure supports, because the logical times are
/// promoted to data and updates are batched together. The `UnorderedSession` type does this batching.
///
/// # Examples
///
/// ```
/// extern crate timely;
/// extern crate differential_dataflow;
///
/// use timely::Configuration;
/// use differential_dataflow::input::Input;
///
/// fn main() {
///     ::timely::execute(Configuration::Thread, |worker| {
///
///			let (mut handle, probe) = worker.dataflow(|scope| {
///				// create input handle and collection.
///				let (handle, data) = scope.new_collection_from(0 .. 10);
///         	let probe = data.map(|x| x * 2)
///				            	.inspect(|x| println!("{:?}", x))
///				            	.probe();
///				(handle, probe)
///     	});
///
///			handle.insert(3);
///			handle.advance_to(1);
///			handle.insert(5);
///			handle.advance_to(2);
///			handle.flush();
///
///			while probe.less_than(handle.time()) {
///				worker.step();
///			}
///
///			handle.remove(5);
///			handle.advance_to(3);
///			handle.flush();
///
///			while probe.less_than(handle.time()) {
///				worker.step();
///			}
///
///		}).unwrap();
/// }
/// ```
pub struct UnorderedSession<T: Timestamp + Clone, D: Data, R: Monoid> {
    time: T,
    cap: ActivateCapability<T>,
    buffer: Vec<(D, T, R)>,
    handle: UnorderedHandle<T, (D, T, R)>,
}

impl<T: Timestamp + Clone, D: Data, R: Monoid> UnorderedSession<T, D, R> {
    /// Creates a new session from a reference to an input handle.
    pub fn from(handle: UnorderedHandle<T, (D, T, R)>, cap: ActivateCapability<T>) -> Self {
        UnorderedSession {
            time: cap.time().clone(),
            cap,
            buffer: Vec::new(),
            handle,
        }
    }

    /// Adds to the weight of an element in the collection.
    pub fn update(&mut self, element: D, change: R) {
        if self.buffer.len() == self.buffer.capacity() {
            if self.buffer.len() > 0 {
                self.handle
                    .session(self.cap.clone())
                    .give_iterator(self.buffer.drain(..));
            }
            // TODO : This is a fairly arbitrary choice; should probably use `Context::default_size()` or such.
            self.buffer.reserve(1024);
        }
        self.buffer.push((element, self.cap.time().clone(), change));
    }

    /// Adds to the weight of an element in the collection at a future time.
    pub fn update_at(&mut self, element: D, time: T, change: R) {
        assert!(self.cap.time().less_equal(&time));
        if self.buffer.len() == self.buffer.capacity() {
            if self.buffer.len() > 0 {
                self.handle
                    .session(self.cap.clone())
                    .give_iterator(self.buffer.drain(..));
            }
            // TODO : This is a fairly arbitrary choice; should probably use `Context::default_size()` or such.
            self.buffer.reserve(1024);
        }
        self.buffer.push((element, time, change));
    }

    /// Forces buffered data into the timely dataflow input, and
    /// advances its time to match that of the session.
    ///
    /// It is important to call `flush` before expecting timely
    /// dataflow to report progress. Until this method is called, all
    /// updates may still be in internal buffers and not exposed to
    /// timely dataflow. Once the method is called, all buffers are
    /// flushed and timely dataflow is advised that some logical times
    /// are no longer possible.
    pub fn flush(&mut self) {
        // @TODO get rid of the double buffering maybe?
        self.handle
            .session(self.cap.clone())
            .give_iterator(self.buffer.drain(..));

        if self.cap.time().less_than(&self.time) {
            self.cap = self.cap.delayed(&self.time);
        }
    }

    /// Advances the logical time for future records.
    ///
    /// Importantly, this method does **not** immediately inform
    /// timely dataflow of the change. This happens only when the
    /// session is dropped or flushed. It is not correct to use this
    /// time as a basis for a computation's `step_while` method unless
    /// the session has just been flushed.
    #[inline]
    pub fn advance_to(&mut self, time: T) {
        assert!(self.epoch().less_equal(&time));
        assert!(&self.time.less_equal(&time));

        self.time = time;
    }

    /// Reveals the current time of the session.
    pub fn epoch(&self) -> &T {
        &self.time
    }
    /// Reveals the current time of the session.
    pub fn time(&self) -> &T {
        &self.time
    }

    /// Closes the input, flushing and sealing the wrapped timely input.
    pub fn close(self) {}
}

impl<T: Timestamp + Clone, D: Data, R: Monoid> Drop for UnorderedSession<T, D, R> {
    fn drop(&mut self) {
        self.flush();
    }
}
