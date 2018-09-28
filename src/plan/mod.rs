//! Types and traits for implementing query plans.

use timely::dataflow::scopes::Child;
use timely::progress::timestamp::Timestamp;
use timely::communication::Allocate;
use timely::worker::Worker;

use differential_dataflow::lattice::Lattice;

use super::{ImplContext, RelationMap, QueryMap, SimpleRelation};

pub mod predicate;
pub mod aggregate;
pub mod project;
pub mod union;
pub mod join;
pub mod antijoin;

pub use self::predicate::PredExpr;
pub use self::aggregate::Aggregate;
pub use self::project::Projection;
pub use self::union::Union;
pub use self::join::Join;
pub use self::antijoin::Antijoin;

/// A type that can be implemented as a simple relation.
pub trait Implementable<'a, 'b, A: Allocate, T: Timestamp+Lattice> {
    /// Implements the type as a simple relation.
    fn implement(
        &self,
        db: &ImplContext<Child<'a, Worker<A>, T>>,
        nested: &mut Child<'b, Child<'a, Worker<A>, T>, u64>,
        relation_map: &RelationMap<'b, Child<'a, Worker<A>, T>>,
        queries: &mut QueryMap<T, isize>
    )
    -> SimpleRelation<'b, Child<'a, Worker<A>, T>>;
}
