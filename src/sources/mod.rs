//! Types and operators to work with external data sources.

extern crate timely;
extern crate differential_dataflow;

use timely::dataflow::{Scope, Stream};
use timely::progress::timestamp::{RootTimestamp};
use timely::progress::nested::product::{Product};

use {Value};

pub mod plain_file;
pub use self::plain_file::{PlainFile};

/// An external data source that can provide Datoms.
pub trait Sourceable {
    /// Creates a timely operator reading from the source and
    /// producing inputs.
    fn source<G: Scope>(&self, scope: &G) -> Stream<G, (Vec<Value>, Product<RootTimestamp, usize>, isize)>;
}

/// Supported external data sources.
#[derive(Deserialize, Clone, Debug)]
pub enum Source {
    /// Plain files
    PlainFile(PlainFile),
}

impl Sourceable for Source {
    fn source<G: Scope>(&self, scope: &G) -> Stream<G, (Vec<Value>, Product<RootTimestamp, usize>, isize)> {
        match self {
            &Source::PlainFile(ref source) => source.source(scope),
        }
    }
}

// @TODO can't quite do this yet, because Implementable works with any
// timestamp, while Sourceable must fix a specific one. For static
// sources it would be possible to utilize that Timestamp satisfies
// Default.
//
// impl Implementable for Source {
//     fn implement<'a, 'b, A: Allocate>(
//         &self,
//         nested: &mut Child<'b, Child<'a, Worker<A>, T>, u64>,
//         local_arrangements: &RelationMap<'b, Child<'a, Worker<A>, T>>,
//         global_arrangements: &mut QueryMap<T, isize>
//     ) -> SimpleRelation<'b, Child<'a, Worker<A>, T>> {
//         SimpleRelation {
//             symbols: vec![], // @TODO
//             tuples: self.source(&nested.parent).as_collection(),
//         }
//     }
// }
