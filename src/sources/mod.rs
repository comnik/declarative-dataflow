//! Types and operators to work with external data sources.

extern crate differential_dataflow;
extern crate timely;

use timely::dataflow::{Scope, Stream};

use Value;

pub mod plain_file;
pub use self::plain_file::PlainFile;
pub mod json_file;
pub use self::json_file::JsonFile;

/// An external data source that can provide Datoms.
pub trait Sourceable {
    /// Creates a timely operator reading from the source and
    /// producing inputs.
    fn source<G: Scope<Timestamp = u64>>(&self, scope: &G) -> Stream<G, (Vec<Value>, u64, isize)>;
}

/// Supported external data sources.
#[derive(Deserialize, Clone, Debug)]
pub enum Source {
    /// Plain files
    PlainFile(PlainFile),
    /// Files containing json objects
    JsonFile(JsonFile),
}

impl Sourceable for Source {
    fn source<G: Scope<Timestamp = u64>>(&self, scope: &G) -> Stream<G, (Vec<Value>, u64, isize)> {
        match self {
            &Source::PlainFile(ref source) => source.source(scope),
            &Source::JsonFile(ref source) => source.source(scope),
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
//         nested: &mut Child<'b, Child<'a, Worker<A>, u64>, u64>,
//         local_arrangements: &RelationMap<'b, Child<'a, Worker<A>, u64>>,
//         global_arrangements: &mut QueryMap<isize>
//     ) -> SimpleRelation<'b, Child<'a, Worker<A>, u64>> {
//         SimpleRelation {
//             symbols: vec![], // @TODO
//             tuples: self.source(&nested.parent).as_collection(),
//         }
//     }
// }
