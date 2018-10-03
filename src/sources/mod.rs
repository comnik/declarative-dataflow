//! Types and operators to work with external data sources.

extern crate timely;

use timely::dataflow::{Scope, Stream};

use {Value};

pub mod plain_file;

pub use self::plain_file::{PlainFile};

/// An external data source that can provide Datoms.
pub trait Sourceable {
    /// Creates a timely operator reading from the source and
    /// producing inputs.
    fn source<G: Scope>(&self, scope: &G) -> Stream<G, (Vec<Value>, usize, isize)>;
}

/// Supported external data sources.
#[derive(Deserialize, Clone, Debug)]
pub enum Source {
    /// Plain files
    PlainFile(PlainFile),
}

impl Sourceable for Source {
    fn source<G: Scope>(&self, scope: &G) -> Stream<G, (Vec<Value>, usize, isize)> {
        match self {
            &Source::PlainFile(ref source) => source.source(scope),
        }
    }
}
