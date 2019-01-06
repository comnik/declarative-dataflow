//! Dataflow operators for actually making the server interact with
//! the outside world.

extern crate timely;

use {Value};

// pub mod handler;
// pub mod output;

// pub use self::handler::{Handler};

/// A mutation of server state.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Abomonation, Serialize, Deserialize, Debug)]
pub struct Command {
    /// The worker that received this command from a client originally
    /// and is therefore the one that should receive all outputs.
    pub owner: usize,
    /// The client token that issued the command. Only relevant to the
    /// owning worker, as no one else has the connection.
    pub client: Option<usize>,
    /// Unparsed representation of the command.
    pub cmd: String,
}

/// (tuple, diff) as sent back to external clients.
pub type Result = (Vec<Value>, isize, u64);
