# [0001] Server Architecture

Date: 2018-09-15
Status: ACCEPTED

## Decision

A timely dataflow system is made up of any number of worker peers,
running the exact same dataflows. Timely workers expect to be run at
full speed without blocking. The initial design will therefore extend
the worker loop with non-blocking networking accepting both pure TCP
and WebSocket connections.

Because each worker may accept client commands, workers will utilize
[timely's sequencer
primitive](https://github.com/frankmcsherry/blog/blob/master/posts/2018-08-19.md#event-serialization)
to ensure that every worker processes commands in the same order.

During its busy loop, each worker will...

- ...accept new client connections
- ...accept commands on a connection and push them to the sequencer
- ...step registered dataflows
- ...forward results to clients

By keeping everything within a single loop, we can easily experiment
with trade-offs, such as limiting the number of commands consumed, in
order to ensure timely progress on registered queries, or vice versa.

We want each client to have access to the power of the whole cluster,
while talking only to a single peer. This is achieved by the notion of
an *owner*, i.e. the worker which manages the client connection and
originally accepted a given request. All workers forward results back
to the owner.

Apart from connection state, workers will thus also maintain a mapping
from query names to a list of clients interested in them. Each worker
will only maintain this mapping for clients connected to it. Thus,
this mapping will vary from worker to worker.

This design also makes it easy to move parts of the loop into their
own threads, should that become neccessary.

When running on top of an external source-of-truth a separate,
dedicated event loop might be run to consume that connection at full
speed.

## Consequences

All existing websocket libraries seem to run their own, encapsulated
event loop, thus it was neccessary [to fork
`ws-rs](https://github.com/comnik/ws-rs)`.

Care must be taken to always step workers, even if no queries are
registered, otherwise the sequencer dataflow will not make progress.
