# Declarative Differential Dataflows

This server exposes a Datalog-inspired, declarative interface on top
of differential dataflow. It allows for the dynamic creation of
dataflows. The query language aims to be a drop-in replacement for
[the one used by the Datomic
database](https://docs.datomic.com/on-prem/query.html).

At the point of this writing, the server accepts only a lower-level
query representation, in the form of a so called query plan. A parser
generating such plans from the actual query language is provided [in
ClojureScript](https://github.com/comnik/kalkuel).

## Examples

The main binary is `bin/server.rs`, which can be invoked by running

    cargo run --bin server
