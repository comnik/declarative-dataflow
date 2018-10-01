# Declarative Differential Dataflows

**This is currently a research project, not feature-complete in any
way, and overall not ready for use in production services.**

This project provides dynamic synthetization of [differential
dataflows](https://github.com/frankmcsherry/differential-dataflow) at
runtime. Included in this repository is the library itself, as well as
a server accepting query plans serialized as JSON, via CLI, TCP, or
WebSocket connections. A parser generating such plans from a
Datalog-like query language is provided [in
Clojure(Script)](https://github.com/comnik/clj-3df), but other query
grammars or parsers from other host languages should be able to
utilize the same query plan interface.

## Build / Run

Assuming an up-to-date Rust environment, the server can be built and
run via

    cargo run --release --bin server -- <timely args> -- <server args>

The server executable accepts two sets of arguments separated by `--`,
one for [configuring timely dataflow](https://github.com/frankmcsherry/timely-dataflow)
and the other for configuring the server.

## Configuration

    OPTION           | DESCRIPTION                | DEFAULT
    --port           | port to listen at          | 6262
    --enable-cli     | accept commands via stdin? | false
    --enable-history | keep full traces           | false

Logging at a specific level can be enabled by setting the `RUST_LOG`
environment variable to `RUST_LOG=server=info`.

## Documentation

Read the [high-level motivation for this
project](https://www.nikolasgoebel.com/2018/09/13/incremental-datalog.html).

Important architectural decisions are documented in the
[docs/adr/](docs/adr/) sub-directory.

Documentation for this package can be built via `cargo doc --no-deps`
and viewed in a browser via `cargo doc --no-deps --open`. Please refer
to `declarative_dataflow::plan::Plan` for documentation on the
available operators. The [tests/](tests/) directory contains usage
examples.

## Front ends

Query plans are rather cumbersome to write manually and do not map to
any interesting, higher-level semantics. Currently we provide a
[Datalog front end](https://github.com/comnik/clj-3df) written in
Clojure.
