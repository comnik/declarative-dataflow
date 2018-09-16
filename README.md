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

Server configuration:

    OPTION       | DESCRIPTION                | DEFAULT
    --port       | port to listen at          | 6262
    --enable-cli | accept commands via stdin? | false

Logging at a specific level can be enabled by setting the `RUST_LOG`
environment variable to

    RUST_LOG=server=info

## Documentation

Read the [high-level motivation for this project](https://www.nikolasgoebel.com/2018/09/13/incremental-datalog.html).

Architectural decisions are documented in the [docs/adr/](docs/adr/)
sub-directory.

The synthesizer supports the following operators:

```
Plan := Project Plan Var
        | 'Union Var Plan+
        | 'Join Plan Plan Var
        | 'Not Plan
        | 'Lookup Entity Attribute Var
        | 'Entity Entity Var Var
        | 'HasAttr Var Attribute Var
        | 'Filter Var Attribute Value
        | 'RuleExpr String Var+
```

The following sample plan is intended to be an examples of what a
non-trivial query plan looks like. It also illustrates that query
plans are cumbersome to write manually (i.e. without a parser like
[clj-3df](https://github.com/comnik/clj-3df)), because symbols and
attributes are encoded by numerical ids.

``` json
{"Union": [[0, 1]
           [{"HasAttr": [0, 600, 1]}
            {"Join": [{"HasAttr": [2, 400, 1]}
            {"RuleExpr": ["propagate", [0, 2]]} 2]}]]}
```

Please also refer to the [clj-3df
repository](https://github.com/comnik/clj-3df) for a roadmap of
planned features.
