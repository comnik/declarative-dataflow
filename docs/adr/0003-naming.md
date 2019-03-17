# [0003] Naming

Date: 2019-03-16
Status: ACCEPTED

## Context

Taken together, Timely, Differential, and Declarative throw around
quite a few names. This is compounded by the proliferation of names
caused by structs. It is therefore important to pick good, consistent
names for things wherever possible.

In particular, we have already had naming chaos with the following:

(1) `symbol` vs `variable`
(2) `binding` vs `constraint`
(3) `context` vs `server` vs `domain`

## Decision

(1) We will stick to `variable`, as it is more specific than symbol,
and more descriptive of our use cases. It also has the right
conntations w.r.t solving for variable bindings given certain
constraints.

(2) We will stick to `binding`. Binding is more generally applicable,
because some bindings (such as attributes) extend the space of
possible values for a given variable, rather than exclusively
constrain it.

(3) All of those names are needed, but we need to tease them apart
better. The intent is that `server` should be the name for the
structure holding the entire application state of a Declarative
worker. `context` is a subset of that state, which is all state
required for synthesis of rules. Finally, a `context` models `domain`
which handles all the time semantics.

## Consequences

-
