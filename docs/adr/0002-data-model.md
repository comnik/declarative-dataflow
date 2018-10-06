# [0002] Data Model

Date: 2018-10-03
Status: ACCEPTED

## Context

The current data model assumes all inputs to arrive via a single
collection of `(e a v)` tuples. Although this is very flexible, we are
facing three major problems:

(1) Reading data from sources other than Datomic (e.g. Kafka or files
on disk) is awkward, as those sources usually keep data in separate
relations (e.g. Kafka topics). Reading across sources also invalidates
the assumption of a single time axis on which all inputs reside.

(2) Inflexible arrangements. The hard assumption on a single input
collection is reflected in similarly hardcoded indexing structures
(the `DB` and `ImplContext` structs). External sources would therefore
not benefit from any shared arrangements currently. Ultimately, we
want all decisions on what computations to store in arrangements to be
made by some kind of optimiser. The current indexing scheme is merely
a special case of that problem and should not require special
constructs.

(3) Layout inefficiencies. In principle we could do away with tuples
entirely and simply rely on `Vec<Value>` everywhere. As most queries
usually interact with many attributes directly, this would mean
leaving a lot of optimisation potential on the table.

## Decision

We will move to a column-oriented data model, by maintaining a
separate collection for each attribute.

We will separate specialized operator implementations from the process
of chosing the right operators in every situation. E.g. we will not
only provide a general `Join`, but also something like
`JoinColumns`. We will also encode information on existing
arrangements to use in the plan type, e.g. `MatchE -> MatchArrangedE`.

## Consequences

Keeping per-attribute collections allows for more efficient and less
redundant representation of Datoms in the system. It will make it
harder to implement queries that contain logic variables in attribute
position. For now those will not be possible. We will potentially add
separate arrangements to support those.

Separating operator implementations from optimisation policy should
greatly simplify the optimiser implementation to be a transformation
`f :: [Rule] -> [Rule]`.

For example, the current policy of indexing all base relations would
correspond to a plan transformation replacing all leave expressions of
type `Match*` with the appropriate `MatchArranged*` along with
additional rules registering all the neccessary arrangements. This
would even improve on the current implementation, in that only the
required indices would be created.

A smarter policy could also start re-ordering `Joins` and pick things
like delta queries or column-on-column joins.

Working with external sources has an effect on the probing policy.
