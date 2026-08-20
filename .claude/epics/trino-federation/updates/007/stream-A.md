---
issue: 007
stream: aggregation
agent: task-007-aggregation
started: 2026-08-20T05:30:00Z
completed: 2026-08-20T05:55:55Z
status: completed
---

# Task 007 — Aggregation pushdown with connector-side combine

## Design

`PushAggregationIntoTableScan` matches only `Step.SINGLE` and the engine does **not**
re-aggregate connector output, so the connector returns final values: `applyAggregation`
produces a handle whose scan is one `FederationAggregateSplit`; its
`AggregateCombiningPageSource` runs the partial-aggregate SQL on every active region
concurrently and combines the partials before emitting.

### Model (reshaped from the task-004 placeholders)
- `FederationAggregateColumn(Optional<FederationColumnHandle> argument, String outputName,
  Type outputType, CombineKind combineKind)` — the `AggregateKind` field was dropped; the
  remote partial queries are **derived from the combine kind** by `AggregateCombiners`, so
  one aggregate can map to several remote partials.
- `CombineKind`: `SUM_LONG`, `SUM_DOUBLE`, `SUM_REAL` (new), `SUM_DECIMAL`, `COUNT_SUM`,
  `MIN`, `MAX`, `AVG_DOUBLE`, `AVG_REAL` (new, replacing the rejected `AVG_DECIMAL`).
- `AggregateCombiners` holds both the partial derivation (`remotePartials`) and the
  combiner factories, so the remote column layout and the combiner input layout cannot
  diverge. The AVG kinds contribute a remote `sum(x)+count(x)` pair read from two adjacent
  channels and divided at emit time (count 0 → NULL).

### applyAggregation (FederationMetadata)
Accepts only: one grouping set (empty = global, important), plain-column arguments (no
DISTINCT / filter / ordering), functions `count(*)`, `count(x)`, `sum(x)` for
bigint/real/double/decimal(p,s) (remote partial decimal(38,s)), `min`/`max` on every
pushable type, `avg(x)` for double/real. Rejects: aggregation/limit/topN already in the
handle, `_region` as an aggregate argument, avg on decimal (exactness) and on integers
(engine divides the exact global sum), any other function, output-type mismatches, and a
synthetic-name collision. Result wiring: one `Assignment` + `Variable` projection per
aggregate named `$agg_<i>` (same order as the input list), identity
`groupingColumnMapping` (empty map), `precalculateStatistics=false`; the new handle keeps
the original columns and appends the synthetic aggregate columns so page-source resolution
serves both.

**Deviation discovered while testing**: the engine plans `sum(tinyint/smallint/integer)` as
`sum(CAST(x AS bigint))` and `avg(integer)` as `avg(CAST(x AS bigint))` — the cast is a
projection between the aggregation and the scan, so the pushdown rule never fires and those
shapes fall back to the engine by construction (covered by fallback tests). `SUM_LONG` in
practice only sees bigint columns; the narrow-int mapping is kept for completeness.

### Execution
- `federation.fanout-threads` (int, default 16, min 1) caps concurrent regional queries.
- `FederationSplitManager`: aggregation → exactly one `FederationAggregateSplit`, **even
  with zero active regions** (a global aggregation still emits its one row: count 0, sums
  NULL — matching Trino's empty-input semantics).
- `AggregateCombiningPageSource`:
  - Per-page-source `newFixedThreadPool(min(regions, fanoutThreads))` with
    `daemonThreadsNamed("trino-federation-aggregate-%s")`, shut down in a `finally`; each
    region task drains its rows into its **own local list** (type-validated against the
    expected remote layout); the page-source thread merges after each future completes —
    no shared mutable combine state, no locks. Open `RegionQueryResults` are tracked so
    `close()` cancels in-flight remote queries. Region errors propagate with the region
    name in the message.
  - Combine hash table: `LinkedHashMap<GroupKey, List<Combiner>>`. Key equality/hashing
    uses **TypeOperators** (`IDENTICAL` + `HASH_CODE` over stack values), matching GROUP BY
    semantics exactly (NaNs group together, -0.0 with 0.0). `MIN`/`MAX` combine through the
    types' `COMPARISON_UNORDERED_LAST`/`_FIRST` operators — the same operators the engine's
    min/max aggregates bind.
  - Grouping on `_region` is supported: it is excluded from the remote GROUP BY and its key
    component is the constant region name per merged stream. When it is the **only**
    grouping column the remote query degenerates to a global aggregate that returns a row
    even for an empty shard, so a `count(*)` **emptiness probe** partial is appended and
    zero-probe rows are dropped (`GROUP BY _region` over an empty table correctly yields no
    groups; `SELECT DISTINCT _region` works with no aggregates at all).
  - Overflow semantics match the engine: `SUM_LONG` via `Math.addExact` →
    `NUMERIC_VALUE_OUT_OF_RANGE` "bigint addition overflow"; `SUM_DECIMAL` accumulates
    exactly in BigInteger and checks the decimal(38) range at emit (transient intermediate
    overflow is fine, like the engine's overflow-counting accumulator) → "Decimal
    overflow". `SUM_REAL` sums in double and narrows at emit, like `RealSumAggregation`.
  - Memory: coarse retained-size tracking of the combine table + page builder into the
    `MemoryContext`; completedBytes/positions from emitted pages.
- Scan path untouched for non-aggregated handles (provider split into
  `createScanPageSource` / `createAggregatePageSource`).

## Tests (TestFederationAggregationPushdown, 20 tests; module total 123 green)
- Matrix: 23 supported aggregates × {global, GROUP BY category, GROUP BY _region,
  GROUP BY category, day} — `isFullyPushedDown()` (plan has no AggregationNode **and**
  results equal a pushdown-disabled control run of the same query).
- Evidence: `count(*)` sends each region exactly
  `SELECT count(*) AS "$agg_0" FROM "memory"."default"."sales"` (1 row/region by
  construction); grouped sums show `GROUP BY` in regional SQL; WHERE + GROUP BY compose;
  `_region='east'` pruning + global agg → west receives zero queries; pruned-to-nothing
  still emits the count-0 row.
- Semantics: exact decimal sums incl. negatives; near-limit bigint combine without
  overflow; bigint and decimal combine overflow raise the engine's messages; avg over
  all-NULL input → NULL; min/max on varchar/date/timestamp; empty/one-sided tables incl.
  `GROUP BY _region` probe behavior.
- Fallbacks (isNotFullyPushedDown + control-run comparison): count(DISTINCT), FILTER,
  avg(decimal), avg(integer), sum(narrow ints), arbitrary(), aggregation over a pushed
  LIMIT subquery (regional SQL shows LIMIT and no aggregate).
- Handle codec test updated for the reshaped `FederationAggregateColumn` (incl. an
  AVG_DOUBLE column); config test covers `federation.fanout-threads`.
- Test-harness fix: regional query-log diffing is keyed on query ids because
  `system.runtime.queries` evicts old entries under load.

## Build
`./mvnw -pl plugin/trino-federation install` (all airbase checks + 123 tests) green in
Docker `trino-builder:jdk25`; module airstyle-formatted.
