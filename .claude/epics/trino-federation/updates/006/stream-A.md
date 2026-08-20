---
issue: 006
stream: pushdowns
agent: task-006-pushdowns
started: 2026-08-20T04:55:00Z
completed: 2026-08-20T05:15:59Z
status: completed
---

# Task 006 — Scan pushdowns

## What was implemented

### sql package
- `RemoteSqlBuilder.isPushableType(Type)` — the supported-type set, public; used to gate
  pushed-down ORDER BY items.
- `RemoteSqlBuilder.isPushableDomain(Type, Domain)` — whether `buildSql` renders a domain as a
  WHERE conjunct. `toConjuncts` was refactored onto a shared `tryRenderPredicate` helper, so the
  predicate check and the rendering literally run the same code and cannot diverge (covers both
  the supported-type gate and non-finite REAL/DOUBLE literal rejection).

### FederationTableHandle
- New invariant enforced in the compact constructor: `limit` and `topN` are mutually exclusive
  (`checkArgument`), matching what `RemoteSqlBuilder.buildSql` requires.
- `withTopN` now drops any previously stored plain limit (TopN supersedes it).

### FederationMetadata
- `applyFilter`: splits `constraint.getSummary()` domains three ways —
  1. `_region` domain: **consumed**; prunes `activeRegions` via
     `Domain.includesNullableValue(utf8Slice(regionName))` (an only-NULL domain matches nothing;
     a none summary empties the region list). Never stored in the handle constraint, never left
     in the remainder.
  2. Pushable domains (`isPushableDomain`): intersected into `handle.constraint`.
  3. Everything else: returned as the remaining `TupleDomain`; `constraint.getExpression()` is
     passed through untouched (no ConnectorExpression handling in v1).
  Refuses (returns empty) when the handle already carries limit/topN/aggregation — filtering
  below an already pushed reduction would change results. Idempotency: `Optional.empty()` when
  neither `activeRegions` nor `constraint` changed. `precalculateStatistics=false`.
- `applyProjection`: variable-only; shrinks `handle.columns` to the assignment set preserving
  the original column order; returns unchanged expressions + per-column `Assignment`s; empty
  when any expression is not a `Variable` or columns unchanged.
- `applyLimit`: empty when topN present or existing limit <= new; else `withLimit`,
  `limitGuaranteed=false`.
- `applyTopN`: refuses when handle already has topN, any sort column is `_region` (constant
  per region — a per-region ORDER BY cannot honor it), or a sort type is not pushable; else
  `withTopN` (which clears limit), `topNGuaranteed=false`.

Split manager already emits an empty `FixedSplitSource` for an empty region list — verified by
the `_region = 'nowhere'` test (zero rows, zero regional queries).

## Invariants upheld
- Handle never carries both `limit` and `topN` (constructor check + `withTopN` clears limit).
- Handle constraint only ever contains pushable non-region domains, so
  `FederationPageSourceProvider`'s `checkState`s (no region domains, no unsupported filter
  columns after `buildSql`) hold on every path.

## Tests
- `TestFederationPushdown` (new, 18 tests): 2-region harness via `AbstractTestQueryFramework`
  (enables `isFullyPushedDown()` plan assertions) + regional SQL text evidence from each
  region's `system.runtime.queries` (diffed around each query; `@Execution(SAME_THREAD)`
  because of that). Covers: equality/range/date/IN/IS NULL predicates (fully pushed down +
  regional WHERE text); LIKE and infinity-literal fallbacks (FilterNode retained, regional SQL
  has no WHERE, results correct); `_region='east'` (west receives zero scan queries),
  `_region IN ('east','west')` (no pruning), `_region='nowhere'` (zero rows, zero queries on
  both regions), `_region='east' AND id > 3` combined; LIMIT text + row count; TopN asc/desc,
  NULLS FIRST with correct global order; ORDER BY `_region` fallback (no regional ORDER BY);
  filter+projection+topN composition in one regional query; repeated-query stability.
- `TestRemoteSqlBuilder`: +3 tests for `isPushableType` / `isPushableDomain`, including an
  agreement check that `isPushableDomain` matches `buildSql`'s `unsupportedFilterColumns` for
  every probed domain.
- `TestFederationTableHandle`: adjusted for the new invariant + new mutual-exclusion test +
  withTopN-clears-limit assertions.
- `TestFederationScan.testLimitCompletes`: stale "LIMIT not pushed down yet" comment updated.

## Evidence
```
Tests run: 103, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS   (./mvnw -pl plugin/trino-federation install — full checks: checkstyle,
                 modernizer, dependency analysis, airstyle)
```
Regional SQL observed in tests, e.g.:
```
SELECT "id", "name" FROM "memory"."default"."items" WHERE "id" BETWEEN 3 AND 9 ORDER BY "id" ASC NULLS LAST LIMIT 2
```

## Deviations
- Added `trino-main` test-jar to the module pom (required by the dependency analyzer for
  `io.trino.sql.query.QueryAssertions` used through `AbstractTestQueryFramework`).
- `applyFilter` also refuses when the handle carries an aggregation (defensive for task 007's
  planning order; same for projection/limit/topN guards).
