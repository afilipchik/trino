---
issue: 008
stream: tests
agent: task-008-tests
started: 2026-08-20T06:00:00Z
completed: 2026-08-20T06:25:00Z
status: completed
---

# Task 008 — Multi-cluster integration test suite

Tasks 001–007 had already built most of the planned suite, so this stream audited the
acceptance criteria against existing coverage, filled the gaps, and stabilized the shared
test plumbing. Module total: **143 tests** (was 123), all green in three consecutive Docker
runs.

## Criterion → coverage audit

| Acceptance criterion | Coverage |
| --- | --- |
| Harness: N regions + central, reusable; regional query-log hook | Existing: `FederationQueryRunner` (supports N regions and static URIs) with evidence via each region's `system.runtime.queries`. Stabilized: the two divergent per-class log-diff helpers were consolidated into a shared `RegionalQueryCapture` keyed on query ids — `TestFederationPushdown` still diffed by log *size*, which breaks when `system.runtime.queries` evicts entries between snapshots. |
| Correctness: unioned SELECT = UNION ALL; `_region` values | Existing: `TestFederationScan.testSelectAllUnionsAllRegions` / `testRegionColumn` / `testEmptyRegionStillUnions`. |
| ORDER BY / LIMIT / aggregations match a single-cluster control | Existing: every `isFullyPushedDown()` assertion in `TestFederationAggregationPushdown` compares against a pushdown-disabled control run. New: `TestFederationThreeRegionCorrectness` (12 tests) — TPC-H tiny `orders`/`customer` sharded across **three** regions by orderkey/custkey ranges, compared against unsharded control tables in a memory catalog on the central cluster: full scan, filters (incl. ranges crossing shard boundaries), projections, global + grouped aggregates, TopN (ordered compare), LIMIT, mixed filter+aggregate+TopN, and a central-level join of two federated tables (row-level and aggregated). Aggregated measures use exact types (decimal/bigint/date/varchar) so control and combine cannot diverge by FP summation order. |
| Pushdown plan assertions (predicate/projection/limit/TopN/aggregation) | Existing: `TestFederationPushdown` (18), `TestFederationAggregationPushdown`. Nothing new needed. |
| Region pruning: `WHERE _region='a'` → region b gets zero queries | Existing: `testRegionPredicatePrunesRegions`, `testNonexistentRegionContactsNoRegion`, `testRegionPruningComposesWithGlobalAggregation`, `testPrunedToNoRegionsStillEmitsGlobalRow`. New: partial pruning to a strict subset (2 of 3 regions) composed with aggregation — `testPartialRegionPruningWithAggregation`. |
| Data movement: `count(*)` ships 1 row/region | Existing: `testCountStarShipsOneRowPerRegion` asserts the exact partial SQL (1 row by construction). |
| Data movement: grouped agg ships ≤ groups/region | New: `testGroupedAggregateShipsOneRowPerLocalGroup`. Mechanism: `system.runtime.queries` exposes no per-query output-row count, so the test re-runs the **exact captured partial SQL text** each region received and asserts its result row count equals the region's local group count (3 in east, 4 in west — both below the 5 local rows). The rows shipped are by definition the result rows of that query. |
| Fallback correctness (DISTINCT, FILTER, unsupported types/shapes) | Existing: `testUnsupportedShapesFallBackWithCorrectResults`, `testSumOverNarrowIntegersFallsBack`, `testUnsupportedFunctionFallsBack`, `testAggregationOverPushedLimitFallsBack`. New: `count(DISTINCT clerk)` at three regions vs control. |
| Failure modes: region down → error names the region | Existing (scan): `TestFederationScan.testUnreachableRegionFailsWithRegionName`. New: `TestFederationFailureModes` (4 tests) — unreachable region fails global and grouped **aggregation** naming `Region 'down'` while `SHOW TABLES` (metadata from first reachable region) and `_region`-pruned queries keep working; a table dropped on one region only fails the scan path and the fan-out **combine** path naming `Region 'west'`, and pruning to the healthy region isolates the failure. |
| `_region` semantics under aggregation pruning | Verified covered by 007 (`GROUP BY _region` global partials + emptiness probe, pruning composition, empty-table probe semantics). Extended only with the 3-region partial-pruning case above. |
| Schema drift (008 briefing addition) | New: `TestFederationSchemaDrift` (3 tests) pinning actual behavior: metadata comes from the first configured reachable region, so a column added on a *non-metadata* region is invisible and harmless (common columns read correctly); a column added on the *metadata* region is visible, and selecting it fails with `FEDERATION_REMOTE_ERROR` naming the region that lacks it, while common columns and `_region`-pruned reads keep working; a column whose type differs on one region fails scan-time validation with `FEDERATION_TYPE_MISMATCH` ("Region 'west' returned column types …"), fails pushed aggregates with an error naming the region, and stays readable for type-consistent columns and via pruning. |

## New/changed test files

- `RegionalQueryCapture.java` — new shared helper: query-id-keyed regional query-log diff
  with a configurable remote-query LIKE pattern (eviction-safe; documents the SAME_THREAD
  requirement).
- `TestFederationPushdown.java` — refactored onto the helper (removes the size-based diff
  flake); no assertion changes.
- `TestFederationAggregationPushdown.java` — refactored onto the helper; added
  `testGroupedAggregateShipsOneRowPerLocalGroup` (now 21 tests).
- `TestFederationThreeRegionCorrectness.java` — new, 12 tests (three regions, TPC-H shards,
  control tables, join at central).
- `TestFederationFailureModes.java` — new, 4 tests.
- `TestFederationSchemaDrift.java` — new, 3 tests.

No main-code changes.

## Flake check (Docker `trino-builder:jdk25`, `./mvnw -pl plugin/trino-federation test`)

| Run | Result |
| --- | --- |
| 1 | Tests run: 143, Failures: 0, Errors: 0, Skipped: 0 — BUILD SUCCESS (total 27.5 s) |
| 2 | Tests run: 143, Failures: 0, Errors: 0, Skipped: 0 — BUILD SUCCESS |
| 3 | Tests run: 143, Failures: 0, Errors: 0, Skipped: 0 — BUILD SUCCESS |

Module airstyle-formatted (`airstyle:format`); the airstyle check runs as part of the test
build and is green.
