---
issue: 005
stream: scan-path
started: 2026-08-20T04:15:00Z
completed: 2026-08-20T04:53:11Z
status: completed
---

# Task 005 — Scan path: splits and streaming page source

## What was built

### Splits (`io.trino.plugin.federation`)

- `FederationSplit(String regionName)` — plain JSON-serializable record implementing
  `ConnectorSplit`. Remotely accessible (default), no host addresses, retained size accounted
  via `SizeOf.instanceSize` + `estimatedSizeOf(regionName)`.
- `FederationSplitManager.getSplits(...)` — casts to `FederationTableHandle`, throws
  `IllegalStateException` (via `checkState`) when the handle carries an aggregation (task 007
  wires the single fan-out split), otherwise returns a `FixedSplitSource` with one
  `FederationSplit` per entry of `activeRegions`, in configured order.

### Page source

- `FederationPageSourceProvider` (`@Inject RegionClients, FederationConfig` — only
  `remoteCatalog` is kept, not the config object):
  - Partitions the projected `columns` into data columns and the synthetic `_region` column,
    producing an `outputChannels` mapping (`REGION_CHANNEL = -1` marks `_region`) so output
    pages match the engine's requested channel order exactly.
  - Translates the handle's `TupleDomain<FederationColumnHandle>` to
    `TupleDomain<RemoteColumn>`; `checkState`s that no domain targets a region column (those
    are consumed into `activeRegions` by task 006's `applyFilter`) and that
    `RemoteQuery.unsupportedFilterColumns()` is empty (task 006 stores only renderable
    domains).
  - Passes the handle's topN (`FederationTopN` → `TopNSpec` via `SortSpec.of`) and limit
    through to `RemoteSqlBuilder.buildSql`.
- `FederationPageSource` implements `ConnectorPageSource` (current SPI:
  `getNextSourcePage()` returning `SourcePage`):
  - Lazily opens `RegionClient.execute(sql)` on the first `getNextSourcePage()` call —
    constructing the page source never touches the network, so split scheduling stays fast.
  - Validates the region stream's mapped types against the handle's projected column types,
    failing with `FEDERATION_TYPE_MISMATCH` naming the region on drift.
  - Batches rows through `RegionPageBuilder` (PageBuilder-default ~1MB/positions bound);
    `_region` is materialized per page as `RunLengthEncodedBlock.create(VARCHAR, region, n)`.
  - Empty data projection (raw `count(*)`, or `_region`-only): the builder's
    `SELECT 1 AS "$dummy"` form is used and only row counts are consumed; pages are
    `new Page(positionCount)` plus the `_region` RLE block when projected.
  - Metrics: completed bytes (sum of output `Page.getSizeInBytes()`), completed positions,
    read time nanos (measured around the blocking section), memory reported through the
    provided `MemoryContext` (`setBytes(pageBuilder.retainedSizeInBytes())`, reset on close).
  - `close()` is idempotent and closes the `RegionQueryResults`, which cancels the remote
    regional query mid-stream.
- `RegionPageBuilder` gained `retainedSizeInBytes()` (delegates to
  `PageBuilder.getRetainedSizeInBytes()`).
- No `FederationModule` changes were needed: the existing singleton bindings pick up the new
  `@Inject` constructor.

### Test harness (test scope, `io.trino.plugin.federation`) — task 008 reuses this

`FederationQueryRunner implements Closeable`:

- `FederationQueryRunner.builder()`
  - `.addRegion(String name)` — embedded coordinator-only `DistributedQueryRunner` with a
    `memory` catalog (`REMOTE_CATALOG = "memory"`), session schema `default`.
  - `.addStaticRegion(String name, URI uri)` — region entry with no cluster behind it, for
    unreachable-region tests. Ordering of both kinds follows builder call order.
  - `.addCatalogProperty(String key, String value)` — extra federation catalog properties.
  - `.build()` — starts the regions, then a central coordinator-only runner with
    `FederationPlugin` and `createCatalog("federation", "trino_federation",
    {federation.regions=<name=baseUrl,...>, federation.remote-catalog=memory, ...})`.
    Cleans up every started runner if construction fails midway.
- Accessors: `central()`, `region(name)`, `regionNames()`, `execute(sql)` (central),
  `executeOnRegion(name, sql)`, `executeOnAllRegions(sql)`; `close()` closes central then
  regions. Constants `FEDERATION_CATALOG` / `REMOTE_CATALOG`.

## Test evidence

`TestFederationScan` (JUnit `@TestInstance(PER_CLASS)`, two embedded regions east/west,
mixed-type `orders` table with ids 1–5 / 6–10 incl. varchar/decimal/date/timestamp/boolean
NULLs, plus `returns` populated only in east):

- `testSelectAllUnionsAllRegions` — SELECT * (+ `_region`) equals the union of both regions'
  rows, exact result types asserted.
- `testColumnSubsetProjection` — subset projection correct; regional
  `system.runtime.queries` (filtered on `source = 'trino-federation'`) shows a remote query
  with only the projected column.
- `testRegionColumn` — `_region` alone (5×east, 5×west) and paired with ids per region.
- `testCountStar` — raw `count(*)` path (zero projected columns → dummy query, counted
  positions): 10 and 3.
- `testValuesRoundTrip` — pinned values for decimal/date/timestamp(3)/boolean and an
  all-NULL row.
- `testEmptyRegionStillUnions` — table empty in one region still unions correctly.
- `testUnreachableRegionFailsWithRegionName` — static region at `http://127.0.0.1:1`; scan
  fails with a message containing `Region 'down'`.
- `testLimitCompletes` — `LIMIT 1` completes although regions hold more rows (close path
  cancels remote queries; no hang).

Full module build (all airbase checks + airstyle):

```
./mvnw -pl plugin/trino-federation install   (in trino-builder:jdk25 Docker)
Tests run: 81, Failures: 0, Errors: 0, Skipped: 0
  ... Tests run: 8 ... in io.trino.plugin.federation.TestFederationScan
BUILD SUCCESS
```

## Deviations from the task brief

- `getMemoryUsage()` was not overridden: the current SPI deprecates it in favor of the
  `MemoryContext` passed to `createPageSource`, which this implementation uses instead.
- The remote-SQL-text assertion was kept (not skipped) via `system.runtime.queries` with the
  `source = 'trino-federation'` filter; task 008 can formalize it further.
