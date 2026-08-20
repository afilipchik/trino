---
issue: 004
stream: metadata
started: 2026-08-20T04:20:00Z
completed: 2026-08-20T04:36:52Z
status: completed
---

# Task 004 — Metadata, handles, and _region column

## What was built

### Handle model (all JSON-serializable plain records, package `io.trino.plugin.federation`)

- `FederationColumnHandle(String name, Type type, boolean regionColumn)` implements
  `ColumnHandle`; `columnMetadata()` helper builds the `ColumnMetadata`.
- `FederationColumns` — holder for the synthetic region column:
  `REGION_COLUMN_NAME = "_region"`, `REGION_COLUMN` (`VARCHAR`, `regionColumn=true`,
  not hidden). Appended LAST to every table's columns.
- `FederationTableHandle(SchemaTableName schemaTableName, List<FederationColumnHandle> columns,
  List<String> activeRegions, TupleDomain<FederationColumnHandle> constraint, OptionalLong limit,
  Optional<FederationTopN> topN, Optional<FederationAggregation> aggregation)` implements
  `ConnectorTableHandle`. Static factory `of(schemaTableName, columns, activeRegions)` gives the
  unconstrained handle (constraint `all()`, everything else empty). With-ers for later tasks:
  `withColumns`, `withActiveRegions`, `withConstraint`, `withLimit(long)`,
  `withTopN(FederationTopN)`, `withAggregation(FederationAggregation)`.
- `FederationTopN(List<FederationSortColumn> ordering, long count)`;
  `FederationSortColumn(FederationColumnHandle column, SortOrder sortOrder)` (SPI `SortOrder`).
- `FederationAggregation(List<FederationColumnHandle> groupingColumns,
  List<FederationAggregateColumn> aggregates)`;
  `FederationAggregateColumn(AggregateKind kind, Optional<FederationColumnHandle> argument,
  String outputName, Type outputType, CombineKind combineKind)` — `kind` reuses
  `sql.AggregateKind`; argument empty iff `COUNT_ALL` (enforced).
- `CombineKind` enum: `SUM_LONG, SUM_DOUBLE, SUM_DECIMAL, COUNT_SUM, MIN, MAX, AVG_DOUBLE,
  AVG_DECIMAL` (AVG kinds reserved; task 007 finalizes).

### Naming collision resolved

`client.RemoteColumn` renamed to `client.RemoteColumnMetadata` (git mv + usages in
`RegionClient.describeTable` and `TestRegionClient`); `RemoteColumn` now unambiguously means
the SQL model record in `io.trino.plugin.federation.sql`.

### FederationMetadata (replaces task-001 stub)

- Injected `RegionClients`; one instance per connector (already bound singleton in
  `FederationModule`, no module change needed).
- **First-reachable-region reads**: `fromFirstReachableRegion(Function<RegionClient, T>)` walks
  regions in configured order; `FEDERATION_REGION_UNREACHABLE` failures try the next region
  (originals kept as suppressed exceptions); other errors propagate immediately; all regions
  down throws `TrinoException(FEDERATION_REGION_UNREACHABLE)` naming every attempted region.
- **Caching**: three `EvictableCacheBuilder` caches (repo idiom from `io.trino:trino-cache`;
  plain Guava `CacheBuilder.build()` is banned by modernizer), `expireAfterWrite`
  `METADATA_CACHE_TTL = 10s`: schema list, table list per `Optional<schema>`, columns per
  `SchemaTableName`. `TrinoException`s are unwrapped from Guava's
  `UncheckedExecutionException` after `uncheckedCacheGet`.
- `listSchemaNames` / `listTables` (both forms) — `information_schema` excluded from results
  and short-circuited as a requested schema.
- `getTableHandle` — versioned reads rejected (`NOT_SUPPORTED`); missing table returns null
  (empty remote column listing == table absent, since an existing table always lists at least
  one column); handle snapshot carries all mapped columns + `_region` last and all configured
  region names.
- `getTableMetadata` / `getColumnHandles` / `getColumnMetadata` — served from the handle's
  column snapshot.
- `streamRelationColumns` implemented (the current non-deprecated SPI listing method; memory
  connector pattern). Deprecated `streamTableColumns`/`listTableColumns` are NOT overridden —
  overriding them trips `-Werror` deprecation warnings under `air.compiler.fail-warnings`.
- Unmapped remote types skipped at listing with one `Logger.debug` per column per cache load;
  a remote column literally named `_region` is also skipped (shadowed by the synthetic column).

### Handle resolution

No registration needed — current SPI resolves handle classes by class name through the
connector classloader (same as `trino-kubernetes`, which registers nothing).

### pom changes

Added `io.trino:trino-cache`, `io.airlift:log` (compile) and
`com.fasterxml.jackson.core:jackson-databind`, `io.airlift:json` (test, for the codec test).

## Test evidence

`./mvnw -pl plugin/trino-federation install` (Docker, full checks incl. airstyle, checkstyle,
modernizer, dependency analysis): BUILD SUCCESS.

- `TestFederationTableHandle` — 5 tests: column handle round trip (incl. `_region`), minimal
  handle round trip, fully populated round trip (constraint with two domains — varchar single
  value + bigint range with nulls, limit, 2-column TopN, aggregation with count(*)+sum),
  with-er behavior + round trip, JSON field spot checks. Codec wired like
  `TestMongoTableHandle` + `TestPatternRecognitionNodeSerialization`: airlift
  `JsonMapperProvider` with `TypeDeserializer(TESTING_TYPE_MANAGER)` and
  `BlockJsonSerde` over `TESTING_BLOCK_ENCODING_SERDE`.
- `TestFederationMetadata` — 10 tests against one embedded `DistributedQueryRunner` (memory
  catalog, schema `default` + `sales`, table with unmapped `array(integer)` column):
  listSchemaNames (excludes information_schema), listTables (both forms), getTableHandle
  (columns snapshot with `_region` last, array column skipped, active regions, empty pushdown
  state), missing table/schema → null, information_schema → null, versioned read →
  NOT_SUPPORTED, getTableMetadata, getColumnHandles/getColumnMetadata, streamRelationColumns,
  failover (down + real region works, activeRegions keeps both names), all-unreachable →
  FEDERATION_REGION_UNREACHABLE naming both regions.
- Pre-existing suites still green: TestRegionClient 11, TestFederationTypeMapper,
  TestRemoteSqlBuilder, TestFederationConfig, TestFederationPlugin.

## Notes for downstream tasks

- Task 005/006/007 mutate the handle exclusively through the with-ers.
- `FederationTableHandle.columns()` is the full selectable column list; projections narrow it
  via `withColumns`.
- Region pruning = `withActiveRegions` with a subset of the configured names (order preserved
  by convention).
