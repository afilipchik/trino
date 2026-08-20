---
name: trino-federation
status: backlog
created: 2026-08-20T03:40:23Z
updated: 2026-08-20T03:40:23Z
progress: 30%
prd: .claude/prds/trino-federation.md
github: (local only — origin is upstream trinodb/trino, no issue sync)
---

# Epic: trino-federation

## Overview

New plugin `plugin/trino-federation`: a read-only connector that presents a dataset sharded across
N regional Trino clusters as one logical catalog on a central Trino. Each federated table is the
`UNION ALL` of the homogeneous table on every region. The connector fans queries out over the
Trino client protocol (`io.trino:trino-client`, same library the JDBC driver uses) and pushes
predicates, projections, LIMIT, TopN, and aggregations to the regions so only reduced data crosses
region boundaries.

## Architecture Decisions

- **Transport: `trino-client` `StatementClient`** (in-repo, streams typed JSON rows) rather than
  the JDBC driver — no DriverManager global state, direct access to `ClientTypeSignature` for type
  mapping, easy async cancellation.
- **Aggregation pushdown = connector-side final combine.** Verified in
  `PushAggregationIntoTableScan` (core/trino-main): the rule matches only `Step.SINGLE` and the
  engine does **not** re-aggregate connector output. Therefore `applyAggregation` produces a
  handle whose scan is a **single fan-out split**: its page source runs the rewritten partial
  aggregate (`SELECT k…, agg(x)… GROUP BY k…`) on every region **concurrently**, then combines
  partials in the connector (hash on grouping keys; sum-of-sums, sum-of-counts, min-of-mins,
  max-of-maxes; `avg` decomposed to sum+count remotely and divided at combine time). Unsupported
  shapes (DISTINCT, filters/masks, ordered aggregates, unsupported functions/types) return
  `Optional.empty()` — engine falls back to raw-row shipping, correctness preserved.
- **Non-aggregated scans: one split per region**, so regional streams are read in parallel by
  central workers. LIMIT (`limitGuaranteed=false`) and TopN (`topNGuaranteed=false`) pushdowns
  keep the engine's final pass while each region pre-reduces.
- **Synthetic `_region` VARCHAR column** on every table; `applyFilter` extracts its domain and
  prunes the region list in the table handle (fewer splits / fan-out targets). It is also
  materialized per-row so it can be selected and grouped on (constant per split/regional stream).
- **Metadata from first reachable region** (homogeneous-shard assumption), cached briefly;
  remote type → Trino type mapping restricted to common scalars, unsupported columns skipped at
  listing time.
- **SQL generation:** small dedicated `RemoteSqlBuilder` (quoted identifiers, typed literals from
  TupleDomain, SELECT list, WHERE, GROUP BY, ORDER BY, LIMIT). Trino-to-Trino means no dialect
  impedance; we do not reuse JDBC's `QueryBuilder` (JDBC-coupled).
- **Read-only connector**, static credentials (user + optional basic-auth password over HTTPS),
  per-region named endpoints in catalog config.

## Technical Approach

### Backend Services (the plugin)

- `FederationPlugin` / `FederationConnectorFactory` / `FederationConnector` — standard Airlift
  bootstrap + Guice module, modeled on an existing lightweight connector.
- `FederationConfig` (`trino.federation.regions=name1=uri1,name2=uri2`-style or per-region
  properties, remote catalog, user, password, timeouts) with `@Config` + config tests.
- `RegionClient` — wraps `StatementClient` per region: execute SQL, stream rows, map
  `ClientTypeSignature` → Trino `Type`, build `Page`s; used by metadata (listing via
  `SHOW SCHEMAS`/`information_schema` queries) and by page sources.
- `FederationMetadata` — listing, `FederationTableHandle` (schema/table, columns, region list,
  constraint, limit, topN, optional aggregation), `applyFilter` / `applyProjection` /
  `applyLimit` / `applyTopN` / `applyAggregation`.
- `FederationSplitManager` — per-region splits, or single fan-out split when the handle carries an
  aggregation.
- `FederationPageSourceProvider` — streaming page source per region; `AggregateCombiningPageSource`
  for the fan-out split (bounded thread pool, per-region iterators, hash combine keyed on group
  values using Trino `Block` equality via `TypeOperators`).
- Registration: root `pom.xml` module, `core/trino-server/src/main/provisio/trino.xml`,
  connector doc page.

### Infrastructure (demo)

Extend the kind assets from the kubernetes-connector epic: manifests for `trino-region-a`,
`trino-region-b` (each with sharded data, e.g. memory/TPC-H-derived tables filtered by region key)
plus central Trino with a `federation` catalog pointing at both cluster services; setup script and
verification queries that show per-region row counts vs. rows shipped.

### Frontend Components

None — surface is SQL through existing clients.

## Implementation Strategy

Build bottom-up in one epic branch (`epic/trino-federation`): skeleton+config first, then the
region client and SQL builder in parallel, then metadata/splits/page sources, then pushdowns
(scan pushdowns before aggregation), then tests, docs, demo. Multi-cluster integration testing
uses `DistributedQueryRunner` instances as fake regions (one runner per region + one central) —
pure-Java, no containers, runs in CI.

## Task Breakdown Preview

1. Plugin skeleton: module, poms, plugin/factory/connector, `FederationConfig`, registration
   (root pom, provisio). *(foundation)*
2. `RegionClient`: StatementClient wrapper, type mapping, row→Page building, listing queries.
   *(parallel with 3 after 1)*
3. `RemoteSqlBuilder`: SQL generation for identifiers/literals/predicates/limit/topN/aggregates.
   *(parallel with 2 after 1)*
4. Metadata + handles: listing, `_region` column, `FederationTableHandle`/column handles.
   *(after 2)*
5. Scan path: split manager + per-region streaming page source; end-to-end SELECT works.
   *(after 3, 4)*
6. Scan pushdowns: `applyFilter` (incl. `_region` pruning), `applyProjection`, `applyLimit`,
   `applyTopN`. *(after 5)*
7. Aggregation pushdown: `applyAggregation` rewrite + single fan-out split +
   `AggregateCombiningPageSource`. *(after 6)*
8. Integration tests: multi-`DistributedQueryRunner` harness, correctness + pushdown plan
   assertions + region-pruning assertions. *(after 7; harness can start after 5)*
9. Docs + `./mvnw validate` + full-build checks. *(after 7, parallel with 8)*
10. Kind demo: regional clusters, central catalog, setup script, verification queries.
    *(after 7, parallel with 8/9)*

## Dependencies

- In-repo: `trino-client`, `trino-spi`, `trino-plugin-toolkit`, `trino-testing`, existing kind
  assets under the kubernetes-connector deployment directory.
- No new third-party dependencies.
- Regions must run a compatible Trino version (same version assumed/tested).

## Success Criteria (Technical)

- `TestFederationConnectorTest`-style IT: unioned SELECT correctness across 2 embedded regions;
  `count(*)` ships exactly 1 row per region; predicate/projection/limit/topN/aggregation pushdown
  asserted via plan patterns; `WHERE _region = 'a'` contacts only region a.
- Unsupported aggregate shapes produce correct results via fallback (verified by test).
- `./mvnw -pl plugin/trino-federation install` and `./mvnw validate` pass; airstyle clean.
- Kind demo script provisions central + 2 regions and verification queries pass.

## Estimated Effort

~4–6 focused days single-threaded; tasks 2/3 and 8/9/10 parallelizable. Largest risk item is
task 7 (aggregation combine); its fallback path keeps the connector shippable even if some
aggregate shapes are deferred.

## Tasks Created
- [x] 001.md - Plugin skeleton, config, and registration (parallel: false)
- [x] 002.md - RegionClient — Trino client protocol wrapper and type mapping (parallel: true)
- [x] 003.md - RemoteSqlBuilder — regional SQL generation (parallel: true)
- [ ] 004.md - Metadata, handles, and _region column (parallel: false)
- [ ] 005.md - Scan path — splits and streaming page source (parallel: false)
- [ ] 006.md - Scan pushdowns — filter, projection, limit, TopN, region pruning (parallel: false)
- [ ] 007.md - Aggregation pushdown with connector-side combine (parallel: false)
- [ ] 008.md - Multi-cluster integration test suite (parallel: true)
- [ ] 009.md - Documentation and repo validation (parallel: true)
- [ ] 010.md - Kind demo — central + regional clusters (parallel: true)

Total tasks: 10
Parallel tasks: 5 (002+003 after 001; 008+009+010 after 007)
Sequential tasks: 5 (001 → 004 → 005 → 006 → 007 spine)
Estimated total effort: 66 hours
