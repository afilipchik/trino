---
issue: 009
stream: docs
agent: task-009-docs
started: 2026-08-20T05:58:00Z
completed: 2026-08-20T06:12:00Z
status: completed
---

# Task 009 — Documentation and repo validation

## Deliverables

- `docs/src/main/sphinx/connector/federation.md` — new connector page modeled on
  `kubernetes.md` (structure/tone) and `postgresql.md` (pushdown section): overview
  (UNION ALL of homogeneous regional shards, fan-out over the Trino client protocol),
  requirements, full configuration property table (`federation.regions`,
  `federation.remote-catalog`, `federation.user` default `federation`,
  `federation.password`, `federation.connect-timeout` 10s, `federation.request-timeout`
  30s, `federation.fanout-threads` 16 — all verified against `FederationConfig.java`),
  schemas-and-tables section (first-reachable-region metadata, 10s cache, scan-time
  per-region type validation, unsupported columns hidden, remote `_region` shadowed),
  `_region` column section with pruning example, querying examples (union SELECT,
  `GROUP BY _region` aggregate showing per-region partials + connector combine),
  pushdown section (column pruning / limit / topN refs, aggregate function list,
  `pushdown-correctness-behavior.fragment` include, aggregation fan-out semantics with
  connector-side final combine, exact fallback list incl. DISTINCT/FILTER/ordered,
  avg over decimal/integer, sum over narrow ints via engine-planned cast, other
  functions, grouping sets, aggregation over pushed LIMIT/TopN, and
  timestamp-with-time-zone arguments/grouping columns — verified against
  `FederationMetadata.java`, `RemoteSqlBuilder.isSupportedType`, and
  `updates/007/stream-A.md`), predicate pushdown support (renderable domains only;
  LIKE/expressions and non-finite REAL/DOUBLE literals fall back; timestamp with time
  zone not renderable), identity type-mapping list, limitations (read-only,
  homogeneous shards, same version, static credentials/no identity propagation, no
  join pushdown, region list changes need catalog reload).
- Registered in the connector toctree, alphabetically:
  `docs/src/main/sphinx/connector.md` gains `Federation      <connector/federation>`
  between Faker and Google Sheets. No other index references connector pages.

## Validation evidence

- Sphinx docs build (`docs/build`, `ghcr.io/trinodb/build/sphinx:114`, runs with `-W
  --keep-going` so warnings are errors): exit 0, `target/html/connector/federation.html`
  generated; all cross-reference anchors (`projection-pushdown`, `limit-pushdown`,
  `topn-pushdown`, `aggregation-pushdown`, `prop-type-duration`) verified present.
- `./mvnw -nsu -pl docs -pl plugin/trino-federation validate` in
  `trino-builder:jdk25`: BUILD SUCCESS (license, airstyle, checkstyle, sortpom,
  enforcer all green for both modules — provisio/root-pom entries from task 001
  survive sortpom).
- Repo-wide `./mvnw -nsu validate`: fails in `plugin/trino-thrift-testing-server` —
  enforcer `EnforceBytecodeVersion` (Rule 13) cannot resolve
  `io.trino:trino-thrift-api:jar:484-SNAPSHOT` because no full 484-SNAPSHOT install
  exists in the shared `~/.m2` and `-nsu` blocks remote snapshot lookup.
  **Pre-existing environmental failure, not caused by this epic**: reproduced
  identically with `-pl plugin/trino-thrift-testing-server validate` in isolation
  (docs-only changes are not in that module's graph); every module validated before
  the failure (including all of core and trino-main checkstyle) passed, and
  `trino-federation` + `docs` validate green when selected directly. Fix would be a
  full `install` of the branch, which task instructions reserve for another agent.
- No installs performed; nothing written to `~/.m2` for this module's artifacts
  (validate-only invocations).

## Notes

- Doc facts double-checked in code: connector name `trino_federation`
  (`FederationConnectorFactory`), `_region` definition (`FederationColumns`), split
  model (`FederationSplitManager`: one split per region, single fan-out aggregate
  split), scan-time type validation (`FederationPageSource.validateResultTypes`),
  supported remote types (`FederationTypeMapper.toTrinoType`), pushable predicate
  types (`RemoteSqlBuilder.isSupportedType` — excludes timestamp with time zone).
