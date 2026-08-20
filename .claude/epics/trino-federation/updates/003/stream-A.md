---
issue: 003
stream: sql-builder
started: 2026-08-20T04:05:00Z
completed: 2026-08-20T04:13:46Z
status: completed
---

# Task 003 — RemoteSqlBuilder

## Design summary

Pure, side-effect-free SQL generation in `io.trino.plugin.federation.sql`, engine-free (only
trino-spi types, no ConnectorSession/ColumnHandle). Public API:

- `RemoteColumn(String name, Type type)`, `RemoteTable(String catalog, String schema, String table)`
- `SortSpec(RemoteColumn column, boolean ascending, boolean nullsFirst)` + `SortSpec.of(column, SortOrder)`
- `TopNSpec(List<SortSpec> ordering, long count)` — carries the TopN row count; mutually
  exclusive with the plain `limit` argument (checked)
- `AggregateKind {COUNT_ALL, COUNT, SUM, MIN, MAX}`,
  `AggregateSpec(kind, Optional<RemoteColumn> argument, RemoteColumn output)` (output carries
  alias + caller-decided output type), `AggregationSpec(groupingColumns, aggregates)`
- `RemoteSqlBuilder.buildSql(table, projections, TupleDomain<RemoteColumn> constraint,
  Optional<AggregationSpec>, Optional<TopNSpec>, OptionalLong limit)` →
  `RemoteQuery(String sql, List<RemoteColumn> outputColumns, Set<RemoteColumn> unsupportedFilterColumns)`

Key behaviors:

- Identifiers always `"`-quoted with embedded-quote doubling.
- Typed literals rendered inline: varchar `'…'` doubling + non-ASCII passthrough, varbinary
  `X'…'`, `DATE/TIME/TIMESTAMP '…'` at exact type precision from epoch-days/picos-of-day/
  epoch-micros(+picos), `DECIMAL '…'` via unscaled+scale plain string, `REAL '…'`/`DOUBLE '…'`
  canonical Java text, TRUE/FALSE, bare integers.
- Non-finite double/real values (Infinity; NaN defensively — SPI rejects NaN range bounds) and
  unsupported types (arrays, color/equatable-only types, etc.) make the whole column domain
  non-pushable: omitted from WHERE and reported in `unsupportedFilterColumns` for the caller's
  post-filter.
- TupleDomain rendering: none → `WHERE FALSE`; all → no clause; onlyNull/notNull → IS [NOT] NULL;
  nullAllowed → `(… OR "c" IS NULL)`; ranges → `>`/`>=`/`<`/`<=`, inclusive-both → BETWEEN,
  singles collapse to `=`/IN, complement-discrete → `<>`/NOT IN; EquatableValueSet → IN/NOT IN
  (shares the same helpers; unreachable for renderable types today since all supported scalars
  are orderable and the SPI forces SortedRangeSet for them). Conjuncts sorted by column name for
  deterministic SQL.
- Aggregation SELECT list = grouping columns then aliased aggregate expressions; GROUP BY by
  identifier; grouping-only (distinct) supported; projections must be empty when aggregation
  present. Empty projections on plain scans → `SELECT 1 AS "$dummy"` (output column
  `$dummy`:INTEGER).
- TopN emits `ORDER BY … ASC/DESC NULLS FIRST/LAST … LIMIT n`; plain limit emits `LIMIT n`;
  passing both throws.

## Test evidence

Run in Docker (trino-builder:jdk25) against an isolated git worktree containing the committed
task-001 skeleton + this task's files (main working tree contains task 002's in-progress
`client/` files, which do not compile yet — see coordination note):

```
[INFO] Tests run: 33, Failures: 0, Errors: 0, Skipped: 0 -- io.trino.plugin.federation.sql.TestRemoteSqlBuilder
[INFO] Tests run: 40, Failures: 0, Errors: 0, Skipped: 0   (whole module)
[INFO] BUILD SUCCESS
```

Full airbase gate passed on that tree: checkstyle, modernizer, dependency analysis, airstyle
check, license check. `airstyle:format` was run scoped to `**/sql/*.java`.

## Coordination note

`plugin/trino-federation` currently does not compile in the shared working tree because task
002's `client/` package is mid-flight (missing trino-client/okhttp deps in the pom). All errors
are outside this task's files; verification used the isolated worktree instead (since removed).
