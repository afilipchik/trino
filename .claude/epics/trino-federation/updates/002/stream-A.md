---
issue: 002
stream: region-client
started: 2026-08-20T03:43:45Z
completed: 2026-08-20T04:18:45Z
status: completed
---

# Task 002 — RegionClient: Trino client protocol wrapper and type mapping

## Design

Transport layer lives in `plugin/trino-federation/src/main/java/io/trino/plugin/federation/client/`:

- **`RegionClients`** — Guice singleton (bound in `FederationModule`) built from `FederationConfig`.
  Owns one shared `OkHttpClient` (connect/read/write timeouts from config, basic-auth interceptor
  when a password is configured) and one `RegionClient` per configured region, in config order.
  Exposes `clients()` and `client(name)` lookup; `@PreDestroy close()` shuts the HTTP client down
  via the Airlift lifecycle.
- **`RegionClient`** — per-region, thread-safe, synchronous. Each query gets its own
  `StatementClient` (via `StatementClientFactory.newStatementClient`) with a `ClientSession`
  pinned to the remote catalog, UTC session zone, configured user and request timeout,
  source `trino-federation`. `execute(sql)` returns a streaming `RegionQueryResults`; listing
  helpers (`listSchemas()`, `listTables(Optional<schema>)`, `describeTable(schema, table)`) run
  materialized `information_schema` queries with proper identifier quoting / literal escaping.
- **`RegionQueryResults`** — Guava `AbstractIterator<List<Object>>` + `Closeable`. Advances the
  `StatementClient` to the column-bearing response, maps every result column to a Trino `Type`
  (throws `FEDERATION_TYPE_MISMATCH` for unmapped result types), then streams rows with values
  converted to Trino stack types. `close()` cancels the remote query (protocol DELETE).
  Terminal error handling: remote `QueryError` → `FEDERATION_REMOTE_ERROR`; transport failures
  with an `IOException` in the causal chain → `FEDERATION_REGION_UNREACHABLE`; both messages
  carry the region name.
- **`FederationTypeMapper`** — static mapper. `toTrinoType(String)` parses remote type display
  strings (same format from `Column.getType()` and `information_schema.columns.data_type`) into
  `Optional<Type>`: boolean, tinyint/smallint/integer/bigint, real/double, decimal(p[,s]),
  varchar[(n)], varbinary, date, time[(p)], timestamp[(p)], timestamp[(p)] with time zone.
  Unsupported types (arrays, maps, rows, char, json, uuid, intervals, time with time zone,
  out-of-range precisions, …) return `Optional.empty()` so listing can skip them.
  `toNativeValue(Type, Object)` converts JSON-decoded protocol values to stack types: longs for
  fixed-width ints / date epoch-days / time picos-of-day / short timestamps (epoch micros) /
  short decimals / real bits, `Int128` for long decimals, `Slice` for varchar/varbinary,
  `LongTimestamp` for timestamp(p>6), packed long or `LongTimestampWithTimeZone` for
  timestamp with time zone (p<=3 / p>3), preserving the remote zone id.
- **`RegionPageBuilder`** — appends converted rows into a `PageBuilder` for a list of types
  (`appendRow` / `isFull` / `isEmpty` / `flush`); used by page sources in task 005.
- **`ResultColumn`** (name + mapped `Type`) and **`RemoteColumn`** (name + remote type string +
  `Optional<Type>`) records.

## Files

Main:
- plugin/trino-federation/src/main/java/io/trino/plugin/federation/client/FederationTypeMapper.java
- plugin/trino-federation/src/main/java/io/trino/plugin/federation/client/RegionClient.java
- plugin/trino-federation/src/main/java/io/trino/plugin/federation/client/RegionClients.java
- plugin/trino-federation/src/main/java/io/trino/plugin/federation/client/RegionQueryResults.java
- plugin/trino-federation/src/main/java/io/trino/plugin/federation/client/RegionPageBuilder.java
- plugin/trino-federation/src/main/java/io/trino/plugin/federation/client/ResultColumn.java
- plugin/trino-federation/src/main/java/io/trino/plugin/federation/client/RemoteColumn.java
- plugin/trino-federation/src/main/java/io/trino/plugin/federation/FederationModule.java (bind RegionClients)
- plugin/trino-federation/pom.xml (deps: okhttp-jvm, trino-client, jakarta.annotation-api;
  test: trino-memory, trino-testing, trino-testing-services, trino-tpch)

Tests:
- plugin/trino-federation/src/test/java/io/trino/plugin/federation/client/TestFederationTypeMapper.java
- plugin/trino-federation/src/test/java/io/trino/plugin/federation/client/TestRegionClient.java

## Test evidence

`./mvnw -pl plugin/trino-federation install` (Docker, jdk25): **BUILD SUCCESS**,
`Tests run: 58, Failures: 0, Errors: 0, Skipped: 0`, including:
- TestFederationTypeMapper: 7 tests (mapping, unsupported-type skips, value conversion units)
- TestRegionClient: 11 tests against an embedded `DistributedQueryRunner` (tpch + memory):
  literal round-trip for every mapped type incl. unicode varchar, long/short decimals,
  timestamps at p 0/3/6/9 and tz p 3/6; NULLs for all types; multi-page streaming (15000 rows);
  listSchemas/listTables/describeTable (unsupported `array(integer)` column reported unmapped,
  missing table → empty); bad SQL → FEDERATION_REMOTE_ERROR with region name; unreachable URI →
  FEDERATION_REGION_UNREACHABLE; close() mid-stream cancels the remote query (asserted via
  system.runtime.queries); RegionClients ordering/lookup/unknown-region.

`./mvnw -pl plugin/trino-federation validate` and `install -DskipTests` (checkstyle, airstyle,
sortpom, license, modernizer, dependency analysis, duplicate-finder): all pass.

## Deviations from spec

- Column listing helper is named `describeTable(schema, table)` (per the task acceptance
  criteria) rather than `getColumns` (DEVELOPMENT.md discourages `get` prefixes).
- Type mapping parses the remote type display string (covers both `Column.getType()` and
  `information_schema.data_type` with one code path) instead of walking
  `ClientTypeSignature` parameters; unit tests cover the string forms.
- Unit round-trip uses `DistributedQueryRunner` (coordinator-only) rather than bare
  `TestingTrinoServer` — same embedded server, richer helpers for catalogs and
  system.runtime assertions.
