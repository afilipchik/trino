---
issue: 001
stream: skeleton
started: 2026-08-20T03:45:00Z
completed: 2026-08-20T03:54:32Z
status: completed
---

# Task 001 — Plugin skeleton, config, and registration

## What was built

New Maven module `plugin/trino-federation` (packaging `trino-plugin`, version 484-SNAPSHOT),
modeled on `plugin/trino-kubernetes`:

- `FederationPlugin` — registers the single connector factory.
- `FederationConnectorFactory` — connector name `trino_federation`; Airlift `Bootstrap` +
  `ConnectorContextModule` + `checkStrictSpiVersionMatch`.
- `FederationConnector` — read-only: `READ_COMMITTED` transactions via
  `FederationTransactionHandle.INSTANCE`, wires metadata/split manager/page source provider,
  no page sink.
- `FederationModule` — Guice bindings (config + singletons).
- `FederationConfig` — `federation.regions` (ordered `name=uri` comma list parsed into
  `List<Region>`; validates unique names, non-empty parts, http/https scheme),
  `federation.remote-catalog` (required), `federation.user` (default `federation`),
  `federation.password` (optional, `@ConfigSecuritySensitive`),
  `federation.connect-timeout` (10s), `federation.request-timeout` (30s).
- `Region` — top-level record `(String name, URI uri)` for reuse by later tasks.
- `FederationErrorCode` — base `0x0522_0000`: FEDERATION_REGION_UNREACHABLE,
  FEDERATION_REMOTE_ERROR, FEDERATION_TYPE_MISMATCH (all EXTERNAL).
- Placeholders: `FederationMetadata` (no schemas/tables yet, honest defaults),
  `FederationSplitManager` / `FederationPageSourceProvider` throw
  `UnsupportedOperationException` (unreachable until metadata exposes tables).

Registration: `plugin/trino-federation` added to root `pom.xml` `<modules>` (sorted, after
trino-faker) and `plugin/federation` artifactSet added to
`core/trino-server/src/main/provisio/trino.xml` (alphabetical, after faker).

## Tests

- `TestFederationConfig` — ConfigAssertions defaults + full mapping, ordered parsing,
  duplicate-name rejection, malformed entry rejection, non-http(s)/invalid URI rejection.
- `TestFederationPlugin` — creates connector through the factory with
  `TestingConnectorContext` and a minimal config map, then shuts it down.

## Build evidence

`./mvnw -nsu -pl plugin/trino-federation install` (in trino-builder:jdk25 Docker):

```
Tests run: 7, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
Total time:  6.060 s
```

`airstyle:format`: "Processed 13 files, formatted 0" (already canonical).

## Deviations from the task spec

- `trino-client` and jackson/json dependencies were NOT added to the pom yet: the skeleton has
  no code using them, and the airbase dependency analysis fails the build on unused declared
  dependencies. Task 002 (RegionClient) adds `trino-client` together with the code that uses it.
- Standard SPI-transitive deps (jackson-annotations, slice, opentelemetry-api/context) declared
  `provided`, required by the trino-maven-plugin `check-spi-dependencies` goal.
