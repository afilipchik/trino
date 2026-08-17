# Execution summary — kubernetes-connector epic

Completed 2026-08-17 (single autonomous overnight session). All 9 tasks closed.

## What shipped

`plugin/trino-kubernetes` (connector name `kubernetes`) on Trino master
(484-SNAPSHOT), branch `epic/kubernetes-connector`:

- 20 main classes: plugin/factory/module/config, minimal REST client
  (JDK HttpClient + Jackson; kubeconfig with client-cert/token/CA/inline-data
  auth via airlift PemReader), discovery (classic /api + /apis), OpenAPI v3 →
  Trino type mapper (ROW/ARRAY/MAP/TIMESTAMP_TZ/VARBINARY/JSON fallback with
  cycle detection), metadata with namespace/name pushdown + limit pushdown,
  page source (paginated list → typed Blocks), page sink (INSERT → POST),
  merge sink (UPDATE → PUT with resourceVersion guard, DELETE → DELETE,
  paradigm CHANGE_ONLY_UPDATED_COLUMNS, row-id ROW(namespace,name,resource_version)).
- Synthetic visible `name`/`namespace` VARCHAR columns per table: pushdown +
  ergonomic INSERT (no need to construct full metadata row); override
  semantics so `SET name=...` raises a clear rename error.
- Registration: root pom modules + dependencyManagement, provisio trino.xml,
  ci.yml matrix, labeler-config, trino-server-dev, docs page + toc.
- Tests (25 green, run via `TESTING_KUBERNETES_ASSETS=.../.tools/controller-tools/envtest`):
  config, plugin smoke, type-mapper fixtures, harness lifecycle, and 17
  integration tests against a real kube-apiserver 1.36 + etcd 3.6 launched as
  child processes (`TestingKubernetesCluster`), covering typed nested reads,
  UNNEST-by-container-image, pushdown, INSERT (values + typed spec round-trip
  copy), UPDATE, DELETE with arbitrary predicates, rename rejection, CRD
  auto-discovery with full DML, unknown-table errors.
- `bin/kind-smoke-test.sh` — kind cannot run on this machine (no docker,
  AppArmor userns restriction); script is UNVALIDATED-ON-CLUSTER, connector
  validated against envtest instead (same API surface).

## Verification gates passed

- `./mvnw install -pl plugin/trino-kubernetes` — BUILD SUCCESS with all
  airbase checks (checkstyle, airstyle, sortpom, license, dependency
  analysis, modernizer, SPI-dependency check) and 25/25 tests.
- `./mvnw validate -N` green after root pom edits.

## Notable decisions / gotchas (for future work)

- Trino SPI on master: `TypeSignature` → `TypeDescriptor`, page source
  provider overload takes `MemoryContext`, `ObjectMapperProvider` deprecated
  → `JsonMapperProvider`, kudu/RowChangeParadigm.FULL_ROW no longer exist —
  merge modeled on trino-base-jdbc's JdbcMergeSink.
- Guava CacheBuilder banned → io.trino:trino-cache EvictableCacheBuilder.
- UNNEST(array(row)) flattens fields: `AS c` then `c.image` (not `AS t(c)`).
- Test classes named `*ConnectorTest` must extend BaseConnectorTest —
  standalone suite named TestKubernetesIntegration.
- envtest apiserver flags recorded in updates/apiserver-probe-notes.md.
