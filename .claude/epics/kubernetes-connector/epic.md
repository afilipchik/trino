---
name: kubernetes-connector
status: completed
created: 2026-08-17T07:06:06Z
updated: 2026-08-17T07:43:29Z
progress: 100%
prd: .claude/prds/kubernetes-connector.md
github: (skipped — no gh CLI on this machine; local-only tracking)
---

# Epic: kubernetes-connector

## Overview

New in-tree plugin `plugin/trino-kubernetes` (connector name `kubernetes`).
Dynamic catalog: schemas/tables/columns are discovered live from the target
cluster's discovery + OpenAPI v3 endpoints; rows are the JSON objects decoded
into native Trino structural types; mutations flow back as REST create /
replace / delete calls.

## Architecture Decisions

- **No fabric8 / official k8s client.** A minimal internal REST client
  (`KubernetesClient`) built on JDK `HttpClient` + Jackson + airlift PEM
  utilities. Rationale: the connector is fully dynamic (raw JSON + OpenAPI
  schemas), typed client models add nothing; avoids dependency-convergence
  fights with airbase; keeps TLS/kubeconfig handling explicit.
- **Schema = API group** (`core` alias for the legacy group), **table =
  resource plural**. Preferred group version only. Cluster-scoped and
  namespaced resources both included; only resources with verbs `list`+`get`.
- **Types from OpenAPI v3** (`/openapi/v3/<path>`): recursive JSON-schema →
  Trino type mapping with cycle detection → fallback to Trino `JSON` type.
  Field name case preserved; dereference in SQL is case-insensitive.
- **Mutations via merge SPI** (like kudu): synthetic row-id column (varchar
  JSON `{"namespace","name","resourceVersion"}`); RowChangeParadigm
  FULL_ROW → UPDATE issues a PUT (replace) built from the full new row;
  DELETE issues a DELETE call. INSERT via ConnectorPageSink → POST.
  Optimistic concurrency: replace carries the read resourceVersion.
- **Value codec symmetry**: one component (`JsonTrinoConverter`-style) maps
  JSON→Block for reads and Trino values→JSON for writes, sharing the type
  mapping so INSERT/UPDATE round-trips are lossless.
- **Testing without Docker**: `TestingKubernetesCluster` spawns real
  `etcd` + `kube-apiserver` (envtest 1.36 binaries) as child processes with
  static-token auth + AlwaysAllow; integration tests use
  DistributedQueryRunner in-JVM. A `kind` smoke script ships for
  docker-capable machines (UNVALIDATED-ON-CLUSTER here).

## Technical Approach

### Connector core (read path)
- `KubernetesPlugin`, `KubernetesConnectorFactory`, Guice module, config
  (`KubernetesConfig`: kubeconfig path OR api-server-uri/token/ca/insecure,
  cache TTL, page size).
- `KubernetesMetadata`: listSchemaNames (groups), listTables (resources),
  getTableHandle/getTableMetadata/getColumnHandles from cached discovery +
  OpenAPI type mapper; `applyFilter` extracts namespace/name equality into
  the table handle.
- `KubernetesSplitManager`: one split per table scan (list API paginates).
- `KubernetesPageSourceProvider`/`KubernetesPageSource`: streams list pages,
  decodes JSON into Blocks via the converter; appends synthetic row-id.

### Mutation path
- `beginInsert`/`finishInsert` + `KubernetesPageSink`: row → JSON → POST.
- `getMergeRowIdColumnHandle`, `getRowChangeParadigm`(FULL_ROW),
  `beginMerge`/`finishMerge` + `KubernetesMergeSink`: op channel → POST /
  PUT / DELETE per row.

### Infrastructure
- Register module in root `pom.xml` and server provisio descriptor.
- Docs page `docs/src/main/sphinx/connector/kubernetes.md` + toc entry.
- `plugin/trino-kubernetes/bin/kind-smoke-test.sh` for docker-capable hosts.

## Implementation Strategy

Scaffold → client+discovery → type mapping → read path → tests
(envtest harness early, since everything integration-tests against it) →
mutations → CRD coverage → docs/polish. Iterate `./mvnw install
-pl plugin/trino-kubernetes` until green with airbase checks on.

## Task Breakdown Preview

1. Module scaffold + build registration (pom, Plugin, factory, config, docs stub)
2. Testing harness: TestingKubernetesCluster (etcd+apiserver) + kubectl fixtures
3. Kubernetes REST client + kubeconfig/TLS auth
4. Discovery + OpenAPI → Trino type mapping
5. Read path (metadata, splits, page source, pushdown)
6. Insert path (page sink)
7. Update/Delete via merge SPI
8. CRD end-to-end coverage + BaseConnectorTest-style suite
9. Docs + kind smoke script + final polish (checkstyle, license, README)

## Dependencies

- Built: trino-main/trino-spi/trino-testing 484-SNAPSHOT in local .m2 (done)
- envtest binaries + kubectl in `.tools` (done); JDK 25 (done)

## Success Criteria (Technical)

- `./mvnw install -pl plugin/trino-kubernetes` green including airbase checks
- Integration proof: typed nested predicate query (UNNEST containers,
  `c.image` filter), CRD table auto-discovery, INSERT/UPDATE/DELETE verified
  against the live apiserver via the raw API
- No new unmanaged dependency versions in the build

## Estimated Effort

~1 overnight autonomous session; the long poles are merge-SPI correctness and
airbase/checkstyle compliance.

## Tasks Created
- [x] 001.md - Module scaffold and build registration (parallel: true)
- [x] 002.md - TestingKubernetesCluster harness (envtest) (parallel: true)
- [x] 003.md - Kubernetes REST client and auth (parallel: true)
- [x] 004.md - Discovery and OpenAPI-to-Trino type mapping (parallel: false)
- [x] 005.md - Read path — metadata, splits, page source, pushdown (parallel: false)
- [x] 006.md - Insert path — page sink (parallel: true)
- [x] 007.md - Update and Delete via merge SPI (parallel: true)
- [x] 008.md - CRD end-to-end coverage and full integration suite (parallel: false)
- [x] 009.md - Docs, kind smoke script, final polish (parallel: false)

Total tasks: 9 | Parallel: 5 | Sequential: 4
