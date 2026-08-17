---
name: kubernetes-connector
description: Trino connector exposing Kubernetes primitives and CRDs as strongly-typed SQL tables with full read + mutation support
status: active
created: 2026-08-17T07:06:06Z
---

# PRD: kubernetes-connector

## Executive Summary

A new Trino connector plugin (`plugin/trino-kubernetes`, catalog connector name
`kubernetes`) that exposes a Kubernetes cluster's API as a SQL catalog. Every
API resource — core primitives (pods, services, configmaps, …), group
resources (deployments, jobs, ingresses, …) and Custom Resources (CRDs) — is a
table. Object fields are mapped to *native Trino types* derived from the
cluster's OpenAPI v3 schemas (ROW/ARRAY/MAP/VARCHAR/BIGINT/TIMESTAMP/…), so
queries can dereference nested structure directly, e.g.:

```sql
SELECT metadata.name, c.image
FROM kubernetes.core.pods
CROSS JOIN UNNEST(spec.containers) AS t(c)
WHERE c.image LIKE 'nginx%';
```

Mutations are first-class: `INSERT` creates objects, `UPDATE` patches them,
`DELETE` deletes them.

## Problem Statement

Operators reach for `kubectl ... -o json | jq` pipelines to answer questions
that are naturally relational ("which pods run image X", "join deployments to
their pods", "count restarts per node"). Kubernetes API objects are structured
and schema-described, which maps cleanly onto Trino's structural type system.
No in-tree Trino connector exposes Kubernetes.

## User Stories

1. **As an SRE**, I can `SELECT` over any resource kind, including CRDs, with
   typed nested columns — acceptance: `DESCRIBE kubernetes.apps.deployments`
   shows `spec` as a typed `row(...)`; predicates on nested fields work.
2. **As an SRE**, I can query across namespaces and filter by
   `metadata.namespace` / `metadata.name` efficiently — acceptance: those
   predicates are pushed into the API call (field selector / scoped list).
3. **As a platform engineer**, my CRDs appear automatically as tables in a
   schema named after their API group, typed from the CRD's OpenAPI schema —
   acceptance: creating a CRD + CRs then querying them works without any
   connector config.
4. **As an operator**, I can mutate the cluster from SQL — acceptance:
   `INSERT INTO configmaps ...` creates an object; `UPDATE ... SET ... WHERE`
   patches matching objects; `DELETE FROM ... WHERE` deletes them; changes are
   visible via kubectl.

## Functional Requirements

- Schema = Kubernetes API group (`core` for the legacy `v1` group, otherwise
  the group name, e.g. `apps`, `batch`, `networking.k8s.io`, plus CRD groups).
  Table = resource plural name (`pods`, `deployments`). Preferred version per
  group is used. Subresources (`pods/log`, `*/status`) are not tables.
- Columns = top-level object fields from the OpenAPI v3 schema (`metadata`,
  `spec`, `status`, `data`, …) mapped recursively:
  object→ROW, array→ARRAY, `additionalProperties`→MAP, string→VARCHAR,
  `date-time`→TIMESTAMP WITH TIME ZONE, int32→INTEGER, int64→BIGINT,
  number→DOUBLE, boolean→BOOLEAN, int-or-string→VARCHAR, untyped or
  recursive → JSON.
- Reads: full list with pagination; pushdown of `metadata.namespace` and
  `metadata.name` equality into API requests.
- Mutations: INSERT (create), UPDATE (replace/patch), DELETE (delete) via the
  Trino merge SPI so arbitrary WHERE clauses work.
- Auth: kubeconfig file (client certs, bearer token, CA), or explicit
  config properties (api server URI + token + TLS settings). In-cluster
  service account credentials supported via the same properties.
- `SHOW SCHEMAS`, `SHOW TABLES`, `DESCRIBE` all reflect live discovery;
  schema/table listing cached with configurable TTL.

## Non-Functional Requirements

- Follows Trino in-tree conventions: airbase checks, checkstyle, license
  headers; module registered in root pom and server provisio descriptor.
- No heavyweight Kubernetes client dependency: a minimal REST client on JDK
  HTTP/airlift + Jackson (avoids dependency-convergence pain, and the
  connector is fully dynamic anyway).
- Integration tests must run on this machine (no Docker): a real
  kube-apiserver + etcd (envtest binaries) launched as child processes.

## Success Criteria

- `./mvnw install -pl plugin/trino-kubernetes` passes with airbase checks and
  all tests green.
- Integration tests prove: schema discovery incl. CRDs; typed nested queries
  (UNNEST over containers, predicate on `container.image`); INSERT, UPDATE,
  DELETE round-trips verified through the Kubernetes API.
- A `kind`-ready smoke-test script + docs page ship with the connector
  (kind cannot run on this dev machine — validated against envtest instead;
  the script is marked UNVALIDATED-ON-CLUSTER per project convention).

## Constraints & Assumptions

- This machine has no Docker/kind (AppArmor userns restriction, no
  passwordless sudo) — envtest kube-apiserver 1.36 + etcd 3.6 binaries at
  `/media/afilipchik/nvme6tb/src/local/.tools/controller-tools/envtest` are
  the cluster substitute. `kubectl` v1.36.3 available.
- Trino master (484-SNAPSHOT), JDK 25 (Temurin, local at `.tools/jdk-25.0.4+7`).
- No `gh` CLI — CCPM GitHub sync phase is skipped; tasks tracked locally.
- Watch-based streaming, exec/attach, pod logs, aggregated metrics are
  assumed out of the initial cut.

## Out of Scope

- `pods/log`, `exec`, port-forward, events streaming/watch.
- Label-selector pushdown DSL (labels are queryable as a MAP column;
  engine-side filtering).
- Multi-cluster federation in one catalog (use one catalog per cluster).
- CREATE/DROP TABLE (CRD lifecycle management via SQL DDL).
- Exec-plugin kubeconfig auth (cloud provider credential helpers).

## Dependencies

- Trino SPI/merge machinery (in-tree), airlift, Jackson (managed versions).
- envtest binaries (etcd, kube-apiserver) for tests; kubectl for the kind
  script.
