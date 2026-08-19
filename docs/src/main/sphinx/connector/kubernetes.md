# Kubernetes connector

The Kubernetes connector exposes the resources of a
[Kubernetes](https://kubernetes.io/) cluster as SQL tables. Every API group is a
schema (the legacy core group appears as `core`), every listable resource —
including custom resources from CRDs — is a table, and object fields are mapped
to native Trino types derived from the cluster's OpenAPI v3 schemas. The
connector supports reads and mutations: `INSERT` creates objects, `UPDATE`
replaces them, and `DELETE` removes them.

## Requirements

To connect to a Kubernetes cluster, you need:

- Network access from the Trino coordinator and workers to the Kubernetes API
  server.
- Credentials with permission to list and get the resources you want to query,
  and to create, update, or delete objects if you use mutations.

## Configuration

Create a catalog properties file that specifies the Kubernetes connector by
setting the `connector.name` to `kubernetes`, and either point it at a
kubeconfig file:

```text
connector.name=kubernetes
kubernetes.kubeconfig-path=/etc/kubernetes/kubeconfig
```

or configure the API server directly:

```text
connector.name=kubernetes
kubernetes.api-server-uri=https://kubernetes.example.com:6443
kubernetes.token=${ENV:KUBERNETES_TOKEN}
kubernetes.ca-certificate-path=/etc/kubernetes/ca.crt
```

The following table contains a list of all available configuration properties.

:::{list-table} Kubernetes configuration properties
:widths: 40, 60
:header-rows: 1

* - Property name
  - Description
* - `kubernetes.kubeconfig-path`
  - Path to a kubeconfig file used to locate and authenticate to the cluster.
    The current context's cluster address, certificate authority, bearer token,
    and client certificate credentials are used. Exactly one of
    `kubernetes.kubeconfig-path` or `kubernetes.api-server-uri` must be set.
* - `kubernetes.kubeconfig-context`
  - Kubeconfig context to use instead of the current context. Point several
    catalogs at the same kubeconfig with different contexts to expose several
    clusters as separate catalogs. Requires `kubernetes.kubeconfig-path`.
* - `kubernetes.multi-cluster.enabled`
  - Serve all kubeconfig contexts through this one catalog. Every table gains a
    `cluster` column holding the context name, and queries fan out to all
    clusters. See [](kubernetes-multiple-clusters). Requires
    `kubernetes.kubeconfig-path`; defaults to `false`.
* - `kubernetes.api-server-uri`
  - URI of the Kubernetes API server, for example `https://127.0.0.1:6443`.
* - `kubernetes.token`
  - Bearer token used to authenticate to the API server, for example a service
    account token.
* - `kubernetes.ca-certificate-path`
  - Path to a PEM file with the certificate authority of the API server.
* - `kubernetes.insecure-tls`
  - Skip verification of the API server TLS certificate. Defaults to `false`.
* - `kubernetes.default-namespace`
  - Namespace used for inserted objects that do not specify one. Defaults to
    `default`.
* - `kubernetes.metadata-cache-ttl`
  - How long to cache API discovery and OpenAPI schema information. New
    resource types, such as freshly registered CRDs, appear after the cache
    expires. Defaults to `1m`.
* - `kubernetes.list-page-size`
  - Number of objects requested per page from the API server list endpoints.
    Defaults to `500`.
:::

## Schemas and tables

Schemas map to API groups, and tables map to resource plural names. The legacy
core group (`pods`, `services`, `configmaps`, ...) appears as the `core`
schema. Group names containing dots must be quoted:

```sql
SHOW SCHEMAS FROM example;
SHOW TABLES FROM example.core;
SELECT * FROM example."networking.k8s.io".ingresses;
```

Custom resource definitions are discovered automatically: their group appears
as a schema and their resources as tables, typed from the CRD's structural
schema.

(kubernetes-multiple-clusters)=
## Multiple clusters

A kubeconfig file with several contexts can serve more than one cluster, in
either of two ways.

To expose each cluster as its own catalog, create one catalog properties file
per cluster and select the context with `kubernetes.kubeconfig-context`:

```text
connector.name=kubernetes
kubernetes.kubeconfig-path=/etc/kubernetes/kubeconfig
kubernetes.kubeconfig-context=production-west
```

To query all clusters through a single catalog, enable multi-cluster mode:

```text
connector.name=kubernetes
kubernetes.kubeconfig-path=/etc/kubernetes/kubeconfig
kubernetes.multi-cluster.enabled=true
```

In multi-cluster mode every table gains a synthetic `cluster` column
(`VARCHAR`) holding the kubeconfig context name of the cluster each object was
read from. Reads fan out to every context in parallel, so cross-cluster
aggregation is a plain SQL query:

```sql
SELECT cluster, count(*) FROM example.core.pods GROUP BY cluster;
```

An equality predicate on `cluster` prunes the fan-out so only the matching
cluster is contacted:

```sql
SELECT name FROM example.core.pods WHERE cluster = 'production-west';
```

`INSERT` rows are routed to the cluster named by the inserted `cluster` value,
defaulting to the current context's cluster when the column is not set.
`UPDATE` and `DELETE` are routed to the cluster each row was read from; setting
`cluster` to a different value in `UPDATE` fails, because objects cannot move
between clusters.

Table and column metadata comes from the default cluster (the kubeconfig
current context). A resource that is not served by some cluster — for example
a CRD installed on only one of them — simply contributes no rows from the
clusters that lack it. All contexts must use bearer token or client certificate
credentials, and every cluster is queried with the single set of credentials
its context names, so restrict the catalog appropriately.

The multi-cluster catalog is usually the better experience for BI tools: one
connection, one schema tree, and the cluster is just another column to filter
and group by. Prefer separate catalogs when clusters need different access
controls or very different schema shapes.

## Type mapping

Columns are derived from the OpenAPI v3 schema of each resource:

:::{list-table}
:widths: 40, 60
:header-rows: 1

* - Kubernetes schema type
  - Trino type
* - `object` with properties
  - `ROW` (field name case is preserved)
* - `object` with `additionalProperties`, such as labels and annotations
  - `MAP(VARCHAR, ...)`
* - `array`
  - `ARRAY`
* - `string`
  - `VARCHAR`
* - `string` with `date-time` format, such as `creationTimestamp`
  - `TIMESTAMP(3)` (Kubernetes serializes all timestamps in UTC; the plain
    timestamp type keeps `ROW` columns castable to `JSON` with
    `json_format(CAST(spec AS JSON))`)
* - `string` with `byte` format
  - `VARBINARY`
* - `integer`
  - `INTEGER` or `BIGINT` depending on format
* - `number`
  - `DOUBLE`
* - `boolean`
  - `BOOLEAN`
* - Quantities and int-or-string values
  - `VARCHAR`
* - Recursive or unconstrained schemas
  - `JSON`
:::

In addition to the object fields, every table has synthetic `name` and
`namespace` columns (`VARCHAR`) derived from the object metadata. Equality
predicates on them are pushed down into the API server request as a scoped
list and field selector.

Every table also has a synthetic `manifest` column (`VARCHAR`) holding the raw
JSON of the whole object, both as a readable escape hatch alongside the typed
columns and as a writable target for partial inserts.

In [multi-cluster catalogs](kubernetes-multiple-clusters), every table
additionally has a synthetic `cluster` column (`VARCHAR`) with the kubeconfig
context name the object was read from.

## Querying

Nested fields use standard row dereference, and arrays can be expanded with
`UNNEST`:

```sql
SELECT p.name, c.image
FROM example.core.pods p
CROSS JOIN UNNEST(p.spec.containers) AS c
WHERE c.image LIKE 'nginx%';

SELECT name
FROM example.core.pods
WHERE element_at(metadata.labels, 'app') = 'web';
```

## Mutations

`INSERT` creates objects. The synthetic `name` and `namespace` columns can be
used instead of constructing the full `metadata` row:

```sql
INSERT INTO example.core.configmaps (name, namespace, data)
VALUES ('app-settings', 'default', MAP(ARRAY['mode'], ARRAY['fast']));
```

Typed values read from one object can be inserted into another:

```sql
INSERT INTO example.core.pods (name, namespace, spec)
SELECT 'pod-copy', 'staging', spec
FROM example.core.pods
WHERE name = 'pod-original' AND namespace = 'production';
```

Because Trino row literals are positional and require every field, use the
`manifest` column to create objects from partial JSON instead of spelling out
the full typed row. The manifest becomes the base object, missing `apiVersion`
and `kind` are filled in from the table, and typed columns overlay it:

```sql
INSERT INTO example.core.pods (name, namespace, manifest)
VALUES ('busybox', 'default',
    '{"spec": {"containers": [{"name": "busybox", "image": "busybox:1.36", "command": ["sleep", "3600"]}]}}');
```

`UPDATE ... SET manifest = ...` replaces the whole object with the given
manifest, keeping the object's name, namespace, and `resourceVersion` guard.

`UPDATE` replaces the changed objects. The update is guarded by the object's
`resourceVersion` read during the scan, so concurrent modifications fail
instead of being overwritten:

```sql
UPDATE example.core.configmaps
SET data = MAP(ARRAY['mode'], ARRAY['slow'])
WHERE name = 'app-settings';
```

`DELETE` removes all objects matching the `WHERE` clause, which can use any
column:

```sql
DELETE FROM example.core.pods
WHERE element_at(metadata.labels, 'app') = 'legacy';
```

Renaming objects or moving them across namespaces with `UPDATE` is not
supported, because `metadata.name` and `metadata.namespace` are immutable in
Kubernetes.

## Limitations

- Subresources such as `pods/log`, `exec`, and `status` are not exposed as
  tables, and `status` updates through the main resource are ignored by the
  API server for resources with a status subresource.
- `CREATE TABLE`, `CREATE SCHEMA`, and other DDL statements are not supported;
  the table set is defined by the cluster.
- Exec-plugin based kubeconfig authentication, such as cloud provider
  credential helpers, is not supported. Use a bearer token or client
  certificates.
