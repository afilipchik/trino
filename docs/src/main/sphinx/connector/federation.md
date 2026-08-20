# Federation connector

The Federation connector presents a dataset that is sharded across several
regional Trino clusters as a single catalog. Every table in the catalog is the
`UNION ALL` of the table with the same name on each region, so applications
query one logical table while the data stays in place. The connector fans
queries out to the regions over the Trino client protocol and pushes
predicates, column pruning, `LIMIT`, `ORDER BY ... LIMIT`, and many
aggregations down to the regions, so only reduced data crosses region
boundaries.

The connector is read-only.

## Requirements

To federate regional Trino clusters, you need:

- Network access from the Trino coordinator and workers to the HTTP(S)
  endpoint of every regional cluster.
- Regional clusters running the same Trino version as the federating cluster.
- A catalog with the same name, schemas, tables, and column types on every
  region. The regions are assumed to hold homogeneous shards of the same
  tables.
- A user, and optionally a password, accepted by every regional cluster.

## Configuration

Create a catalog properties file that specifies the Federation connector by
setting the `connector.name` to `trino_federation`, and list the regional
clusters and the catalog to query on them:

```text
connector.name=trino_federation
federation.regions=us-east=http://trino-us-east.example.com:8080,eu-west=http://trino-eu-west.example.com:8080
federation.remote-catalog=sales
```

The following table contains a list of all available configuration properties.

:::{list-table} Federation configuration properties
:widths: 40, 60
:header-rows: 1

* - Property name
  - Description
* - `federation.regions`
  - Ordered, comma-separated list of `name=uri` pairs naming the regional
    Trino clusters, for example
    `us-east=http://trino-a:8080,eu-west=http://trino-b:8080`. Region names
    must be unique, and each URI must use `http` or `https`. The names are the
    values of the [`_region` column](federation-region-column). At least one
    region is required.
* - `federation.remote-catalog`
  - Name of the catalog to query on the regional clusters. Required.
* - `federation.user`
  - User sent to the regional clusters. Defaults to `federation`.
* - `federation.password`
  - Password sent to the regional clusters. Optional; when set, the regional
    clusters must be accessed over HTTPS.
* - `federation.connect-timeout`
  - [Duration](prop-type-duration) to wait when establishing a connection to
    a regional cluster. Defaults to `10s`.
* - `federation.request-timeout`
  - [Duration](prop-type-duration) to wait for individual requests to a
    regional cluster. Defaults to `30s`.
* - `federation.fanout-threads`
  - Maximum number of concurrent regional queries for an aggregated scan.
    Defaults to `16`.
:::

## Schemas and tables

The regions are assumed to hold homogeneous shards: the same remote catalog
with the same schemas, tables, and column types on every region. Schema,
table, and column listings are read from the first reachable region in the
configured order and cached for ten seconds. At scan time the connector
validates the column types reported by each region against that metadata, and
a query fails with a type mismatch error if a region diverges.

Columns with [unsupported types](federation-type-mapping) do not appear in
table listings. A remote column named `_region` is hidden, because it is
shadowed by the synthetic column of the same name.

(federation-region-column)=
## The `_region` column

Every table has a synthetic `_region` column (`VARCHAR`) holding the name of
the region each row was read from, as configured in `federation.regions`. The
column can be selected, filtered, and grouped on like any other column.

A predicate on `_region` prunes the fan-out so only the matching regions are
contacted:

```sql
SELECT order_id, price
FROM example.store.orders
WHERE _region = 'us-east';
```

## Querying

A plain query reads all regions in parallel and returns the union of their
rows:

```sql
SELECT order_id, price
FROM example.store.orders;
```

Cross-region aggregation is a plain SQL query. In the following example each
region computes its own count, one pre-aggregated row per region crosses the
region boundary, and the connector combines the partial counts into the final
values:

```sql
SELECT _region, count(*)
FROM example.store.orders
GROUP BY _region;
```

(federation-pushdown)=
## Pushdown

The connector supports pushdown of the following operations:

- [Column pruning](projection-pushdown)
- {ref}`limit-pushdown`
- {ref}`topn-pushdown`

{ref}`Aggregate pushdown <aggregation-pushdown>` for the following functions:

- {func}`count`, also `count(*)`
- {func}`sum` of `BIGINT`, `REAL`, `DOUBLE`, and `DECIMAL` columns
- {func}`min` and {func}`max`
- {func}`avg` of `DOUBLE` and `REAL` columns

```{include} pushdown-correctness-behavior.fragment
```

With `LIMIT` and `ORDER BY ... LIMIT` (Top-N) pushdown each region pre-reduces
its rows to at most the requested count, and the engine performs the final
limit or sort over the pre-reduced regional streams.

### Aggregation fan-out

A pushed-down aggregation runs as a single fan-out operation: the connector
sends a partial aggregation query, such as
`SELECT category, count(*), sum(price) ... GROUP BY category`, to every active
region concurrently — at most `federation.fanout-threads` regions at a time —
and combines the partial results inside the connector into final values, for
example by summing the per-region counts, taking the minimum of the per-region
minimums, and computing `avg` from per-region sum and count pairs. Only
pre-aggregated rows leave each region. `GROUP BY` clauses that include the
`_region` column are also pushed down; the other grouping columns are grouped
remotely and the region name is attached to each region's groups.

The following aggregation shapes are not pushed down. They always return
correct results: the engine ships the raw rows and aggregates them centrally.

- Aggregations with `DISTINCT`, `FILTER`, or an `ORDER BY` clause, including
  `count(DISTINCT ...)`.
- {func}`avg` of `DECIMAL` or integer columns, because combining per-region
  partial averages cannot reproduce the exact result.
- {func}`sum` of `TINYINT`, `SMALLINT`, and `INTEGER` columns, because the
  engine plans them as `sum` over a cast that blocks the pushdown.
- Aggregation functions other than the listed ones.
- Aggregations with an argument or a grouping column of type
  `TIMESTAMP WITH TIME ZONE`.
- Multiple grouping sets, such as `CUBE` and `ROLLUP`.
- Aggregations over an already pushed-down `LIMIT` or `ORDER BY ... LIMIT`.

### Predicate pushdown support

Predicates that constrain a column to a set of values or ranges, such as `=`,
`!=`, `IN`, `<`, `BETWEEN`, and `IS NULL`, are pushed down for all
[supported types](federation-type-mapping) except
`TIMESTAMP WITH TIME ZONE`. Predicates the connector cannot render as remote
SQL — for example `LIKE` and other expressions, or comparisons with infinite
`REAL` and `DOUBLE` values — are evaluated by the engine instead.

(federation-type-mapping)=
## Type mapping

The regions are Trino clusters, so column types map to themselves. The
connector supports the following types:

- `BOOLEAN`
- `TINYINT`
- `SMALLINT`
- `INTEGER`
- `BIGINT`
- `REAL`
- `DOUBLE`
- `DECIMAL`
- `VARCHAR`
- `VARBINARY`
- `DATE`
- `TIME`
- `TIMESTAMP`
- `TIMESTAMP WITH TIME ZONE`

Columns of any other remote type, such as `ARRAY`, `MAP`, `ROW`, `CHAR`,
`JSON`, or `UUID`, do not appear in table listings.

## Limitations

- The connector is read-only. `INSERT`, `UPDATE`, `DELETE`, and DDL statements
  are not supported.
- The regions must hold homogeneous shards: the same remote catalog, schemas,
  tables, and column types on every region, on the same Trino version as the
  federating cluster.
- All regions are accessed with the single static user and optional password
  from the catalog configuration; the identity of the querying user is not
  propagated.
- Join pushdown is not supported. Joins run on the federating cluster over
  the shipped rows.
- Changes to `federation.regions` require reloading the catalog.
