# Multi-region federation demo on kind

End-to-end demo of the `trino-federation` connector: two "regional" Trino
clusters (`trino-region-east`, `trino-region-west`) run as single-node
Deployments inside one kind cluster, each holding a disjoint shard of TPC-H
tiny data in a `memory` catalog. A central Trino runs the `trino-federation`
plugin with a `federation` catalog that presents both shards as one logical
catalog (every table gains a synthetic `_region` column), pushing filters,
projections, LIMIT/TopN, and aggregations down to the regions.

Requires `docker`, `kind`, `kubectl`, `curl`, and `python3`. No host JDK is
needed; the build runs in a container. The whole flow is automated by
[`scripts/setup-kind-trino-federation.sh`](../../../scripts/setup-kind-trino-federation.sh):

```bash
scripts/setup-kind-trino-federation.sh               # build, deploy, seed, verify
scripts/setup-kind-trino-federation.sh --skip-build  # reuse the trino-federation:dev image
scripts/setup-kind-trino-federation.sh --delete      # tear down
```

The script reuses the kind cluster from the kubernetes-connector demo
(`trino-superset`) when it exists — both demos share one node, this one in its
own `trino-federation` namespace — and otherwise creates a
`trino-federation-demo` cluster. The rest of this file documents the same
steps for running them by hand.

## Build the Trino image

One image serves both roles; the regions simply never mount the federation
catalog. Build the slim server plus the `memory`, `tpch`, and `federation`
plugins (in a JDK 25 container; `trino-builder` is `eclipse-temurin:25-jdk`
plus git — the explicit `-Dmaven.repo.local` matters because Java derives
`user.home` from `/etc/passwd`, not `$HOME`):

```bash
docker run --rm -u 1000:1000 -e HOME=/tmp/h \
    -v ~/.m2:/tmp/h/.m2 -v "$PWD":/repo -w /repo trino-builder:jdk25 \
    ./mvnw install -nsu -Dmaven.repo.local=/tmp/h/.m2/repository \
    -DskipTests -Dmaven.javadoc.skip=true -Dair.check.skip-all=true \
    -pl core/trino-server-main,plugin/trino-exchange-filesystem,plugin/trino-blob-cache-alluxio,plugin/trino-blob-cache-memory,plugin/trino-functions-python,plugin/trino-geospatial,plugin/trino-password-authenticators,plugin/trino-resource-group-managers,plugin/trino-session-property-managers,plugin/trino-spooling-filesystem,core/trino-server-core,plugin/trino-memory,plugin/trino-tpch,plugin/trino-federation -am
```

Server and plugin are built from the same repo at the same version, so there
is no SPI compatibility gap. Assemble the image context and build:

```bash
cd plugin/trino-federation/kind-deploy/image
tar xzf ../../../../core/trino-server-core/target/trino-server-core-*.tar.gz
mv trino-server-core-* trino-server
for p in memory tpch federation; do
    unzip -q ../../../trino-$p/target/trino-$p-*.zip
    mv trino-$p-* plugin-$p
done
docker build -t trino-federation:dev .
```

## Deploy

```bash
kind load docker-image trino-federation:dev --name <cluster>
kubectl apply -f plugin/trino-federation/kind-deploy/regions.yaml
kubectl apply -f plugin/trino-federation/kind-deploy/central.yaml
kubectl -n trino-federation rollout status deployment/trino-central
```

- `regions.yaml` — the `trino-federation` namespace, and the two regional
  Deployments + Services. Each region mounts a catalog ConfigMap with
  `memory` (the shard store — the name must match `federation.remote-catalog`)
  and `tpch` (the seed source).
- `central.yaml` — the central Deployment + Service with the single
  `federation` catalog:

  ```properties
  connector.name=trino_federation
  federation.regions=east=http://trino-region-east:8080,west=http://trino-region-west:8080
  federation.remote-catalog=memory
  ```

## Seed the regional shards

Each region gets a disjoint `orderkey` range of `tpch.tiny` (the setup script
does this through a port-forward to each regional Service):

```sql
-- on trino-region-east
CREATE TABLE memory.default.orders   AS SELECT * FROM tpch.tiny.orders   WHERE orderkey <= 30000;
CREATE TABLE memory.default.lineitem AS SELECT * FROM tpch.tiny.lineitem WHERE orderkey <= 30000;
-- on trino-region-west: the same with orderkey > 30000
```

## Verify

The setup script runs these automatically and fails if any check does not
hold; by hand, against the central cluster
(`kubectl -n trino-federation port-forward svc/trino-central 18080:8080`):

```sql
-- fan-out: matches the sum of the regional counts
SELECT count(*) FROM federation.default.orders;

-- grouped aggregate, combined centrally from per-region partials
SELECT _region, count(*), sum(totalprice) FROM federation.default.orders GROUP BY _region;

-- region pruning: only region east receives this query
SELECT count(*) FROM federation.default.orders WHERE _region = 'east';
```

Pushdown evidence lives on the regions: the exact SQL each region received is
in `system.runtime.queries` under `source = 'trino-federation'` (e.g.
`SELECT count(*), sum("totalprice") FROM "memory"."default"."orders"` for the
grouped aggregate — one result row shipped instead of the whole shard), and
rows-read vs rows-shipped counts come from the coordinator's
`/v1/query/<query_id>` detail API.
