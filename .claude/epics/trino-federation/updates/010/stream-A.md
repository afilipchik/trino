---
issue: 010
stream: demo
agent: task-010-demo
started: 2026-08-20T06:00:00Z
completed: 2026-08-20T06:22:15Z
status: completed
---

# Task 010 — Kind demo: central + regional clusters

## What was built

- `plugin/trino-federation/kind-deploy/` — demo assets mirroring the kubernetes-connector
  demo structure:
  - `image/Dockerfile` + `image/etc/` — one image (`trino-federation:dev`) built from the
    repo's slim server (`core/trino-server-core` tarball, same 484-SNAPSHOT version as the
    plugin, so no SPI gap) plus the `memory`, `tpch`, and `federation` plugin dirs. The same
    image serves regions and central; only the mounted catalog ConfigMap differs.
  - `regions.yaml` — `trino-federation` namespace, `trino-region-east` / `trino-region-west`
    single-node Deployments + Services, each with `memory` (shard store, name matches
    `federation.remote-catalog`) and `tpch` (seed source) catalogs; 1Gi/2Gi resources.
  - `central.yaml` — `trino-central` Deployment + Service with the federation catalog:
    `connector.name=trino_federation`,
    `federation.regions=east=http://trino-region-east:8080,west=http://trino-region-west:8080`,
    `federation.remote-catalog=memory`.
  - `README.md` — demo flow, manual steps, and verification queries.
- `scripts/setup-kind-trino-federation.sh` — idempotent end-to-end script following the
  conventions of `setup-kind-trino-superset.sh`: tool checks, reuse of the existing
  `trino-superset` kind cluster when present (own `trino-federation-demo` cluster otherwise),
  containerized Maven build (worktree-aware: mounts the repo at its real path plus the git
  common dir so the git-commit-id plugin works from a git worktree), image assembly + `kind
  load`, manifest apply + rollout waits, port-forward based seeding (DROP + CTAS of disjoint
  `tpch.tiny` orders/lineitem shards split at `orderkey = 30000`), and a verification pass
  that fails loudly on any mismatch. `--skip-build` and `--delete` supported; `--delete` only
  removes the `trino-federation` namespace when the cluster is shared.

## Verification output (captured from the real run)

```
==> Verification
  orders count: east=7503 west=7497 east+west=15000 federated=15000

  grouped aggregate (pushed down to the regions, combined centrally):
    SELECT _region, count(*), sum(totalprice) FROM federation.default.orders GROUP BY _region
    east	7503	1067014012.0
    west	7497	1060382818.0

  grouped aggregate on a data column (remote GROUP BY):
    SELECT orderpriority, count(*) FROM federation.default.orders GROUP BY orderpriority
    1-URGENT	3020
    2-HIGH	3065
    3-MEDIUM	2941
    4-NOT SPECIFIED	3024
    5-LOW	2950

  _region-pruned query: WHERE _region = 'east' AND orderkey <= 424242
    count=7503 (east shard has 7503 rows)
    regions that received the marker query: east=1 west=0

  partial-aggregate SQL received by each region (source = 'trino-federation'):
    east: SELECT "orderpriority", count(*) AS "$agg_0" FROM "memory"."default"."orders" GROUP BY "orderpriority"
    east:   rows read: 7503, rows shipped: 5 (112B)
    east: SELECT count(*) AS "$agg_0", sum("totalprice") AS "$agg_1", count(*) AS "$probe" FROM "memory"."default"."orders"
    east:   rows read: 7503, rows shipped: 1 (27B)
    west: SELECT "orderpriority", count(*) AS "$agg_0" FROM "memory"."default"."orders" GROUP BY "orderpriority"
    west:   rows read: 7497, rows shipped: 5 (112B)
    west: SELECT count(*) AS "$agg_0", sum("totalprice") AS "$agg_1", count(*) AS "$probe" FROM "memory"."default"."orders"
    west:   rows read: 7497, rows shipped: 1 (27B)

==> Done
```

Notes on the evidence:

- Fan-out: the federated `count(*)` equals the sum of the disjoint regional counts.
- `GROUP BY _region` reaches each region as a global partial (`count(*)`, `sum(totalprice)`
  with no GROUP BY — `_region` is constant within a region) and each region ships exactly
  one row (27B) instead of ~7.5k rows; the connector combines the partials centrally.
- `GROUP BY orderpriority` shows a real remote GROUP BY; 5 rows shipped per region.
- Region pruning: the `_region = 'east'` query (tagged with the `orderkey <= 424242`
  marker literal, itself proof of predicate pushdown) appears in east's
  `system.runtime.queries` and never reaches west.
- Rows read vs shipped come from each region coordinator's `/v1/query/<id>` stats
  (`physicalInputPositions` / `outputPositions` / `outputDataSize`).

## How to re-run

```bash
sg docker -c "scripts/setup-kind-trino-federation.sh"               # full: build, deploy, seed, verify
sg docker -c "scripts/setup-kind-trino-federation.sh --skip-build"  # reuse trino-federation:dev
sg docker -c "scripts/setup-kind-trino-federation.sh --delete"      # tear down (namespace only on shared cluster)
```

(`sg docker -c` is only needed when the invoking shell lacks the docker group.)
Timing on this machine: ~5 min with a warm `~/.m2` (Maven 1:35, image build/load ~2 min,
deploy + seed + verify ~2 min); `--skip-build` re-run ~3 min. Deployed onto the existing
`trino-superset` kind cluster (shared with the kubernetes-connector demo) in its own
`trino-federation` namespace.
