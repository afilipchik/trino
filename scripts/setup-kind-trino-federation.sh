#!/usr/bin/env bash
#
# Multi-region federation demo on kind: two "regional" Trino clusters
# (trino-region-east / trino-region-west), each holding a disjoint shard of
# TPC-H tiny data in a memory catalog, plus a central Trino running the
# trino-federation plugin with a `federation` catalog fanning out to both.
#
# Requires: docker, kind, kubectl, curl, python3. A host JDK 25 is used when
# available, otherwise the build runs inside an eclipse-temurin container.
#
# Usage:
#   scripts/setup-kind-trino-federation.sh               # build, deploy, seed, verify
#   scripts/setup-kind-trino-federation.sh --skip-build  # reuse trino-federation:dev
#   scripts/setup-kind-trino-federation.sh --delete      # tear the demo down
#
# The demo reuses the kind cluster from the kubernetes-connector demo
# (trino-superset) when it exists, otherwise it creates its own cluster
# (trino-federation-demo). Everything lives in the trino-federation namespace.
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEPLOY_DIR="$REPO_ROOT/plugin/trino-federation/kind-deploy"
TRINO_IMAGE="trino-federation:dev"
BUILDER_IMAGE="trino-builder:jdk25"
NAMESPACE="trino-federation"
# orderkey split point between the regional shards of tpch.tiny (orderkeys
# run 1..60000; this puts roughly half the rows in each region)
SHARD_SPLIT=30000
# local ports used for the temporary port-forwards during seeding/verification
CENTRAL_PORT="${CENTRAL_PORT:-18080}"
EAST_PORT="${EAST_PORT:-18081}"
WEST_PORT="${WEST_PORT:-18082}"

SKIP_BUILD=false
DELETE=false
for arg in "$@"; do
    case "$arg" in
        --skip-build) SKIP_BUILD=true ;;
        --delete) DELETE=true ;;
        *) echo "Unknown option: $arg" >&2; exit 1 ;;
    esac
done

log() {
    echo "==> $*"
}

fail() {
    echo "ERROR: $*" >&2
    exit 1
}

for tool in docker kind kubectl curl python3; do
    command -v "$tool" >/dev/null 2>&1 || fail "$tool is required but not installed"
done
docker info >/dev/null 2>&1 || fail "docker daemon is not reachable"

# Reuse the kubernetes-connector demo cluster when present so both demos share
# one kind node; otherwise use a dedicated cluster.
if [ -z "${CLUSTER_NAME:-}" ]; then
    if kind get clusters 2>/dev/null | grep -qx "trino-superset"; then
        CLUSTER_NAME=trino-superset
    else
        CLUSTER_NAME=trino-federation-demo
    fi
fi
KUBECTL=(kubectl --context "kind-$CLUSTER_NAME")

if [ "$DELETE" = true ]; then
    if [ "$CLUSTER_NAME" = "trino-federation-demo" ]; then
        log "Deleting kind cluster $CLUSTER_NAME"
        kind delete cluster --name "$CLUSTER_NAME" 2>/dev/null || true
    else
        log "Deleting namespace $NAMESPACE from shared cluster $CLUSTER_NAME"
        "${KUBECTL[@]}" delete namespace "$NAMESPACE" --ignore-not-found
    fi
    exit 0
fi

WORK_DIR="$(mktemp -d)"
PORT_FORWARD_PIDS=""
cleanup() {
    # shellcheck disable=SC2086
    [ -z "$PORT_FORWARD_PIDS" ] || kill $PORT_FORWARD_PIDS 2>/dev/null || true
    rm -rf "$WORK_DIR"
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# Build the Trino server and the memory, tpch, and federation plugins
# ---------------------------------------------------------------------------

# Modules needed by the slim server (core/trino-server-core) plus the plugins
# baked into the demo image.
BUILD_MODULES=core/trino-server-main,plugin/trino-exchange-filesystem,plugin/trino-blob-cache-alluxio,plugin/trino-blob-cache-memory,plugin/trino-functions-python,plugin/trino-geospatial,plugin/trino-password-authenticators,plugin/trino-resource-group-managers,plugin/trino-session-property-managers,plugin/trino-spooling-filesystem,core/trino-server-core,plugin/trino-memory,plugin/trino-tpch,plugin/trino-federation

host_jdk_is_usable() {
    command -v java >/dev/null 2>&1 || return 1
    java_major="$(java -version 2>&1 | sed -n 's/.*version "\([0-9]*\).*/\1/p' | head -1)"
    [ -n "$java_major" ] && [ "$java_major" -ge 25 ]
}

build_trino() {
    if host_jdk_is_usable; then
        log "Building Trino with the host JDK"
        (cd "$REPO_ROOT" && ./mvnw install -nsu -DskipTests \
            -Dmaven.javadoc.skip=true -Dair.check.skip-all=true \
            -pl "$BUILD_MODULES" -am)
    else
        log "No host JDK 25; building inside a container"
        if ! docker image inspect "$BUILDER_IMAGE" >/dev/null 2>&1; then
            log "Creating builder image $BUILDER_IMAGE"
            printf 'FROM eclipse-temurin:25-jdk\nRUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*\n' \
                | docker build -t "$BUILDER_IMAGE" -
        fi
        mkdir -p "$HOME/.m2"
        # The repo is mounted at its real absolute path: when $REPO_ROOT is a
        # git worktree, its .git is a file pointing into the main repo, which
        # the git-commit-id plugin needs to see too (second mount below).
        mounts=(-v "$REPO_ROOT":"$REPO_ROOT")
        if [ -f "$REPO_ROOT/.git" ] && command -v git >/dev/null 2>&1; then
            git_common_dir="$(git -C "$REPO_ROOT" rev-parse --path-format=absolute --git-common-dir)"
            mounts+=(-v "$git_common_dir":"$git_common_dir")
        fi
        # -Dmaven.repo.local matters: in the container Java derives user.home
        # from /etc/passwd, not $HOME, so Maven would otherwise use a throwaway
        # repository.
        docker run --rm -u "$(id -u):$(id -g)" -e HOME=/tmp/h \
            -v "$HOME/.m2":/tmp/h/.m2 "${mounts[@]}" -w "$REPO_ROOT" "$BUILDER_IMAGE" \
            ./mvnw install -nsu -Dmaven.repo.local=/tmp/h/.m2/repository \
            -DskipTests -Dmaven.javadoc.skip=true -Dair.check.skip-all=true \
            -pl "$BUILD_MODULES" -am
    fi
}

build_image() {
    log "Assembling the Trino image"
    server_tarball="$(ls "$REPO_ROOT"/core/trino-server-core/target/trino-server-core-*.tar.gz 2>/dev/null | head -1 || true)"
    [ -n "$server_tarball" ] || fail "trino-server-core tarball not found; run without --skip-build"

    context="$WORK_DIR/image"
    mkdir -p "$context"
    cp "$DEPLOY_DIR/image/Dockerfile" "$context/"
    cp -R "$DEPLOY_DIR/image/etc" "$context/etc"
    tar xzf "$server_tarball" -C "$context"
    mv "$context"/trino-server-core-* "$context/trino-server"
    for plugin in memory tpch federation; do
        plugin_zip="$(ls "$REPO_ROOT/plugin/trino-$plugin/target/trino-$plugin"-*.zip 2>/dev/null | head -1 || true)"
        [ -n "$plugin_zip" ] || fail "trino-$plugin plugin zip not found; run without --skip-build"
        (cd "$context" && unzip -q "$plugin_zip")
        mv "$context/trino-$plugin"-* "$context/plugin-$plugin"
    done
    docker build -t "$TRINO_IMAGE" "$context"
}

if [ "$SKIP_BUILD" = true ]; then
    docker image inspect "$TRINO_IMAGE" >/dev/null 2>&1 || fail "--skip-build given but image $TRINO_IMAGE does not exist"
else
    build_trino
    build_image
fi

# ---------------------------------------------------------------------------
# Cluster and deployments
# ---------------------------------------------------------------------------

if ! kind get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME"; then
    log "Creating kind cluster $CLUSTER_NAME"
    kind create cluster --name "$CLUSTER_NAME" --wait 120s
else
    log "Reusing existing kind cluster $CLUSTER_NAME"
fi

log "Loading $TRINO_IMAGE into $CLUSTER_NAME"
kind load docker-image "$TRINO_IMAGE" --name "$CLUSTER_NAME"

log "Deploying the regional and central Trino clusters"
"${KUBECTL[@]}" apply -f "$DEPLOY_DIR/regions.yaml"
"${KUBECTL[@]}" apply -f "$DEPLOY_DIR/central.yaml"

# restart so a freshly loaded image or changed catalog is picked up
for deployment in trino-region-east trino-region-west trino-central; do
    "${KUBECTL[@]}" -n "$NAMESPACE" rollout restart "deployment/$deployment" >/dev/null
done
for deployment in trino-region-east trino-region-west trino-central; do
    log "Waiting for $deployment"
    "${KUBECTL[@]}" -n "$NAMESPACE" rollout status "deployment/$deployment" --timeout=300s
done

# ---------------------------------------------------------------------------
# Port-forwards (regions are only needed for seeding and verification)
# ---------------------------------------------------------------------------

start_port_forward() {
    "${KUBECTL[@]}" -n "$NAMESPACE" port-forward "svc/$1" "$2:8080" >/dev/null 2>&1 &
    PORT_FORWARD_PIDS="$PORT_FORWARD_PIDS $!"
}

log "Starting port-forwards (central :$CENTRAL_PORT, east :$EAST_PORT, west :$WEST_PORT)"
start_port_forward trino-central "$CENTRAL_PORT"
start_port_forward trino-region-east "$EAST_PORT"
start_port_forward trino-region-west "$WEST_PORT"

CENTRAL="http://localhost:$CENTRAL_PORT"
EAST="http://localhost:$EAST_PORT"
WEST="http://localhost:$WEST_PORT"

wait_ready() {
    for _ in $(seq 1 90); do
        if curl -fs "$1/v1/info" 2>/dev/null | grep -q '"starting":false'; then
            return 0
        fi
        sleep 2
    done
    fail "Trino at $1 did not become ready"
}

for url in "$CENTRAL" "$EAST" "$WEST"; do
    wait_ready "$url"
done

# Runs one statement through the Trino HTTP API and prints the result rows
# tab-separated. Fails loudly (message on stderr, non-zero exit) on any query
# error.
run_sql() {
    python3 - "$1" "$2" <<'PYEOF'
import json
import sys
import time
import urllib.error
import urllib.request

base, sql = sys.argv[1], sys.argv[2]
headers = {"X-Trino-User": "federation-demo", "X-Trino-Source": "setup-script"}

def get(request):
    for attempt in range(5):
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                return json.loads(response.read())
        except urllib.error.HTTPError as e:
            if e.code == 503 and attempt < 4:
                time.sleep(1)
                continue
            raise
    raise AssertionError("unreachable")

result = get(urllib.request.Request(base + "/v1/statement", data=sql.encode(), headers=headers))
rows = []
deadline = time.time() + 300
while True:
    if "error" in result:
        print("query failed: %s\n%s" % (json.dumps(result["error"].get("message")), sql), file=sys.stderr)
        sys.exit(1)
    rows.extend(result.get("data") or [])
    next_uri = result.get("nextUri")
    if not next_uri:
        break
    if time.time() > deadline:
        print("query timed out: %s" % sql, file=sys.stderr)
        sys.exit(1)
    result = get(urllib.request.Request(next_uri, headers=headers))
for row in rows:
    print("\t".join("NULL" if value is None else str(value) for value in row))
PYEOF
}

# Prints raw input rows vs output rows of a finished query on a region, from
# the coordinator's query detail API — the rows-shipped evidence.
query_stats() {
    python3 - "$1" "$2" <<'PYEOF'
import json
import sys
import urllib.request

base, query_id = sys.argv[1], sys.argv[2]
request = urllib.request.Request(
    "%s/v1/query/%s" % (base, query_id),
    headers={"X-Trino-User": "federation"})
with urllib.request.urlopen(request, timeout=60) as response:
    info = json.loads(response.read())
stats = info["queryStats"]
print("rows read: %s, rows shipped: %s (%s)" % (
    stats["physicalInputPositions"], stats["outputPositions"], stats["outputDataSize"]))
PYEOF
}

# ---------------------------------------------------------------------------
# Seed each region with its disjoint shard of tpch.tiny
# ---------------------------------------------------------------------------

seed_region() {
    region_url="$1"
    predicate="$2"
    for table in orders lineitem; do
        run_sql "$region_url" "DROP TABLE IF EXISTS memory.default.$table" >/dev/null
        run_sql "$region_url" "CREATE TABLE memory.default.$table AS SELECT * FROM tpch.tiny.$table WHERE $predicate" >/dev/null
    done
}

log "Seeding region east (orderkey <= $SHARD_SPLIT)"
seed_region "$EAST" "orderkey <= $SHARD_SPLIT"
log "Seeding region west (orderkey > $SHARD_SPLIT)"
seed_region "$WEST" "orderkey > $SHARD_SPLIT"

# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------

echo
log "Verification"

east_count="$(run_sql "$EAST" "SELECT count(*) FROM memory.default.orders")"
west_count="$(run_sql "$WEST" "SELECT count(*) FROM memory.default.orders")"
global_count="$(run_sql "$CENTRAL" "SELECT count(*) FROM federation.default.orders")"
echo "  orders count: east=$east_count west=$west_count east+west=$((east_count + west_count)) federated=$global_count"
[ "$east_count" -gt 0 ] || fail "east shard is empty"
[ "$west_count" -gt 0 ] || fail "west shard is empty"
[ "$global_count" -eq $((east_count + west_count)) ] || fail "federated count does not match the sum of the regional counts"

echo
echo "  grouped aggregate (pushed down to the regions, combined centrally):"
echo "    SELECT _region, count(*), sum(totalprice) FROM federation.default.orders GROUP BY _region"
grouped="$(run_sql "$CENTRAL" "SELECT _region, count(*) AS orders, round(sum(totalprice)) AS total FROM federation.default.orders GROUP BY _region ORDER BY _region")"
echo "$grouped" | sed 's/^/    /'
[ "$(echo "$grouped" | wc -l)" -eq 2 ] || fail "expected one group per region"

# grouping on a data column reaches the regions as a real remote GROUP BY
# (grouping on _region alone does not: it is constant within a region)
echo
echo "  grouped aggregate on a data column (remote GROUP BY):"
echo "    SELECT orderpriority, count(*) FROM federation.default.orders GROUP BY orderpriority"
priorities="$(run_sql "$CENTRAL" "SELECT orderpriority, count(*) AS orders FROM federation.default.orders GROUP BY orderpriority ORDER BY orderpriority")"
echo "$priorities" | sed 's/^/    /'
[ "$(echo "$priorities" | wc -l)" -eq 5 ] || fail "expected the five TPC-H order priorities"

# The 424242 literal is a marker: it shows up in the SQL each region receives,
# proving both predicate pushdown and (below) that region pruning kept the
# query away from west entirely.
echo
echo "  _region-pruned query: WHERE _region = 'east' AND orderkey <= 424242"
filtered_count="$(run_sql "$CENTRAL" "SELECT count(*) FROM federation.default.orders WHERE _region = 'east' AND orderkey <= 424242")"
echo "    count=$filtered_count (east shard has $east_count rows)"
[ "$filtered_count" -eq "$east_count" ] || fail "_region-filtered count does not match the east shard"

east_marker="$(run_sql "$EAST" "SELECT count(*) FROM system.runtime.queries WHERE source = 'trino-federation' AND query LIKE '%424242%'")"
west_marker="$(run_sql "$WEST" "SELECT count(*) FROM system.runtime.queries WHERE source = 'trino-federation' AND query LIKE '%424242%'")"
echo "    regions that received the marker query: east=$east_marker west=$west_marker"
[ "$east_marker" -ge 1 ] || fail "east did not receive the _region-filtered query"
[ "$west_marker" -eq 0 ] || fail "west was not pruned from the _region-filtered query"

echo
echo "  partial-aggregate SQL received by each region (source = 'trino-federation'):"
for region in "east|$EAST" "west|$WEST"; do
    name="${region%%|*}"
    url="${region#*|}"
    received="$(run_sql "$url" "SELECT query_id, query FROM system.runtime.queries WHERE source = 'trino-federation' AND (query LIKE '%GROUP BY%' OR query LIKE '%sum(%') ORDER BY created DESC LIMIT 2")"
    [ -n "$received" ] || fail "region $name has no partial-aggregate query in system.runtime.queries"
    echo "$received" | while IFS="$(printf '\t')" read -r query_id query_text; do
        stats="$(query_stats "$url" "$query_id")" || exit 1
        echo "    $name: $query_text"
        echo "    $name:   $stats"
    done
done

echo
log "Done"
echo "  Central Trino:  ${KUBECTL[*]} -n $NAMESPACE port-forward svc/trino-central 18080:8080"
echo "  then e.g.:      SELECT _region, count(*) FROM federation.default.orders GROUP BY _region"
echo
echo "  Tear down with: scripts/setup-kind-trino-federation.sh --delete"
