#!/usr/bin/env bash
#
# Launches a local kind cluster running Trino (built from this repo, with the
# kubernetes catalog) and Apache Superset pointed at it. Works on macOS and
# Linux; only bash 3.2 features are used.
#
# Requires: docker, kind, kubectl, helm. A host JDK 25 is used when available,
# otherwise the build runs inside an eclipse-temurin container.
#
# Usage:
#   scripts/setup-kind-trino-superset.sh                  # build, deploy, wait
#   scripts/setup-kind-trino-superset.sh --multi-cluster  # add a second kind
#                                                         # cluster and serve
#                                                         # both through one
#                                                         # catalog (cluster
#                                                         # column + fan-out)
#   scripts/setup-kind-trino-superset.sh --skip-build     # reuse trino-k8s:dev
#   scripts/setup-kind-trino-superset.sh --delete         # tear everything down
#
# After it finishes:
#   Superset  http://localhost:30088  (admin/admin)
#   Trino     http://localhost:30080  (user: any, catalog: kubernetes)
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEPLOY_DIR="$REPO_ROOT/plugin/trino-kubernetes/kind-deploy"
CLUSTER_NAME="${CLUSTER_NAME:-trino-superset}"
WEST_CLUSTER_NAME="${WEST_CLUSTER_NAME:-${CLUSTER_NAME}-west}"
TRINO_IMAGE="trino-k8s:dev"
BUILDER_IMAGE="trino-builder:jdk25"
SUPERSET_PORT=30088
TRINO_PORT=30080

MULTI_CLUSTER=false
SKIP_BUILD=false
DELETE=false
for arg in "$@"; do
    case "$arg" in
        --multi-cluster) MULTI_CLUSTER=true ;;
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

if [ "$DELETE" = true ]; then
    log "Deleting kind clusters"
    kind delete cluster --name "$CLUSTER_NAME" 2>/dev/null || true
    kind delete cluster --name "$WEST_CLUSTER_NAME" 2>/dev/null || true
    exit 0
fi

for tool in docker kind kubectl helm; do
    command -v "$tool" >/dev/null 2>&1 || fail "$tool is required but not installed"
done
docker info >/dev/null 2>&1 || fail "docker daemon is not reachable"

# On Linux hosts, kind clusters exhaust the default inotify limits quickly and
# the control plane fails with "Failed to create inotify object: Too many open
# files". macOS runs kind inside the Docker Desktop VM, which has its own limits.
if [ "$(uname -s)" = "Linux" ]; then
    inotify_instances="$(sysctl -n fs.inotify.max_user_instances 2>/dev/null || echo 0)"
    if [ "$inotify_instances" -lt 256 ]; then
        message="fs.inotify.max_user_instances=$inotify_instances is too low for kind; raise it with:
       sudo sysctl fs.inotify.max_user_instances=512 fs.inotify.max_user_watches=1048576"
        if [ "$MULTI_CLUSTER" = true ]; then
            fail "$message"
        fi
        echo "WARNING: $message" >&2
    fi
fi

WORK_DIR="$(mktemp -d)"
trap 'rm -rf "$WORK_DIR"' EXIT

# ---------------------------------------------------------------------------
# Build the Trino server and the kubernetes plugin
# ---------------------------------------------------------------------------

# Modules needed by the slim server (core/trino-server-core) plus this plugin.
BUILD_MODULES=core/trino-server-main,plugin/trino-exchange-filesystem,plugin/trino-blob-cache-alluxio,plugin/trino-blob-cache-memory,plugin/trino-functions-python,plugin/trino-geospatial,plugin/trino-password-authenticators,plugin/trino-resource-group-managers,plugin/trino-session-property-managers,plugin/trino-spooling-filesystem,core/trino-server-core,plugin/trino-kubernetes

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
        # -Dmaven.repo.local matters: in the container Java derives user.home
        # from /etc/passwd, not $HOME, so Maven would otherwise use a throwaway
        # repository.
        docker run --rm -u "$(id -u):$(id -g)" -e HOME=/tmp/h \
            -v "$HOME/.m2":/tmp/h/.m2 -v "$REPO_ROOT":/repo -w /repo "$BUILDER_IMAGE" \
            ./mvnw install -nsu -Dmaven.repo.local=/tmp/h/.m2/repository \
            -DskipTests -Dmaven.javadoc.skip=true -Dair.check.skip-all=true \
            -pl "$BUILD_MODULES" -am
    fi
}

build_image() {
    log "Assembling the Trino image"
    server_tarball="$(ls "$REPO_ROOT"/core/trino-server-core/target/trino-server-core-*.tar.gz 2>/dev/null | head -1 || true)"
    plugin_zip="$(ls "$REPO_ROOT"/plugin/trino-kubernetes/target/trino-kubernetes-*.zip 2>/dev/null | head -1 || true)"
    [ -n "$server_tarball" ] || fail "trino-server-core tarball not found; run without --skip-build"
    [ -n "$plugin_zip" ] || fail "trino-kubernetes plugin zip not found; run without --skip-build"

    context="$WORK_DIR/image"
    mkdir -p "$context"
    cp "$DEPLOY_DIR/image/Dockerfile" "$context/"
    cp -R "$DEPLOY_DIR/image/etc" "$context/etc"
    tar xzf "$server_tarball" -C "$context"
    mv "$context"/trino-server-core-* "$context/trino-server"
    (cd "$context" && unzip -q "$plugin_zip")
    mv "$context"/trino-kubernetes-* "$context/plugin-kubernetes"
    docker build -t "$TRINO_IMAGE" "$context"
}

if [ "$SKIP_BUILD" = true ]; then
    docker image inspect "$TRINO_IMAGE" >/dev/null 2>&1 || fail "--skip-build given but image $TRINO_IMAGE does not exist"
else
    build_trino
    build_image
fi

# ---------------------------------------------------------------------------
# Primary kind cluster (NodePorts mapped so localhost works on macOS)
# ---------------------------------------------------------------------------

if ! kind get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME"; then
    log "Creating kind cluster $CLUSTER_NAME"
    cat > "$WORK_DIR/kind-config.yaml" <<EOF
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
    extraPortMappings:
      - containerPort: $SUPERSET_PORT
        hostPort: $SUPERSET_PORT
      - containerPort: $TRINO_PORT
        hostPort: $TRINO_PORT
EOF
    kind create cluster --name "$CLUSTER_NAME" --config "$WORK_DIR/kind-config.yaml" --wait 120s
else
    log "Reusing existing kind cluster $CLUSTER_NAME"
fi
KUBECTL=(kubectl --context "kind-$CLUSTER_NAME")

log "Loading $TRINO_IMAGE into $CLUSTER_NAME"
kind load docker-image "$TRINO_IMAGE" --name "$CLUSTER_NAME"

log "Deploying Trino"
"${KUBECTL[@]}" apply -f "$DEPLOY_DIR/trino.yaml"

# ---------------------------------------------------------------------------
# Optional second cluster served through the same catalog (multi-cluster mode)
# ---------------------------------------------------------------------------

if [ "$MULTI_CLUSTER" = true ]; then
    if ! kind get clusters 2>/dev/null | grep -qx "$WEST_CLUSTER_NAME"; then
        log "Creating second kind cluster $WEST_CLUSTER_NAME"
        kind create cluster --name "$WEST_CLUSTER_NAME" --wait 120s
    else
        log "Reusing existing kind cluster $WEST_CLUSTER_NAME"
    fi

    log "Building the multi-cluster kubeconfig"
    # Context "west": the second cluster, reached over the shared docker
    # network. The node's internal IP is in the API server certificate SANs;
    # the container name is not resolvable from pods, so use the IP.
    kind get kubeconfig --internal --name "$WEST_CLUSTER_NAME" > "$WORK_DIR/west-context.yaml"
    # the internal kubeconfig's server name only resolves on the docker network,
    # so ask through the host-side context kind registered
    west_ip="$(kubectl --context "kind-$WEST_CLUSTER_NAME" get nodes \
        -o 'jsonpath={.items[0].status.addresses[?(@.type=="InternalIP")].address}')"
    [ -n "$west_ip" ] || fail "could not determine the internal IP of $WEST_CLUSTER_NAME"
    west_ca="$(kubectl --kubeconfig "$WORK_DIR/west-context.yaml" config view --raw \
        -o 'jsonpath={.clusters[0].cluster.certificate-authority-data}')"
    west_client_certificate="$(kubectl --kubeconfig "$WORK_DIR/west-context.yaml" config view --raw \
        -o 'jsonpath={.users[0].user.client-certificate-data}')"
    west_client_key="$(kubectl --kubeconfig "$WORK_DIR/west-context.yaml" config view --raw \
        -o 'jsonpath={.users[0].user.client-key-data}')"
    [ -n "$west_ca" ] && [ -n "$west_client_certificate" ] && [ -n "$west_client_key" ] \
        || fail "could not extract credentials for $WEST_CLUSTER_NAME"

    # Context "local": the cluster Trino runs in, through the in-cluster
    # endpoint and the mounted service account credentials.
    cat > "$WORK_DIR/kubeconfig" <<EOF
apiVersion: v1
kind: Config
current-context: local
clusters:
  - name: local
    cluster:
      server: https://kubernetes.default.svc
      certificate-authority: /var/run/secrets/trino-sa/ca.crt
  - name: west
    cluster:
      server: https://$west_ip:6443
      certificate-authority-data: $west_ca
users:
  - name: local
    user:
      tokenFile: /var/run/secrets/trino-sa/token
  - name: west
    user:
      client-certificate-data: $west_client_certificate
      client-key-data: $west_client_key
contexts:
  - name: local
    context:
      cluster: local
      user: local
  - name: west
    context:
      cluster: west
      user: west
EOF

    log "Installing the kubeconfig and switching the catalog to multi-cluster"
    "${KUBECTL[@]}" -n trino create secret generic trino-kubeconfig \
        --from-file=config="$WORK_DIR/kubeconfig" \
        --dry-run=client -o yaml | "${KUBECTL[@]}" apply -f -

    cat > "$WORK_DIR/kubernetes.properties" <<EOF
connector.name=kubernetes
kubernetes.kubeconfig-path=/etc/trino/kubeconfig/config
kubernetes.multi-cluster.enabled=true
kubernetes.default-namespace=default
EOF
    "${KUBECTL[@]}" -n trino create configmap trino-catalog \
        --from-file=kubernetes.properties="$WORK_DIR/kubernetes.properties" \
        --dry-run=client -o yaml | "${KUBECTL[@]}" apply -f -
fi

# restart so catalog changes are picked up whether this run switched the
# catalog to multi-cluster or back to the single-cluster default
"${KUBECTL[@]}" -n trino rollout restart deployment/trino >/dev/null

log "Waiting for Trino"
"${KUBECTL[@]}" -n trino rollout status deployment/trino --timeout=300s

# ---------------------------------------------------------------------------
# Superset
# ---------------------------------------------------------------------------

log "Deploying Superset"
helm repo add superset https://apache.github.io/superset >/dev/null 2>&1 || true
helm repo update superset >/dev/null
helm upgrade --install superset superset/superset \
    --kube-context "kind-$CLUSTER_NAME" \
    -n superset --create-namespace \
    -f "$DEPLOY_DIR/superset-values.yaml" \
    --timeout 15m --wait

log "Registering the Trino database in Superset"
"${KUBECTL[@]}" exec -n superset deploy/superset -- superset set-database-uri \
    -d Trino-Kubernetes -u trino://trino@trino.trino.svc.cluster.local:8080/kubernetes

# ---------------------------------------------------------------------------
# Smoke check: run one query through the Trino HTTP API
# ---------------------------------------------------------------------------

log "Waiting for Trino to accept queries on localhost:$TRINO_PORT"
for _ in $(seq 1 60); do
    if curl -fs "http://localhost:$TRINO_PORT/v1/info" 2>/dev/null | grep -q '"starting":false'; then
        break
    fi
    sleep 2
done

run_sql() {
    response="$(curl -fs -X POST -H "X-Trino-User: setup-script" \
        --data "$1" "http://localhost:$TRINO_PORT/v1/statement")" || return 1
    for _ in $(seq 1 120); do
        if echo "$response" | grep -q '"error"'; then
            echo "$response" >&2
            return 1
        fi
        next_uri="$(echo "$response" | sed -n 's/.*"nextUri":"\([^"]*\)".*/\1/p')"
        if [ -z "$next_uri" ]; then
            return 0
        fi
        sleep 0.5
        response="$(curl -fs "$next_uri")" || return 1
    done
    return 1
}

if [ "$MULTI_CLUSTER" = true ]; then
    SMOKE_QUERY="SELECT cluster, count(*) FROM kubernetes.core.pods GROUP BY cluster"
else
    SMOKE_QUERY="SELECT count(*) FROM kubernetes.core.pods"
fi
if run_sql "$SMOKE_QUERY"; then
    log "Smoke query succeeded: $SMOKE_QUERY"
else
    fail "smoke query failed: $SMOKE_QUERY"
fi

echo
log "Done"
echo "  Superset:  http://localhost:$SUPERSET_PORT  (admin/admin)"
echo "  Trino:     http://localhost:$TRINO_PORT  (any user, catalog 'kubernetes')"
echo
echo "  Try in Superset SQL Lab:"
echo "    SELECT p.name, p.namespace, c.image"
echo "    FROM kubernetes.core.pods p"
echo "    CROSS JOIN UNNEST(p.spec.containers) AS c;"
if [ "$MULTI_CLUSTER" = true ]; then
    echo
    echo "  Both clusters are served through the one catalog; every table has a"
    echo "  'cluster' column ('local' and 'west'):"
    echo "    SELECT cluster, count(*) FROM kubernetes.core.pods GROUP BY cluster;"
fi
echo
echo "  Tear down with: scripts/setup-kind-trino-superset.sh --delete"
