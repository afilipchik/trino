#!/usr/bin/env bash
#
# Smoke test for the Kubernetes connector against a real kind cluster.
#
# STATUS: UNVALIDATED-ON-CLUSTER — authored on a machine without Docker
# (validated instead against envtest kube-apiserver via the module's
# integration tests). Requires: docker, kind, kubectl, curl, and a built
# trino repo (JDK 25). Run from the repo root:
#
#   plugin/trino-kubernetes/bin/kind-smoke-test.sh
#
set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-trino-k8s-smoke}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
WORK_DIR="$(mktemp -d)"
trap 'kind delete cluster --name "$CLUSTER_NAME" >/dev/null 2>&1 || true; rm -rf "$WORK_DIR"' EXIT

echo "==> Creating kind cluster $CLUSTER_NAME"
kind create cluster --name "$CLUSTER_NAME" --wait 120s
kind get kubeconfig --name "$CLUSTER_NAME" > "$WORK_DIR/kubeconfig"

echo "==> Seeding test objects"
kubectl --kubeconfig "$WORK_DIR/kubeconfig" create namespace trino-smoke
kubectl --kubeconfig "$WORK_DIR/kubeconfig" -n trino-smoke run smoke-nginx --image=nginx:1.25.3 --labels=app=web
kubectl --kubeconfig "$WORK_DIR/kubeconfig" -n trino-smoke create configmap smoke-settings --from-literal=mode=fast

echo "==> Starting Trino with the kubernetes catalog"
ETC_DIR="$WORK_DIR/etc"
mkdir -p "$ETC_DIR/catalog"
cat > "$ETC_DIR/config.properties" <<EOF
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
discovery.uri=http://127.0.0.1:8080
EOF
cat > "$ETC_DIR/node.properties" <<EOF
node.environment=test
node.data-dir=$WORK_DIR/data
EOF
cat > "$ETC_DIR/jvm.config" <<EOF
-Xmx2G
EOF
cat > "$ETC_DIR/catalog/kubernetes.properties" <<EOF
connector.name=kubernetes
kubernetes.kubeconfig-path=$WORK_DIR/kubeconfig
EOF

SERVER_TARBALL=$(ls "$REPO_ROOT"/core/trino-server/target/trino-server-*.tar.gz 2>/dev/null | head -1 || true)
if [[ -z "$SERVER_TARBALL" ]]; then
    echo "trino-server tarball not found; build it first:"
    echo "  ./mvnw install -DskipTests -pl core/trino-server -am"
    exit 1
fi
tar -xzf "$SERVER_TARBALL" -C "$WORK_DIR"
SERVER_DIR=$(ls -d "$WORK_DIR"/trino-server-*/)
"$SERVER_DIR/bin/launcher" start --etc-dir "$ETC_DIR" --data-dir "$WORK_DIR/data"
trap '"$SERVER_DIR/bin/launcher" stop --etc-dir "$ETC_DIR" --data-dir "$WORK_DIR/data" || true; kind delete cluster --name "$CLUSTER_NAME" >/dev/null 2>&1 || true; rm -rf "$WORK_DIR"' EXIT

echo "==> Waiting for Trino"
for _ in $(seq 1 60); do
    if curl -fs http://127.0.0.1:8080/v1/info | grep -q '"starting":false'; then
        break
    fi
    sleep 2
done

run_sql() {
    java -jar "$REPO_ROOT"/client/trino-cli/target/trino-cli-*-executable.jar --server http://127.0.0.1:8080 --output-format TSV --execute "$1"
}

echo "==> SELECT pods by container image"
run_sql "SELECT p.name, c.image FROM kubernetes.core.pods p CROSS JOIN UNNEST(p.spec.containers) AS c WHERE c.image LIKE 'nginx%' AND p.namespace = 'trino-smoke'" | grep smoke-nginx

echo "==> INSERT a configmap"
run_sql "INSERT INTO kubernetes.core.configmaps (name, namespace, data) VALUES ('smoke-inserted', 'trino-smoke', MAP(ARRAY['k'], ARRAY['v']))"
kubectl --kubeconfig "$WORK_DIR/kubeconfig" -n trino-smoke get configmap smoke-inserted -o jsonpath='{.data.k}' | grep -q v

echo "==> UPDATE the configmap"
run_sql "UPDATE kubernetes.core.configmaps SET data = MAP(ARRAY['mode'], ARRAY['slow']) WHERE name = 'smoke-settings' AND namespace = 'trino-smoke'"
kubectl --kubeconfig "$WORK_DIR/kubeconfig" -n trino-smoke get configmap smoke-settings -o jsonpath='{.data.mode}' | grep -q slow

echo "==> DELETE the configmap"
run_sql "DELETE FROM kubernetes.core.configmaps WHERE name = 'smoke-inserted' AND namespace = 'trino-smoke'"
if kubectl --kubeconfig "$WORK_DIR/kubeconfig" -n trino-smoke get configmap smoke-inserted >/dev/null 2>&1; then
    echo "configmap was not deleted"
    exit 1
fi

echo "==> Kubernetes connector kind smoke test PASSED"
