# Trino + Superset on kind with the Kubernetes connector

End-to-end demo environment: a kind cluster running Trino (built from this repo,
with the `kubernetes` catalog authenticating in-cluster via a service account)
and Apache Superset pointed at it. Requires `docker`, `kind`, `kubectl`, and
`helm`. No host JDK is needed; the build runs in a container.

The whole flow below is automated by
[`scripts/setup-kind-trino-superset.sh`](../../../scripts/setup-kind-trino-superset.sh),
which also maps the NodePorts to localhost (so it works on macOS with Docker
Desktop) and, with `--multi-cluster`, adds a second kind cluster served through
the same catalog via the connector's `kubernetes.multi-cluster.enabled` mode:

```bash
scripts/setup-kind-trino-superset.sh                  # Superset on :30088, Trino on :30080
scripts/setup-kind-trino-superset.sh --multi-cluster  # plus a second cluster ('local'/'west')
scripts/setup-kind-trino-superset.sh --delete         # tear down
```

The rest of this file documents the same steps for running them by hand.

## Build the Trino image

Build the slim server plus this plugin (in a JDK 25 container; `trino-builder`
is `eclipse-temurin:25-jdk` plus git):

```bash
docker run --rm -u 1000:1000 -e HOME=/tmp/h \
    -v ~/.m2:/tmp/h/.m2 -v "$PWD":/repo -w /repo trino-builder:jdk25 \
    ./mvnw install -nsu -Dmaven.repo.local=/tmp/h/.m2/repository \
    -DskipTests -Dmaven.javadoc.skip=true -Dair.check.skip-all=true \
    -pl core/trino-server-main,plugin/trino-exchange-filesystem,plugin/trino-blob-cache-alluxio,plugin/trino-blob-cache-memory,plugin/trino-functions-python,plugin/trino-geospatial,plugin/trino-password-authenticators,plugin/trino-resource-group-managers,plugin/trino-session-property-managers,plugin/trino-spooling-filesystem,core/trino-server-core,plugin/trino-kubernetes -am
```

The explicit `-Dmaven.repo.local` matters: Java derives `user.home` from
`/etc/passwd`, not `$HOME`, so without it Maven uses a throwaway repository
inside the container.

Assemble the image context and build:

```bash
cd plugin/trino-kubernetes/kind-deploy/image
tar xzf ../../../../core/trino-server-core/target/trino-server-core-*.tar.gz
mv trino-server-core-* trino-server
unzip -q ../../target/trino-kubernetes-*.zip
mv trino-kubernetes-* plugin-kubernetes
docker build -t trino-k8s:dev .
```

## Deploy

```bash
kind create cluster --name trino-superset
kind load docker-image trino-k8s:dev --name trino-superset
kubectl apply -f plugin/trino-kubernetes/kind-deploy/trino.yaml
```

`trino.yaml` creates the `trino` namespace, a service account with a
cluster-admin binding (demo only — scope it down for anything real), a
long-lived token secret whose token and CA feed the catalog through
`kubernetes.api-server-uri`/`kubernetes.token`/`kubernetes.ca-certificate-path`,
and a single-replica deployment with zero-downtime rollouts.

## Superset

```bash
helm repo add superset https://apache.github.io/superset
helm install superset superset/superset -n superset --create-namespace \
    -f plugin/trino-kubernetes/kind-deploy/superset-values.yaml
kubectl exec -n superset deploy/superset -- superset set-database-uri \
    -d Trino-Kubernetes -u trino://trino@trino.trino.svc.cluster.local:8080/kubernetes
```

Login is admin/admin (change it). The values file carries two important pieces:

- `bootstrapScript` installs the `trino` driver with
  `uv pip install --python /app/.venv/bin/python` — plain `pip` installs into
  system Python, invisible to the app's uv-built virtualenv.
- `configOverrides.trino_named_rows` patches the Trino engine spec so ROW
  values render as named JSON objects in SQL Lab instead of positional arrays
  (Trino's wire protocol drops row field names; the trino client's
  NamedRowTuple carries them, and the hook re-serializes with names).

The service is a NodePort on 30088, so the UI is reachable from the host at
`http://<kind-node-ip>:30088` without a port-forward.
