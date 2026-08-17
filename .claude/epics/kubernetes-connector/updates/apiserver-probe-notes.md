# envtest apiserver probe findings (2026-08-17)

Validated flags (kube-apiserver 1.36.2, etcd 3.6.8, ~3s to ready):

```
etcd --data-dir D --listen-client-urls http://127.0.0.1:P1 \
     --advertise-client-urls http://127.0.0.1:P1 --listen-peer-urls http://127.0.0.1:P2
kube-apiserver --etcd-servers=http://127.0.0.1:P1 --secure-port=P3 --bind-address=127.0.0.1 \
  --cert-dir=CERTS (self-signed serving certs auto-generated) \
  --token-auth-file=tokens.csv   # line: testtoken,testuser,testuid,"system:masters"
  --authorization-mode=AlwaysAllow \
  --service-account-issuer=https://kubernetes.default.svc \
  --service-account-key-file=sa.pub --service-account-signing-key-file=sa.key \
  --disable-admission-plugins=ServiceAccount --allow-privileged=true
```

- Readiness: poll GET /readyz with bearer token until HTTP 200 (one poststarthook
  can transiently fail; keep polling, don't parse body).
- OpenAPI v3: GET /openapi/v3 → {paths: {"api/v1": {serverRelativeURL: "/openapi/v3/api/v1?hash=..."}}};
  per-group doc has components.schemas keyed like io.k8s.api.core.v1.Pod.
- Resolve resource schema generically: group doc paths["/api/v1/namespaces/{namespace}/pods"].get
  .responses.200.content."application/json".schema.$ref → PodList → properties.items.items.$ref → Pod.
  Cluster-scoped path form: /apis/GROUP/VERSION/RESOURCE or /api/v1/RESOURCE.
- $refs frequently wrapped: {"allOf":[{"$ref":...}], "default":{}} — unwrap single-element allOf.
- Time: {type: string, format: date-time} → TIMESTAMP(3) WITH TIME ZONE.
- Quantity: oneOf[string, number] → VARCHAR (canonical string form).
- labels/annotations: object + additionalProperties string → MAP(varchar,varchar).
- Aggregated discovery v2 Accept header got 406 on this build — use classic discovery:
  GET /api (core versions), /api/v1 (APIResourceList), /apis (APIGroupList with preferredVersion),
  /apis/<group>/<version> (APIResourceList). Filter resources: no '/' in name, verbs contain list+get.
- Create works via POST /api/v1/namespaces/default/configmaps with Content-Type: application/json.
