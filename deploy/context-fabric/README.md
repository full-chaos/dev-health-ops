# Context Fabric container integration

Context Fabric (the private Agent Context Runtime, or ACR) is opt-in. The
`dev-health-ops` entrypoints in this directory consume the deployment artifacts
from a sibling `dev-health-acr` checkout; they do not copy the ACR chart or
redefine the Ops stack.

Keep the repositories as siblings for the default paths:

```text
workspace/
  dev-health-ops/
  dev-health-acr/
  dev-health-web/
```

Set `DEV_HEALTH_ACR_DIR` or `DEV_HEALTH_WEB_DIR` when the checkouts are elsewhere.

## Supported container paths

| Path | ACR workload | Intended use |
| --- | --- | --- |
| Docker Compose | `acr-api` container plus host-local `acr-mcp` | Complete local service and plugin testing |
| Kubernetes with Helm | `acr-api` Deployment plus gated migration Job | Deploy ACR beside an existing Ops environment |
| ACR Kustomize overlays | `acr-api` Deployment plus migration Job | ACR-owned operator workflow; run from `dev-health-acr` |
| Docker Swarm | Not defined | Do not infer ACR support from the Ops Swarm stack |

`acr-mcp` is always a local STDIO process. It is never deployed as a Compose
service, Kubernetes Pod, or sidecar container.

## Docker Compose: full local plugin fixture

From `dev-health-ops`:

```bash
bash scripts/context-fabric-local.sh
```

The launcher renders the selected checkout's real `compose.yml`, takes the Ops
services required by the Context Fabric test lifecycle, and layers
`dev-health-acr/deploy/compose/acr.compose.yml` plus generated TLS and secret
configuration. It starts a uniquely named `acr-e2e-*` Compose project so an
already-running default `dev-health` project and its volumes are not modified.
There is no checked-in replacement copy of the Ops Compose stack.

The fixture creates an organization, grants `agent_context_runtime`, seeds
`acme/live-e2e`, starts `acr-api` in Docker, builds the host-local `acr-mcp`,
verifies both read tools, rotates the ACR credential, and prints a generated
`client.env` path. In another shell:

```bash
source <printed-client-env-path>
acr-mcp doctor --live
acr-mcp metadata
```

Then install one bundled client package from
`$DEV_HEALTH_ACR_DIR/clients/<client>/README.md`. Use this explicit test request:

> Use Context Fabric for repository `acme/live-e2e` on branch `main`. Call
> `context_for_task`, then expand one returned evidence ID with
> `source_evidence`.

Press Ctrl-C in the launcher shell to remove only its project-scoped containers,
volumes, network, credentials, and generated environment.

## Kubernetes: deploy the ACR service beside Ops

The Ops-owned launcher uses the canonical private chart at
`$DEV_HEALTH_ACR_DIR/deploy/helm/acr` and the non-secret integration values in
`helm-values.yaml`.

### Required infrastructure

Before installing ACR, provide all of the following:

- an immutable `acr-api` image reference ending in `@sha256:<digest>`;
- a TLS-verified PostgreSQL database with separate ACR migration and runtime
  roles;
- a TLS-native, read-only ClickHouse endpoint, normally port `9440`;
- an HTTPS Dev Health Ops origin that exposes the entitlement API;
- the `agent_context_runtime` entitlement on every organization that will use
  ACR; and
- the existing Kubernetes Secrets listed below.

The stock Ops Kubernetes `dev-health-api` Service is HTTP and the stock
ClickHouse manifest exposes plaintext ports. Those defaults are not sufficient
for ACR. Front Ops with an HTTPS Gateway or service-mesh endpoint, and use a
TLS-enabled ClickHouse deployment or managed service. The Ops Kubernetes stack
also does not provision ACR's PostgreSQL database.

### Existing Secret contract

Create these Secrets in the target namespace, `dev-health` by default. Use an
external secret controller in shared environments. For local evaluation, create
them from owner-readable files so values do not enter shell history.

| Secret | Required keys |
| --- | --- |
| `acr-runtime-credentials` | `ACR_POSTGRES_DSN`, `ACR_CLICKHOUSE_DSN`, `ACR_EVIDENCE_ID_ACTIVE_KID`, `ACR_EVIDENCE_ID_KEYS` |
| `acr-migration-credentials` | `ACR_POSTGRES_MIGRATION_DSN` |
| `acr-entitlement-token` | `token` |
| `acr-postgres-ca` | `ca.crt` |
| `acr-clickhouse-ca` | `ca.crt` |
| `acr-entitlement-ca` | `ca.crt` |
| `acr-registry-pull` | `.dockerconfigjson` |

The runtime PostgreSQL role must be least privilege; the migration role owns the
ACR schema and is used only by the migration Job. The ClickHouse DSN must use a
read-only user and certificate verification. Keep the runtime and migration
PostgreSQL DSNs in different Secret/key references.

An example file-backed Secret application, assuming the files already exist:

```bash
namespace=dev-health
secret_dir="${CONTEXT_FABRIC_SECRET_DIR:?set CONTEXT_FABRIC_SECRET_DIR}"

kubectl -n "$namespace" create secret generic acr-runtime-credentials \
  --from-file=ACR_POSTGRES_DSN="$secret_dir/runtime-postgres-dsn" \
  --from-file=ACR_CLICKHOUSE_DSN="$secret_dir/clickhouse-dsn" \
  --from-file=ACR_EVIDENCE_ID_ACTIVE_KID="$secret_dir/evidence-active-kid" \
  --from-file=ACR_EVIDENCE_ID_KEYS="$secret_dir/evidence-keys" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl -n "$namespace" create secret generic acr-migration-credentials \
  --from-file=ACR_POSTGRES_MIGRATION_DSN="$secret_dir/migration-postgres-dsn" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl -n "$namespace" create secret generic acr-entitlement-token \
  --from-file=token="$secret_dir/ops-entitlement-token" \
  --dry-run=client -o yaml | kubectl apply -f -

for dependency in postgres clickhouse entitlement; do
  kubectl -n "$namespace" create secret generic "acr-${dependency}-ca" \
    --from-file=ca.crt="$secret_dir/${dependency}-ca.crt" \
    --dry-run=client -o yaml | kubectl apply -f -
done

kubectl -n "$namespace" create secret generic acr-registry-pull \
  --type=kubernetes.io/dockerconfigjson \
  --from-file=.dockerconfigjson="$secret_dir/dockerconfig.json" \
  --dry-run=client -o yaml | kubectl apply -f -
```

Create the Ops service credential without printing it to the terminal:

```bash
umask 077
kubectl -n dev-health exec deployment/dev-health-api -c api -- \
  dev-hops service-credentials create --service acr --scope entitlements:read \
  > "$secret_dir/ops-entitlement-token"
```

Grant the product entitlement separately for the organization:

```bash
kubectl -n dev-health exec deployment/dev-health-api -c api -- \
  dev-hops admin bundles assign-org \
  --org-id "$ORG_ID" \
  --feature-key agent_context_runtime \
  --reason 'Context Fabric Kubernetes evaluation' \
  --expires-days 1
```

### Render and apply

Render the exact chart and values without contacting a cluster:

```bash
bash scripts/context-fabric-kubernetes.sh render \
  --image "$ACR_IMAGE" \
  --entitlement-url "$OPS_HTTPS_ORIGIN" \
  > /tmp/context-fabric.yaml
```

Install or upgrade after the namespace and all required Secrets exist:

```bash
bash scripts/context-fabric-kubernetes.sh apply \
  --image "$ACR_IMAGE" \
  --entitlement-url "$OPS_HTTPS_ORIGIN"

bash scripts/context-fabric-kubernetes.sh status
```

`apply` runs the ACR chart's migration hook before the Deployment and uses
`--atomic --wait`. A failed migration does not produce a ready API rollout.
Mutable image tags, plain-HTTP entitlement URLs, missing Secrets, and injected
MCP workloads fail closed.

### Test a host plugin against Kubernetes

Create a repository-scoped client credential. The command stores the one-time
value in a mode-`0600` file and does not echo it:

```bash
token_dir="${XDG_CONFIG_HOME:-$HOME/.config}/dev-health/context-fabric"
install -d -m 700 "$token_dir"

bash scripts/context-fabric-kubernetes.sh create-credential \
  --org-id "$ORG_ID" \
  --repository acme/repository \
  --output "$token_dir/acr-token"
```

Build or install `acr-mcp` on the host, not in the cluster:

```bash
(
  cd "$DEV_HEALTH_ACR_DIR"
  go build -o "$token_dir/acr-mcp" ./cmd/acr-mcp
)
export PATH="$token_dir:$PATH"
```

Keep a loopback port-forward running in one shell:

```bash
bash scripts/context-fabric-kubernetes.sh port-forward --local-port 18080
```

In the client shell:

```bash
export ACR_API_URL=http://127.0.0.1:18080
export ACR_API_ALLOW_INSECURE_LOOPBACK=true
export ACR_API_TOKEN_FILE="$token_dir/acr-token"
export ACR_LOCAL_INDEX_PROVIDER=disabled
export ACR_ENABLE_WRITEBACK=false
export ACR_SIDECAR_VERSION=1.0.0
export ACR_SIDECAR_CLIENT_VERSION=1.0.0

acr-mcp doctor --live
```

The plain HTTP exception is limited to the loopback port-forward. Cluster and
non-loopback service boundaries remain TLS-verified. Install the desired client
package from `$DEV_HEALTH_ACR_DIR/clients/<client>/README.md` after exporting
this environment.

### Remove the ACR workloads

```bash
bash scripts/context-fabric-kubernetes.sh delete
```

Uninstalling retains caller-owned Secrets, PostgreSQL state, ClickHouse evidence,
and the Ops entitlement. Revoke the generated client and Ops service credentials
through their respective administration commands when the evaluation is over.
