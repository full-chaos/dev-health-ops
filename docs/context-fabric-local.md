# Run Context Fabric with Dev Health Ops

Context Fabric, also called the Agent Context Runtime (ACR), is a private,
opt-in service that reads engineering evidence and organization entitlements
from `dev-health-ops`. The MCP adapter (`acr-mcp`) remains a host-local STDIO
process for OpenCode, Claude Code, Codex, or Cursor.

The supported setup uses sibling checkouts:

```text
workspace/
  dev-health-ops/
  dev-health-acr/
  dev-health-web/
```

Set `DEV_HEALTH_ACR_DIR` or `DEV_HEALTH_WEB_DIR` when the repositories are in a
different layout.

## Container support

| Environment | How ACR runs | Status |
| --- | --- | --- |
| Docker Compose | ACR API container layered onto the real Ops Compose definition | Supported for complete local plugin testing |
| Kubernetes | ACR Helm Deployment and migration Job in the Ops namespace | Supported with caller-provided TLS dependencies and Secrets |
| Kustomize | ACR-owned overlays from `dev-health-acr` | Supported for ACR operators |
| Docker Swarm | No ACR stack is defined | Not supported by this integration |

## Docker Compose quick start

From `dev-health-ops`:

```bash
bash scripts/context-fabric-local.sh
```

The launcher does not use a copied Ops stack. It renders this checkout's actual
`compose.yml`, selects the Ops services required by the test lifecycle, and
layers the canonical ACR Compose file from
`dev-health-acr/deploy/compose/acr.compose.yml` together with generated TLS and
file-backed secrets.

The services run under a unique `acr-e2e-*` Compose project. That preserves the
real Ops service definitions while preventing the test from mutating an
already-running default `dev-health` project or reusing its volumes.

The command:

1. builds the current Ops and ACR sources;
2. starts containerized PostgreSQL, PgBouncer, ClickHouse, Valkey, Ops API, ACR
   migrations, and `acr-api`;
3. enables certificate-verified local service and data boundaries;
4. creates an organization and grants `agent_context_runtime`;
5. seeds `acme/live-e2e` on branch `main`;
6. creates and rotates a repository-scoped ACR credential;
7. builds the host-local `acr-mcp` binary and verifies both read tools; and
8. prints a generated `client.env` path before holding the stack open.

In another shell:

```bash
source <printed-client-env-path>
acr-mcp doctor --live
acr-mcp metadata
```

Install a bundled package using
`$DEV_HEALTH_ACR_DIR/clients/<client>/README.md`, then test with:

> Use Context Fabric for repository `acme/live-e2e` on branch `main`. Call
> `context_for_task`, then expand one returned evidence ID with
> `source_evidence`.

Press Ctrl-C in the launcher terminal to remove only the generated containers,
volumes, network, credentials, and environment file.

## Kubernetes quick start

The Ops-owned entrypoint consumes the canonical private Helm chart from the ACR
checkout and the non-secret values file at
`deploy/context-fabric/helm-values.yaml`.

Render without a cluster:

```bash
bash scripts/context-fabric-kubernetes.sh render \
  --image "$ACR_IMAGE" \
  --entitlement-url "$OPS_HTTPS_ORIGIN" \
  > /tmp/context-fabric.yaml
```

Install or upgrade after the required namespace and Secrets exist:

```bash
bash scripts/context-fabric-kubernetes.sh apply \
  --image "$ACR_IMAGE" \
  --entitlement-url "$OPS_HTTPS_ORIGIN"

bash scripts/context-fabric-kubernetes.sh status
```

The image must be an immutable `@sha256` reference, and the entitlement value
must be an HTTPS origin with no path. `apply` checks for the required Secrets,
runs the chart's migration hook, and waits atomically for the API Deployment.

### Kubernetes dependency boundary

The default Ops Kubernetes manifests are not, by themselves, an ACR backing
stack:

- `dev-health-api` is exposed inside the cluster over HTTP;
- the bundled ClickHouse manifest exposes plaintext ports; and
- the Ops Kubernetes stack does not provision ACR PostgreSQL state.

Before applying ACR, provide an HTTPS Ops origin, TLS-native read-only
ClickHouse, and TLS PostgreSQL with separate runtime and migration roles. Create
these existing Secrets in the target namespace:

| Secret | Keys |
| --- | --- |
| `acr-runtime-credentials` | `ACR_POSTGRES_DSN`, `ACR_CLICKHOUSE_DSN`, `ACR_EVIDENCE_ID_ACTIVE_KID`, `ACR_EVIDENCE_ID_KEYS` |
| `acr-migration-credentials` | `ACR_POSTGRES_MIGRATION_DSN` |
| `acr-entitlement-token` | `token` |
| `acr-postgres-ca` | `ca.crt` |
| `acr-clickhouse-ca` | `ca.crt` |
| `acr-entitlement-ca` | `ca.crt` |
| `acr-registry-pull` | `.dockerconfigjson` |

Use an external secret manager in shared environments. File-backed `kubectl`
examples, entitlement setup, credential rotation boundaries, and the exact
Secret contract are in `deploy/context-fabric/README.md`.

### Test a local plugin against Kubernetes

After the ACR Deployment is ready, create a repository-scoped token file:

```bash
token_dir="${XDG_CONFIG_HOME:-$HOME/.config}/dev-health/context-fabric"
install -d -m 700 "$token_dir"

bash scripts/context-fabric-kubernetes.sh create-credential \
  --org-id "$ORG_ID" \
  --repository acme/repository \
  --output "$token_dir/acr-token"
```

Build `acr-mcp` from the ACR checkout:

```bash
(
  cd "$DEV_HEALTH_ACR_DIR"
  go build -o "$token_dir/acr-mcp" ./cmd/acr-mcp
)
export PATH="$token_dir:$PATH"
```

Keep a loopback port-forward running:

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

Plain HTTP is allowed only because `kubectl port-forward` is bound to loopback.
The cluster dependency boundaries remain TLS-verified. Install the selected
client package from `$DEV_HEALTH_ACR_DIR/clients/<client>/README.md` after
exporting the environment.

Remove the ACR workloads with:

```bash
bash scripts/context-fabric-kubernetes.sh delete
```

The uninstall retains caller-owned Secrets, databases, evidence, and Ops
entitlements. Revoke the generated ACR and Ops service credentials separately.

## Web verification

The MCP setup does not place Web assertion keys in either repository. Run the
Web-owned Context Fabric browser/BFF harness separately:

```bash
cd ../dev-health-web
corepack enable
pnpm install --frozen-lockfile
pnpm test:e2e:context-fabric
```

The live Web assertion path remains part of the ACR service verification suite.
