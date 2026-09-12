---
page_id: op-rb-query-api-bootstrap
summary: First-time bootstrap of the Go query-api on prod k3s — image build, envelope key generation, digest pairing, and routing enablement.
content_type: runbook
owner: platform-operations
applicability: current
lifecycle: active
---

# Query-api bootstrap on prod k3s (first-time deploy)

Query-api provides the schema digest, routing tables, and proof harness for the Go API footprint on prod k3s. This runbook covers the one-time bootstrap.

## Prerequisites

- **Prod k3s cluster running** (CHAOS-5590).
- **Go API binary and routing state** — the `go-api-routing` binary and proof CLI available.
- **Image registry credentials** — `ghcr-pull` Secret with `write:packages` scope.
- **PostgreSQL query role** — `devhealth` user via pgbouncer-transaction (not direct `:5432` in this design, though current prod still uses direct due to chart limitations — see section **"Registry DSN"** below).

## Step 1: Build and push query-api image

### Build the image

Build `query-api` at a known Git commit:
```bash
docker build --tag ghcr.io/full-chaos/dev-health-query-api:sha-<COMMIT_SHA> \
  --tag ghcr.io/full-chaos/dev-health-query-api:latest \
  -f Dockerfile.query-api .
```

### Multi-arch build (optional but recommended for prod)

```bash
docker buildx build --tag ghcr.io/full-chaos/dev-health-query-api:sha-<COMMIT_SHA> \
  --platform linux/amd64,linux/arm64 \
  -f Dockerfile.query-api --push .
```

### Push to GHCR

```bash
docker push ghcr.io/full-chaos/dev-health-query-api:sha-<COMMIT_SHA>
docker push ghcr.io/full-chaos/dev-health-query-api:latest
```

Record the manifest-list sha256 from the push output.

## Step 2: Verify schema digest pairing

The schema digest must match between the API instance and the query-api image. This is verified at first-time routing enable via three parity checks:

1. **Inside the prod API pod**: `current_schema_digest()` — the digest computed from the API's local SDL.
2. **Query-api `GET /registry`** response: `schema_digest` field in the JSON.
3. **Routing binary's local digest check**: `[AGREE]` line in the enable output.

All three must be identical before routing is enabled. If they diverge, a mismatch is caught immediately at step 6.

### Generate the envelope Ed25519 keypair and JWKS

No standalone CLI exists for this. Use the library:

```go
// pseudocode; see internal/middleware/envelope/keypair_test.go for the real implementation
import "full-chaos/go-api/internal/middleware/envelope"

func main() {
    sk, err := envelope.GenerateKeyPair()
    if err != nil { panic(err) }
    
    jwks := envelope.ToJWKS(sk)
    // Write jwks (public keys only) to a JSON file
    // Keep sk private; write to a Kubernetes Secret
}
```

Record the JWKS (public keys only, safe to commit) and the private key (SECRET, never committed).

## Step 3: Create the Kubernetes Secret

The JWKS and private key are created as separate Secrets:

```bash
kubectl create secret generic dev-health-query-api-jwks \
  --from-literal=envelope-jwks.json='{"keys": [...]}'  # public only

kubectl create secret generic dev-health-query-api-envelope \
  --from-literal=GO_API_ENVELOPE_PRIVATE_KEY='...' \
  --from-literal=GO_API_ENVELOPE_KEY_ID='<key-id>' \
  --from-literal=GO_API_ENVELOPE_ISSUER='query-api' \
  --from-literal=GO_API_ENVELOPE_AUDIENCE='go-api'
```

## Step 4: Apply the query-api Deployment and Service

Digest pairing is verified automatically at step 6 (routing enable). Apply the manifest and wait for the pod to reach ready.

### Manifest values

Manifest applies with:
- Image: `ghcr.io/full-chaos/dev-health-query-api:<sha-COMMIT>@sha256:<manifest-list-sha>`
- `imagePullSecrets: ghcr-pull`
- Environment from `dev-health-query-api-envelope` Secret by `secretKeyRef` (never flags/argv — Trap #121, R167).
- Registry PostgreSQL DSN: **currently direct `:5432` form** (see section below).
- `GO_API_QUERY_API_URL`: the Service DNS name (e.g., `http://dev-health-query-api:8000`).

## Step 5: Wire the API side

On the prod API pod, add environment variables:

```bash
kubectl patch deployment dev-health-ops -p '{"spec":{"template":{"spec":{"containers":[{"name":"api","env":[
  {"name":"GO_API_QUERY_API_URL","value":"http://dev-health-query-api:8000"},
  {"name":"PLANE_HEADER_ENABLED","value":"true"}
]}}]}}'
```

Or use `--patch-file` to avoid credential exposure (Trap #121).

## Step 6: First-time routing enable (canary set only)

The routing state table is empty on first deploy. Routing enables with 12 canary operations only (shadow set disabled by design).

```bash
# Run from a one-off tools Pod (see Operator commands § Workerctl on k8s)
go-api-routing enable -operations <op1> <op2> ... <op12> \
  -mode python -apply \
  -recorded-by <operator> \
  -review-evidence "First-time enable on prod k3s, JOB 7 step 6"
```

**Shadow set (3 ops) intentionally NOT enabled** — the `disable -mode shadow` verb never INSERTs a row, so enabling known-mismatch operations as canary first is a stop condition. Canary operations route to the real query-api against the baseline API. Real proof compares both planes; shadow refused-by-name (expected on prod) is never compared.

## Step 7: Real proof (go-api-prove)

Real proof run after the metrics drain completes (Trap #174 repair landed). Tools image carries `go-api-prove` binary built from the same `dev-health-api` digest.

```bash
# From tools Pod
go-api-prove -api-url=http://dev-health-ops.default.svc.cluster.local:8000 \
  -query-api-url=http://dev-health-query-api:8000 \
  -bearer="<minted-token>" \
  -org=c6a38355-dad6-42e4-8cc9-4c712450827d \
  -from=<baseline-date> -to=<proof-date>
```

Proof runs against the 12 canary operations (shadow set is empty by design, not deferred). Compare baseline and candidate legs; `-proof-url` left empty (Trap #163: `/query/proof` cannot mount on prod). Result: `{12 total, 11 match, 1 mismatch}` or better.

## Step 8: Enable the investment explanation (optional, requires encryption keys)

Only if investment explanation text is needed:

1. Set `GO_API_INVESTMENT_EXPLAIN_ENABLED=true`.
2. **Simultaneously** set `SETTINGS_ENCRYPTION_KEY` and `SETTINGS_ENCRYPTION_SALT` on the query-api Deployment (both from the same Secret, by secretKeyRef, same change).
3. Roll query-api alone and verify both keys are in place (byte-length check, Trap #168).
4. Run a real proof on `investmentFull` and `investmentBreakdown` operations.
5. Chris refreshes the Investment page to verify explanation text.

Without both keys in the same change, encrypted org settings silently fall back to the platform provider (not an error, a fallback).

## Registry DSN (current and future)

### Current (prod 2026-09-11)

Query-api connects directly to `:5432` (`POSTGRES_URI=postgres://devhealth:…@prod:5432/…`). This works but ties the pool to the main Postgres host.

**Design of record** (per CHAOS-3087): use `devhealth` role through pgbouncer-transaction. The chart reserves one role+passwordKey per pooler, and pgbouncer generates a single-line `userlist.txt` with `auth_type=scram-sha-256` requiring plaintext password for onward auth.

### Future (CHAOS-5604)

Userlist exposure and pooler convergence move to CHAOS-5604 scope. For now, direct `:5432` form is the working prod shape.

**Never** use a GRANT or bypass the pooler on an explicit routing change — consult the architecture record.

## Known limitations

- **`/query/proof` cannot mount on prod** (Trap #163). `GO_API_PROOF_ROUTE_ENABLED` gates on a non-production `DEV_HEALTH_ENV`; prod never sets it. Real proof runs use `-proof-url=""` (empty).
- **Tools Pod image** (`dev-health-go-api-tools`) lacks the `go-worker` image's sync-dispatch contracts. Use only the tools image, not go-worker, for one-off `go-api-prove` and corrective `go-api-routing` runs.
- **Shadow set never lands on prod**. The 3 operations that `disable -mode python` skipped are disabled by design (CHAOS-5606 note: enabling would surface unproven operations to traffic before baseline is established).

## See also

- [Operator commands § workerctl one-off Pod](operator-commands.md#workerctl-on-k8s) — running corrective verbs from a one-off Pod.
- [Prod k3s operate](prod-k3s-deploy-and-operate.md) — restore procedures, secrets, rollouts.
- CHAOS-5606 — Query-api bootstrap epic (JOB 7).
- CHAOS-5604 — Chart and pooler convergence (pooler exposures, CPU limits, future DSN form).
