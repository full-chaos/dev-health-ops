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
- **Go API binary and routing state** — the `dho` operator binary (`goapi routing` and `goapi prove`/`goapi rest-prove`) available.
- **Image registry credentials** — `ghcr-pull` Secret with `write:packages` scope.
- **PostgreSQL query role** — `devhealth` user via pgbouncer-transaction (not direct `:5432` in this design, though current prod still uses direct due to chart limitations — see section **"Registry DSN"** below).

## Step 1: Build and push the dho image

query-api runs as `dho query-api` from the dho image (the `dho` target of `docker/go-worker.Dockerfile`). CI publishes that image on every merge to main; build it by hand only for a commit CI has not published.

### Build the image

Build the dho image at a known Git commit:
```bash
docker build --target dho \
  --tag ghcr.io/full-chaos/dev-health-go-dho:sha-<COMMIT_SHA> \
  -f docker/go-worker.Dockerfile .
```

### Multi-arch build (optional but recommended for prod)

```bash
docker buildx build --target dho \
  --tag ghcr.io/full-chaos/dev-health-go-dho:sha-<COMMIT_SHA> \
  --platform linux/amd64,linux/arm64 \
  -f docker/go-worker.Dockerfile --push .
```

### Push to GHCR

```bash
docker push ghcr.io/full-chaos/dev-health-go-dho:sha-<COMMIT_SHA>
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

Digest pairing is verified automatically when routing is enabled (end of step 7). Apply the manifest and wait for the pod to reach ready.

### Manifest values

Manifest applies with:
- Image: `ghcr.io/full-chaos/dev-health-go-dho:<sha-COMMIT>@sha256:<manifest-list-sha>`, with `args: [query-api]`
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

## Step 6: First-time routing rows (canary set only)

The routing state table is empty on first deploy, and nothing that reads it can create a row: `dho goapi prove` routes a `shadow` operation through the proof route only when a row exists, `dho goapi routing disable` never inserts one, and `dho goapi routing enable` refuses an operation with no recorded proof run for the running build (there is no waiver flag; the only exception is a written limit in the compiled go-served ledger). So the first rows are seeded by hand in `shadow` mode, at the digests the running query-api reports, then proven (Step 7), then enabled (end of Step 7).

Read the digests from the running query-api (`/registry` is unauthenticated; `/buildinfo` takes the envelope) and insert one row per canary operation, candidate build first:

```sql
-- <schema> and <document> come from GET /registry (schema_digest, and the
-- operation's document_digest); <build> is the candidate_build /buildinfo reports.
INSERT INTO go_api_candidate_build (schema_digest, document_digest, selected_operation, candidate_build)
VALUES ('<schema>', '<document>', '<operation>', '<build>') ON CONFLICT DO NOTHING;

INSERT INTO go_api_routing_state
  (schema_digest, document_digest, selected_operation, current_candidate_build,
   owner, mode, rollout_percentage, review_evidence, recorded_by, updated_at)
VALUES ('<schema>', '<document>', '<operation>', '<build>',
        'go', 'shadow', 0, 'first-time bootstrap: shadow row for the proof run', '<operator>', now())
ON CONFLICT (schema_digest, document_digest, selected_operation) DO UPDATE
  SET current_candidate_build = EXCLUDED.current_candidate_build, mode = EXCLUDED.mode, updated_at = EXCLUDED.updated_at;
```

**Shadow set (3 ops) intentionally NOT seeded** — enabling known-mismatch operations as canary first is a stop condition. Canary operations route to the real query-api against the baseline API. Real proof compares both planes; shadow refused-by-name (expected on prod) is never compared.

## Step 7: Real proof (go-api-prove)

### Grant the proof service principal read access to the org (once per org)

Alembic `0133` creates the proof service principal's `users` row
(`00000000-0000-4000-8000-00000000e0e1`) with no membership, and
`mint-edge-token` refuses it until it holds a read-role membership in the org
being proven. Grant `viewer`, never `owner` or `admin` (the minter refuses
those too). Re-running is a no-op:

```sql
INSERT INTO memberships (id, user_id, org_id, role, created_at, updated_at)
VALUES (gen_random_uuid(), '00000000-0000-4000-8000-00000000e0e1', '<org>', 'viewer', now(), now())
ON CONFLICT (user_id, org_id) DO NOTHING;
```

Seat note: the principal then shows as a `viewer` member of that org
(`go-api-prove@service.dev-health.invalid`) in member lists and may count
toward the org's seats. Remove the membership to revoke it for that org, or
set `users.is_active = false` to revoke it everywhere.

Real proof run after the metrics drain completes (Trap #174 repair landed). Tools image carries the `dho` operator binary (spec S1, CHAOS-6280) built from the same `dev-health-api` digest.

```bash
# From tools Pod. GO_API_ENVELOPE_PRIVATE_KEY reaches this Pod's own
# environment via secretKeyRef (same Secret/key the api Deployment reads),
# and `dho goapi prove` mints the envelope IN PROCESS from it -- calling
# internal/mintcli/envelope directly, no subprocess. JWT_SECRET_KEY and
# POSTGRES_URI reach it the same way, and the edge access token for the
# proof service principal is minted in process too
# (internal/mintcli/edgetoken).
dho goapi prove \
  -registry-url=http://dev-health-query-api:8000/registry \
  -buildinfo-url=http://dev-health-query-api:8000/buildinfo \
  -edge-url=http://dev-health-ops.default.svc.cluster.local:8000/graphql \
  -documents=/app/go-api/documents.json \
  -artifact-dir=/tmp/proof/artifacts \
  -org=c6a38355-dad6-42e4-8cc9-4c712450827d \
  -recorded-by=<operator> -review-evidence="<why this run>" \
  -since-utc=<baseline-start RFC3339> -until-utc=<proof-end RFC3339>
```

Both credentials are minted automatically, in process -- no flag names a helper or a path. `-documents` is the registry dump baked into the tools image at the same commit as the binary.

The REST routes are proven the same way from the same Pod:

```bash
dho goapi rest-prove \
  -query-api-url=http://dev-health-query-api:8000 \
  -python-api-url=http://dev-health-ops.default.svc.cluster.local:8000 \
  -candidate-bearer-exec='["mint-envelope","-org","c6a38355-dad6-42e4-8cc9-4c712450827d"]' \
  -baseline-bearer-exec='["mint-edge-token","-org","c6a38355-dad6-42e4-8cc9-4c712450827d"]' \
  -org=c6a38355-dad6-42e4-8cc9-4c712450827d \
  -recorded-by=<operator> -review-evidence="<why this run>" \
  -artifact-dir=/tmp/proof/artifacts -report=/tmp/proof/rest-report.json
```

`dho goapi rest-prove`'s `-candidate-bearer-exec`/`-baseline-bearer-exec` still name an ALLOWLISTED HELPER NAME (`mint-envelope`, `mint-edge-token`), never a path -- since spec S1 (CHAOS-6280) these mint the credential IN PROCESS, calling `internal/mintcli`'s own packages directly rather than exec'ing a subprocess, so no secret ever crosses a process boundary at all.

**Prover build.** `dho goapi prove` and `dho goapi rest-prove` carry every declaration, shape and corpus entry they apply compiled in, so both print `prover_build=<own commit> ... candidate_build=<commit /buildinfo names> prover_build_skew=<bool>` first and refuse to measure when the two commits differ or the prover carries no commit. A released image is built with `-buildvcs=false` and takes its commit from `-ldflags`, so it reports `prover_build_modified=false`; the modified arm names a locally built, VCS-stamped prover, and the commit comparison is what guards a released one. Run the tools image tagged with the candidate's own commit (`sha-<first 7>`). `-allow-prover-build-skew` measures anyway; the report then records `prover_build_skew_allowed: true` beside both commits.

Proof runs against the 12 canary operations (shadow set is empty by design, not deferred). Compare baseline and candidate legs; `-proof-url` left empty (Trap #163: `/query/proof` cannot mount on prod). Result: `{12 total, 11 match, 1 mismatch}` or better.

**Retirement note.** `dho goapi prove` no longer accepts a hand-minted static
edge bearer; the edge credential is always minted in process now (above).
Two prod cleanup steps remain, owned separately from this repo change:
remove the now-unused bearer key from the `dev-health-go-api-prove`
Secret, and remove the operator script that used to hand-mint it from the
prod host. Re-run the 12-operation proof above afterward to confirm the
in-process path alone still produces a clean result.

### Enabling a GraphQL mutation (write proof)

A mutation is never proven by the two-plane run above (it would write twice). Its proof is one execution inside the
per-environment Fixture Org, recorded as a `write_executed` receipt:

```bash
GO_API_PROVE_WRITE_FIXTURE_ORG=<fixture org id> dho goapi prove-write \
  -org <fixture org id> -case <registered case> -via query-api \
  -documents <registrydump output> -recorded-by <operator> -review-evidence "<why>" \
  -postgres-uri "$POSTGRES_URI"
```

The verb writes only in the Fixture Org named by the environment (it never creates an org), posts the mutation once,
compares the persisted effects with the case's committed baseline digest, and refuses to call a run a match unless the
response carries the candidate build. A match removes its dataset; anything else keeps it and names it. `-via query-api`
(a direct POST to query-api) records route `proof` and admits `canary`; `-via edge` records route `edge`, which
`primary` requires. `enable` then admits the mutation only on that receipt.

### Enable the canary set

After the proof run records a `deployed_executed` result for each canary operation at the running build, enable them with the Go verb. The candidate build is read from the deployed process's `/buildinfo`; there is no build flag and no waiver flag. Operations are one comma-separated value:

```bash
GO_API_ROUTING_BEARER=<envelope> dho goapi routing enable \
  -registry-url  http://dev-health-query-api:8000/registry \
  -buildinfo-url http://dev-health-query-api:8000/buildinfo \
  -operations <op1>,<op2>,<op12> \
  -mode canary \
  -recorded-by <operator> \
  -review-evidence "First-time enable on prod k3s, JOB 7 step 6"
```

Add `-dry-run` first to see which operations would be admitted; a refusal names the operations with no proof run.

## Step 8: Enable the investment explanation (optional, requires encryption keys)

Only if investment explanation text is needed:

1. Set `GO_API_INVESTMENT_EXPLAIN_ENABLED=true`.
2. **Simultaneously** set `SETTINGS_ENCRYPTION_KEY` and `SETTINGS_ENCRYPTION_SALT` on the query-api Deployment (both from the same Secret, by secretKeyRef, same change).
3. Roll query-api alone and verify both keys are in place (byte-length check, Trap #168).
4. Run a real proof on `investmentFull` and `investmentBreakdown` operations.
5. Chris refreshes the Investment page to verify explanation text.

Without both keys in the same change, encrypted org settings silently fall back to the platform provider (not an error, a fallback).

## Platform environment is declared, not inherited

query-api's Deployment lists `env` explicitly and has **no `envFrom` by
default** — it does not pick up any key from the platform Secret
(`dev-health-ops`) the way the Python `api`/`metricsApi`
workloads do. For investment-explain, query-api needs four platform vars:
`LLM_PROVIDER`, `OPENAI_API_KEY`, `LLM_MODEL`, and `SETTINGS_ENCRYPTION_KEY`.

Two ways to supply them:

1. **`queryApi.extraEnv`** — list each var as a `secretKeyRef` entry pointing
   at the platform Secret's key. Most explicit; what prod runs today.
2. **`queryApi.envFrom`** — pull the whole platform Secret in one entry:
   ```yaml
   queryApi:
     envFrom:
       - secretRef: {name: dev-health-ops}
   ```
   Simpler, but it also imports every other key in that Secret.

Either way, Kubernetes resolves `env`/`extraEnv` ahead of `envFrom`: an
`extraEnv` entry for a name always wins over the same name arriving through
`envFrom`, so the two can be combined without a collision risk.

## Registry DSN (current and future)

### Current (prod 2026-09-11)

Query-api connects directly to `:5432` (`POSTGRES_URI=postgres://devhealth:…@prod:5432/…`). This works but ties the pool to the main Postgres host.

**Design of record** (per CHAOS-3087): use `devhealth` role through pgbouncer-transaction. The chart reserves one role+passwordKey per pooler, and pgbouncer generates a single-line `userlist.txt` with `auth_type=scram-sha-256` requiring plaintext password for onward auth.

### Future (CHAOS-5604)

Userlist exposure and pooler convergence move to CHAOS-5604 scope. For now, direct `:5432` form is the working prod shape.

**Never** use a GRANT or bypass the pooler on an explicit routing change — consult the architecture record.

## Known limitations

- **`/query/proof` cannot mount on prod** (Trap #163). `GO_API_PROOF_ROUTE_ENABLED` gates on a non-production `DEV_HEALTH_ENV`; prod never sets it. Real proof runs use `-proof-url=""` (empty).
- **Tools Pod image** (`dev-health-go-api-tools`) lacks the `go-worker` image's sync-dispatch contracts. Use only the tools image, not go-worker, for one-off `go-api-prove` and corrective `go-api-routing` runs. Its baked-in `mint-envelope` helper mints the envelope locally and needs `GO_API_ENVELOPE_PRIVATE_KEY` mounted into the Pod's environment via `secretKeyRef` (same Secret/key the api Deployment reads) — see [Tools pod (operator image)](../../contribute/architecture/go-api-wave-0-proof-infrastructure.md#tools-pod-operator-image) for the `kubectl run` form and what is (and is not) on the image. Its `mint-edge-token` helper mints the edge access token the same way and needs `JWT_SECRET_KEY` and `POSTGRES_URI` by `secretKeyRef`, plus a `viewer` membership in the org for the proof service principal (the row itself comes from alembic `0133`; the membership SQL is under Step 7).
- **Shadow set never lands on prod**. The 3 operations that `disable -mode python` skipped are disabled by design (CHAOS-5606 note: enabling would surface unproven operations to traffic before baseline is established).

## See also

- [Operator commands § Workerctl on Kubernetes](operator-commands.md#workerctl-on-kubernetes-trap-169-amended) — running corrective verbs from a one-off Pod.
- [Go API Wave 0 § Tools pod (operator image)](../../contribute/architecture/go-api-wave-0-proof-infrastructure.md#tools-pod-operator-image) — what the tools image carries and the `kubectl run` form for it.
- [Prod k3s operate](prod-k3s-deploy-and-operate.md) — restore procedures, secrets, rollouts.
- CHAOS-5606 — Query-api bootstrap epic (JOB 7).
- CHAOS-5604 — Chart and pooler convergence (pooler exposures, CPU limits, future DSN form).
