---
page_id: op-rb-prod-k3s-operate
summary: Operating the single-host k3s production stack on prod (10.0.0.151), including restore procedures, secrets management, and safe rollouts.
content_type: runbook
owner: platform-operations
applicability: current
lifecycle: active
---

# Operating prod k3s — restore, secrets, and rollouts

Prod (10.0.0.151) runs as a single-host k3s cluster. The deploy umbrella (`values.prod.yaml`) manages the application tier; the data tier (ClickHouse, PostgreSQL, Valkey, Traefik, cloudflared) remains as a compose stack serving as rollback reserve.

Cutover executed 2026-09-11 21:30Z (CHAOS-5590). Records on bigboy: `/var/lib/oci-cache/lane-scratch/_records/prod-k8s/`; devhealth workspace: `.remember/_records/prod-k8s/`.

## Restore procedure (ClickHouse + PostgreSQL)

### Prerequisites
- Backup dump with schema and data.
- Encryption keys available if the dump is encrypted.
- **Dump must include VIEW objects, not tables only** (Trap #174).

### Procedure

1. **Restore tables** to a fresh or wiped target store.

2. **Restore VIEW objects in two passes**:
   - First pass: all `View` objects (6 types).
   - Second pass: all `TO-MV` (materialized views targeting other tables, 3 types).
   - **Critical**: recreate views **after all tables** — view-on-view dependencies require ordering.

3. **For Materialized Views (TO-MV)**: use `CREATE ... IF NOT EXISTS` and **do NOT use `POPULATE`**. The target tables were restored with their data; `POPULATE` re-reads sources and **double-counts**.

4. **Verify object parity** by row count:
   ```sql
   SELECT engine, count() FROM system.tables 
   WHERE database = 'metrics' AND engine like '%View%' 
   GROUP BY engine
   ```
   Compare before/after counts to confirm all view objects landed.

5. **After any Postgres restore**, analyze the River queue table:
   ```sql
   ANALYZE river.river_job;
   ```
   This prevents subsequent queue queries from full-table scans under a large backlog. Related: CHAOS-5615.

### Symptom recognition

**All `metrics.daily_partition` jobs fail `pre_bridge_family_incomplete` with "Unknown table expression identifier"**: the views were not restored. The queue enters a retry treadmill, not a drain. Row-count parity alone cannot detect views.

**Stream runners fail `posture_manifest_lockstep` with "connection is closed"**: likely ClickHouse finishing post-restore merges. Wait for merge completion and node CPU to stabilize before assuming a defect.

## Secrets loading on Kubernetes

### kubectl quote-stripping behavior (Trap #168)

`kubectl create secret --from-env-file` **does not strip quotes** the way `docker compose` does. A `.env` file with:
```
LLM_PROVIDER="openai"
```
stores the value as `"openai"` (11 bytes, with literal quotes), not `openai` (7 bytes).

### Verification

**Never verify by printing values.** Verify by byte length inside a running pod:
```bash
kubectl exec -it <pod> -- sh -c 'echo -n $LLM_PROVIDER | wc -c'
```
Expected: 7 bytes (no quotes). If 9 or more, the secret carries literal quotes.

### Correction

Re-run the secrets loader after stripping quotes from the source `.env` file:
```bash
# Source env file with quotes removed
sed 's/"//g' <input.env> > <cleaned.env>
kubectl create secret generic <secret-name> --from-env-file=<cleaned.env> --dry-run=client -o yaml | kubectl apply -f -
```

## Safe rollouts on k3s

### Batch size rule (Trap #172)

**Never roll all Deployments at once.** On a 4-core node:
- Rolling 12 Deployments simultaneously with `maxUnavailable: 0` doubles the pod count.
- Nothing reaches readiness → nothing terminates → node deadlock.
- Billing edge experienced 503 at 97% CPU / load 26.

**Rule: batch rollouts ≤2 at a time. Wait for ready before the next batch.**

### HPA and manual scaling (Trap #167)

**Never scale an HPA-managed Deployment to zero** to quiesce it. The HPA cannot lift it back:
- `helm upgrade` will not restore it because the chart omits `replicas` for HPA-managed workloads.
- 30-minute outage occurred when scaling to zero without re-enabling HPA.

Alternative: scale the HPA's `minReplicas` instead, or scale a non-HPA workload.

### CPU limit sizing (Trap #171)

**Size CPU limits by the workload's longest request, not idle usage.**

Billing-edge throttled at 250m (chart default) while `uvicorn` was up but `/health` missed the 1s probe timeout:
- Process killed with exit 137 (out of memory, actually throttled).
- Crash loop until limit raised to 1 CPU.
- Actual usage: 13m (5% of 250m).

**Chart defaults are dev sizing.** Measure unthrottled usage in staging, then set limit at ~1.5× peak.

## Go-heavy queue after restore

### Queue telemetry timeout under large backlog (CHAOS-5615)

A large backlog left behind by a restore saturates the operator's telemetry gate:
- River's `queued_contract_versions` telemetry seq-scans the queue table and misses the hardcoded 2s `defaultQueueTelemetryTimeout`.
- Preclaim readiness abort → 13+ restarts.

**Temporary workaround (values.prod.yaml)**: `extraArgs: [--health-check-timeout=10s]` on the heavy worker group only. This raises the timeout but is not a permanent fix.

**Permanent fix tracked in CHAOS-5615**: expose the timeout as a tuning knob.

### Stream runners failing bootstrap (CHAOS-5614)

Stream runners returning `nil, nil` from a bootstrap check < 2s timeout:
- Deferred `closeOnError` closes pools.
- Process lives forever serving readiness against closed clients.
- Restart clears.

**Temporary**: restart the stream-runner pods. **Permanent fix**: CHAOS-5614.

## Prod values and digest pinning

Live production config: `values.prod.yaml` sha `69714296` (or later per release notes).

Carries:
- `global.imagePullSecrets: ghcr-pull` (authenticate image pulls).
- ClickHouse 6Gi PVC.
- API request 512Mi, CPU-only HPA (no memory target, Trap #166).
- Web 256Mi request, 1 CPU limit.
- Billing-edge enabled with **1 CPU limit** (see Trap #171 above).
- Three stream-runner groups via `runtimeProfile`.

Ingress: www, api, billing through Traefik to Cloudflare.

## Rollback to compose

**Application tier**: `cutover-replay.sh rollback` (starts compose app services).

**Cloudflare repoint**: chris repoints `:80` (compose) or `:30080` (k3s).

**Data tier**: compose stack remains UP as reserve. **Never `down`, never remove volumes.**

See CHAOS-5590 for rollback sequence and verify before attempting.

## Related tickets

- **CHAOS-5615**: permanent knob for queue health-check timeout + runbook.
- **CHAOS-5614**: stream-runner bootstrap nil-nil fix.
- **CHAOS-5609**: ClickHouse password rotation (deferred post-cutover).
- **CHAOS-5604**: Chart/pooler convergence, CPU limit exposure.

## See also

- [Query-api bootstrap](query-api-bootstrap.md) — first-time deploy of the Go query API.
- [Operator commands](operator-commands.md) — workerctl one-off Pod pattern (Trap #169).
- [Prod cutover 2026-09-11 lessons](../prod-cutover-2026-09-11-lessons/index.md) — index of all related changes.
