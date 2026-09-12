---
page_id: op-prod-cutover-2026-09-11-lessons
summary: Index and lessons from the prod k3s cutover (2026-09-11), including operational traps, runbooks, and bootstrap procedures.
content_type: index
owner: platform-operations
applicability: current
lifecycle: active
---

# Prod cutover 2026-09-11 — lessons and documentation

On 2026-09-11 21:30Z, prod moved to a single-host k3s cluster (CHAOS-5590). This index collects the operational runbooks, bootstrap procedures, and traps discovered during cutover and the subsequent query-api deployment (JOB 7, CHAOS-5606).

## Operational runbooks

**Primary docs**: read these for operating prod k3s day-to-day.

- [**Prod k3s operate**](../runbooks/prod-k3s-deploy-and-operate.md) — restore procedures, secrets loading, safe rollouts, and tuning knobs.
  - Trap #165: Secrets loader order (must run AFTER overlay settles).
  - Trap #168: `kubectl create secret --from-env-file` quote-stripping.
  - Trap #171: CPU limit sizing by peak demand, not idle.
  - Trap #172: Batch k3s rollouts ≤2, wait ready.
  - Trap #173: Inline `gitleaks:allow` covers only the commit carrying it.
  - Trap #174: ClickHouse restore must include views + object-count parity; row parity alone cannot see views.

- [**Query-api bootstrap**](../runbooks/query-api-bootstrap.md) — first-time deploy of the Go query API on prod k3s (JOB 7).
  - Schema digest pairing (must match on API and query-api).
  - Ed25519 envelope key generation and JWKS creation.
  - Registry DSN design (currently direct `:5432`, pooler route future-tracked in CHAOS-5604).
  - First-time routing enable (canary set only, shadow disabled by design).
  - Real proof run via `go-api-prove` after metrics drain.

## Operator commands & patterns

- [**Operator commands § Workerctl on k8s**](../runbooks/operator-commands.md#workerctl-on-kubernetes-trap-169-amended) — running corrective verbs from a one-off Pod.
  - **Trap #169 (amended)**: Use `dev-health-go-operator` image, NOT go-worker (lacks sync-dispatch contracts).
  - One Pod per verb; credentials via secretKeyRef only (Trap #121, R167), never flags/argv.
  - Delete after completion.
  - `--review-evidence` required even with `--dry-run` on trigger verbs.
  - Never use `--daily-redrive` on already-succeeded days.

- [**Operator commands §f: Corrective-run sequence**](../runbooks/operator-commands.md#corrective-run-sequence-after-a-metric-defect-fix) — post-defect-fix backfill.
  - `metrics finalize-redrive` re-runs finalize for completed days, backfilling new fields.
  - Trap #103 (ClickHouse ReplacingMergeTree): Always use `FINAL` on `work_item_team_attributions` queries.

## Traps and rulings (cross-reference)

| Trap/Ruling | Scope | Runbook / Details |
|---|---|---|
| **Trap #163** | `go-api-prove -dry-run` never validates routing (pool skipped by design). | [Query-api bootstrap § Known limitations](../runbooks/query-api-bootstrap.md#known-limitations) |
| **Trap #165** | Secrets loader must run AFTER overlay settles; re-run on every overlay change. | [Prod k3s operate § Restore procedure §5](../runbooks/prod-k3s-deploy-and-operate.md#procedure) |
| **Trap #168** | `kubectl create secret --from-env-file` does NOT strip quotes. | [Prod k3s operate § Secrets loading](../runbooks/prod-k3s-deploy-and-operate.md#secrets-loading-on-kubernetes-trap-168) |
| **Trap #169 (amended)** | Use `dev-health-go-operator` image for workerctl one-off Pod, NOT go-worker. | [Operator commands § Workerctl on k8s](../runbooks/operator-commands.md#workerctl-on-kubernetes-trap-169-amended) |
| **Trap #171** | CPU limits under 250m throttle edge tiers; size by peak request time, not idle. | [Prod k3s operate § CPU limit sizing](../runbooks/prod-k3s-deploy-and-operate.md#cpu-limit-sizing-trap-171) |
| **Trap #172** | Rolling all k8s Deployments at once deadlocks small node; batch ≤2, wait ready. | [Prod k3s operate § Batch size rule](../runbooks/prod-k3s-deploy-and-operate.md#batch-size-rule-trap-172) |
| **Trap #173** | Inline `// gitleaks:allow` covers only its commit; HISTORY requires `.gitleaksignore`. | [Prod k3s operate § Restore procedure](../runbooks/prod-k3s-deploy-and-operate.md#procedure) (reference only; not operator-facing) |
| **Trap #174** | ClickHouse dump and restore must include VIEW objects; row-count parity blind to views. | [Prod k3s operate § Restore procedure § Procedure](../runbooks/prod-k3s-deploy-and-operate.md#procedure) |
| **Trap #103** | `work_item_team_attributions` is ReplacingMergeTree; every query needs `FINAL`. | [Operator commands § Note on ClickHouse ReplacingMergeTree](../runbooks/operator-commands.md#note-on-clickhouse-replacingmergetree-trap-103) |
| **R167** | On k8s, DSNs/bearers reach processes via pod env by secretKeyRef only, never flags/argv. | [Query-api bootstrap § Step 4](../runbooks/query-api-bootstrap.md#step-4-apply-the-query-api-deployment-and-service) |
| **R168** | Query-api registry DSN = `devhealth` role through pgbouncer-transaction (design of record). | [Query-api bootstrap § Registry DSN](../runbooks/query-api-bootstrap.md#registry-dsn-current-and-future) |
| **R171** | Classifier-denied credential helper never re-authored by another agent; one guarded line to chris. | [Prod k3s operate](../runbooks/prod-k3s-deploy-and-operate.md) (reference only; not operator-facing) |
| **R172** | Edge-token mint classifier-denied to team-lead too; chris's line, no exception. | [Prod k3s operate](../runbooks/prod-k3s-deploy-and-operate.md) (reference only; not operator-facing) |

## Records location

Handoff and records from the cutover live at:
- **Devhealth workspace**: `.remember/_records/prod-k8s/` + `.remember/lanes/lane-prod-k8s/`
- **Bigboy cache**: `/var/lib/oci-cache/lane-scratch/_records/prod-k8s/`

## Related tickets

- **CHAOS-5590** ✓ Done — Prod single-host k3s foundation.
- **CHAOS-5606** — Query-api on prod k3s.
- **CHAOS-5607** — This documentation update.
- **CHAOS-5614** — Stream-runner bootstrap nil-nil fix (permanent, follow-up).
- **CHAOS-5615** — Queue health-check timeout knob + runbook (permanent, follow-up).
- **CHAOS-5604** — Chart/pooler convergence, CPU limit exposure (follow-up).
- **CHAOS-5609** — ClickHouse password rotation (deferred post-cutover).

## Prod scripts and tools of record

All scripts below are located at `/home/ubuntu/lane-prod-k8s/` on prod. Records live in `.remember/_records/prod-k8s/` (devhealth workspace) and `/var/lib/oci-cache/lane-scratch/_records/prod-k8s/` (bigboy).

| Script | Purpose | Context |
|---|---|---|
| `load-secrets.sh` | Load Kubernetes Secrets from env file (quote-stripped, Trap #168). | Secret provisioning. |
| `fill-secrets.sh` | Fill in CLICKHOUSE_URI/POSTGRES_URI and other computed DSNs. | Post-load overlay. |
| `add-ch-native.sh` | Add CLICKHOUSE_NATIVE_URI to existing Secret (`:9000` form). | Optional; CHAOS-5604 scope. |
| `cutover-replay.sh` | Restore ClickHouse from dump (views + parity check, Trap #174). | DR/replay. |
| `mint-operator-token.sh` | Mint WORKER_OPERATOR_TOKEN for operator commands. | One-time setup. |
| `mint-envelope-keys.sh` | Generate query-api Ed25519 envelope keypair and JWKS. | JOB 7 bootstrap. |
| `drain-watch.sh` / `drain-watch-daemon.sh` | Systemd unit to watch metrics drain post-CH-restore. | Long-running background monitoring. |
| `workerctl-run.sh` | Helper to run workerctl verbs from one-off Pod. | Corrective operations. |

## See also

- [Prod k3s operate](../runbooks/prod-k3s-deploy-and-operate.md)
- [Query-api bootstrap](../runbooks/query-api-bootstrap.md)
- [Operator commands](../runbooks/operator-commands.md)
- [Go migration matrix](../../go-migration-matrix.md) — Go/Python executor status table (regenerate via CLI, never hand-edit).
- [Team attribution architecture](../../contribute/architecture/team-attribution.md) — precedence tiers and recovery.
- CHAOS-5590 epic (prod k3s foundation).
- CHAOS-5606 epic (JOB 7, query-api bootstrap).
