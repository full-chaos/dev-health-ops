---
page_id: ref-auth-workload-audit
summary: One audit of every workload row of the auth registry (and the ACR binding table) on 2026-11-02, with owner, audience, actions, binding, rotation and recovery, and a verdict per row.
content_type: reference
owner: engineering
source_of_truth:
  - contracts/auth/v1/vocabulary/workload-registry.json
  - internal/auth/workload/bootstrap_lifecycle.go
  - internal/auth/workload/assertion_keys.go
  - internal/auth/workload/rebind.go
applicability: current
lifecycle: active
---

# Workload audit

This page records the audit of every workload row (CHAOS-11257, 2026-11-02). It is a dated record, not a
live list: the registry is [`workload-registry.json`](https://github.com/full-chaos/dev-health-ops/blob/main/contracts/auth/v1/vocabulary/workload-registry.json)
and the [workload registry page](workload-registry.md). The production rows were read once, read-only, with the
read script `prod-read-11257-1.sh` (sha256 `860226d0c2ba6968435cf54e5bf230471b765d4aaca6b942c1e21c5c70a6adfa`).
The output record has sha256 `acb6e952fcd1e9bcba17f6a97ce78c7e85023b5599a9f82a6c43ff401ec628b0`.
The read printed ids, names, audiences, actions, binding references and key ids only; no secret.
{: .fc-page-lede }

## How a row is judged

- **Owner.** At the time of the audit the registry had no owner. The owner of a row is the chart component that runs it, and the team
  that owns the runbooks that operate it (`platform-operations`). The registry contract now holds the team as `owner` (see
  [Update 2026-11-03](#update-2026-11-03)); the component stays in the table below.
- **Unused action.** An action in a row is used when a code path asks for it. The authorization step
  (`internal/auth/authorization/grant.go`, `workloadDenial`) and the token exchange refuse every action that a row
  does not list. No row lists one, so no action is unused, and no workload token carries one.
- **Tested rotation and recovery.** A row is PASS only when all four hold: a documented verb, a test that runs it,
  a recorded live drill, and a recovery path with a test. A row whose drill is not done is PARTIAL and names the
  drill ticket. A row with no documented verb or no test is a GAP. A row with no audience, no binding and no
  credential has nothing to rotate: it is N/A (identity only), and only its recovery path is checked.
- Integration tests named below run in hosted CI only. They were cited, not run, for this audit.

## Registry rows

Production holds the same 5 rows as the JSON: no row is missing, none is unlisted, none is disabled, every
`allowed_actions` is empty. All 5 were registered at 2026-11-01 06:44Z.

| Workload | Owner (component / team) | Audiences | Actions | Binding (prod) | Credential (prod) | Verdict |
| --- | --- | --- | --- | --- | --- | --- |
| `api` | go-api / platform-operations | `auth-service` | none | `prod` `prod.fullchaos.dev`, `dev-health/dev-health-ops-go-api`, UID pinned, enabled (added 2026-11-02 19:28Z) | Kubernetes exchange; no bootstrap credential | PARTIAL |
| `acr-api` | acr-api / platform-operations | `auth-service`, `dev-health-api` | none | none | none: `authWorkloadClient.enabled` is `false` in the acr chart | PARTIAL |
| `web` | web / platform-operations | none | none | none | one assertion key, `web-2026-09`, active, issuer `https://www.fullchaos.dev` | GAP |
| `auth-service` | auth-service / platform-operations | none | none | none | none | N/A (identity only) |
| `query-api` | query-api / platform-operations | none | none | none | none | N/A (identity only) |

Prod holds no bootstrap credential row: the bootstrap subject is for Compose and self-hosted installs.

## Rotation and recovery by row

| Workload | Rotation | Recovery | Proof today | Missing |
| --- | --- | --- | --- | --- |
| `api` | The kubelet refreshes the projected token; go-api reads the file at every exchange ([federation runbook](../../operate/runbooks/auth-workload-kubernetes-federation.md), "The file is read again for every exchange"). A binding is changed with `rebind`. | `rebind` after a ServiceAccount is recreated, `binding-disable`, `binding-enable`, `enable`, `disable` ([bootstrap runbook](../../operate/runbooks/auth-workload-bootstrap-credential.md), "Rebind a recreated ServiceAccount", "Disable or enable one binding", "Enable a disabled workload"). The restore fence disables the rows (`internal/auth/restorefence/specs.go`, rows `auth.workload_bindings`, `auth.workloads`). | `internal/auth/workload/rebind_integration_test.go:17` (rebind), `:63` (refusals), `:131` (disable stops every binding); `enable_integration_test.go:151`, `:204`. | The live drill CHAOS-11237 (go-api, Backlog). Until it is done the row is PARTIAL. |
| `acr-api` | No credential in prod yet. The client is off (`acr/deploy/helm/acr/values.yaml`, `authWorkloadClient.enabled: false`) and the row has no binding. The same verbs as `api` apply once it is bound. | Same verbs as `api`. | The registry row and the verbs are tested as for `api`. | A binding and the live drill CHAOS-11238 (Backlog). PARTIAL. |
| `web` | `auth-migrate workload-assertion-key-set` (new active key, `-retire-previous-after` up to 24 h) and `workload-assertion-key-list` (`cmd/auth-migrate/assertionkey.go`). | Set a new active key; a retired key never returns (migration 0039 trigger). | `assertion_keys_integration_test.go:111` (rotation), `:146` (overlap bound), `:204` (retired is terminal, one active key); production has one key and one `assertion_key_set` audit event. | At the audit: no runbook named the verbs, and no drill. Now: the [web assertion key rotation runbook](../../operate/runbooks/auth-workload-assertion-key-rotation.md) names the verbs and the recovery, and the live rotation drill on ACR ran (web pull request 1794, `DRILL-11236-5.log`). The production key has not been rotated, and the verbs' database leg is tested in hosted CI only. PARTIAL. |
| `auth-service`, `query-api` | None: no audience, no binding, no credential. | A re-run of `auth-migrate workloads seed` restores the row (`seed` is idempotent). `enable` clears a disable. | `internal/auth/workload/store_integration_test.go` (seed), `enable_integration_test.go:151`. | None. |

Bootstrap credentials (`bootstrap-create`, `bootstrap-rotate`, `bootstrap-revoke`, `bootstrap-disable`;
`internal/auth/workload/bootstrap_lifecycle.go`) serve any registry row that runs without Kubernetes. Their tests:
`bootstrap_lifecycle_integration_test.go:139` (overlap bound), `:187` (cut over at once), `:243` (revoke is terminal),
`:315` (disable refuses every live value). No production row holds one, so no live drill applies to it here.

## ACR binding table

`acr.workload_bindings` is a different table from `auth.workload_bindings`. It maps a Kubernetes identity (trust
domain, namespace, ServiceAccount, UID) to an ACR organization, role and repository scopes, for the ACR exchange
server (`acr/migrations/postgres/0030_workload_token_exchange.sql`, `0070_workload_bindings_org_key.sql`;
`acr/internal/storage/postgres/workload_bindings.go`).

| Item | Result |
| --- | --- |
| Rows in prod | 0 |
| Owner | acr / platform-operations |
| Audience and actions | Not a row field: the binding names a role and repository scopes. |
| Rotation and recovery | None documented: the table has no write path or verb by design, rows are made by an operator. |
| Proof | `workload_bindings_integration_test.go:57` (exact lookup), `:89` (a disabled binding is rejected by the resolver), `:232` (key includes the organization). |
| Verdict | At the audit: GAP, no documented way to add, move or disable a row. Now: PARTIAL. The ACR operations guide has the procedure (acr repository, the operations guide, section "Workload bindings: add, move and disable"; acr pull request 2007). No live drill, and the table has no rows, so nothing is exposed today. |

## Summary

- Rows audited: 5 registry rows plus the ACR binding table (0 rows).
- PASS 0, N/A 2 (`auth-service`, `query-api`: identity only), PARTIAL 2 (`api`, `acr-api`; drills CHAOS-11237 and CHAOS-11238 pending), GAP 2 (`web`
  assertion key runbook; ACR binding table procedure). Both GAP rows moved to PARTIAL: see the update below.
- Actions removed: 0 (none exist). Owners: derived from the component at the audit; the registry contract now holds an owner.
- Follow-up: one ticket covered the owner field, the web assertion-key runbook and the ACR binding-table procedure.

## Update 2026-11-03

- Owner: `workload-registry.json` holds `owner` for every row (`platform-operations`). The loader refuses a missing or malformed owner, and the
  registry page compares its Owner column with the JSON. The database does not store it.
- `web`: GAP to PARTIAL (runbook and drill, above).
- ACR binding table: GAP to PARTIAL (procedure, above).
- Now PASS 0, N/A 2, PARTIAL 4 (`api`, `acr-api`, `web`, the ACR binding table), GAP 0.
