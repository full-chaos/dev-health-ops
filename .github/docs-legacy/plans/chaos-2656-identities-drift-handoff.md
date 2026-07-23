# Handoff: CHAOS-2656 — Identity / Member Drift-Review Slice

> Continuation handoff for the second slice of the CHAOS-2622 ClickHouse drift-review
> rebuild. The **teams slice is done and in review**; this slice extends drift-review to
> identities/members. Read the design first: [`chaos-2622-clickhouse-drift-review-plan.md`](chaos-2622-clickhouse-drift-review-plan.md) §4.4.

## 1. Where the teams slice landed (context you inherit)

- **ops PR**: `full-chaos/dev-health-ops#1044` — branch `feat/chaos-2622-clickhouse-drift-review`
- **web PR**: `full-chaos/dev-health-web#708` — branch `feat/chaos-2654-drift-review-change-id-wire` (change_id wire; merge together with ops)
- **Linear**: CHAOS-2622 (parent) + sub-issues 2649/2650/2651/2652/2653/2654/2655 (all delivered). **CHAOS-2656 is this slice** — its blockers (2651 projector, 2652 service) are now satisfied.
- **Implementation review**: an Oracle code-review of the teams-slice implementation was launched; check its findings and fold any fixes in before building on top.

### What exists now (reuse these)
| Concern | File |
| --- | --- |
| Drift projector (observation + policy + change_id + lifecycle) | `src/dev_health_ops/api/services/configuration/clickhouse_team_drift_projector.py` |
| Drift service (get_pending / approve / dismiss) | `src/dev_health_ops/api/services/configuration/clickhouse_team_drift.py` |
| 3 CH tables | migrations `056_team_sync_policies.sql`, `057_team_provider_observations.sql`, `058_team_drift_changes.sql` |
| Store inserts | `storage/clickhouse.py` `insert_team_sync_policies` / `insert_team_provider_observations` / `insert_team_drift_changes` |
| Worker | `workers/team_drift_sync.py` (+ registered in `workers/tasks.py`) |
| Endpoints | `api/admin/routers/teams.py` (4 drift endpoints, change_id wire) |
| Test double | `tests/_clickhouse_team_store.py` (`FakeClickHouseTeamStore`) |
| Tests | `tests/api/services/configuration/test_clickhouse_team_drift_{projector,service}.py` |

## 2. CHAOS-2656 scope (this slice)

Member/identity drift-review + manual-mapping reconciliation:
- Surface conflicts where auto-import would change a **manually-curated membership**, or where `manual_attribution_fallbacks(scope_type='member')` disagrees with discovered membership. **Flag instead of clobber**; approve applies the change surgically (add/remove by facet — never full recompute, preserve Auto Import members).
- **MUST gate the attribution-impacting `team_memberships` dimension**, not just `teams.members` display. Reviewing only `teams.members` gives a misleading UI while attribution silently changes underneath (Oracle's key warning).
- The `team_drift_changes` table already has an `entity_type` column — reuse it with `entity_type='identity'`, OR add a parallel `identity_drift_changes` table if the membership change shape diverges enough in review.

### Existing identity/member surface to integrate with
| Concern | File / table |
| --- | --- |
| CH identities table | migration `054_identities.sql`; `ClickHouseIdentityStore` in `clickhouse_identity_admin.py` (surgical-by-facet membership) |
| Manual member fallbacks (source 6) | migration `053_manual_attribution_fallbacks.sql` (`scope_type='member'`) |
| Attribution dimension (read at metrics time) | `team_memberships` (migration `051`); read in `metrics/loaders/clickhouse.py` (~L449) |
| Identity admin endpoints | `api/admin/routers/identities.py` (discover_team_members / confirm_team_members / infer+confirm from Jira) |
| Member facets helper | `clickhouse_team_admin.py` `member_facets(...)` |

## 3. Design rules to carry over (do NOT re-derive)

- **change_id** = `hash(org_id, entity_type, entity_id, change_type, field, old_value_json, new_value_json)` — value-fingerprinted; include old+new values.
- **Lifecycle**: never re-insert pending for an already-decided (dismissed/approved) change with the same value pair; a changed value supersedes the prior pending; drift that disappears is resolved.
- **Default policy 0 = no behavior change.** Policy lives in the `team_sync_policies` sidecar (consider an `identity_sync_policies` sidecar or reuse). Never store policy on the `identities`/`teams` rows (they are rewritten wholesale on update).
- **Manual fallback never overrides WTI-native facts** (precedence: native_team 0 → … → manual_fallback 6 → unassigned 7). See `team-attribution.md` §0.
- Low-cardinality strings, **not `Enum8`**, for new status/type columns.
- New CH tables are auto-purged by org-deletion (`org_deletion._clickhouse_tables_from_migrations`) — no manual edit. **Next migration number: 059+.**

## 4. Gotchas / hard-won lessons (will save you hours)

1. **Circular import trap**: any module that top-imports something under `api/admin/*` (e.g. `schemas_flat`) triggers the eager router chain in `api/admin/__init__.py` → re-enters `routers/teams.py` mid-init → `ImportError`. **Use `TYPE_CHECKING` + function-local imports** for admin schemas inside service modules. (See the fix in `clickhouse_team_drift.py`: `FlaggedChange` is imported lazily.) `py_compile`/LSP do **not** catch this — only a real `python -c import` does.
2. **Test double must track new tables**: `tests/_clickhouse_team_store.py` `FakeClickHouseTeamStore` needs an `insert_*` method **and** a `client.query` branch for every new CH table your code reads/writes, else admin-router tests fail with `AttributeError`. Add these when you add tables.
3. **mypy**: avoid type-narrowing conflicts — annotate `value: Any` when reassigning a variable across branches with different inferred types.
4. **Pre-existing local-env test failures (10, NOT regressions)**: `test_credential_resolver` ×3, `test_linear_provider` 429-retry ×3, `test_core_extraction` cache ×2, `test_cli_preflight`, `test_org_deletion` log-sanitization. They fail **identically on clean `main`** (Redis MagicMock pollution, timing, DB-fallback). To attribute your own failures: stash your work, run the full gate on clean `main`, diff the failure set.
5. **Gate**: `bash ci/local_validate.sh` is REQUIRED before push. It provisions a **scratch** ClickHouse (`ci_local_validate`) and drops it — never point `CLICKHOUSE_URI` at `default`. It runs the FULL `tests/` dir (unmarked guards: migration splitter, org_id parity, pyformat-`%%`). The lefthook **pre-push** hook only runs ruff + mypy (not pytest), so a push succeeds even though the manual gate is heavier.
6. **web governance**: `dev-health-web` has an `enforce-src-test-policy` CI check — any `src/` change needs a test change or a `TEST-WAIVER:` line in the PR body.

## 5. Orchestration approach that worked (team-mode)

- 4 **file-disjoint** lanes (schema / projector / service / web) + dependency-staged tasks via `team_task_create` with `blockedBy`.
- Code projector/service against the **documented schema** in parallel with migrations (the schema was fully specified in the plan, so lanes didn't block on each other).
- The **lead does integration QA** (import-smoke + ruff/mypy/tests) after lanes land — this caught the circular import and the test-double gap that per-lane checks missed.
- Run tree-wide gates **only on a quiesced tree** (all members idle) — concurrent member writes race file reads/edits.

## 6. First steps for the next session

1. Pull the merged teams slice (or branch off `feat/chaos-2622-...` if building pre-merge).
2. Read plan §4.4 + `team-attribution.md` §0 (membership precedence) + this file.
3. Fold in any Oracle code-review findings on the teams slice.
4. Decide: reuse `team_drift_changes` with `entity_type='identity'` vs. a new `identity_drift_changes` table (lean reuse unless the membership change shape diverges).
5. Build the identity projector path that gates `team_memberships`, mirroring the team projector + lifecycle.
6. Add fake-store coverage + lifecycle tests; run `ci/local_validate.sh`; open the PR with the test-policy satisfied.
