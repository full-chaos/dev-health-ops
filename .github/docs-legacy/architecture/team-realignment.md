# Corrective Plan: ClickHouse-Only Team Attribution

## Purpose

Eliminate recurring team-attribution regressions by removing the split-brain model where Postgres stores team mappings while ClickHouse stores analytics attribution.

This environment is not production. There is no legacy data-preservation requirement. Old Postgres team attribution paths can be removed outright.

## Target outcome

After this change:

- ClickHouse is the only persistence layer used for team, project, member, repo, and manual fallback attribution.
- Postgres is not used for team attribution in any runtime, sync, analytics, or resolver path.
- Legacy Postgres team mapping models, services, bridge code, CLI paths, tests, and docs are removed or rewritten.
- Manual mappings become explicit ClickHouse fallback records, not hidden overrides.
- WTI-native issue facts outrank all fallback logic.
- PR/MR attribution comes from actual linked issue donors.
- External issue-key prefixes do not count as linked-issue inheritance.
- Attribution emits provenance everywhere.

## Core model

```text
WTI issue = canonical work unit
PR/MR = code-change evidence linked to the issue
commit = lower-level evidence linked through PR/MR when possible
manual mapping = explicit fallback only
ClickHouse = single attribution store
Postgres = auth/org/config only
```

## Non-negotiable invariants

### 1. ClickHouse owns attribution

ClickHouse owns all data used to resolve work attribution:

- teams
- team memberships
- project ownership
- repo ownership
- work items
- work item dependencies
- work item team attributions
- manual attribution fallbacks

Postgres must not be read by attribution code.

### 2. Manual fallback is not an override

Manual mappings are fallback attribution records.

They are only used when WTI-native attribution and linked issue attribution fail.

Manual mappings must never override:

- issue team
- issue project
- issue assignee/member
- actual linked issue donor attribution
- imported provider ownership facts

### 3. Issues are the canonical unit of work

Work Tracking Integrations include:

- GitHub
- GitLab
- Linear
- Jira

Normalized WTI entities:

- team
- project
- member
- issue

Provider-specific quality varies. Linear and Jira generally provide stronger issue/project semantics. GitHub and GitLab may require repo/project/group/member normalization.

### 4. PRs/MRs connect code to work

PRs and MRs are not the canonical unit of work. They are code-change evidence linked to an issue.

Correct attribution flow:

```text
Issue carries team/project/member/work intent
PR/MR carries implementation/change evidence
PR/MR links to issue
Commits link to PR/MR
Work attribution flows issue -> PR/MR -> commits/evidence
```

Allowed:

```text
PR/MR -> actual linked Linear/Jira/GitHub/GitLab issue -> donor team/project/member
```

Not allowed as linked-issue inheritance:

```text
PR/MR -> text CHAOS-123 -> prefix CHAOS -> team
```

If prefix-based mapping is needed, it must be represented as explicit manual fallback with low confidence.

## Attribution precedence

Implement deterministic staged resolution in this order:

1. WTI-native work item facts
   - native issue team
   - native issue project
   - native member/assignee
2. Imported provider ownership facts
   - provider project ownership
   - provider repo ownership
   - provider team/member ownership
3. Linked issue fallback
   - only from an actual linked donor issue row
   - only if the PR/MR has no stronger attribution
4. Manual fallback mapping
   - ClickHouse-only
   - explicit source
   - lower precedence than all imported/native facts
5. Unassigned

Preferred source order:

```python
_SOURCE_ORDER = {
    "native_team": 0,
    "issue_project": 1,
    "project_ownership": 2,
    "repo_ownership": 3,
    "assignee_membership": 4,
    "linked_issue": 5,
    "manual_fallback": 6,
    "unassigned": 7,
}
```

Prefer explicit staged resolution over relying only on numeric ranking.

## Provider × entity consumption (what we pull, and from where)

We pull **teams, projects, and members** for every integration that supports them (auto-import,
when the option is selected). One ingestion path: `run_team_autoimport(provider, org_id)` →
`team_autoimport_<provider>.populate()` → `discover_*` → writes ClickHouse directly. The Postgres
bridge/reconcile/`TeamMapping` were the *separate* path and are deleted (CHAOS-2600 CS5/CS6).

| provider | teams | projects | members | repo ownership | member store |
|---|---|---|---|---|---|
| linear | yes (`discover_linear`) | yes (`associations.project_keys`) | yes (`discover_members_linear`) | — | edges + roster |
| jira   | yes (`discover_jira`) | yes (`associations.project_keys`) | yes (`discover_members_jira_bulk`) | — | edges + roster |
| github | yes (`discover_github`) | n/a (repo = scope) | yes (`discover_members_github`) | yes (`team_repo_ownership`) | edges (+ roster, CS-COV) |
| gitlab | yes (`discover_gitlab`) | yes (GitLab project paths) | yes (`discover_members_gitlab`) | — | edges (+ roster, CS-COV) |

**Members are stored two ways — do not conflate:**
- `team_memberships` (edge table) — written by all four; this is what the **attribution precedence
  ladder reads** (`load_team_attribution_context` → `member_by_identity` → `assignee_membership`).
  All four providers' members are consumed here.
- `teams.members` (roster on the team row) — written by linear/jira auto-import + the admin
  surgical-replacement surface; read by the secondary resolver + admin/display. **We also pull
  members of orgs and teams from github and gitlab into this roster** (CS-COV) for display +
  secondary-resolver consistency.

**Chain & deferred reconciliation:** members → assignee identity → issues → PRs/MRs → (maybe)
commits. Commit **authors** are pulled separately from the git side; reconciling commit-author
identity against team-member identity is acknowledged future work, **not** part of CHAOS-2600.

## Phase 1: Remove Postgres attribution model

Delete or neutralize Postgres team attribution components.

Remove runtime dependency on models similar to:

```text
TeamMapping
IdentityMapping
```

Remove code paths that:

- read team mappings from Postgres
- read identity mappings from Postgres for attribution
- bridge Postgres team mappings into ClickHouse
- treat Postgres team mappings as source of truth
- expose CLI commands that sync teams through Postgres first

Likely target areas:

```text
src/dev_health_ops/providers/team_bridge.py
src/dev_health_ops/models/settings.py
src/dev_health_ops/api/services/configuration/
src/dev_health_ops/cli/
docs/architecture/database-architecture.md
docs/ops/cli-reference.md
tests/test_team_reconcile_guards.py
```

Expected result:

```text
grep -R "TeamMapping" src tests docs
grep -R "IdentityMapping" src tests docs
grep -R "bridge_teams_to_clickhouse" src tests docs
```

These should return no runtime attribution usage. Deleted-code references in removed migrations are acceptable only if they are inert and not imported.

## Phase 2: Add ClickHouse manual fallback table

Create a ClickHouse table for manual fallback attribution.

```sql
CREATE TABLE IF NOT EXISTS manual_attribution_fallbacks
(
    org_id String,
    provider LowCardinality(String),
    scope_type LowCardinality(String),
    scope_id String,
    team_id String,
    team_name String,
    reason String,
    priority Int32 DEFAULT 100,
    valid_from DateTime DEFAULT now(),
    valid_to Nullable(DateTime),
    created_by Nullable(String),
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY org_id
ORDER BY (org_id, provider, scope_type, scope_id, team_id);
```

Allowed `scope_type` values:

```text
repo
project
member
issue_key_prefix
```

Rules:

- `issue_key_prefix` is manual fallback only.
- It must never be treated as linked-issue inheritance.
- It must emit `source = manual_fallback`.
- It must emit low/manual confidence.
- It must include evidence showing the matched fallback record.

## Phase 3: Create ClickHouse-native team and ownership writers

Ensure the system can write these directly to ClickHouse:

```text
teams
team_memberships
team_project_ownership
team_repo_ownership
manual_attribution_fallbacks
```

Do not write these to Postgres.

Do not bridge these from Postgres.

Do not use a Postgres staging table.

Expected service boundary:

```text
WTI connector -> normalization -> ClickHouse writer
manual admin/config input -> ClickHouse writer
metrics resolver -> ClickHouse loader -> attribution result
```

## Phase 4: Fix attribution resolver behavior

Update attribution resolver logic so linked issue inheritance and manual fallback cannot override stronger attribution.

Required behavior:

```python
def resolve_team_attribution(...):
    native = resolve_native_issue_facts(...)
    if native:
        return native

    ownership = resolve_imported_provider_ownership(...)
    if ownership:
        return ownership

    linked_issue = resolve_actual_linked_issue_donor(...)
    if linked_issue:
        return linked_issue

    manual = resolve_manual_fallback(...)
    if manual:
        return manual

    return unassigned()
```

Remove bad behavior:

```python
_SOURCE_ORDER = {
    "native_team": 0,
    "linked_issue": 1,
    "project_ownership": 2,
    "repo_ownership": 3,
    "assignee_membership": 4,
    "unassigned": 5,
}
```

Remove any resolver behavior where linked issue wins over:

- project ownership
- repo ownership
- assignee membership
- native provider team/project/member facts

## Phase 5: Fix linked issue inheritance

Linked issue inheritance requires an actual donor work item.

Allowed donor path:

```text
work_item_dependencies.source_work_item_id = PR/MR
work_item_dependencies.target_work_item_id = issue
target issue exists in work_items
target issue has resolved team attribution
PR/MR inherits target issue attribution only if PR/MR has no stronger attribution
```

Not allowed:

```text
PR/MR body contains CHAOS-123
CHAOS prefix maps to team
PR/MR inherits linked_issue source
```

If no donor issue row exists, attribution remains unresolved until manual fallback runs.

If manual fallback matches, source must be:

```text
manual_fallback
```

not:

```text
linked_issue
```

## Phase 6: Persist provenance

Every final attribution should include provenance.

Required fields:

```text
org_id
work_item_id
provider
team_id
team_name
source
confidence
evidence
is_primary
computed_at
```

Required source enum:

```text
native_team
issue_project
project_ownership
repo_ownership
assignee_membership
linked_issue
manual_fallback
unassigned
```

Recommended confidence enum:

```text
high
medium
low
manual
none
```

Examples:

```json
{
  "source": "linked_issue",
  "confidence": "high",
  "evidence": {
    "dependency_type": "implements",
    "donor_work_item_id": "linear:issue:CHAOS-123",
    "donor_provider": "linear"
  }
}
```

```json
{
  "source": "manual_fallback",
  "confidence": "manual",
  "evidence": {
    "scope_type": "repo",
    "scope_id": "full-chaos/dev-health-ops",
    "reason": "explicit manual fallback configured"
  }
}
```

```json
{
  "source": "unassigned",
  "confidence": "none",
  "evidence": {
    "reason": "no native issue facts, linked issue donor, ownership, or manual fallback matched"
  }
}
```

## Phase 7: Delete or invert incorrect tests

Delete or invert tests that encode linked issue as stronger than real attribution.

Search for tests with names or assertions similar to:

```text
test_linked_issue_wins_over_assignee_membership
test_state_duration_linked_issue_wins_over_assignee_membership
linked_issue primary over assignee
linked_issue wins over project
linked_issue wins over repo
```

Replacement tests:

```python
def test_native_issue_team_wins_over_everything():
    ...

def test_project_ownership_wins_over_linked_issue():
    ...

def test_repo_ownership_wins_over_linked_issue():
    ...

def test_assignee_membership_wins_over_linked_issue():
    ...

def test_linked_issue_applies_when_no_stronger_attribution_exists():
    ...

def test_linked_issue_requires_actual_donor_issue_row():
    ...

def test_issue_key_prefix_without_donor_does_not_inherit_linked_issue():
    ...

def test_issue_key_prefix_can_match_manual_fallback_only():
    ...

def test_manual_fallback_does_not_override_native_issue_team():
    ...

def test_manual_fallback_does_not_override_linked_issue_donor():
    ...

def test_unassigned_when_no_native_linked_ownership_or_manual_fallback():
    ...
```

## Phase 8: Delete obsolete legacy pathways

Because this is not production, do not preserve compatibility for legacy Postgres attribution.

Delete rather than migrate:

- Postgres team mapping models
- Postgres identity mapping models used for attribution
- Postgres-to-ClickHouse bridge code
- CLI bridge commands
- tests that validate bridge behavior
- docs that describe Postgres team mappings as active attribution infrastructure

If import deletion causes admin/API tests to fail, either:

1. remove the obsolete admin surface, or
2. rewrite it to write directly to ClickHouse.

Do not add a backfill command.

Do not keep a compatibility shim unless required only to print a hard deprecation error.

Acceptable shim behavior:

```text
This command was removed. Team attribution is ClickHouse-only.
```

Unacceptable shim behavior:

```text
Read Postgres mappings and write them to ClickHouse.
```

## Phase 9: Documentation updates

Update backend docs:

```text
docs/architecture/team-attribution.md
docs/architecture/database-architecture.md
docs/architecture/data-pipeline.md
docs/ops/cli-reference.md
AGENTS.md
```

Add this invariant prominently:

```md
## Team attribution source of truth

ClickHouse is the only source used for analytics attribution.

Postgres does not store or resolve team attribution mappings.

Manual mappings are ClickHouse fallback records only. They are never overrides and never outrank WTI-native facts.

PR/MR attribution comes from actual linked issue donors. An external issue key prefix alone is not linked-issue inheritance.
```

Remove language implying:

- Postgres owns `team_mappings`
- Postgres owns `identity_mappings` for attribution
- team sync writes to Postgres before bridging to ClickHouse
- Postgres manual mappings are part of normal attribution resolution

## Phase 10: Frontend boundary documentation

In `dev-health-web`, add:

```text
docs/architecture/team-attribution-boundary.md
```

Content:

```md
# Team Attribution Boundary

dev-health-web never resolves team attribution.

The frontend renders persisted backend attribution and provenance only.

If team coverage is low or unassigned is high, do not add frontend mapping logic. Fix backend sync, linked issue capture, ClickHouse ownership data, or ClickHouse manual fallback records.

Manual fallback mappings must be visible as `manual_fallback`, not hidden as issue/team truth.
```

Link it from:

```text
AGENTS.md
docs/user-journeys/investment-view.md
```

## Phase 11: Documentation freshness and drift prevention

Documentation must be updated as part of the implementation, not after the fact.

Any code change that affects team attribution, WTI normalization, PR/MR issue linking, manual fallback behavior, ClickHouse attribution tables, API response provenance, or frontend attribution display must update the relevant docs in the same PR.

Required backend documentation updates:

```text
docs/architecture/team-attribution.md
docs/architecture/database-architecture.md
docs/architecture/data-pipeline.md
docs/ops/cli-reference.md
AGENTS.md
```

Required frontend documentation updates:

```text
docs/architecture/team-attribution-boundary.md
docs/user-journeys/investment-view.md
AGENTS.md
```

Add or update a documentation guardrail in `AGENTS.md`:

```md
## Documentation freshness requirement

When changing attribution behavior, update the matching architecture and product docs in the same PR.

Do not change team attribution code without updating:

- backend attribution contract docs
- database ownership docs
- CLI/admin behavior docs
- frontend attribution boundary docs, if API or display behavior changes

If docs and code disagree, the implementation is incomplete.
```

Add this to the PR checklist or developer notes if one exists:

```md
- [ ] Team attribution docs updated
- [ ] Database ownership docs updated
- [ ] CLI/admin docs updated, if behavior changed
- [ ] Frontend attribution boundary docs updated, if API/display behavior changed
- [ ] Tests assert the documented attribution precedence
```

Documentation acceptance criteria:

- Every removed Postgres attribution path is removed from docs.
- Every new ClickHouse attribution table is documented.
- Manual fallback behavior is documented as fallback, not override.
- Linked issue inheritance is documented as requiring an actual donor issue row.
- Issue-key-prefix attribution is documented only as manual fallback.
- API provenance fields are documented.
- Frontend docs state that web never resolves attribution.
- Tests and docs describe the same precedence order.


## Phase 12: API and UI expectations

Backend APIs should expose attribution provenance when returning team-attributed work.

Minimum response shape:

```json
{
  "team_id": "platform",
  "team_name": "Platform",
  "team_attribution_source": "linked_issue",
  "team_attribution_confidence": "high",
  "team_attribution_evidence": {
    "donor_work_item_id": "linear:issue:CHAOS-123"
  }
}
```

Frontend should:

- display team name as today
- avoid recomputing attribution
- optionally surface manual fallback as lower-confidence attribution
- show unassigned when backend says unassigned
- not implement repo/team mapping logic

## Acceptance criteria

Codex is complete only when all are true:

- No runtime attribution code reads Postgres team mappings.
- No runtime attribution code reads Postgres identity mappings.
- `team_bridge.py` is deleted or converted to a hard deprecation error.
- No Postgres-to-ClickHouse team bridge remains.
- Manual mappings are stored in ClickHouse only.
- Manual mappings emit `source = manual_fallback`.
- Linked issue inheritance is lower precedence than native/imported ownership attribution.
- Linked issue inheritance requires an actual donor issue row.
- Issue-key-prefix matching is not linked-issue inheritance.
- Issue-key-prefix fallback, if implemented, emits `manual_fallback`.
- Attribution output includes source, confidence, and evidence.
- Tests explicitly protect the precedence model.
- Backend docs state ClickHouse-only attribution.
- Frontend docs state the UI never resolves team attribution.
- Documentation freshness is enforced in `AGENTS.md` or the repo PR checklist.
- Any attribution behavior change updates docs in the same PR.
- Tests and docs describe the same attribution precedence order.
- No backfill, migration-retention, or legacy compatibility path exists for Postgres team attribution.

## Suggested branch

```bash
git checkout -b fix/clickhouse-only-team-attribution
```

## Suggested commit sequence

```text
docs: define clickhouse-only team attribution contract
feat(clickhouse): add manual attribution fallback storage
refactor(metrics): remove postgres attribution dependency
fix(metrics): make linked issue attribution a true fallback
feat(metrics): persist attribution provenance
test(metrics): lock team attribution precedence
docs(web): document attribution rendering boundary
chore: remove legacy postgres team mapping paths
```

## Final warning for Codex

Do not preserve the old Postgres attribution path.

Do not write a backfill.

Do not add a compatibility bridge.

Do not make manual mappings an override.

Do not infer team attribution from issue-key prefixes unless the result is explicit `manual_fallback` provenance.
