# CHAOS-3024 Review Ledger

## Frozen review metadata

- Plan: `.omo/plans/chaos-3024.md`
- Plan version: v9
- Rubric version: 1
- Baseline Ops SHA: `5aab0696e9d81666113e476548795d3b2ffb591f`
- Baseline Web SHA: `0bd2fa06df4e96e6a68a932ff63ec4aed7919a97`
- Review state: `awaiting-baseline-domain-reviews`

## Rules

1. IDs never change or get recycled.
2. Blockers require repository/provider evidence and a minimal closure condition.
3. Delta reviews address changed lines and open IDs only.
4. Late blockers against unchanged text are valid only for security, cross-tenant access, irreversible/silent data loss, gate bypass, or provider-semantic contradiction.
5. Non-blocking findings become Linear follow-ups and do not trigger another full plan rewrite.
6. One adjudication pass is allowed when reviewers conflict.
7. Passing means zero open blocker IDs, not an unqualified fresh rewrite review.

## V8 validation findings closed by v9

| ID | Domain | Severity | V8 finding | Minimal closure | V9 status |
| --- | --- | --- | --- | --- | --- |
| A-001 | architecture | blocker | New SourceState lifecycle/mode/generation system duplicated the existing integration, source, dataset, feature, and worker state models. | Remove it; use the org feature plus current integration and worker state. | resolved |
| A-002 | architecture | blocker | New canonical incident lease duplicated existing `SyncRunUnit` lease/retry and outbox claim state. | Reuse current leases; add gate checks at worker/write boundaries. | resolved |
| A-003 | storage | blocker | Intent/digest columns across all twelve ClickHouse tables created a new transaction protocol unrelated to the release gate. | Retain deterministic IDs and `source_version_at`; use provider-specific idempotency. | resolved |
| A-004 | delivery | blocker | Bespoke manifest/receipt/aggregate/verifier modules created a release-governance product inside the feature remediation. | Use normal CI, JUnit/Playwright, GitHub Actions artifacts, and Linear/GitHub links. | resolved |
| A-005 | review | blocker | “Fresh unconditional PASS” from two reviewers made convergence undefined and allowed endless new rubrics. | Freeze reviewer domains, stable findings, delta review, late-finding rule, and zero-open-blocker pass. | resolved |
| A-006 | execution | blocker | One immutable run ID across every task had no legitimate retry/attempt model. | Remove the cross-task run ID. CI runs and provider evidence retain their native attempt identities. | resolved |
| A-007 | evidence | blocker | Receipts required `dirty=false` before the task’s commit, which cannot hold for an implementation task testing uncommitted changes. | Use PR commit SHAs and CI on committed code. | resolved |
| A-008 | tests | blocker | Global zero-skips conflicted with conditional Atlassian GO/NO_GO branches and env-gated provider tests. | Fail unexpected skips; represent NO_GO as deterministic tests; require separate live evidence for GO. | resolved |
| A-009 | contract | blocker | Task 1 said backfill acquires no lease while later tasks required backfill claims. | Remove the new lease protocol; reuse each existing execution/idempotency mechanism. | resolved |
| A-010 | queues | blocker | Scratch workers named `system-webhook`, but the repository consumes `webhooks`. | Reuse the existing `webhooks` and `external-ingest` queues. | resolved |
| A-011 | scope | blocker | Global sink migration included GitHub while another task prohibited GitHub canonical incident writes. | Exclude GitHub from the native canonical path; avoid a global write-intent rewrite. | resolved |
| A-012 | verification | blocker | “Hermetic live” combined isolated local infrastructure with real third-party provider calls. | Separate local scratch proof from credentialed provider-live evidence. | resolved |
| A-013 | cross-repo | blocker | The plan assumed untracked sibling `ops/` and `web/` worktrees plus a custom root manifest. | Use separate PRs and a final CI compatibility workflow pinned to SHAs. | resolved |
| A-014 | migrations | non-blocking | Hard-coded future migration numbers would drift before implementation. | Allocate migration revisions at implementation time and verify a single Alembic head. | resolved |
| A-015 | commands | blocker | Several “exact” commands referenced not-yet-defined groups, inconsistent wrapper ancestry, or nonexistent files. | Task PRs name existing tests plus files created in the same task; normal CI is authoritative. | resolved |
| A-016 | release | blocker | Canary/waiver evidence blocked implementation merge even though the feature remains default off. | Separate implementation merge readiness from CHAOS-3031 controlled enablement. | resolved |

## Baseline reviewer findings

Add new findings below. Do not rewrite the closed V8 rows.

| ID | Reviewer | Domain | Severity | Plan section / evidence | Minimal closure | Status |
| --- | --- | --- | --- | --- | --- | --- |
| M-001 | Momus | — | — | Awaiting baseline review | — | open |
| O-001 | Oracle | — | — | Awaiting baseline review | — | open |

When a reviewer has no finding, replace its placeholder row with a signed `PASS` row referencing the reviewed plan commit SHA. Do not create an empty new full-review cycle.
