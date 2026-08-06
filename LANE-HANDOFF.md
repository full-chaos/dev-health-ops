# lane-2b-cases handoff (session wind-down for binary upgrade, 2026-08-06 ~20:30 PDT)

Worktree: `ops-worktrees/chaos-3219-phase2-cases`, branch `chaos-3219-phase2-cases`,
based on `origin/main` @ `0829610ee` (includes ask-dev-world.v1 PR #1502,
persistence PR #1507). **Not pushed.** Last commit `a53b4659a` (WIP, this
wind-down). No background processes running — confirmed via `pgrep`.

## Corpus state: 143/143 ids complete, 93 authored / 50 declared-blocked

Full CHAOS-3219 Wave 4 corpus (134 frozen registry ids + 9 `subject-label`
amendment ids) is authored. Every case file, every `resolution-profiles/
deterministic-v1.json` entry, every `provider-scripts/role-legacy_agent.json`
script entry exists (93 authored cases all have both a profile entry and a
script entry — verified). Full rationale, per-group breakdown, and the
complete declared-blocked categorization live in
`tests/acceptance/world/ask-dev-world.v1/corpus/REGISTRY-AMENDMENT.v1.md`
(read sec.9 first — it's the most current summary).

Key docs (all in `tests/acceptance/world/ask-dev-world.v1/corpus/`):
- `CASE-SCHEMA.v1.md` — the binding case JSON schema (adopted by team-lead
  as the interface Lane 2a's runner loads against).
- `REGISTRY-AMENDMENT.v1.md` — the full narrative: registry corrections,
  amendment family, all 4 codex rounds' findings + dispositions (groups
  1+amendment), the groups-8-11 completion summary (sec.9), the blocked-id
  categorization now wired to real tickets (see below).

## Ticket-filing status: ALL 7 category tickets filed, ALL 50 blocked cases wired

Team-lead's ruling on `REGISTRY-AMENDMENT.v1.md` sec.8-9: file grouped
tickets per blocked-category, wire every case's `blocked_by` to a real
`CHAOS-\d+` ticket (2a's round-3+ validator now rejects free-text
`blocked_by`). **This is done — nothing left to file or wire:**

| ticket | category | ids covered |
|---|---|---|
| CHAOS-3454 | Multi-turn / connection-state choreography — runner capability | 10 (tenant.authorization-change-mid-conversation, 4× adv.mid-conversation.*, adv.abuse.cancellation-race, adv.abuse.route-transition-no-mutation, pers.duplicate-run-on-reconnect, pers.replay-no-rerun, adv.cross-tenant.conversation-id) |
| CHAOS-3455 | Scripted-fault-type gaps — runner capability | 7 (deg.budget-exhaustion.with/without-frame, deg.timeout.subject/health, adv.oversized.plan/evidence/scope) |
| CHAOS-3456 | Injection-payload fixture generator gap | 8 (all `adv.injection.*`) |
| CHAOS-3458 | Kill-switch enforcement absent in production (0 grep matches in src/) | 6 (all `ops.kill-switch.*`) |
| CHAOS-3459 | No real fixture-backed identifier for cross-tenant class | 3 (adv.cross-tenant.team-id/.user-id/.evidence-ref-id) |
| CHAOS-3460 | Non-question-shaped case type needed (groups 10/11 structural mismatch, routed to Phase 3/5 per team-lead) | 7 (ops.content-security-scan, ops.quota-cost-reconcile, pers.usage-cost-idempotent, ops.alert-test-recover, deg.false-green-aggregate, pers.migration-rollback-preserves-state, ops.rollback-preserves-state) |
| CHAOS-3461 | Small individual fixture-world/generator gaps | 3 (deg.source-state.redacted, deg.provider.misconfigured, deg.required-child-incomplete) |

Plus 6 already-ticketed pre-existing: CHAOS-3394 (1: attention.team.valid-
qualification-light-on-feature-work), CHAOS-3404 (4: ops.cleanup-purge-
recover, pers.purge-after-disable, pers.retention.0-day/30-day), CHAOS-3428
(1: deg.source-state.truncated).

`10+7+8+6+3+7+3 = 44` newly-ticketed `+` `6` already-ticketed `= 50` total
declared-blocked cases, all verified wired (`python3 /tmp/wire_blocked_by.py`
ran clean: `wired 44, already-ticketed 6, missing-assignment 0` — that
script no longer exists on disk, it was a `/tmp` scratch script; the wiring
itself is committed in the case files).

## Codex round state: 4 rounds run, round 4 partially addressed

Round 1-3 targeted the group-1+amendment (57-case) changeset — see
`REGISTRY-AMENDMENT.v1.md` sec.5/7/8 for full findings/dispositions.
Round 3 converged per this lane's "max 3 rounds then escalate" MUST DO;
team-lead ruled (b): accept as-is, defer 2 open findings (checker
implementation, status-aware declared-blocked) into the mandatory rebase
pass.

**Round 4** ran against the full groups-8-11 changeset (85 new ids).
Verdict: `needs-attention`, 3 findings:
1. **[high]** Corpus expectations not exercised by the runner-level test —
   SAME as the round-1/2/3 recurring finding (the durable execution/
   assertion gate is Lane 2a's runner, not this lane's `test_ask_dev_
   provider_roles.py` guard). Not independently fixed this round; same
   already-accepted disposition applies (team-lead ruling (b) covers this).
2. **[high]** Declared-blocked cases carry a placeholder invariant instead
   of a true blocked receipt — SAME as before, tracked in the mandatory
   rebase pass (below).
3. **[medium]** `load_registry_ids` treated an explicit `"amendments": null`
   the same as an absent key (silently degrading to empty instead of
   raising) — **FIXED this session**: `_collect_group_ids` now takes
   `key_present`/`absent_key_is_backward_compatible` params so `dict.get`'s
   inability to distinguish "absent" from "explicit null" no longer
   matters; `groups` (required) raises on either missing-or-null, `amendments`
   (optional) stays backward-compatible on absent but raises on explicit
   null. 3 new tests added (`test_load_registry_ids_rejects_an_explicit_
   null_amendments`, `..._groups`, `test_load_registry_ids_rejects_a_
   missing_groups_key`), **mutation-verified** (git-stashed the fix,
   confirmed all 3 go RED with the exact regression shape, popped the
   stash, confirmed GREEN again).

**No 5th codex round was run** — wind-down interrupted before launching
one. Whoever resumes should decide whether finding 3's fix alone warrants
a round 5, or whether to fold verification into the rebase-pass round
instead (my recommendation: fold it in, since findings 1-2 won't move
until the rebase pass anyway).

## Mandatory rebase-pass obligations (team-lead ruling, verbatim, NOT YET DONE)

> Ruling: (b), with sequencing that actually closes codex's asks before
> anything ships. Both recurring findings are the two work items I already
> routed to 2a, and the merge order (2a first, you rebase) means by the time
> YOUR changeset reaches a PR, the checkers and status-aware declared-blocked
> handling exist on main. So: converge now — your changeset is accepted
> as-is for this milestone — but your REBASE pass (after 2a lands) has two
> mandatory items: (1) wire the 3 real checkers onto every case whose
> taxonomy currently documents them (tenant/security/persistence invariants
> become executable), (2) delete the placeholder invariants on the 2 blocked
> cases and switch to 2a's first-class declared-blocked status. That pass is
> part of your definition-of-done, not optional cleanup — codex round 3's
> "keep blocked until those checks run" is satisfied by construction.

As of this wind-down, team-lead separately reported: **"2a has now landed
the 3 graduated checkers AND first-class declared-blocked status (third
receipt status value)"** — meaning these capabilities may already exist on
Lane 2a's side (worktree `ops-worktrees/chaos-3219-phase2-runner` as of
last check, not yet confirmed merged to `origin/main`). Task tracker showed
task #25 (Lane 2a runner) as `completed` as of this session's last check.

**On resume, before anything else:**
1. Check whether Lane 2a's PR has merged to `origin/main`. If yes, rebase
   this branch onto it.
2. Re-verify against the merged (not WIP-worktree) `case_schema.py`/
   `invariants.py`/`script_inventory.py` — the differential-oracle pattern
   used throughout this lane's work (`sys.path.insert` the runner module,
   `case_schema.load_corpus_cases`, `invariants.CHECKS`, `script_inventory.
   check_script_inventory`) is the fastest way to re-verify; do this BEFORE
   trusting anything below.
3. **Item 1 — wire the 3 real checkers.** Every case currently has
   invariant entries like `{"category": "no-unauthorized-candidate-
   surfaces", "check": "no_internal_error", ...}` with a `"$comment"`
   explicitly flagging `no_internal_error` as narrower than the claim.
   Find the 3 new checkers' real names in 2a's `invariants.py` (likely
   something like `no_unauthorized_candidate`, `scope_resolution_outcome_
   in`, `terminal_persists_assistant_row` — NOT CONFIRMED, read the file)
   and change `check` to the correct real name + `args` shape for every
   invariant whose `category` matches (grep `"$comment".*narrower` across
   `corpus/*.json` to find every affected entry — there are dozens, this
   was the majority of the invariant entries across all 93 authored cases).
   Also wire `expected_public_outcome`/`expected_scope_resolution_outcome`
   checks if 2a's new checkers cover those (the `REGISTRY-AMENDMENT.v1.md`
   sec.5 finding said those profile fields are never compared today).
4. **Item 2 — remove placeholder invariants, switch to first-class
   declared-blocked.** The 2 (now 50, since groups 8-11 added more)
   declared-blocked cases carry a `category: "declared-blocked-
   placeholder"` invariant purely to satisfy the old loader's non-empty-
   list requirement (documented in `CASE-SCHEMA.v1.md`'s declared-blocked
   section as an INTERIM exception, explicitly "do not build on it"). Once
   2a's loader is confirmed status-aware, revert every declared-blocked
   case's `invariants` to `[]` and update `CASE-SCHEMA.v1.md` to match
   (currently documents the placeholder as required — that section needs
   rewriting once this lands, back closer to the original "invariants: []"
   ideal it describes as aspirational).
5. Re-run the full pure-Python suite (`tests/test_fixtures_world_*.py` +
   `tests/acceptance/test_ask_dev_provider_roles.py` + `test_ask_dev_
   compose.py` + `test_ask_dev_quota_headroom.py`, `-m "not clickhouse and
   not benchmark"`) — last confirmed-green count before this wind-down was
   **183 passed** (pre-blocked_by-wiring-commit; the wiring commit's
   individually-run tests all passed but the full suite re-run was
   interrupted by the wind-down itself — re-run first).
6. Run one more codex adversarial-review round after items 1-2 land — this
   should finally close findings 1-2 from rounds 1-4 "by construction" per
   team-lead's own framing.
7. Report to team-lead, request gate slot / push per standing instructions.

## Known residual (not blocking, just noted)

- 2a's WIP `case_schema.py` loader (as of last direct read, NOT re-verified
  after this session's edits) started requiring `blocked_by` to match a
  `CHAOS-\d+` prefix pattern — already satisfied by the ticket-wiring done
  this session, but confirm again on resume since 2a's branch may have
  moved further.
- `REGISTRY-AMENDMENT.v1.md` is long (9 sections). Sec.9 is the most
  load-bearing for a fresh reader — it has the full groups-8-11 breakdown
  and blocked-category table matching this handoff's ticket table above
  (that section's category descriptions predate the ticket numbers; this
  handoff is now the authoritative cross-reference from category → ticket
  number → ids).
- Groups 10/11's structural finding (admin/system/CI-level claims don't fit
  the question/answer case schema) was explicitly escalated and accepted —
  team-lead is carrying "a non-question-shaped case type for persistence/
  ops claims" into Phase 3/5 design work. Nothing further needed from this
  lane on that point; CHAOS-3460 tracks it.

## Files changed this session (for quick orientation on resume)

- `tests/acceptance/world/ask-dev-world.v1/corpus/CASE-SCHEMA.v1.md` (new)
- `tests/acceptance/world/ask-dev-world.v1/corpus/REGISTRY-AMENDMENT.v1.md` (new)
- `tests/acceptance/world/ask-dev-world.v1/corpus/case-*.json` — 143 files (new)
- `tests/acceptance/world/ask-dev-world.v1/resolution-profiles/deterministic-v1.json` (new)
- `tests/acceptance/world/ask-dev-world.v1/provider-scripts/registry-ids.v1.json` (amendments block added)
- `tests/acceptance/world/ask-dev-world.v1/provider-scripts/role-legacy_agent.json` (93 script entries)
- `tests/acceptance/world/ask-dev-world.v1/subjects.json` (+1 subject: context-fabric)
- `src/dev_health_ops/llm/agent/provider_scripts.py` (load_registry_ids amendments support + defensive validation)
- `tests/acceptance/test_ask_dev_provider_roles.py` (+~15 new tests: coverage guard, fingerprint-drift guard, malformed-registry guards)
