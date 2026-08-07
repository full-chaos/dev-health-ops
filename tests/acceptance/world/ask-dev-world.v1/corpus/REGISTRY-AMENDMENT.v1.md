# Corpus registry amendment v1 (CHAOS-3219 Phase 2 Lane 2b)

Amends `corpus-registry-v1.md` (accepted 2026-08-05, 134 ids / 43 families,
checked in verbatim as `../provider-scripts/registry-ids.v1.json`). **The
freeze governs case IDs, not their count or blocked-status accuracy** —
per the CHAOS-3389 fold-in ticket comment: "3389's eval fixtures land as
corpus cases now... the handful that don't [already have registry ids] get
added as a registry amendment." This document is that amendment, plus two
blocked-status corrections the frozen registry's checked-in copy had gone
stale on by authoring time.

## 1. Blocked-status corrections (no new ids, existing ids only)

`registry-ids.v1.json`'s `blocked` maps were last written before CHAOS-3393
landed and before CHAOS-3428/CHAOS-3404 were filed. Corrected in this pass:

| id | was | now | evidence |
|---|---|---|---|
| `portfolio.multi-project.status` | blocked on CHAOS-3393 | **unblocked, authored** | CHAOS-3393 confirmed `State: Done` (linear-cli, checked live) |
| `portfolio.org-wide.status` | blocked on CHAOS-3393 | **unblocked, authored** | same |
| `deg.source-state.truncated` | not marked blocked (group 9 `blocked: {}`) | **blocked on CHAOS-3428** | `sources.json`'s own `truncated` row already carries `"status": "declared-blocked", "blocked_by": "CHAOS-3428"` (Lane 1a, live-verified 2026-08-05: max observed work-graph fan-out is 2, generator has no many-PRs-per-item clustering to reach `MAX_NEIGHBORS=25`) — the registry-ids copy had never been updated to match |
| `pers.retention.0-day` | not marked blocked | **blocked on CHAOS-3404** | CHAOS-3404 confirmed open (`Backlog`, `Priority: High`): `DevPersistenceService.cleanup_expired` has no beat schedule/CLI/caller — 0/30-day retention never executes in production, so this case cannot pass against real production code yet |
| `pers.retention.30-day` | not marked blocked | **blocked on CHAOS-3404** | same |

`attention.team.valid-qualification-light-on-feature-work` was already
correctly marked blocked on CHAOS-3394 (confirmed still open, `Backlog`) —
no change.

## 2. New ids — CHAOS-3389 eval-fixture family `subject-label` (9 cases)

CHAOS-3389's discovery comments (ticket 2026-08-05, `c3407-oracle.md` /
`t-llm-matching.md` scratchpad evidence) pin four concrete, *live*
`_NAME`-regex failures against one real subject
(`"Dev Health Agent Context Runtime (Context Fabric)"` — the literal
fixture string already canonical across `test_alias_matching.py`,
`test_subject_preflight.py`, `test_chaos_3421_leak_graceful_terminal.py`)
plus five further mention-shape scenarios the fold-in eval-set list names
that had no dedicated registry id. Filed as one new family, `subject-label`
(44th family), all under corpus group 1 (`scope.*`'s sibling — same
group-1 `Family scope` machinery, kept as a distinct family because these
cases are about mention-parsing robustness specifically, not the 8
`subjects.json` shape classes `scope.*` already exhausts).

Realized against the **real generated world**: a new `subjects.json` entry
(`subject.exact.context-fabric`, `entity_kind: project`, backing repo
`meridian/context-fabric`) added via the same generic declarative pipeline
every other `entity_kind: project` subject already uses
(`fixtures/world.py`'s `_run_clickhouse_postprocess` builds a real
`projects` row + repo roster entry for any `subjects.json` entry with
`entity_kind: "project"` — confirmed by reading the function directly, not
assumed). No generator code changes needed for 8 of the 9; see the
`wrong-kind-collision` note below for the 9th.

| id | scenario | mention | subject_ref | resolution_profile_ref expectation (deterministic-v1) | proves |
|---|---|---|---|---|---|
| `subject-label.parenthetical-unquoted` | CHAOS-3407 primary bug: unquoted full label with parenthetical qualifier extracts ZERO typed mentions | "…the Dev Health Agent Context Runtime (Context Fabric) project…" | `subject.exact.context-fabric` | today: graceful `needs_clarification` or `not_found` (never internal_error / never silent org-wide) — CHAOS-3407 fix not yet landed, so exact resolution is NOT asserted, only the invariant is | CHAOS-3389 eval set; CHAOS-3407 evidence |
| `subject-label.parenthetical-quoted-control` | positive control: quoting the full label resolves fine today | "\"Dev Health Agent Context Runtime (Context Fabric)\"" | `subject.exact.context-fabric` | `answered`, exact resolution | CHAOS-3389 eval set ("quoted controls") |
| `subject-label.five-word-truncation` | no-parenthetical ≥5-word form; `_NAME`'s 4-capitalized-word cap silently drops "Dev" | "…the Dev Health Agent Context Runtime project?" | `subject.exact.context-fabric` | today: same graceful-terminal invariant as `parenthetical-unquoted`, exact resolution NOT asserted | CHAOS-3389 eval set ("≥5-word labels"); CHAOS-3407 evidence |
| `subject-label.acronym-mention` | acronym mention for the same subject | "ACR" | `subject.exact.context-fabric` | today: NOT expected to resolve via `alias_matching.py` (no acronym derivation is registered for this subject — CHAOS-3388's `acronym_candidates` only fires off a display name carrying a parenthetical alias registered in `subjects.json`, which this subject deliberately does NOT have, to isolate the "bare 3-letter acronym with no catalog alias backing it" shape from `scope.acronym-alias-subject`'s already-covered "MWA" case); graceful terminal only | CHAOS-3389 eval set ("ACR acronym") |
| `subject-label.typo-tolerance` | single-character-edit misspelling of an existing exact subject | "meridin/web-app" (typo of `meridian/web-app`) | `subject.exact.meridian-web-app` | today: graceful terminal (deterministic stack has no fuzzy/edit-distance matching) — NOT expected to resolve | CHAOS-3389 eval set ("typos") |
| `subject-label.word-order-variation` | reordered words of an existing project's display name | "Web Application Meridian" (reorder of "Meridian Web Application") | `subject.acronym-alias.mwa-project` | today: graceful terminal, NOT expected to resolve (alias_matching is literal/acronym, not order-invariant) | CHAOS-3389 eval set ("word-order variation") |
| `subject-label.wrong-kind-collision` | typed PROJECT mention loses to unrelated ISSUE/PR-title substring hits (CHAOS-3422) | "What's the status of meridian/api-gateway?" | `subject.bounded-set.web-app-and-api-gateway` member `meridian/api-gateway` | today: candidate ranking is kind-unfiltered — invariant-only (must not surface a WRONG-KIND unauthorized result, may legitimately need clarification); NOT expected to cleanly auto-resolve | CHAOS-3389 eval set ("wrong-kind collisions"); CHAOS-3422 evidence. **Realized from existing data, no generator change**: `fixtures/generators/work_items.py generate_work_items` already titles every work item `f"[{project}] {category}/{sub_category} {item_type} {i}"` — `meridian/api-gateway`'s own work items already produce real substring collisions against its own project-name mention, matching CHAOS-3422's shape (a project mention ranked behind issue/PR substring hits) without inventing new fixture rows. |
| `subject-label.valid-prior-turn-reference` | positive counterpart to `scope.stale-context-subject`: a bare follow-up ("it") correctly resolves because the earlier commit is still within the conversation's rotation window | multi-turn: turn 1 names `meridian/web-app`, turn 2 (same conversation, <1h later) asks "it" | `subject.exact.meridian-web-app` | `answered`, resolves to the turn-1 subject | CHAOS-3389 eval set ("prior-turn references") — the fold-in list distinguishes this from the already-covered STALE case (`subject.stale-context.rotated-service`, >9 days) |
| `subject-label.unauthorized-candidate-excluded` | a fuzzy/close-match shortlist must never include a candidate outside the requester's authorized catalog, even when it would otherwise rank as a plausible textual match | "What's the status of the private-repo project?" asked from **primary** org (textually close to sibling's `sibling-only/private-repo`) | `subject.tenant.sibling-only-private-repo` | `not_found` (never a clarification candidate list containing the sibling row) | CHAOS-3389 eval set ("unauthorized candidates"); `inv.no-unauthorized-candidate-surfaces` |

**Not minted as new ids (explicit, not a silent drop):**

- **"ambiguous/stale aliases"** — substantially covered by existing
  `scope.ambiguous` (two same-named repos), `scope.stale-context-subject`
  (`subject.stale-context.rotated-service`), and
  `scope.acronym-alias-subject` (`subject.acronym-alias.mwa-project`,
  `subject.acronym-alias.ground-control-team`). No dedicated
  alias-of-an-alias fixture exists in the world and building one adds a
  second axis of ambiguity without a distinct invariant to prove beyond
  what these three already exercise — flagged here rather than silently
  omitted.
- **"true no-match"** — already `scope.no-match`
  (`subject.no-match.ask-dev-project`) verbatim. No new id.

## 3. Registry totals after amendment

| | ids | families |
|---|---|---|
| Frozen (2026-08-05) | 134 | 43 |
| Amendment (`subject-label`) | +9 | +1 |
| **Total** | **143** | **44** |

`registry-ids.v1.json` gets a new top-level `"amendments"` key (additive,
the original `"groups"` 134-id block is untouched byte-for-byte) recording
this exact table, so the checked-in referential-integrity oracle stays the
single source of truth for provider-scripts cross-checks.

## 4. Registry-sketch corrections found during group-1 authoring/QA (2026-08-06)

`corpus-registry-v1.md`'s per-id "outcome" column is a pre-implementation
sketch, not a code-verified contract — three group-1 cases disagreed with
it once checked against real production code or the real fixture roster.
Recorded here (per team-lead request) as the audit trail a future reader
checks when a case's `resolution-profiles/deterministic-v1.json` entry
disagrees with the original registry table; the *case* and *profile*
entries are the corrected, authoritative values, not the table below.

| id | registry sketch said | corrected to | evidence |
|---|---|---|---|
| `provider-fail.before-frame` | outcome "failed" | `expected_public_outcome: temporarily_unavailable` | `terminal_frames.PUBLIC_OUTCOME_BY_ERROR_CODE` is a totality-checked table (import-time guard over every `ORCHESTRATOR_ERROR_CODES` member, read directly) mapping `provider_unavailable` → `PublicOutcome.TEMPORARILY_UNAVAILABLE` unconditionally, keyed purely by error code with no frame-state branch. Settled by the code, not a judgment call. |
| `provider-fail.after-frame` | outcome "answered_with_gaps or failed" | `expected_public_outcome: temporarily_unavailable` | same table/mechanism as `before-frame` — the round-0 committed `query_metric.v1` frame (CHAOS-3423 savepoint fix, `d91fd769e`) survives as transcript evidence but does not change the wire outcome bucket, since the mapping never branches on frame state. |
| `health.team.exact-subject` | subject class "exact" / outcome "answered" | `subject_class: acronym-alias`, `subject_ref: subject.acronym-alias.ground-control-team`, `expected_public_outcome: needs_clarification` | The only real team-health fixture for "Ground Control" is the literal-parenthetical alias subject (`subjects.json`), not a bare exact-name team. Per `alias_matching.py`, acronym/literal-alias hits are never auto-commit eligible (the same rule `scope.acronym-alias-subject` already established for MWA) — a single-candidate confirmation surfaces, not a direct `answered`. |

**Not a correction, a substitution (subject didn't exist in the real world,
per the generate-from-the-producer rule):** `health.project.not-applicable-fixture-only`'s
registry sketch names a project called "Nightfall", which has no `subjects.json`
row. Substituted `meridian/api-gateway` (a real `entity_kind: project` row,
already a `subject.bounded-set.web-app-and-api-gateway` member) — same
scenario shape, real fixture data instead of an invented name.

**Also worth recording (a documented judgment call, not strictly a
correction):** `portfolio.multi-project.status`'s registry sketch question
("the meridian and atlas projects") would pull in `subject.ambiguous.atlas`
— testing scope-ambiguity resolution, not the CHAOS-3393 multi-project
portfolio path this id exists to prove. Reworded to name two unambiguous
repos (`meridian/web-app`, `meridian/api-gateway`) so the case exercises
what its id promises.

## 5. Codex round-1 findings (2026-08-06) — remediation log

First codex adversarial-review round returned `needs-attention` / "do not
ship" against the 57-case group-1+amendment changeset, having read Lane
2a's real runner code (`ops-worktrees/chaos-3219-phase2-runner`) directly.
All 5 findings independently validated against source before fixing, per
the medium+ mandatory-validation rule. Disposition:

| finding | severity | disposition |
|---|---|---|
| Case/profile schema shape incompatible with Lane 2a's `case_schema.py` loader (`invariants[]` needs `category`/`check`, profile needs `schema_version` prefixed `resolution-profile.` + `profile_id`) | high | **FIXED** — every case's `invariants[]` rewritten to `{category, check, args}` (real checks: `no_internal_error`, `resolution_path_in`); `deterministic-v1.json` renamed keys. Richer taxonomy (no-silent-widening, no-unauthorized-candidate, terminal-persists specifically) preserved as `assert` text + `category` label but honestly marked `executable_today` only via the narrower `no_internal_error` floor — Lane 2a's `invariants.py` has no checker yet for the fuller claim (tracked, not silently claimed enforced) |
| 50 of 57 cases had no `provider-scripts/role-legacy_agent.json` entry; `script_inventory.py` requires 100% coverage | high | **FIXED** — added entries for every non-blocked case (§6 below); question text de-duplicated first since production requires globally-unique questions per role file |
| `tenant.authorization-change-mid-conversation` claims multi-turn/authorization-mutation behavior neither the case schema nor the runner can express or execute | high | **FIXED by declaring blocked** — `blocked_by: "runner: no structured multi-turn / actor-authorization-mutation support"`, per codex's own recommendation. Removed from `deterministic-v1.json` |
| `subject-label.wrong-kind-collision`'s bare lowercase slug question extracts zero mentions; can't reach the CHAOS-3422 collision path at all | high | **FIXED, twice** — first to a quoted/kind-noun form (fixed the immediate parse failure), then to a PARTIAL slug (`project "api-gateway"`, not the full `"meridian/api-gateway"`) once further analysis showed a full, exact, quoted slug hits the catalog directly and never reaches the close-match/fuzzy path CHAOS-3422 is actually about |
| `resolution-profiles/deterministic-v1.json`'s `expected_public_outcome`/`expected_scope_resolution_outcome` fields are never compared by the runner; non-executable case invariants could let a wrong answer pass silently | high | **PARTIALLY FIXED** — added the real `resolution_path_in` executable check (wired to `resolution_path`, which the runner DOES derive and compare) to every authored case; `expected_public_outcome`/`expected_scope_resolution_outcome` remain unchecked by any Lane 2a checker today — same documented gap as the invariant taxonomy above, not silently claimed as enforced |

**Cascading discovery during finding-4/5 remediation, beyond codex's own
findings:** `question_interpreter.py`'s own module docstring states "a
lowercase slug ... produces no mention" — `extract_mentions` requires
either a capitalized token or a QUOTED span, always adjacent to a closed
kind noun. This bug was NOT unique to `wrong-kind-collision`; **21 more
cases** used a bare, unquoted, lowercase repo slug (`meridian/web-app`,
`meridian/api-gateway`, `legacy-billing`, `sibling-only/private-repo`,
`probe/source-stale`) and would have silently extracted zero mentions,
degrading to org-wide behavior while their profile entries claimed
`exact`/`deterministic-exact`. Fixed by rewording to the test-proven
`<kind noun> "<slug>"` pattern (`test_question_interpreter.py`'s own
`'How is repo "dev-health-ops" doing?'` example) for every case free to
change its question text. Three cases (`unrelated-evidence.named-subject`,
`provider-fail.before-frame`, `provider-fail.after-frame`) could NOT be
reworded — their question text must stay byte-identical to an
already-landed Lane 1b `role-legacy_agent.json` entry — so their
`resolution_path` was corrected to `null` instead (honest: no mention ever
extracts from that pre-existing text either). Also corrected: a systematic
audit found the same `deterministic-exact`-when-it-should-be-`null`
mistake on every genuinely n/a-subject case (`portfolio.*`, `investment.*`,
`scope.outcome.organization-fallback`, etc.) — `resolution_path.py`
(Lane 2a, read directly) derives this field strictly from whether
`extract_mentions` named a mention this turn, not from whether resolution
"conceptually" ran; CASE-SCHEMA.v1.md's nullability rule section is
corrected to match.

## 6. Provider-script coverage (`role-legacy_agent.json`)

`script_inventory.py` (Lane 2a) requires every authored corpus case id to
have an entry in `role-legacy_agent.json`'s `cases` map — a case with no
entry would otherwise silently fall through to the unscripted default
heuristic, which is exactly correct production behavior but the wrong
behavior for a corpus run (see that module's own docstring). All 48 cases
missing an entry as of the group-1+amendment changeset were given a
`"kind": "delegate_default"` entry: every one of these cases' interesting
behavior is subject/scope resolution (`extract_mentions`/`alias_matching`/
`scope_catalog`), not provider decision-making — scope resolution runs
before the provider is ever invoked, so `delegate_default` (the existing,
unscripted default heuristic) is the honest choice; no bespoke decision
sequence would change what any of these cases actually test. Role-script
coverage is enforced on this branch by
`test_every_authored_corpus_case_has_a_legacy_agent_script_entry`
(`tests/acceptance/test_ask_dev_provider_roles.py`), independent of Lane
2a's own runner-level check (both are needed — one gates CI, one gates a
live run).

## 7. Codex round-2 findings (2026-08-06) — remediation log

Second codex adversarial-review round (after round-1's fixes landed)
returned `needs-attention` again — a real, deeper pass that read Lane 2a's
runner code live (via a Python subprocess) and traced production's
`extract_mentions`/context-fallback semantics directly, not just static
inspection. Disposition:

| finding | severity | disposition |
|---|---|---|
| Tenant-isolation / security-invariant cases only assert `no_internal_error` (a real check) for claims about unauthorized-candidate leakage — an unenforced security invariant could let real leakage pass | high | **ACKNOWLEDGED, not independently re-architected** — same root cause already escalated to team-lead → Lane 2a (the 3 missing checkers, sec.5 above); team-lead has explicitly signed off on "document but flag as not-yet-executable" as the interim state pending those checkers. Not blocking these cases wholesale (codex's stated alternative) without a fresh team-lead decision, since that would discard most of the tenant/subject-label security-shaped coverage for a gap already tracked and in flight on the correct lane |
| `test_every_script_referenced_case_id_exists_in_the_registry`-style coverage checks never actually discover/execute `corpus/case-*.json` — the earlier verification (importing Lane 2a's not-yet-merged `script_inventory.py` by hand) was a one-off manual check, not durable CI | high | **FIXED** — added `test_every_authored_corpus_case_has_a_legacy_agent_script_entry`, expressed using only code that exists on this branch today (`provider_scripts.py`), checked in, mutation-verified (RED when a script entry or case file content is wrong, confirmed by deliberately removing an entry and re-running) |
| Declared-blocked case files (`invariants: []` per the documented schema) don't actually match the checked-in files (which carry a placeholder invariant to satisfy Lane 2a's loader) — the schema and the files had drifted apart | high | **FIXED** — CASE-SCHEMA.v1.md's declared-blocked section rewritten to honestly document the placeholder as an INTERIM, tracked exception (not the ideal `[]`), explicitly says "do not build further on this," and clarifies `blocked_by` may be a ticket id OR a short free-text runner-capability-gap description |
| `provider_scripts.load_registry_ids`'s new `amendments` handling has no shape validation — a malformed/null/list/scalar value could raise an uncaught `AttributeError`/`TypeError` on a path invoked before the default-heuristic fallback | medium | **FIXED** — `_collect_group_ids` helper validates every level (section is a dict, each group is a dict, `ids` is a list of strings) and raises the same typed `UnmappedCaseError` the function already uses for a missing file; 9 new tests (6 malformed-amendments shapes + malformed-groups + missing-amendments-key backward-compat), mutation-verified (7/9 correctly regress to raw `AttributeError`/`TypeError` without the fix) |
| `subject-label.typo-tolerance`'s bare, unquoted, lowercase mention extracts zero mentions (same root bug as sec.5's cascading finding, not independently re-checked for this specific case during that pass) — a question the interpreter treats as "naming nothing" is exactly the condition under which it falls back to any supplied request context, so the case may have silently tested context-fallback resolution instead of typo tolerance | medium | **FIXED** — reworded to a quoted form (`repo "meridin/web-app"`) that extracts a real (misspelled) mention and reaches catalog lookup, where genuine typo-tolerance (or the lack of it) is what determines the outcome, isolated from the extraction-syntax and context-fallback questions |

Verified against Lane 2a's real code again after these fixes (same
differential-oracle method as round 1): 57/57 cases still load, 0 unknown
invariant check names, 0 profile-resolution errors, 55/55 authored cases
have script coverage. 183 pure-Python tests green (was 174 after round 1
— +9 from this round's new guard tests).

## 8. Codex round 3 (2026-08-06) — max rounds reached, converging per MUST DO

Third round, `needs-attention`, 3 findings. Per this lane's MUST DO ("max 3
rounds then converge or escalate"), this is the round-limit stopping point.

| finding | severity | disposition |
|---|---|---|
| Corpus coverage guard (sec.7) checks case-id presence but not question-fingerprint match — a case whose file question drifts from its script entry's question would still pass while silently routing to the unscripted default heuristic at request time | high | **FIXED** — `test_every_authored_corpus_case_has_a_legacy_agent_script_entry` now also compares every authored case's own question fingerprint against `RoleScript.by_fingerprint` (the exact structure `ScriptEngine.resolve` uses at request time — production code, not reimplemented). Mutation-verified: deliberately reworded one case's question in place, confirmed RED with the exact drift shape codex named, reverted, confirmed GREEN |
| Tenant-isolation / persistence invariants still reduce to `no_internal_error` only — codex's escalated recommendation this round: "keep these cases blocked until those checks run" | high | **NOT independently re-architected — converging, escalating to team-lead.** Same root cause as round 2 finding #1 (sec.7), already relayed to Lane 2a as a concrete checker-implementation ask; team-lead has already signed off on the interim disposition once. Re-litigating whether to block a dozen+ security-shaped cases wholesale on my own authority, a third time, on the same unresolved cross-lane dependency, is exactly the "loop instead of escalate" failure mode the round cap exists to prevent |
| Declared-blocked cases carry a runnable placeholder (sec.5/sec.7) instead of a true blocked receipt; codex's escalated recommendation: "fail the acceptance gate if a blocked case is reported as passed" | high | **NOT independently fixable from this branch — converging, escalating to team-lead.** Requires Lane 2a's loader/runner to gain status-aware handling (already flagged twice, sec.5 and sec.7); this lane owns case *content*, not the runner that decides whether to execute a case at all |

**Escalation, not silent stop:** the two open findings are the SAME cross-lane
dependency raised in rounds 1 and 2 (3 missing invariant checkers +
status-aware declared-blocked handling in Lane 2a's loader), now recurring a
third time with escalating severity language from codex. This lane has done
everything addressable from its own branch: documented every gap explicitly
in both the case files and this doc (never claimed false coverage), added
every durable guard expressible with code that exists today, and raised the
concrete, scoped asks to team-lead/Lane 2a twice already. What remains needs
a decision only team-lead can make: hold this changeset for those two Lane 2a
capabilities to land, or accept the corpus with these two gaps explicitly
documented (not hidden) as a scoped follow-up. Requesting that decision via
the gate-slot process rather than spending a 4th round re-arguing the same
unresolved dependency.

## 9. Groups 8-11 (2026-08-06) — full 143-id corpus complete

Team lead ruling on sec.8: accept as-is for this milestone, converge the two
open findings into the mandatory rebase pass (wire the 3 real checkers +
switch to Lane 2a's first-class declared-blocked status once merged — both
now landed on Lane 2a's side per team lead, unblocking that pass). Proceeded
to groups 8 (security/adversarial), 9 (degradation/trust), 10 (persistence/
retention), 11 (operations) — the remaining 85 ids, completing the full
143-id (134 frozen + 9 amendment) corpus.

| group | family count | authored | declared-blocked |
|---|---|---|---|
| 1 (product-question + amendment) | 15 | 55 | 2 |
| 8 (security/adversarial) | 7 + 3 singletons | 19 | 21 |
| 9 (degradation/trust) | 9 bullets | 18 | 9 |
| 10 (persistence/retention) | 7 bullets | 1 | 7 |
| 11 (operations) | 6 bullets | 0 | 11 |
| **total** | **43 families/subfamilies** | **93** | **50** |

**Every one of the 50 declared-blocked ids carries a specific, evidenced
reason — never one blanket excuse.** Categorized:

1. **Ticketed production defects (3 ids):** `deg.source-state.truncated`
   (CHAOS-3428), `pers.retention.0-day`/`.30-day` (CHAOS-3404) — already
   known, carried forward from sec.1.
2. **Real production/generator gaps found this pass, not yet ticketed (5
   ids):** `deg.source-state.redacted` (no such sources.json row exists),
   `deg.provider.misconfigured` (`ReadinessState.missing_credentials` is a
   real code path with no world.json fixture realizing it),
   `deg.budget-exhaustion.with-frame`/`.without-frame` (real, totality-table
   `cost_limit_reached` → FAILED mapping confirmed, but no scripted fault
   type sets it), `deg.required-child-incomplete` (real reason code exists
   in `status_change_service.py`, but no fixture guarantees it fires for any
   named subject — dependency generation is unseeded random).
3. **Runner/schema capability gaps — multi-turn/choreography (17 ids):**
   `tenant.authorization-change-mid-conversation`, all 4
   `adv.mid-conversation.*`, `adv.abuse.route-transition-no-mutation`,
   `adv.abuse.cancellation-race`, `pers.duplicate-run-on-reconnect`,
   `pers.replay-no-rerun`, plus several `adv.cross-tenant.*`/`adv.oversized.*`
   — the single-`question` case schema and Lane 2a's one-fresh-conversation-
   per-case runner cannot express or execute a second turn, a connection-
   state change, or concurrent requests. All ready to author for real the
   moment multi-turn case support lands.
4. **Generator gap — no payload-injection parameter (8 ids):** all of
   `adv.injection.*` — `fixtures/generators/*.py` build commit messages/PR
   bodies/issue bodies/CI logs/incident notes/deployment notes from fixed
   templates with no free-text injection parameter; `dependency-metadata`
   has no generator at all; `acr-evidence` is structurally unreachable
   (ACR is hardcoded `configured=False` org-wide). Real, scoped follow-up:
   a small dedicated generator module (mirroring `conflicts.py`'s precedent)
   adding a payload parameter per source class.
5. **No question-shaped realization exists for the identifier class (4
   ids):** `adv.cross-tenant.team-id`/`.user-id`/`.conversation-id`/
   `.evidence-ref-id` — no real sibling-org fixture (team, user, conversation,
   evidence ref) exists to reference; a raw fabricated id string wouldn't
   extract as a mention anyway and would violate the never-hand-author-
   fixtures rule.
6. **Schema-shape mismatch — not question/answer-shaped at all (11 ids):**
   `deg.false-green-aggregate` (CI-aggregator property), `ops.content-
   security-scan` (a scan job), `ops.quota-cost-reconcile`/`pers.usage-
   cost-idempotent` (billing-layer, no per-question API surface),
   `ops.alert-test-recover` (infra/observability), all 6 `ops.kill-switch.*`
   (grepped src/ for `kill_switch`/`KillSwitch` — **zero matches anywhere in
   production code**; world.json's fixture field has no enforcing code
   behind it), `pers.migration-rollback-preserves-state`/`ops.rollback-
   preserves-state`/`pers.purge-after-disable`/`ops.cleanup-purge-recover`
   (infra/deployment or CHAOS-3404-unwired-service concerns).

**Structural finding, not a per-case detail:** groups 10 and 11's bullets
are almost entirely admin/system/CI-level claims ("provable retention",
"quotas... reconcile", "kill switches...", "rollback preserves state") —
19 of 19 ids in these two groups needed either a real production gap or a
schema-shape mismatch to explain their block, and only 1
(`pers.clarification-persistence`, which reuses `scope.ambiguous`'s real
mechanism from the persistence angle) was genuinely question/answer-shaped.
This suggests groups 10/11 may need a fundamentally different, non-question-
shaped case-schema variant (an "ops/admin verification job" case type) rather
than being force-fit into the single-question corpus schema — flagged for
team lead / Lane 2a to weigh in on, not resolved unilaterally here.

**Answered-vs-answered_with_gaps sweep (cross-lane finding, Lane 2a codex
round 2, relayed by team lead):** `terminal_frames.wrap_legacy_answer_as_frame`
unconditionally maps every real `DevAnswer` terminal to
`PublicOutcome.ANSWERED_WITH_GAPS`, never plain `ANSWERED`, on today's stack
(verified directly — the function's own docstring states this explicitly).
Every `resolution-profiles/deterministic-v1.json` entry that said
`expected_public_outcome: "answered"` (18 in the group-1+amendment set, 3
more across groups 8-9) was corrected to `answered_with_gaps`, documented
inline per entry.

**Verification:** all 143 case files validated (structural shape, subject_ref/
org_alias/user_alias resolve to real world.json/subjects.json entries, every
invariant uses a real Lane 2a check name, every authored case has exactly
one profile entry and zero declared-blocked cases do, zero duplicate ids,
zero duplicate question fingerprints across all 143 questions AND against
Lane 1b's pre-existing 11 role-script entries). 183 pure-Python tests green
(full script coverage + fingerprint-match guards now covering all 93
authored cases, not just the original 55).

## 10. Exit-run-7 corpus re-authoring (2026-08-07) — CHAOS-3490 Part 2, CHAOS-3458, CHAOS-3520

First re-authoring pass grounded in a run where **every active case was
actually measured on the real CHAOS-3292 preflight path** (exit run 7, world
digest `2e1e254d3ee8b799fd58234e865ccf1361c2753c412d1b526bf4e79d1e445232`,
`wave_3_1` preflight enabled, 27 failures). Receipts under
`tests/acceptance/artifacts/wave4/`; log `.p2exit-logs/corpus7.log`.

Of the 27 failures, **14 were NOT touched** — they are the CHAOS-3497
deferred set (a run that fails to ground never emits `scope.resolved`, so
`scope_resolution_outcome_in` and `no_unauthorized_candidate_surfaces` have
nothing to observe). Re-authoring those to expect current behaviour would
bake the product gap into the corpus. They are re-adjudicated after
CHAOS-3497 ships.

### 10.1 Allowed lists widened to the disclosed fallback (7 cases, still active)

`subject-label.acronym-mention`, `subject-label.parenthetical-unquoted`,
`subject-label.word-order-variation`, `scope.ambiguous`,
`deg.acr-unavailable-native-usable`, `deg.source-state.not-applicable`,
`trust.conflicting-evidence`.

All observed `scope_resolution_outcome='organization_fallback'`. The
**expectation was not weakened**: each profile keeps its honest primary
`expected_scope_resolution_outcome`, and the case's own
`scope_resolution_outcome_in` invariant gains a literal
`allowed: ["organization_fallback"]` alongside its `from_profile` key. That
is precisely what the invariant's own `assert` prose already permits —
*"widening is only ever a disclosed, terminal answered_with_gaps/
needs_clarification outcome"*. Disclosure was proved live, not assumed
(`.p2exit-logs/probe-widening-acr.log`, run `61ba05f7-…`):
`fallbacks=['organization']`, `resolved_scope.direct_scope='organization'`,
terminal `answered_with_gaps`, `subject_ref: null`. A *silent* widen — org
scope carrying `outcome='exact'` and no fallbacks — is still caught, because
that is a different enum value and is not in the list.

`scope.ambiguous` keeps its ambiguity proof: `resolution_path_in` PASSED in
the same receipt with `resolution_path='miss-clarification'`. What it stops
asserting is a wire value the preflight miss path never emits — `AMBIGUOUS`
is produced only by `scope_service.py:730`/`:1146`.

The last three widen for a subtler reason worth recording: their named repo
mention *does* commit `exact_match`, but each question also carries an
incidental untyped bare-name mention (`'without acr'`, `'is acr'`, `'ci'` —
confirmed by executing `QuestionInterpreter.interpret`) that never resolves
and drags the **whole-run** scope org-wide. Same whole-run-scalar class the
CHAOS-3462 B6 ruling already dispositioned for `resolution_path`.

### 10.2 Missing `expected_mention_texts` supplied from the producer (4 cases)

`deg.acr-unavailable-native-usable`, `deg.source-state.not-applicable`,
`scope.refused-with-grounding`, `trust.conflicting-evidence` each declared
**no** `expected_mention_texts` at all, so the runner never called
`attach_mention_texts` and the ledger's single-shot `exact_match` reached
`derive_resolution_path` with `mention_text=None` —
`resolution_path_classifiable` and `resolution_path_measured` both FAILED
with absence reason `unclassifiable-resolution-ledger`.

Both existing guards structurally miss this shape, which is why it survived:
`case_schema.py:243-256` requires spans only from cases declaring a
`resolution_path_in` invariant (removed from all four by the B6 ruling), and
`TestDeclaredMentionTextsMatchTheProducer` only checks cases that already
declare spans. Values were **generated by executing the producer**
(`QuestionInterpreter().interpret`, `[m.normalized_lookup_text for m in
interpreted.mentions]`), never hand-authored — and the producer-equality
guard now covers all four, which is the proof they are right.

This does **not** force those runs green: mention 2 still never commits, so
the derived path stays `miss-clarification` against the profile's honest
`deterministic-exact`. Only the ledger becomes *classifiable*, so the cases
measure something trustworthy instead of reporting a broken measurement.

### 10.3 Four cases declared blocked — CHAOS-3520 (unreachable outcomes)

**CORRECTED 2026-08-07, before merge, and recorded rather than rewritten away: this section first said FIVE.** `scope.outcome.organization-fallback` was wrongly among them. It is class (b) — reachable, just not from its old question — and is re-authored in sec.10.6 instead. The trace-first rule caught it; a confident-but-wrong (a) would have deleted real coverage.

> **CORRECTED AGAIN, 2026-08-07 (CHAOS-3219 Phase 3), and this one is a
> correction to a mechanism, not a count. `FILTERED` IS REACHABLE. The
> bullet below is wrong** where it says `team_filter_refs` comes only from
> "a model-authored `DevScope.team_ids`". Executing
> `production_runtime._scope_request` shows a **client-supplied** `DevScope`
> with `direct_scope != team` and non-empty `team_ids` yields non-empty
> `team_filter_refs` (`production_runtime.py:1141-1145`) — and that shape is
> the canonical `dev_scope.v1` positive fixture itself, so no model
> authoring is involved. `scope.outcome.filtered` is therefore **class (b),
> reachable but not from this case's input shape**, not class (a). It stays
> declared-blocked, but on an honest blocker: the runner hardcodes
> `"team_ids": []` for every case (`test_wave4_corpus_runner_live.py:479`)
> and the case schema has no scope field — filed as **CHAOS-3543**.
>
> Two lessons, both already written down and both re-learned here: the
> original bullet read one producer (`scope_service.py`) and stopped, which
> is the "grepped a directory, concluded absence" failure; and a class-(a)
> ruling deletes a coverage claim, so the bar for it must be higher than for
> (b). Sec.10.3's own opening paragraph warns about exactly this and the
> warning still was not enough — the count was re-checked, the mechanisms
> were not.
>
> **Undetermined, stated rather than assumed:** whether the whole-run scalar
> still reports `FILTERED` after `subject_preflight` commits an exact repo
> match. `scope_service.py:998-1008` rebuilds the committed scope with
> `team_ids=[entity.canonical_id] if is_team else []`, i.e. it **drops** the
> caller's team filter. That needs a live run to settle.
>
> `INHERITED` (next bullet) was re-verified at `a07d9ff35` and **is** still
> genuinely unreachable — all 7 `ScopeResolveRequest(` construction sites in
> `src/` pass neither context-ref field. Filed as **CHAOS-3542** (wire it or
> retire the vocabulary). The two outcomes must not be re-conflated: one was
> wrong, one was right.
>
> Also: all four cases in this section cited `blocked_by: CHAOS-3520`, which
> is now **closed** — and closed with no product change. A case blocked on a
> closed ticket is a false claim under the house rule, so all four were
> repointed (3543, 3542, CHAOS-3429, CHAOS-3454 respectively).

`scope.outcome.filtered`, `scope.outcome.inherited`, `scope.partially-resolved-subject`, `scope.stale-context-subject` all observed `exact`. `exact` is not a
near-miss — it means the scenario each case is named for never occurred.
Rewriting the expectation to `exact` would be the "rewrite an honest
expectation to force green" anti-pattern and would delete the coverage claim
while reading as green, so they are blocked instead. Mechanisms, each traced
to a deciding line and recorded per case:

* `INHERITED` (`scope_service.py:747`) needs `page_context_refs` or
  `conversation_context_refs` non-empty (`:1524-1533`). **Neither field is
  assigned anywhere in `src/`** — unreachable in all of production today.
* `FILTERED` (`:749`/`:753`) needs `team_filter_refs`, populated only from a
  model-authored `DevScope.team_ids` (`production_runtime.py:1125-1129`).
  No text parsing derives it, and the scripted acceptance provider never
  offers `resolve_scope.v1` at all.
* `ORGANIZATION_FALLBACK` from `scope_service.py:751` is dead (every caller
  pins `allow_organization_fallback=False`); from `subject_preflight.py:863`
  it needs an unresolved *untyped* mention, and these questions extract
  **zero** mentions (producer executed).

Their `deterministic-v1.json` profile entries were retired with them,
preserving this doc's own "zero declared-blocked cases have a profile entry"
invariant; each entry is quoted verbatim in its case's `notes`.

`scope.outcome.filtered` therefore left `RESOLUTION_PATH_DECLARING_CASE_IDS`
(42 → 41). The pin fired RED first and was updated deliberately, with the
distinction recorded: its `resolution_path_in` **passed**; the check left the
set because the whole case is blocked, not because it was condemned.

**Residual risk, stated rather than assumed away:** `FILTERED` and
`INHERITED` now have ZERO corpus coverage (`ORGANIZATION_FALLBACK` does NOT — see sec.10.6). That is an honest gap on
CHAOS-3520, not five cases that pass while proving nothing.

### 10.4 Kill-switch family re-scoped — CHAOS-3458 (chris ruling, 2026-08-06)

> "single feature flag is the kill mechanism — no per-scope kill switches.
> We don't need to kill pieces of a feature when it's not working, and it's
> simplicity here."

* **RETIRED (case files deleted):** `ops.kill-switch.provider`, `.role`,
  `.surface`, `.contextual-entry`. No per-scope disable is wanted. Their ids
  stay in the frozen `registry-ids.v1.json` `groups` block (the freeze
  governs ids, and no guard requires a registry id to have a case file);
  sec.9 category 6's "all 6 `ops.kill-switch.*`" is superseded by this entry.
* **RE-AUTHORED as flag-off behaviour cases:** `ops.kill-switch.global` is
  now the steady-state refusal (`org_alias: disabled`, `user_alias:
  disabled.ordinary` — a real seeded principal, no fixture change needed);
  `ops.kill-switch.org` is the mid-conversation flip, following
  `adv.mid-conversation.entitlement-change`'s pattern.
* Both stay **declared-blocked**, but the superseded premise ("no kill-switch
  enforcement code exists") is replaced by a proven one:
  `test_wave4_corpus_runner_live.py:825-833` stamps
  `measured_wave_3_1_preflight_path` on every receipt with
  `condition=(org_id in wave_3_1_enabled_orgs)`, unconditionally — so any
  ACTIVE case in the disabled-entitlement org fails a run-validity assertion
  by construction. Unblocking is a Lane 2a runner seam, not more case
  authoring.

> **CORRECTED 2026-08-07 (CHAOS-3219 Phase 3). The "by construction" claim
> in the bullet immediately above is FALSE on today's runner.** It is not
> true that an active case in the disabled-entitlement org must fail the
> run-validity assertion. `wave_3_1_enabled_orgs`
> (`test_wave4_corpus_runner_live.py:397-436`) derives its org set from
> `_representative_alias_per_org()` (`:440-460`), which iterates `_CASES` —
> **active cases only** (`:204`). Making `ops.kill-switch.global` active
> would therefore *add* `('disabled', 'disabled.ordinary')` to that set,
> `ensure_wave_3_1_enabled` would be called for it, and the assertion would
> **pass**. The disabled org is not "by construction" outside that set; it
> is outside it only while the case stays blocked, which makes the argument
> circular.
>
> The conclusion (stay blocked) survives; the reasoning did not. The real
> blocker is earlier and cruder: `_require_ask_dev` (`router.py:376-398`)
> 403s **at `POST /conversations`** (`:1036-1045`), `AcceptanceApi.request`
> converts any `HTTPError` into a failure
> (`prepare_ask_dev_acceptance.py:70-74`), and the runner re-raises
> (`:636-641`). The case aborts with **no receipt and zero assertions** —
> it never reaches the `measured_wave_3_1_preflight_path` check at all.
> What is needed is an `expected_http_refusal` case shape, filed as
> **CHAOS-3549**.
>
> **A second, real hazard this correction surfaced:** if that org set were
> widened, `ensure_wave_3_1_enabled` **writes** `org_feature_overrides`
> (`feature_flags.py:186-215`), a table `WORLD_DIGEST` hashes
> (`fixtures/world.py:1111-1123`). So activating the case naively would
> drift the live world off its pinned digest and fail the *next* armed run's
> verification. Any fix must exclude disabled-entitlement orgs from that
> set rather than enable them.
>
> Both cases cited `blocked_by: CHAOS-3458`, now **closed** as superseded by
> the single-flag ruling. Repointed: `.global` → CHAOS-3549, `.org` →
> CHAOS-3454 (mid-run entitlement mutation, appended to that ticket's scope
> rather than forked).

### 10.5 Registry totals after this pass

| | files | active | declared-blocked |
|---|---|---|---|
| Before (exit run 7) | 143 | 91 | 52 |
| Kill-switch retirement | −4 | 0 | −4 |
| CHAOS-3520 blocks | 0 | −5 | +5 |
| `scope.outcome.organization-fallback` re-authored (sec.10.6) | 0 | +1 | −1 |
| **After** | **139** | **87** | **52** |

Armed-run pytest item count is therefore **140** (87 + 52 + the
`test_at_least_one_corpus_case_is_collected` collection guard).

Principal spreading still holds with room to spare — max **12** cases on any
one principal against production's hard 20-per-15-minute cap (was 13). The
per-case `$comment_principal_pool` "index N of 85" strings are left as the
dated provenance of the CHAOS-3490 assignment and are not rewritten.

`world.json` was deliberately **not touched** — editing it would change
`WORLD_DIGEST` and force a re-mint for a documentation-only reason.

### 10.6 `scope.outcome.organization-fallback` re-authored, not blocked (2026-08-07)

Class (b) under the team-lead's three-way rule: an outcome that is REACHABLE
but not from this question's shape gets its **question** re-authored and keeps
its expectation; only genuinely unreachable outcomes are declared-blocked.

Proved by execution against the live stack post-CHAOS-3497 (digest
`2e1e254d…`): the bare unresolvable name `"What's the status of Zephyr?"`
yields `outcome='organization_fallback'`, `fallbacks=['organization']`, and
CHAOS-3497's disclosure sentence in `warnings`.

The old question, `"How is everything going?"`, extracts **zero** subject
mentions, and both producers of `ORGANIZATION_FALLBACK` need a *named* subject
that fails. So the QUESTION was wrong, not the expectation — which is exactly
the distinction sec.10.3's rule turns on.

`Zephyr` is deliberately absent from `subjects.json` and the world catalog, so
it is unresolvable **by construction**, not by accident. Its question
fingerprint collides with no other corpus case and no `role-legacy_agent.json`
entry (checked by executing `question_fingerprint`, not by eye), and the
provider-script entry's question moved with it — routing is by fingerprint, so
leaving it behind would have silently dropped the case to the unscripted
default heuristic. `expected_mention_texts: ["zephyr"]` is producer-generated.

### 10.7 CHAOS-3497 landed under this branch (2026-08-07)

Rebased onto `fe69de46c`. It is a pure product change — **zero corpus cases
touched** — so re-adjudicating the ~14 previously-deferred cases belongs here.
Two consequences already observed live: non-answer terminals now emit
`scope.resolved` (`scope.no-match` reports `outcome='unresolved'`, which is
what its profile already expected), and the widening now says itself out loud
in `warnings`. Per the team-lead's framing: a case that starts passing after
this rebase passes on NEW EVIDENCE, not on loosening — and anything still
failing afterwards is a genuine finding.

## 11. Nine non-question-shaped ids routed OUT of the corpus (2026-08-07, CHAOS-3219 Phase 3)

Team-lead ruling, applying chris's corpus boundary rule: **the corpus is user
questions over INGESTED data** (the customer's CI, deploys, incidents,
migrations). A claim that is not question-shaped does not become one by being
written as a case file, and force-fitting it produces a file that reads as
coverage while asserting nothing.

CHAOS-3460 was **Canceled** with a note routing *"a non-question-shaped case
type for persistence/ops claims"* into the Phase 3 design brief. That note is
honoured by **routing the claims to the subsystems that own them**, not by
inventing a second case schema inside the corpus.

Retired here (case files deleted; ids stay in the frozen
`registry-ids.v1.json` `groups` block, exactly as the sec.10.4 kill-switch
retirement established — the freeze governs ids, and no guard requires a
registry id to have a case file):

| id | why it is not question-shaped | owning module, for the subsystem test |
|---|---|---|
| `deg.false-green-aggregate` | a CI-aggregator property | `ci/aggregate_gate_results.sh` — **already covered** by `tests/tooling/test_aggregate_gate_results.py` |
| `pers.usage-cost-idempotent` | billing-layer idempotency | `models/llm_budget.py:58-64` unique constraint, enforced `llm/budget.py:485` — **already covered** |
| `ops.quota-cost-reconcile` | billing-layer reconciliation | three independently-computed cost tracks with **no cross-check** — `llm/budget.py`, `orchestrator.py:793`, `admin/routers/ask_dev.py:344`. Filed **CHAOS-3545** |
| `ops.content-security-scan` | a scan job over deployed telemetry | no runtime scanner exists; `tracing.py` / `metrics/prometheus.py` have zero redaction. Manifest already declares it deferred (`wave31_manifest.py:1685`) |
| `ops.alert-test-recover` | alert delivery/recovery | `alerts/rules.yml` is static rules only; no Alertmanager config anywhere, and **no worker-side metrics scrape** (`workers/ask_dev_retention.py:39-46` admits this itself) |
| `ops.rollback-preserves-state` | deployment-level rollback | `.gitlab-ci.yml` has **no deploy stage at all**; `internal/jobroute/control.go:180` is route-level, not deployment |
| `pers.migration-rollback-preserves-state` | migration data survival | Postgres `migrate.py:283` `_run_downgrade`; **ClickHouse has no downgrade subcommand at all** across 82 migration files, and the one PG round-trip test asserts the table is GONE after downgrade (`test_byo_llm_budget_migration.py:68-72`) — schema shape, not data survival |
| `pers.purge-after-disable` | a job, not a turn | `cleanup_expired` has exactly two callers, the Celery task and the CLI; **no `/api/v1/dev/**` route invokes it** |
| `ops.cleanup-purge-recover` | a job, not a turn | its `question` field is literally `"n/a"`, which answers the classification by itself |

**Two of the nine are already proven elsewhere** (`deg.false-green-aggregate`,
`pers.usage-cost-idempotent`) — they were never uncovered, only covered in the
wrong place. That is worth stating plainly, because "routed out" and
"unproven" are different words and only one of them is true here.

**Residual risk, stated rather than assumed away:** the other seven now have
**no automated coverage anywhere** until their subsystem tests are written.
That is a real, named gap — and it is a smaller lie than nine files that
report green while proving nothing. Two findings surfaced while mapping the
owners deserve their own attention regardless of the corpus: the **absent
ClickHouse downgrade path**, and the **three unreconciled cost tracks**.

### Totals after this pass

| | files | active | declared-blocked |
|---|---|---|---|
| Before (sec.10.5) | 139 | 87 | 52 |
| Non-question-shaped retirement | −9 | 0 | −9 |
| **After** | **130** | **87** | **43** |

Armed-run pytest item count is therefore **131** (87 + 43 + the
`test_at_least_one_corpus_case_is_collected` collection guard).

No active case was touched, so the measured behaviour of the corpus is
unchanged by this pass — only claims that were never measurable are gone.
