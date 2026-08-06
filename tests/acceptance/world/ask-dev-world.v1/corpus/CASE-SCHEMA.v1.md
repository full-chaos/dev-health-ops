# `ask_dev_corpus_case.v1` — case file schema (CHAOS-3219 Phase 2 Lane 2b)

Read alongside `../world.json` / `../subjects.json` / `../sources.json` and
`REGISTRY-AMENDMENT.v1.md` (same directory). One file per registry id:
`corpus/case-<id>.json` (dots in the id are kept verbatim in the filename —
e.g. `corpus/case-scope.no-match.json`).

**Status: ADOPTED AS THE BINDING INTERFACE** (team lead, 2026-08-06) between
Lane 2b (this doc + case content + profile entries) and Lane 2a (the
`scripts/acceptance/corpus/run_corpus.py` runner + `wave4_case_result.v1`
receipt, landing in worktree `ops-worktrees/chaos-3219-phase2-runner`,
deliberately decoupled into its own landing zone with self-test fixtures
under `tests/acceptance/corpus/fixtures/` — no collision with this
directory). Field names below are now load-bearing for 2a's loader; treat
any further change as a cross-lane-coordinated schema bump, not a local
edit.

## Design principle (CHAOS-3389 fold-in, ticket comment 2026-08-05)

> Matcher-SPECIFIC expected outcomes go in the versioned `resolution-profile:
> deterministic-v1`, NOT in the case. A case's `invariants` block is the
> contract that must survive ANY matcher swap (today's regex/alias stack,
> a future QUA LLM-backed resolver, anything else) — it never encodes "what
> the deterministic stack happens to return today". When QUA flips a case
> from clarification to resolved, a new resolution-profile entry changes,
> not the case file, not the runner, not the invariants.

Concretely: **the case never says "this returns `not_found`"** — a
`resolution_profile_ref` names which profile file's per-case block declares
that expectation. The case says what must hold no matter which profile
answers it.

## Top-level shape

```jsonc
{
  "schema_version": "ask_dev_corpus_case.v1",
  "id": "scope.no-match",                    // verbatim registry id (frozen, or amendment)
  "family": "scope",                          // registry family
  "group": 1,                                 // registry group number (1/8/9/10/11)
  "status": "authored",                       // "authored" | "declared-blocked"
  "blocked_by": null,                         // "CHAOS-XXXX" when status=declared-blocked; case still gets a file (typed status, per world.json/sources.json precedent), just no live assertions
  "proves": ["group 1 bullet 11"],             // >=1 registry/issue-group cross-reference, verbatim from corpus-registry-v1.md's "proves" column

  "question": "What is the status of the Ask Dev project?",  // EXACT literal text the runner sends; must match provider-scripts/role-*.json's routing key when a scripted decision sequence exists for this id
  "org_alias": "primary",                     // world.json org
  "user_alias": "primary.ordinary",           // world.json user

  "subject_class": "no-match",                // one of the 8 subjects.json classes, or "n/a"
  "subject_ref": "subject.no-match.ask-dev-project",  // subjects.json id this case exercises, or null for n/a-subject cases (portfolio/investment/adv.*/ops.* etc.)

  "invariants": [
    {
      "id": "inv.no-match-terminates-gracefully",
      "assert": "A subject mention with zero catalog candidates reaches a terminal PublicOutcome (not_found) -- never an internal_error, never a silent fallback to org-wide scope.",
      "class": "contract"                      // "contract" (survives any matcher) | "security" (never-violate, e.g. no-unauthorized-candidate) | "persistence" (CHAOS-3423/3424 class: terminal leaves a transcript row)
    }
  ],

  "resolution_profile_ref": "deterministic-v1", // which resolution-profiles/*.json file's `cases[id]` block supplies the matcher-specific expected outcome

  "fault_ref": null,                            // provider-scripts/role-*.json case key, when this id is a provider-fail/adversarial-fault case; null otherwise

  "notes": "free text: fixture provenance, cross-refs, honesty caveats"
}
```

## Invariant taxonomy (the fixed vocabulary every case's `invariants[].class` draws from)

These four survive *any* resolution matcher (deterministic regex stack
today, QUA LLM-backed shortlist tomorrow) because they are asserted about
the **contract**, not the implementation:

1. **`inv.no-silent-widening`** — a named subject mention that misses its
   intended target never silently widens to org-wide/broader scope.
   Widening is only ever a *disclosed*, terminal `answered_with_gaps` /
   `needs_clarification` outcome, never a silent substitution. (CHAOS-3407
   class: the regex bug that dropped "Dev" and then silently ran org-wide.)
2. **`inv.no-internal-error-on-fuzzy-label`** — no shape of subject label
   (parenthetical, ≥5 words, typo, acronym, wrong word order) can crash the
   run to `internal_error`. Worst case is a graceful terminal
   (`not_found` / `needs_clarification` / `answered_with_gaps`). (CHAOS-3421
   class: the leaked `forbidden_or_not_found` internal_error.)
3. **`inv.no-unauthorized-candidate-surfaces`** — every candidate offered in
   a clarification list, or silently chosen, is drawn from the requester's
   own authorized catalog (permission_fingerprint-scoped). A candidate that
   exists in the database but outside that scope (sibling-tenant, inactive,
   soft-deleted) must never appear, ranked or unranked.
4. **`inv.terminal-persists-assistant-row`** — every clarification or error
   terminal persists a real transcript row (`DevAnswer` or the CHAOS-3423
   `dev_error.v1` row) — never a silently-dropped turn. New since PR #1507;
   applies to every case whose outcome is `needs_clarification`, `failed`,
   `denied`, `unsupported`, or `temporarily_unavailable` (the last one
   confirmed load-bearing by the `provider-fail.*` cases: a bare
   `provider_unavailable` error code maps to `TEMPORARILY_UNAVAILABLE` per
   `terminal_frames.PUBLIC_OUTCOME_BY_ERROR_CODE`, and CHAOS-3423 exists
   precisely because that terminal used to be reachable without persisting
   anything).

A case lists only the invariants its scenario actually exercises (not all
four on every case) but must justify a `scope.*`/`readiness.*` case
carrying zero invariants — that would mean the case isn't testing anything
matcher-agnostic, which is a smell for this family specifically.

**Ad-hoc, case-specific invariants** (same `{id, assert, class}` shape,
`id` prefixed `inv.` but outside the four named ones above) are allowed
when a scenario needs a contract assertion the fixed vocabulary doesn't
cover — e.g. `inv.zero-unrelated-evidence-named-subject` on
`unrelated-evidence.named-subject`. Use sparingly; if the same ad-hoc
invariant would apply to 3+ cases, promote it into the fixed vocabulary
instead of copy-pasting it.

## `resolution-profiles/<profile>.json` shape

```jsonc
{
  "schema_version": "ask_dev_resolution_profile.v1",
  "profile": "deterministic-v1",
  "describes": "Today's deterministic matcher stack: question_interpreter.py _NAME extraction, alias_matching.py (CHAOS-3388 acronym/literal-parenthetical), scope_catalog close-match search -- pre-QUA. Superseded (not overwritten) by a future qua-shadow-v1 / qua-committed-v1 profile as CHAOS-3389 lands; a case's invariants must hold under BOTH profiles unchanged.",
  "cases": {
    "scope.no-match": {
      "expected_public_outcome": "not_found",
      "expected_scope_resolution_outcome": "unresolved",
      "resolution_path": "miss-clarification",
      "notes": "..."
    }
  }
}
```

`resolution_path` vocabulary is **decreed exactly** (team lead, 2026-08-06,
kebab-case, matches Lane 2a's `wave4_case_result.v1` receipt field
byte-for-byte so a QUA shadow-mode diff needs no translation):

```
deterministic-exact | deterministic-alias | miss-clarification | qua-shadow | qua-committed
```

**Nullability rule (CORRECTED 2026-08-06 against Lane 2a's real
`resolution_path.py`, read directly — supersedes an earlier, less precise
draft of this rule):** `resolution_path` is `null` whenever the case's
`question` text names **zero** subject mentions that `extract_mentions`
would actually recognize — no `dev_run_resolutions` entries are ever
written, and the module returns `None` for an empty sequence by contract.
This is stricter than "does the case have a `subject_ref`" or "did scope
resolution conceptually run": `scope.outcome.organization-fallback`
(`subject_class: "n/a"`, no subject named at all) is `null` precisely
*because* no mention was ever extracted — even though `ScopeResolutionOutcome.ORGANIZATION_FALLBACK`
is a real, non-null value for `expected_scope_resolution_outcome` (a
different field; the scope CAN fall back to org-wide with zero named
mentions, but `resolution_path` specifically tracks per-*mention* history,
which is empty here). Conversely, a case whose question names a real,
extractable mention gets a real `resolution_path` even without a
`subjects.json` `subject_ref` (e.g. `metric-compare.two-metrics.stale-source`
names `repo "probe/source-stale"` directly). **Practical extraction
gotcha, learned the hard way this round:** `extract_mentions` requires
either a capitalized `_NAME` token or a QUOTED span, always adjacent to a
closed kind noun (`repo`/`project`/`team`/etc.) — a bare, unquoted,
lowercase repo slug like `meridian/web-app` extracts **zero** mentions and
silently degrades to org-wide behavior. Every case's `question` must be
checked against this before assuming `resolution_path` is non-null.

## `status: "declared-blocked"` cases

Mirrors the `world.json` / `sources.json` precedent (`DECLARED_BLOCKED_STATUS`,
CHAOS-3428) — an id whose scenario cannot be realized against real
production code/fixtures yet still gets a real file with the FULL key set
above (uniform shape, easier for a runner to parse without special-casing):
`status: "declared-blocked"`, `blocked_by` set, `proves`/`notes` explaining
exactly what's missing, `resolution_profile_ref: null`, `fault_ref: null`.
`question`/`org_alias`/`user_alias`/`subject_class`/`subject_ref` are still
filled in as documentation of the intended (currently-blocked) scenario.
This is the file a runner iterates over identically to an authored case —
it just skips execution and reports the block, loudly, rather than being
silently absent from the corpus.

**`blocked_by` is a `"CHAOS-XXXX"` ticket id when the blocker is a
production-code/fixture defect** (the common case — e.g.
`attention.team.valid-qualification-light-on-feature-work`'s
`"CHAOS-3394"`). It is a short, free-text, machine-stable prefix like
`"runner: <capability gap>"` when the blocker is a **runner/schema
capability that doesn't exist yet** rather than a ticketed defect (e.g.
`tenant.authorization-change-mid-conversation`'s `"runner: no structured
multi-turn / actor-authorization-mutation support"`) — there is no ticket
to cite for "this case format can't express what it needs to express yet."
Both forms are valid; a consumer should treat `blocked_by` as opaque
documentation, never parse it as strictly ticket-shaped.

**`invariants` — INTERIM, tracked exception (2026-08-06):** the ideal
value is `[]` (empty, not omitted — nothing runs, nothing to check). In
practice, Lane 2a's `case_schema.py` loader (read directly) requires
`invariants` to be a **non-empty** list on every file it globs,
unconditionally — it has no `status`-awareness yet, so an empty list on
one blocked case raises `CaseSchemaError` and fails the **entire** corpus
load, not just that file. Until that loader gains status-aware handling
(flagged to Lane 2a as a real gap, not silently worked around), a
declared-blocked case instead carries exactly one placeholder invariant
whose `category` is `"declared-blocked-placeholder"` and whose `assert`
text says, verbatim, that it is a placeholder and must not be read as an
executed claim about the blocked scenario (see either declared-blocked
case file for the exact wording). **Do not build further on this
placeholder** — it exists solely to keep the corpus loadable pending the
loader fix; a status-aware loader should make it unnecessary and it should
be removed (reverted to `[]`) once that lands.
