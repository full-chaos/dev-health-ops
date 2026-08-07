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
  "status": "active",                         // "active" | "declared-blocked" -- Lane 2a's real case_schema.py vocabulary (ACTIVE_STATUS/DECLARED_BLOCKED_STATUS); "active" is also the default when the field is absent. CORRECTED 2026-08-06 rebase pass -- earlier drafts of this doc and every case file used "authored", which the real merged loader does NOT recognize (_KNOWN_STATUSES = {"active", "declared-blocked"}); every case file was rewritten to "active" in the rebase pass.
  "blocked_by": null,                         // "CHAOS-XXXX[ free text]" when status=declared-blocked -- the real loader anchors `^CHAOS-\d+\b` at the START of the string (case-schema.py's `_BLOCKED_BY_TICKET_PATTERN`); a bare non-ticket string like "runner: ..." is REJECTED, not merely discouraged. Case still gets a file (typed status, per world.json/sources.json precedent), just no live assertions.
  "proves": ["group 1 bullet 11"],             // >=1 registry/issue-group cross-reference, verbatim from corpus-registry-v1.md's "proves" column

  "question": "What is the status of the Ask Dev project?",  // EXACT literal text the runner sends; must match provider-scripts/role-*.json's routing key when a scripted decision sequence exists for this id
  "org_alias": "primary",                     // world.json org
  "user_alias": "primary.ordinary",           // world.json user

  "subject_class": "no-match",                // one of the 8 subjects.json classes, or "n/a"
  "subject_ref": "subject.no-match.ask-dev-project",  // subjects.json id this case exercises, or null for n/a-subject cases (portfolio/investment/adv.*/ops.* etc.)

  "invariants": [
    {
      "category": "no-silent-widening",         // taxonomy label below, or an ad-hoc "inv.<name>" -- REQUIRED by the real loader (case_schema.py's per-entry check)
      "check": "scope_resolution_outcome_in",    // name of a registered checker in scripts/acceptance/corpus/invariants.py's CHECKS registry -- REQUIRED; unknown names fail loud at evaluation time
      "args": {"from_profile": "expected_scope_resolution_outcome"}, // checker-specific: literal values (e.g. "allowed": [...]) and/or "from_profile" pulling a key out of this case's resolved resolution-profile block
      "assert": "A named subject mention that misses its intended target never silently widens to org-wide/broader scope.", // human-readable full claim -- may be broader than what `check` currently verifies; NOT read by the loader/runner
      "executable_today": true,                  // documentation only, NOT read by the loader/runner
      "$comment": "optional: notes on why `check` is narrower than `assert`, or that it was recently wired to a graduated checker"
    }
  ],                                            // CORRECTED 2026-08-06: this doc's earlier draft showed {id, assert, class} -- the real, merged case_schema.py requires {category, check} on every entry (case_schema.load_corpus_case); {id, class} are not read at all. Every case file already uses the real {category, check, args, assert, executable_today, $comment} shape.

  "resolution_profile_ref": "deterministic-v1", // which resolution-profiles/*.json file's `cases[id]` block supplies the matcher-specific expected outcome

  "fault_ref": null,                            // provider-scripts/role-*.json case key, when this id is a provider-fail/adversarial-fault case; null otherwise

  "notes": "free text: fixture provenance, cross-refs, honesty caveats"
}
```

## Invariant taxonomy (the fixed vocabulary every case's `invariants[].category` draws from)

These four survive *any* resolution matcher (deterministic regex stack
today, QUA LLM-backed shortlist tomorrow) because they are asserted about
the **contract**, not the implementation. `check`/`args` below reflect the
REAL checkers wired in the rebase pass (2026-08-06) against Lane 2a's
merged `invariants.py` (PR #1518) -- read that module directly before
changing any of these mappings.

1. **`no-silent-widening`** — a named subject mention that misses its
   intended target never silently widens to org-wide/broader scope.
   Widening is only ever a *disclosed*, terminal `answered_with_gaps` /
   `needs_clarification` outcome, never a silent substitution. (CHAOS-3407
   class: the regex bug that dropped "Dev" and then silently ran org-wide.)
   **Wired to `scope_resolution_outcome_in`** (`args: {"from_profile":
   "expected_scope_resolution_outcome"}`) on every case that CARRIES a
   `no-silent-widening` invariant AND whose resolution profile entry has a
   non-null `expected_scope_resolution_outcome` — this is NOT every active
   case with a resolvable scope outcome; a case only gets this checker if
   its own `invariants[]` documents the `no-silent-widening` claim in the
   first place (codex round-5, confirmed: 24 active cases with a non-null
   `expected_scope_resolution_outcome` — e.g. `adv.abuse.retry-storm`, both
   `provider-fail.*` cases — never claimed `no-silent-widening` at all and
   correctly carry no `scope_resolution_outcome_in` invariant; this is not a
   coverage gap, it's cases whose scenario doesn't exercise that specific
   claim). Within cases that DO carry `no-silent-widening`: this checker
   unconditionally FAILS when no `scope.resolved` event was
   observed (`invariants.py`'s `outcome is None` branch), so it must never
   be wired onto a case whose profile's `expected_scope_resolution_outcome`
   is `null` (no scope resolution is attempted at all, e.g. n/a-subject
   org-wide cases) — those 4 cases (`attention.team.invalid-qualification-
   unknown`, `portfolio.multi-project.status`, `portfolio.org-wide.plan-
   registry-gap-loud`, `portfolio.org-wide.status`) keep the narrower
   `no_internal_error` floor, documented via `$comment`, not silently
   claimed as fully enforced.
2. **`no-internal-error-on-fuzzy-label`** — no shape of subject label
   (parenthetical, ≥5 words, typo, acronym, wrong word order) can crash the
   run to `internal_error`. Worst case is a graceful terminal
   (`not_found` / `needs_clarification` / `answered_with_gaps`). (CHAOS-3421
   class: the leaked `forbidden_or_not_found` internal_error.) Still backed
   by `no_internal_error` only — no graduated checker in `invariants.py`
   asserts the "lands in one of the three graceful terminals" half of this
   claim yet (that would need `public_outcome_in` wired with an
   allowed-terminals list per case, not done in this pass; tracked, not
   silently claimed enforced).
3. **`no-unauthorized-candidate-surfaces`** — every candidate offered in
   a clarification list, or silently chosen, is drawn from the requester's
   own authorized catalog (permission_fingerprint-scoped). A candidate that
   exists in the database but outside that scope (sibling-tenant, inactive,
   soft-deleted) must never appear, ranked or unranked. **Wired to
   `no_unauthorized_candidate_surfaces`** (`args: {"authorized_entity_ids":
   [...]}`, a literal list of org `primary`'s own real fixture entity ids —
   see the checker's own "KNOWN TRUST BOUNDARY" docstring: correctness of
   this declared list is case-authoring's responsibility, not something the
   checker verifies independently).
4. **`terminal-persists-assistant-row`** — every clarification or error
   terminal persists a real transcript row (`DevAnswer` or the CHAOS-3423
   `dev_error.v1` row) — never a silently-dropped turn. New since PR #1507;
   applies to every case whose outcome is `needs_clarification`, `failed`,
   `denied`, `unsupported`, or `temporarily_unavailable` (the last one
   confirmed load-bearing by the `provider-fail.*` cases: a bare
   `provider_unavailable` error code maps to `TEMPORARILY_UNAVAILABLE` per
   `terminal_frames.PUBLIC_OUTCOME_BY_ERROR_CODE`, and CHAOS-3423 exists
   precisely because that terminal used to be reachable without persisting
   anything). **Wired to `terminal_persists_assistant_row`** (no args —
   requires `assistant_schema_versions` populated via the docker-exec
   verification plane).

A case lists only the invariants its scenario actually exercises (not all
four on every case) but must justify a `scope.*`/`readiness.*` case
carrying zero invariants — that would mean the case isn't testing anything
matcher-agnostic, which is a smell for this family specifically.

**Ad-hoc, case-specific invariants** (same `{category, check, args, assert}`
shape, `category` outside the four named ones above) are allowed
when a scenario needs a contract assertion the fixed vocabulary doesn't
cover — e.g. `zero-unrelated-evidence-named-subject` on
`unrelated-evidence.named-subject`. Use sparingly; if the same ad-hoc
invariant would apply to 3+ cases, promote it into the fixed vocabulary
instead of copy-pasting it. Ad-hoc invariants still back onto whichever
real `check` in `invariants.py`'s `CHECKS` registry fits best (often
`no_internal_error`, since most ad-hoc categories don't have a dedicated
graduated checker) — never invent a `check` name the registry doesn't
have.

## `resolution-profiles/<profile>.json` shape

**CORRECTED 2026-08-06 (rebase pass) against the real, merged
`case_schema.load_resolution_profile`** (`origin/main` @ `515adf994`) — an
earlier draft of this doc showed `schema_version: "ask_dev_resolution_
profile.v1"` / `profile` / `describes`, which Lane 2a's landed loader does
NOT accept (`schema_version` must start with the literal prefix
`"resolution-profile."`, and the id field is named `profile_id`, not
`profile`). The wrapper below is the real, load-bearing shape; `cases{}`'s
inner per-case block shape (`expected_public_outcome` etc.) is unchanged
from the original draft and was already correct.

```jsonc
{
  "schema_version": "resolution-profile.v1",
  "profile_id": "deterministic-v1",
  "$comment": "free text describing the matcher stack this profile represents",
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

### The null-profile-value rule, generalized (CHAOS-3462 B4)

The rule §1 states for `scope_resolution_outcome_in` applies to **every**
`*_in` checker, and is now enforced mechanically rather than by review:

> **Never wire an `*_in` invariant onto a case whose profile value for the
> cited `from_profile` key is `null`.**

`invariants._resolve_allowed` turns a null profile value into
`allowed=[None]`, and every `*_in` checker independently and deliberately
refuses to match an unobserved `None` — so the check fails by construction,
on every run, on every stack. The Phase 2 exit evidence run found 26 active
cases in exactly that state via `resolution_path_in`, which made
"invariant floor green on 93" unreachable as authored.

`tests/acceptance/corpus/test_corpus_invariants_are_satisfiable.py` now
fails the unit gate if any active case reintroduces it, and also fails if
`invariants.py` grows a new `*_in` checker the guard does not yet know
about. Note the guard correctly permits a null profile value **beside a
literal non-null `allowed` entry** — `_resolve_allowed` is additive, so that
combination is still satisfiable.

**Deciding between the two remedies is a code question, not a judgment
call.** Run production's own `extract_mentions` against the case's exact
`question` text, and check where the terminal fires relative to the ledger
write:

| Observation | `resolution_path` | Remedy |
| --- | --- | --- |
| zero extractable mentions | genuinely `null` | REMOVE the invariant |
| terminal fires before `orchestrator.run()` (e.g. the org/model capability gate, which rejects in a FastAPI **dependency**) | genuinely `null` | REMOVE the invariant |
| terminal fires after extraction but before ledger construction (e.g. the oversized `> MAX_MENTIONS` rejection, `_terminate(ledger=None)`) | genuinely `null` | REMOVE the invariant |
| mention extracted AND preflight reaches PROCEED | real, non-null | DEFINE the value in the profile |

A mention-bearing question is **not** on its own evidence of a written
ledger — the second and third rows above both have extractable mentions and
still write nothing. Record the disposition and its evidence on the case in
a `$comment_resolution_path_invariant` field.

**Removing the last invariant is not allowed.** If the unpassable check was
a case's ONLY invariant, replace it rather than deleting it: prefer
`public_outcome_in` wired to the profile's own non-null
`expected_public_outcome` (a real terminal assertion), and fall back to the
bare `no_internal_error` floor only when no non-null profile value exists.
An active case with zero invariants is rejected by the loader, and would be
a silent coverage hole even if it were not.

## `expected_mention_texts` — schema addition (CHAOS-3462 B6)

```jsonc
"expected_mention_texts": ["meridian/web-app"],   // REQUIRED iff the case declares resolution_path_in
```

**Why it exists.** `DevResolutionEntry` never persists the original mention
span, so the docker-exec ledger read returns entries with
`mention_text = None`. `derive_resolution_path` raises for any single-shot
`exact_match` without it, the runner records a failed
`resolution_path_classifiable` check, and the case goes red. The effect was
not marginal: **`deterministic-exact` and `deterministic-alias` were
unproducible by the runner in every case, in every catalog world** — roughly
46 active cases were red for a reason unrelated to the product, and
`resolution_path_in` could only pass where the profile said
`miss-clarification`. This is the schema addition `resolution_path.py`'s own
docstring anticipated ("closeable once a corpus case declares its own
expected mention text").

**The loader enforces the pairing:** a case declaring `resolution_path_in`
with no `expected_mention_texts` is a `CaseSchemaError`, because such a case
cannot report a real result.

### Deriving the value — binding method

**Generate it from the producer. Never hand-author it.**

* The producer is **`QuestionInterpreter.interpret`**, driven with the
  runner's real request shape. It is **not** `extract_mentions`: the
  interpreter additionally mints untyped bare-name mentions
  (`_add_untyped_mentions`), so `"Update the ticket status to Done"` yields
  **zero** mentions under `extract_mentions` and **one** under `interpret`.
  Three case dispositions were made wrong by using the narrower function; do
  not repeat it.
* The value is each mention's **`normalized_lookup_text`**, not its surface
  span. `resolution_path.py`'s CALLER CONTRACT is explicit that it needs
  "the exact, already-normalized span that reached the resolver", and that a
  raw natural-language utterance raises rather than silently misclassifying.
* **Never** source it from `subjects.json`'s `mentions` array. Those are
  human-readable descriptive phrases ("the web-app repo"), not resolver
  input — the same CALLER CONTRACT calls this out by name.

### Ordering and multi-mention cases

Declare **every** mention the question produces, in interpreter order — not
just the one the resolution targets. `attach_mention_texts` maps declared
spans onto distinct `mention_id`s in first-seen order, which is sound
because `subject_preflight._build_ledger` builds entries via
`zip(mentions, resolutions, strict=True)` and `_inner_ledger_query` orders
by `entry_ordinal`. Ten active cases produce more than one mention; a
single declared span would leave the others unclassifiable.

The two count mismatches are **not symmetric**:

* **more observed mentions than declared → raises.** The declaration is
  short, so a real mention would get no span, and positional mapping past
  the end is meaningless.
* **fewer observed than declared → attaches nothing and proceeds.** This is
  legitimate, not drift: a PROCEED ledgers every mention, so a short ledger
  means the run TERMINATED, and a terminating entry is only ever
  `ambiguous_candidates` — whose mention never needs a span, because a
  non-`exact_match` final entry short-circuits to `miss-clarification`.
  Raising here would red a correct run and blame the case author.

Neither direction ever truncates or pads: mis-pairing would attach the wrong
span to a real mention, and `classify_match_kind` would then either raise for
a bogus reason or — worse — classify against text that never reached the
resolver.

### Drift guard

`tests/acceptance/corpus/test_corpus_invariants_are_satisfiable.py` asserts
every declared list equals, exactly and in order, what the live interpreter
yields for that question. A question edited by one word therefore fails the
unit gate rather than the live run. Regenerate from the interpreter; do not
patch the JSON by hand.

## `org_alias` / `user_alias` are load-bearing (CHAOS-3462 B5)

These are not documentation. The runner resolves both through `world.json`
and **authenticates as that principal** for the case
(`scripts/acceptance/corpus/principals.py`). A missing, unknown, or
incoherent pair (e.g. `user_alias: sibling.ordinary` with
`org_alias: primary`) fails the case loudly — it never silently falls back
to the acceptance superuser, which is what previously made the cross-tenant
and entitlement families assert nothing about the identities they name.

Two consequences for case authoring:

* the pair must agree with `world.json` — the user's own `org_alias` there
  is authoritative, and a case cannot reassign a user to another org;
* impersonation is deliberately NOT used: the `/api/v1/dev/**` routers read
  the raw JWT claims and ignore the impersonation context (GraphQL does
  honor it — the asymmetry is filed as CHAOS-3472), so an impersonated case
  would evaluate entitlement and readiness against the superuser's org and
  go green for the wrong principal;
* credentials are **seeded at world-generation time** (team-lead ruling
  2026-08-06, CHAOS-3463), and `password_hash` **stays in the world
  digest** — the snapshot/restore model makes hashes frozen bytes restored
  identically per boot, so credential tampering registers as drift, which is
  what the digest is for. `fixtures/world.py::password_for_alias` is the ONE
  derivation: `_build_auth_fixture` hashes it when seeding and the corpus
  runner logs in with it, so seeding and login cannot disagree. The runner
  provisions nothing — every receipt is stamped
  `provisioning=world-seeded-credentials`. (It is not write-free: a
  successful login stamps `users.last_login_at`, which `_VOLATILE_COLUMNS`
  excludes from the digest for exactly that reason. What it never does is
  move a *digested* column.) A stack whose world snapshot
  predates seeded credentials answers 401; re-mint it
  (`scripts/acceptance/mint_ask_dev_world_snapshot.sh`) rather than
  provisioning a password at run time.

  *(Historical: a `ASK_DEV_ACCEPTANCE_ALLOW_PASSWORD_BRIDGE=1` opt-in used to
  set a password via `POST /api/v1/admin/users/{id}/password`, because world
  users then had `password_hash=None` and could not log in at all. It mutated
  a digest-covered column, so a second armed run against the same stack
  reported drift. Removed once CHAOS-3463 landed seeded credentials.)*

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

**CORRECTED 2026-08-06 (codex round-5, confirmed):** `blocked_by` MUST be a
real `"CHAOS-<number>[ free-text description]"` ticket reference, for every
declared-blocked case, with no exception — Lane 2a's real, merged
`case_schema.py` (`_BLOCKED_BY_TICKET_PATTERN = re.compile(r"^CHAOS-\d+\b")`)
anchors this at the start of the string and raises `CaseSchemaError` on
anything else. An earlier draft of this doc described a second, free-text
form (`"runner: <capability gap>"`) for a blocker with no ticket to cite —
that form is REJECTED by the real loader, not merely discouraged; following
it would fail the entire corpus load, not just the one case. All 52
declared-blocked cases in this corpus already satisfy the real pattern
(every "runner capability gap" category was filed as a real ticket instead
— see `REGISTRY-AMENDMENT.v1.md`'s ticket table, e.g.
`tenant.authorization-change-mid-conversation`'s `"CHAOS-3454"`). If a
future case genuinely has no ticket to cite, file one first — do not invent
a free-text `blocked_by` value.

**`invariants` — `[]` (CLOSED 2026-08-06, rebase pass):** every
declared-blocked case's `invariants` is the empty list — nothing runs,
nothing to check. Lane 2a's real, merged `case_schema.py` (PR #1518,
`origin/main` @ `515adf994`) is status-aware: `load_corpus_case` exempts
`status == "declared-blocked"` cases from the non-empty-`invariants`
requirement (`is_blocked` short-circuits the check), so an empty list no
longer raises `CaseSchemaError`. The earlier INTERIM `declared-blocked-
placeholder` invariant entry (a single `no_internal_error`-backed entry
existing solely to satisfy a pre-status-aware loader) has been removed
from every declared-blocked case file — do not reintroduce it.
