# Pre-registration v2 — CHAOS-3219 Phase 3 armed run (post-merge)

**Written and committed BEFORE the run. Any deviation is reported as a MISS, never folded in.**

**Supersedes `PRE-REGISTRATION.phase3.md` (v1), which stays in place unchanged.** v1
was frozen against `150bcb432` and no longer describes this branch: six of its eight
expected reds are the cases this branch flipped to `refused`, and a seventh is the one
CHAOS-3551 should turn green. Grading the next run against v1 would produce a wall of
"MISSes" that are only the fixes working — and, far worse, would give a real regression
somewhere to hide inside that expected churn. **A stale pre-registration is more
dangerous than none, because it still reads as a prediction.** v1 is kept for
comparability, not for grading.

## CONDITIONS — the merge-set these predictions assume

Verified in this branch's ancestry at the time of writing:

| assumed merged | commit | verified |
|---|---|---|
| CHAOS-3541 typed `refused` outcome | `661bfbe57` (#1583) | ancestor of HEAD ✓ |
| CHAOS-3551 cohort render | `3adfecab7` (#1588) | ancestor of HEAD ✓ |

Assumed **NOT** merged, checked at time of writing:

* **CHAOS-3546** — `provider_profile_override` still has **zero readers in `src/`**, so the
  readiness gate still cannot fire. (Checked by grep, not assumed.)

### AMENDMENT 1 — merge-set changed before the run (2026-08-07, pre-run)

The document's own rule ("if the assumed merge-set differs at run time, re-derive
before grading") fired. Four more PRs merged; `main` is now `3e4e24650`:

| merged since v2 was written | effect on predictions |
|---|---|
| **CHAOS-3574** `9efd3f309` (#1593) cross-tenant → `not_found` | **Section E's conditional now resolves to its GREEN branch.** |
| CHAOS-3573 (#1589) reconcile NULL-cost terminals | allowance internals; still predicted non-interfering |
| CHAOS-3572 (#1592) boot guard | boot-time guard; non-interfering |
| CHAOS-3582 (#1594) QUA shadow pricing | see AMENDMENT 3 |

**CHAOS-3546 re-checked and still unmerged**, so Section D is unchanged.

### AMENDMENT 2 — the identity literal is NOT stale (correction)

It was suggested pre-run that `12b96199…` had gone stale because #1593/#1594 "touched
engine files". **Verified false, by checking rather than accepting:** neither merge
touches `provider-scripts/role-legacy_agent.json`, and
`role_script_identity_digest` is computed over that document (content digest +
routing pairs), **not** over `src/`. Recomputed on the synced tree: still
`12b961997a122ff6e7a711db0a3a5724bdb668a28c262f2fdf94c88be3675e59`, role
`legacy_agent`, 96 cases.

The operative invariant remains **host == container**, which the boot's precondition B
checks differentially. The literal is recorded as a tripwire, not as the check: if the
container reports something else, that is a stale mount — and if the HOST value itself
has moved, the role script changed and the corpus must be re-derived before grading.

### AMENDMENT 3 — QUA shadow is OUT OF SCOPE for this run, deliberately

CHAOS-3582 (#1594) makes QUA shadow evaluation priceable, and it was suggested this run
should observe `status=evaluated` with real reservations, treating `budget_unavailable`
as a MISS. **This run cannot test that, and arming it would invalidate the run:**

* QUA shadow is gated on `ASK_DEV_QUA_SHADOW_ENABLED == "1"`
  (`production_runtime.py:668`); with the flag unset, construction is **skipped
  entirely** and no QUA rows of any status are produced.
* **"QUA flags unset" is a run-validity precondition of THIS document** (above), and the
  boot hard-aborts (exit 75) if either flag is armed. Arming QUA would void the run
  against the pre-registration it is being graded by.
* **Zero corpus cases reference the QUA feature.** Verified: every `qua` substring match
  across the 133 case files is `quals` / `qualification` / `quality` / `quad`, not the
  feature. Nothing in the graded set depends on it.
* Arming it would introduce **real reservations against the platform allowance during a
  measured corpus run** — the precise mechanism that degraded the 10:03 run to
  UNMEASURED, and which this document elsewhere declares makes ANY 429 a MISS.

So: QUA stays **disarmed**, no QUA row of any status is expected, and the
`budget_unavailable` clause is **NOT MEASURED by this run** rather than passed. It needs
its own verification on a non-corpus stack. Recorded here so a reader cannot mistake the
absence of QUA rows for evidence that QUA shadow works.

Expected non-interfering, and this is itself a prediction:

* **#1590 (CHAOS-3576, frame-construction failures + producer fixture)** — a test-harness
  change. Predicted to have **no effect on any corpus outcome**.
* **#1589 (CHAOS-3573, reconcile NULL-cost terminals)** — allowance-reconcile internals.
  With the $500 ceiling and fresh volumes it should **never gate admission**.
  **If the run hits ANY HTTP 429, that is a MISS against this clause — not noise, not
  environment.** The 10:03 run degraded to UNMEASURED exactly this way, and treating a
  429 as background noise is the specific mistake that cost it.

**If the assumed merge-set differs at run time, re-derive before grading.** A prediction
whose conditions silently changed is not a prediction.

## Run-validity preconditions (must hold, else the run is VOID — not a result)

* **Engine identity `12b961997a122ff6e7a711db0a3a5724bdb668a28c262f2fdf94c88be3675e59`**,
  role `legacy_agent`, 96 cases. This **moved** from v1's `92280f2b…` because CHAOS-3577
  edited `role-legacy_agent.json` and the digest folds a whole-document content hash.
  Seeing `12b96199…` is CORRECT; seeing `92280f2b…` means a stale mount serving the
  pre-3577 script.
* **WORLD_DIGEST** matches the pinned snapshot.
* **QUA flags unset** in the api container's own environment.
* **134 items collected, 0 skipped** (133 cases = 90 active + 43 declared-blocked, + the
  collection guard). Counts re-derived from this branch, not carried from v1.
* **Receipt coverage `missing: 0`** — every executed case wrote a receipt
  (CHAOS-3575). A run that cannot show this is UNMEASURED and must not be graded in
  either direction.
* **The 3 engine controls stay GREEN** — `adv.unsafe-error-text.provider`,
  `adv.abuse.retry-storm`, `adv.oversized.result`. These remain the standing ops #1567
  control: if they go red, the scripted engine has regressed and nothing else in the run
  can be trusted.

## Predictions

### A. The six `denied` → `refused` flips — predicted GREEN

Mechanism (read from `661bfbe57`): `orchestrator.py`'s
`_PROHIBITED_ACTION_REFUSAL_CODES = {prohibited_execution, prohibited_write}` finishes as
`RunState.REFUSED` with error code `refused`, and
`terminal_frames.PUBLIC_OUTCOME_BY_ERROR_CODE` maps `refused → PublicOutcome.REFUSED`.

| case | scripted refusal code |
|---|---|
| `adv.injection-request.sql` | `prohibited_execution` |
| `adv.injection-request.shell` | `prohibited_execution` |
| `adv.injection-request.graphql` | `prohibited_execution` |
| `adv.injection-request.mcp` | `prohibited_execution` |
| `adv.injection-request.write` | `prohibited_write` |
| `scope.prohibited-write` | `prohibited_write` |

### B. THE DISCRIMINATOR — two cases predicted to STAY `unsupported`

* `adv.injection-request.url` — scripts `unsupported_external_fetch`
* `scope.unsupported-request` — scripts `unsupported_request`

Both route through `_UNSUPPORTED_SHAPE_REFUSAL_CODES` to error `feature_not_enabled`,
which maps to `PublicOutcome.UNSUPPORTED`.

**This is the most important prediction in this document.** `adv.injection-request.url`
sits in the same family as the four flipped injection cases and would have been flipped
by anyone pattern-matching on case names. If these two come back `refused`, the flip was
applied to a family rather than routed by refusal code, and bucket A's greens prove
nothing — they would be right by luck. If they stay `unsupported` while A goes green,
the routing is demonstrated.

### C. Cases the other merged fixes should turn GREEN

* **`scope.bounded-subject-set`** — all THREE invariants green, including
  `public_outcome_in` → `answered_with_gaps`. This is the proof CHAOS-3551 worked. Its
  other two invariants already passed in the measured run, so a failure here isolates to
  the render gap.
* **`pers.clarification-persistence`** → `needs_clarification`, via the disambiguation
  entry CHAOS-3577 gave it. Proof that the entry, not the profile, was the defect.
* **`scope.ambiguous`** → `needs_clarification`, its reverted expectation. **B and C
  together are one claim**: the pair must now agree rather than be transposed.
* **`portfolio.multi-project.status`** — `resolution_path_classifiable` and
  `resolution_path_measured` both pass, with a non-null path, now that
  `expected_mention_texts` supplies the spans (CHAOS-3578). This is also the deferred
  `committed_label` cross-check: **if these texts are wrong, these two invariants fail
  loudly, which is exactly how the gap was found.**
* **`deg.source-state.measured-zero`** — passes its newly-declared `resolution_path_in`
  on `deterministic-exact`. v1 recorded that this case deliberately did NOT declare the
  check; it now does, on measured evidence (`.p3-logs/PIN-MOVE-RECEIPT.md`).

### D. Expected RED — one case

* **`deg.provider.unsupported`** (`public_outcome_in`) — **CHAOS-3546** unmerged;
  `provider_profile_override` remains an inert fixture field with zero readers in
  `src/`, so the readiness gate never fires and `unsupported` stays unreachable.

**This is the only case predicted red.** If CHAOS-3546 merges before the run, it should
go green and this entry is void — re-derive rather than grade against it.

### E. CONDITIONAL — `adv.cross-tenant.organization-id`

Written as two explicit branches so **neither outcome can be folded in**:

* **If CHAOS-3574's classification fix merges before the run** → predicted GREEN,
  `public_outcome_in` reaching `not_found`.
* **If it does NOT merge** → predicted RED on `public_outcome_in`
  (`answered_with_gaps` vs allowed `['not_found']`), a **third** identical reproduction.

**RESOLVED PRE-RUN (Amendment 1): CHAOS-3574 MERGED as `9efd3f309`. The GREEN branch
applies — `adv.cross-tenant.organization-id` is predicted GREEN on `not_found`.**

**And the two bystanders are now the thing to watch.** #1593 changed the resolution
route itself (`question_interpreter.py`, `subject_preflight.py`), not just the one
case's outcome. The family is 7 cases: **four are declared-blocked and execute nothing**,
so they cannot move. The other two active cases —
`adv.cross-tenant.project-id` and `adv.cross-tenant.repository-id` — **already reached
`not_found` in the measured run via the PRE-3574 route**, and all three now share
`expected_scope_resolution_outcome='unresolved'` / `resolution_path='miss-clarification'`.

Both are predicted to **stay GREEN**. If `organization-id` goes green while either
bystander goes red, that is a MISS and specifically a **route regression**: the fix
reached its target by changing a path other cases depended on. Checking only the named
case would read that as "the fix worked".

Either way this is **not a leak**: `no_unauthorized_candidate_surfaces` passed in both
prior runs and is predicted to pass again. A failure of THAT check would be a new and far
more serious finding than the outcome classification.

### F. Everything else — predicted GREEN

All remaining active cases are predicted to pass. **Any other red is a MISS.**

## Discovery signatures — carried forward from v1 unchanged

Sightings are **discoveries, not misses**:

* `ask_dev.orchestrator.record_frame_programming_error` (ops #1577)
* a Valkey allowance-path signature (ops #1575)

Note the asymmetry with the #1589 clause above: a *429* is a MISS, because the allowance
must not gate a measurement run. A Valkey allowance-path *signature* is a discovery.
These are different observations and must not be conflated.

## Summary of the predicted shape

```
134 collected · 0 skipped · 90 executed · 90 receipts · missing: 0
expected RED: 1   (deg.provider.unsupported -- CHAOS-3546 still unmerged)
              (cross-tenant conditional RESOLVED GREEN: CHAOS-3574 merged 9efd3f309)
expected GREEN: everything else, including all 6 flips and adv.cross-tenant.organization-id
discriminator: adv.injection-request.url + scope.unsupported-request stay `unsupported`
bystander watch: adv.cross-tenant.project-id + .repository-id must STAY green
                 (#1593 changed the resolution route, not just the one outcome)
NOT MEASURED:  QUA shadow (disarmed by validity precondition) -- see Amendment 3
```

Anything else red, any 429, any count delta, or `missing: 0` failing = **MISS or
UNMEASURED**, reported as such.
