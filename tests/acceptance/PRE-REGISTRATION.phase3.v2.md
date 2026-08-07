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
* **CHAOS-3574** — not on `main`, no open PR found.

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
expected RED: 1   (deg.provider.unsupported)
              +1  conditionally (adv.cross-tenant.organization-id, if CHAOS-3574 unmerged)
expected GREEN: everything else, including all 6 flips
discriminator: adv.injection-request.url + scope.unsupported-request stay `unsupported`
```

Anything else red, any 429, any count delta, or `missing: 0` failing = **MISS or
UNMEASURED**, reported as such.
