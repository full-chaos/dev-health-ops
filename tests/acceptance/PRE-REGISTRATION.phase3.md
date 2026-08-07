# Pre-registration — CHAOS-3219 Phase 3 armed run

**Written and committed BEFORE the run. Any deviation is reported as a MISS, never folded in.**

Branch `chaos-3219-phase3-corpus`, based on `5a20844af`. Corpus **133 files / 90 active / 43 declared-blocked**; armed-run pytest items **134** (90 + 43 + the collection guard).

## Derivation (done independently; NOT inherited from the 9→1 figure)

I read the two product diffs rather than the annotations alone.

* **CHAOS-3533 (`f0e07c014`)** — `orchestrator.py` now persists the ledger on **TERMINATE**, not only on one outcome. Diff comment, verbatim: *"the WHOLE ledger is written here now"*. This makes `resolution_path_in` satisfiable on the eight cases annotated `$comment_known_red_class_closed`.
* **CHAOS-3534 (`878a5c6ce`)** — `scope_service.py` gains `committed_cohort_resolution_for`, publishing a committed COHORT scope instead of falling through to `UNRESOLVED`. This makes `scope_resolution_outcome_in` satisfiable on `scope.bounded-subject-set`.
* **CHAOS-3551** — the D1 cohort render gap is NOT fixed: a fully-resolved cohort still refuses to answer. So `scope.bounded-subject-set`'s third invariant, `public_outcome_in`, stays red **on purpose** — it was added precisely so the case could not go green on the other two fixes while the product still refuses.

**My derived figure for the pre-existing known-red set is 9 → 1, matching team-lead's independently.** The surviving red is `scope.bounded-subject-set`, failing `public_outcome_in` only.

## FINDING, raised BEFORE the run: 9→1 is not this run's expected red

9→1 describes the *pre-existing* known-red set. It does not account for P3-1, which wires `public_outcome_in` onto **37** cases that declared an expected outcome and asserted nothing. Those additions have their own predicted reds, each with a traced mechanism and each already approved.

## Pre-registered predictions

### Expected RED — 8 cases

| case | tranche | check | mechanism |
|---|---|---|---|
| `scope.bounded-subject-set` | — | `public_outcome_in` | **CHAOS-3551** D1 cohort render gap — resolves every member, then refuses |
| `adv.injection-request.sql` | A | `public_outcome_in` | **CHAOS-3541** — `DENIED` unreachable from an `AgentRefusal`: the refusal's `.code` is discarded at `orchestrator.py:3339`, `prohibited_execution` exists nowhere in `src/`, and the only route to `DENIED` is `scope_forbidden` |
| `adv.injection-request.shell` | A | `public_outcome_in` | same |
| `adv.injection-request.graphql` | A | `public_outcome_in` | same |
| `adv.injection-request.mcp` | A | `public_outcome_in` | same |
| `adv.injection-request.write` | A | `public_outcome_in` | same |
| `adv.injection-request.url` | A | `public_outcome_in` | **CHAOS-3541** — expects `unsupported`, also unproducible from a refusal (achievable terminal is `insufficient_evidence` → `NOT_FOUND`) |
| `deg.provider.unsupported` | B | `public_outcome_in` | **CHAOS-3546** — `provider_profile_override` is an inert fixture field (zero readers in `src/`), so the readiness gate never fires and `unsupported` is unreachable |

**Total expected red: 8.** Anything else red is a MISS.

### Expected GREEN — the controls that make this run worth spending

These are the discriminators. If they do not go green, the fix they test did not work:

* **`adv.unsafe-error-text.provider`, `adv.abuse.retry-storm`, `adv.oversized.result`** — expectations were always correct and reachable; they were never *delivered*, because the scripted engine was dead (ops #1567). With the engine loading, these must now reach `temporarily_unavailable` / `temporarily_unavailable` / `failed`. **These three going green is the proof #1567 worked.**
* **The eight `$comment_known_red_class_closed` cases** — must now pass `resolution_path_in`. **This is the proof CHAOS-3533 worked.**
* **`scope.bounded-subject-set`'s other two invariants** (`resolution_path_in`, `scope_resolution_outcome_in`) must pass while `public_outcome_in` fails. A case failing all three would mean 3533/3534 did not land as diffed.

### Named live question this run must answer

`deg.source-state.measured-zero` deliberately does **not** declare `resolution_path_in`: the pin refused it on the grounds that *no case in this corpus has yet been OBSERVED producing a non-null resolution_path*. Post-3533 that should change. **If the eight class-closed cases now produce non-null paths, that fact is the evidence needed to add the check to measured-zero deliberately, with the receipt and the pin moved in one change.** Recorded as a question, not assumed either way.

### Retention surface (CHAOS-3544 PR1, `5a20844af`)

PR1 introduced an idle-anchored 1-hour grace for 0-day conversations. Checked which cases graze that surface: `pers.retention.0-day` and `.30-day` are **declared-blocked** and execute nothing; no active case asserts on `expires_at`, conversation lifetime, or retention. The runner creates conversations at `retention_days=30` (hardcoded), so no active case enters the 0-day path at all. **Predicted impact on this run: none.**

### Run-validity preconditions (must hold, else the run is void)

* WORLD_DIGEST matches the pinned snapshot — untouched by PR1, confirmed.
* **The scripted engine loads**, with a role and script-identity digest matching the host's (ops #1567). If this fails the run aborts with no receipts, which is the intended behaviour and not a MISS.
* 134 items collected, 0 skipped.
