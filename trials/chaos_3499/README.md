# CHAOS-3499 — shadow temporal-context trial

Discovery artifacts for [CHAOS-3499](https://linear.app/fullchaos/issue/CHAOS-3499),
under epic CHAOS-3498. Shadow-only: isolated store, dev/acceptance scope, no
user-visible output, no Ask Dev runtime change.

The directory is `chaos_3499` rather than `chaos-3499` for one dull reason:
`chaos-3499` is not a valid Python package name, so the corpus and harness
could not import each other. Content and placement are otherwise as approved.

**This directory is deliberately outside `docs/`.** `docs/` is the customer
MkDocs site and every `.md` there must appear in the nav or the docs gate
fails. Trial-internal material does not belong on the customer site.

## Contents

| Path | What |
|---|---|
| [`docs/baseline-inventory.md`](docs/baseline-inventory.md) | **Deliverable 1.** What native temporal support already ships, with `file:line` for every claim, the §15.2 per-class question assignment, and eight corrections the inventory forces on the PRD. |
| [`docs/harness-design.md`](docs/harness-design.md) | **Deliverable 3.** Shadow projection/query design, baseline-vs-arms comparison shape, compose+kind placement, and the gated bring-up plan. Design only — nothing is brought up. |
| [`corpus/cases.py`](corpus/cases.py) | The 21 PRD §15.1 corpus cases. Each names the defect it catches, not the data it holds. |
| [`corpus/questions.py`](corpus/questions.py) | The 7 evaluation questions with class (a)/(b)/(c) assignment and the evidence that decided each. |
| [`corpus/ground_truth.py`](corpus/ground_truth.py) | The synthetic world. Both time axes recorded per fact; pinned constants, never the wall clock. |
| [`corpus/oracles.py`](corpus/oracles.py) | **Deliverable 2.** 20 expected-evidence oracles, authored before any arm ran. |
| [`harness/`](harness/) | Contracts, oracle engine, fault modes, arm-agnostic runner. |
| [`harness/arms/`](harness/arms/) | **Bring-up step 1.** The two baseline-component adapters (`native`, `episode_readback`), against the pinned corpus directly — no fixture files, no live stack, no LLM spend. See its module docstring for what a static-snapshot adapter can and cannot prove. |
| [`harness/arms/extraction.py`](harness/arms/extraction.py), [`harness/arms/source_documents.py`](harness/arms/source_documents.py) | **Bring-up steps 2–3.** The extraction candidate arm (never a baseline component), axis-aware `AS_OF` filtering (step 3), and the hand-authored source prose four oracles read (two smoke, two class-(b)). |
| [`harness/llm/`](harness/llm/) | Provider-agnostic OpenAI-compatible client, env-driven — `LLM_PROVIDER=local` (LM Studio) or `cloud` (real OpenAI API, `gpt-5-mini` via the Responses API, step 3). |
| [`run_measured_sweep.py`](run_measured_sweep.py) | **Step 3.** Standalone (not pytest-collected) script that runs the composed baseline vs the extraction candidate over the full pinned corpus against the real cloud model and writes `docs/measured-trial-results.md`. Real, billable spend — not part of CI or `run_oracles.sh`. |
| [`docs/measured-trial-results.md`](docs/measured-trial-results.md) | **Deliverable 4.** The one measured sweep's rendered per-class `ComparisonReport`, run parameters, and interpretation notes — no headline number. |
| [`docs/adr-draft.md`](docs/adr-draft.md) | **Deliverable 5.** First ADR skeleton: per-class results, §14 framing, closure-expressibility observations, open questions. Deliberately no recommendation. |
| [`tests/`](tests/) | Fault-mode self-tests, corpus coverage guards, independent re-derivation, baseline-component per-class rendering, extraction-arm smoke (env-gated on a local model, includes offline axis-filter pins). |

## Running

```bash
uv sync --all-extras --dev          # once per fresh worktree
bash trials/chaos_3499/run_oracles.sh
```

Current state, **excluding** `test_extraction_smoke.py`: **275 passed, 197
skipped** (the skips are fault×oracle pairs where the fault genuinely cannot
apply — they are reported, and a guard fails if any fault is inapplicable
everywhere). This part never depends on any external service.

`test_extraction_smoke.py` (14 tests as of step 3 — three offline, pinning
the axis filter directly against fake model output, need no live model at
all) IS part of `run_oracles.sh` — it lives in `tests/` like everything
else — but the remainder is gated on a local OpenAI-compatible model (e.g.
LM Studio) at `LOCAL_LLM_BASE_URL` (default `http://localhost:1234/v1`).
Every test that needs the model skips loudly, with the connection failure
as the reason, if it is not reachable, so the total genuinely varies by
environment: **289 passed, 197 skipped** confirmed with a local model up
(step-3 measurement; the tests themselves pin `LOCAL_LLM_BASE_URL` to the
documented default regardless of ambient env, so the "model down" number
could not be reproduced from this shell without stopping a real local
service — expect it a few tests lower, all in
`test_extraction_smoke.py`, per the module docstring's skip discipline).
Neither number is a measured trial result — the smoke suite prints its own
per-oracle
observations, separately, labeled UNSCORED (see `harness/arms/extraction.py`
and `harness/llm/client.py`'s module docstrings).

## Why these tests are not in the CI gate

Root `pytest.ini` sets `testpaths = tests` and `[tool.mypy]` covers
`src`/`tests`/`scripts`, so `trials/` is collected by neither.
That is deliberate: `ci/local_validate.sh` is a **host-wide single-flight**
gate that every worktree on the machine queues behind, and shadow-trial code
must not be able to destabilise it.

The cost is real and is not papered over: these tests run only when someone
runs them. `run_oracles.sh` makes that one command, and an unrun oracle suite
is reported as `NOT MEASURED`, never as absent. Ruff *does* cover `trials/`
(no exclusion in `[tool.ruff]`), so lint and format are enforced normally.

## How results are reported

Amended §14 makes the native work **pre-trial increments feeding a baseline**,
not a competing entrant. So the matrix is:

```
baseline (native increments + episode readback, composed)
    vs  Graphiti arm
    vs  direct-store arm
```

…scored **per question class** as a delta, never as a league table and never
as one headline number (a test asserts the report cannot grow one). Episode
readback is a *baseline component* precisely because `EpisodeArtifacts`
already carries the subsystem signal — if plain readback answers Q4, the
graph's margin there is zero, and scoring readback as a peer would hide that.

Two things the report refuses to do quietly:

- **Class (b) renders NOT COMPARABLE** until CHAOS-3563's branch state is
  recorded. A pre-increment score and a post-increment score are different
  claims, and a report that cannot say which one it holds is not a result.
- **A class (a) control failure is shouted.** If the baseline loses on
  natively-answerable questions the harness is suspect, and every other row is
  marked unexplained until that is resolved.

## The three disciplines this code exists to enforce

1. **Oracles authored before observation.** Every expectation derives from how
   the corpus was constructed. `tests/test_corpus_consistency.py` re-derives
   the as-of expectations by an independent route and fails on disagreement,
   so a typo cannot quietly become the definition of correct.
2. **Every guard observed failing.** `tests/test_oracle_fault_modes.py` pairs
   each oracle with each fault and demands the oracle fail **in the assertion
   that claims to catch it** — a mutation dying elsewhere proves nothing.
   Verified by planting seven defects across the oracle engine and the
   comparison logic; all seven were killed in the right guards, and one
   further attempt was recorded INVALID rather than counted as a kill because
   its anchor no longer matched (see `docs/harness-design.md` §4).
3. **A measurement that did not happen FAILS.** There is no skip path in the
   runner. `Verdict.NOT_MEASURED` is a failure, an arm that raises becomes a
   recorded `NOT_RUN` rather than an aborted sweep, reports mark any aggregate
   containing an unmeasured oracle as NOT COMPARABLE, and an unmeasured
   baseline component degrades the baseline to NOT MEASURED rather than to a
   failure a candidate could take credit for beating.

## What is deliberately not here

- **No ADR outcome.** Adopt / native / do-not-adopt comes from the corpus
  results. Nothing in this directory pre-decides it.
- **No stack.** No compose file authored, no kind pod, no bring-up. All gated
  on an orchestrator-granted environment slot.
- **No Graphiti or direct-store arm yet.** Those follow review of
  deliverables 1–3, as separately reviewable changesets. The two
  *baseline-component* adapters (`harness/arms/`) exist as of bring-up step
  1 — against the pinned corpus, no live stack, no LLM spend.
- **Step 2 (superseded by step 3, kept for history).** Built the first
  *candidate* arm's plumbing (`harness/arms/extraction.py`) and smoke-tested
  it against a local model (`google/gemma-4-e4b` via LM Studio) on exactly
  two oracles authored with source prose — a quality signal and a security
  (prompt-injection) signal, both explicitly labeled UNSCORED. No cloud call
  had been made; no oracle had been scored into an ADR number yet.
- **Step 3: one real, cloud-measured sweep — still a partial corpus, not a
  full one.** `LLM_PROVIDER=cloud` now calls the real OpenAI API
  (`gpt-5-mini`, the repo's own production default, via the Responses API —
  see `harness/llm/client.py`'s module docstring). Class (b)
  (`O2_blocking_valid`/`O2_blocking_observed`) gained real source material
  and axis-aware `AS_OF` filtering (`harness/arms/extraction.py`'s
  `_apply_as_of_filter`), making it the first class to render structurally
  COMPARABLE against CHAOS-3563's now-merged declared-state history. One
  measured sweep has been run and committed as a trial artifact
  (`docs/measured-trial-results.md`) alongside a first ADR draft skeleton
  (`docs/adr-draft.md`, deliberately no recommendation). **Still true:**
  only 4 of 20 oracles have authored source material (the two step-2
  oracles plus the two new class-(b) ones) — classes (a) and (c) are mostly
  `NOT_RUN`, not silently, and not because the candidate lost anything; see
  the artifact's interpretation notes and the ADR draft §5 for exactly what
  is and is not covered. Full corpus source-prose authoring and the
  Graphiti/direct-store arms remain future work.
