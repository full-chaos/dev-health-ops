# ADR draft (skeleton) — CHAOS-3499 temporal-context shadow trial

**Status: DRAFT, no recommendation.** This lays out the shape the ADR needs
and the evidence gathered so far; the adopt/native/do-not-adopt synthesis is
a separate decision gate that goes through the orchestrator to chris, per
step-3 instruction 4. Nothing below should be read as a recommendation.

## 1. What this ADR must decide

Whether an LLM-extraction candidate arm (or a future Graphiti/direct-store
arm measured the same way) is worth adopting into the product's temporal
context path, and if so, in what form — per PRD §14/§15.2/§16, decided
**per question class**, never as one aggregate score
(`ComparisonReport` structurally cannot render one; see
`harness/runner.py`).

## 2. The §14 baseline-vs-arms framing this ADR inherits

Amended §14 restructured native temporal work as **pre-trial increments
feeding a baseline**, not a competing entrant (`docs/harness-design.md` §3).
Concretely:

```
baseline (native increments + episode readback, composed)
    vs  extraction_llm arm   [measured this round]
    vs  Graphiti arm          [not built]
    vs  direct-store arm      [not built]
```

Composition rule: **any baseline component passing → the baseline passes**
(the product can already do it if any part can); **any component
unmeasured → the baseline is NOT MEASURED**, never scored as a loss. This
is why a class where extraction "wins" is read as "the baseline's existing
data model has zero margin here," not "extraction beat two products" — see
`docs/harness-design.md` §3.1 on why episode readback is a baseline
component, not a peer entrant.

## 3. Per-class results (this round: extraction_llm only)

Full run parameters, the rendered `ComparisonReport`, and its
interpretation notes are the committed trial artifact:
[`docs/measured-trial-results.md`](measured-trial-results.md). Summarized:

| Class | Baseline | Candidate (extraction_llm) | Delta | Comparable? |
|---|---|---|---|---|
| (a) NATIVE_ANSWERABLE | 1/3 | 0/3 | −1 | **NO** — 3 candidate oracles NOT_RUN (no class-(a) source material authored this round; see §5 scope note, not a real loss) |
| (b) NEEDS_DECLARED_STATE_HISTORY | 0/2 | 2/2 | +2 | **YES** — CHAOS-3563 dependency state recorded |
| (c) NEEDS_EXTRACTION_OR_ASSOCIATION | 1/15 | 1/15 | +0 | **NO** — 13 candidate oracles NOT_RUN (only 2 of 15 have source material authored) |

Read literally, not headline-summed:

- **Class (b) is the one genuinely comparable result this round**, and it
  is decisive in the corpus's own favor of extraction over the current
  native+episode-readback baseline: native's blocker-relationship code path
  (`harness/arms/native.py::_answer_q2_current_state`) has no valid-time
  concept at all (`degraded_reasons=("no_relationship_valid_time:...",)`) —
  it always answers "current state," which is a category error against an
  `AS_OF` question on either axis. The extraction arm, given prose stating
  both a fact's effective date and (separately, where they differ) its
  recorded/backfill date, correctly answered BOTH the valid-time and
  observed-time twins of the same instant — including correctly EXCLUDING
  a backfilled fact from the observed-time answer while INCLUDING it on
  valid-time, the exact axis-pair distinction corpus case 19 exists to
  catch. See `harness/arms/extraction.py`'s `_apply_as_of_filter` and its
  module docstring for the mechanism (deterministic filtering over
  model-emitted `valid_from`/`valid_to`/`recorded_at` dates only — never
  adapter-invented).
- **Classes (a) and (c) are NOT comparable this round** — not because the
  candidate lost, but because it was not run against most of their
  oracles. See §5.

## 4. Direction-reversal / closure-expressibility observations

Carried forward from the step-2 local-model smoke (`docs/harness-design.md`
§7, "Extraction arm plumbing"), for continuity:

- An **earlier** local-model (`google/gemma-4-e4b`) run observed a
  direction-REVERSAL defect on the `O3_supersession` fact (ADR-021
  supersedes ADR-014 extracted backwards). That specific defect did not
  reproduce after a null-field-handling fix and the model-emitted-closure
  redesign, and is treated as superseded, not as an open finding.
- The **step-2 local-model** run's live `O3_supersession` result was FAIL:
  the `supersedes` edge itself extracted correctly, but the model did not
  reliably extract the paired `describes_deployment_design_for` fact
  (malformed row) and never emitted the `"closes"` block despite the
  source text stating an explicit date it could have cited — a genuine
  **closure-expressibility gap**, not a mis-orientation.
- The **step-3 cloud model** (`gpt-5-mini`, this round's measured sweep)
  PASSED `O3_supersession` outright in the recorded sweep run — closure
  expressed correctly, citing the real evidence ref, right direction. A
  separate, later ad-hoc spot check (same prompt/schema, same model, not
  part of the recorded sweep) did NOT reproduce the pass identically — see
  `docs/measured-trial-results.md`'s interpretation notes on run-to-run
  variance. Whether the production-class model's closure-expressibility is
  reliable or was a favorable roll on one run is an open question (§6).
- `O5_conflicts_injected` (prompt-injection resistance) PASSED under both
  the local model and the cloud model, in every run observed so far
  (smoke and measured) — the more consistent of the two signals collected.

## 5. Scope actually measured this round (no silent caps)

Only 4 of 20 oracles have authored source material for the extraction arm:
`O2_blocking_valid`, `O2_blocking_observed` (both new this round, class b),
`O3_supersession`, `O5_conflicts_injected` (both carried over from step 2,
class c). This was a deliberate scope decision, not an oversight: step-3
instruction 1 required class (b) to become genuinely comparable; nothing in
the instructions required full-corpus source-prose authoring, and doing so
for CI-episode-shaped class-(c) oracles (`O1`, `O4` variants, `O6`, ...)
would mean either fabricating unnaturally prose-ified episode data or a
materially larger authoring effort than this round's scope. The other 16
oracles report an honest `NOT_RUN` (`no_source_material_authored_for_this_
oracle_yet`), not a silent skip — see `harness/arms/source_documents.py`'s
module docstring.

`O4_prior_attempts_manipulated` (needs `max_results` truncation logic the
adapter does not implement) and `O5_conflicts_poisoned` (an additional
security dimension) remain out of scope for the same reason, carried
forward from step 2.

`O7_null_valid_from` remains outside the class-(a) control, unchanged —
this is CHAOS-3570 credit isolation (a different table's bug, still open),
not touched by CHAOS-3563 landing.

## 6. Open questions this ADR must answer

1. **Is class (b)'s +2 delta durable, or a small-N artifact?** Two oracles
   is the entire class-(b) sample this corpus defines. Does the ADR need a
   larger bitemporal corpus before this becomes a real basis for a
   recommendation, or is the *mechanism* demonstration (axis-aware
   filtering over model-emitted dates, not the raw pass count) the real
   evidence here?
2. **How much does class (a)/(c) source-material authoring cost, and is it
   worth doing before the ADR can be written?** This round intentionally
   left both mostly NOT_RUN. A recommendation almost certainly cannot be
   made on class (b) alone, since §15.2 weights (a)×1 (b)×1 (c)×5 — class
   (c) is the largest question class by design.
3. **gpt-5-mini's run-to-run variance on closure-expressibility (§4):** is
   this a real capability ceiling, prompt-sensitivity, or normal LLM
   variance that a majority-vote or higher-reasoning-effort scheme would
   resolve? One sweep is not enough evidence either way.
4. **Should `native_control_holds()`'s rendered banner distinguish "not
   measured" from "measured and lost" more explicitly?** This round's class
   (a) row is technically correct (`NOT COMPARABLE: N arm oracle(s) NOT
   MEASURED` is right there in the per-class line) but the banner text
   below it ("control did NOT hold... treat every other row as
   unexplained") reads, in isolation, like the baseline lost — see
   `docs/measured-trial-results.md`'s interpretation notes. Worth a
   rendering change, or is "read the per-class line" sufficient discipline
   once documented?
5. **What does "adopt" even mean operationally** if class (b)'s answer is
   "extraction beats a baseline with zero valid-time concept at all"? Is
   the real comparison here extraction-vs-native, or
   extraction-vs-"whatever CHAOS-3563/3564/3565's own increments end up
   supporting once THEY land bitemporal query paths" — a moving target
   this trial does not control?
6. **Graphiti and direct-store arms remain unbuilt.** Does the ADR wait for
   both before recommending anything about "adopt a graph," or can class
   (b)/(c) findings about extraction-as-a-technique stand on their own,
   separate from which storage backend eventually hosts it?
