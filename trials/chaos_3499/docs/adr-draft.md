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

**Two sweeps exist; run 2 is canonical.** Run 1 (2026-08-08T10:24Z) shipped
with a prompt/filter contract defect (#1603 finding 1: the prompt told the
model to omit `recorded_at` for a same-day fact, which the `AS_OF`
observed-time filter would have read as "never observed" — run 1's 2/2 on
class (b) happened to work only because the model didn't follow that
instruction). Run 2 (2026-08-08T11:14Z) re-earned every number under the
fixed contract (prompt now asks for `recorded_at` whenever the text states
one, same-day or not) and is what `docs/measured-trial-results.md`
currently holds. Run 1's numbers are kept here ONLY as a historical
data point (§4's variance discussion needs both), never as the record.

Full run-2 parameters, the rendered `ComparisonReport`, and its
interpretation notes are the committed trial artifact:
[`docs/measured-trial-results.md`](measured-trial-results.md). Summarized:

| Class | Baseline | Candidate (extraction_llm), run 2 | Delta | Comparable? |
|---|---|---|---|---|
| (a) NATIVE_ANSWERABLE | 1/3 | 0/3 | −1 | **NO** — 3 candidate oracles NOT_RUN (no class-(a) source material authored this round; see §5 scope note, not a real loss) |
| (b) NEEDS_DECLARED_STATE_HISTORY | 0/2 | 2/2 | +2 | **YES** — CHAOS-3563 dependency state recorded |
| (c) NEEDS_EXTRACTION_OR_ASSOCIATION | 1/15 | 2/15 | +1 | **NO** — 13 candidate oracles NOT_RUN (only 2 of 15 have source material authored) |

(Run 1, contract-defective, for reference: class (a) 0/3, class (b) 2/2,
class (c) 1/15 — class (b) held under BOTH contracts; class (c) differs,
see §4.)

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
  adapter-invented). This 2/2 held under BOTH the contract-defective run 1
  AND the fixed run 2 — the ATL-101/ATL-105 pair's two dates were always
  distinct enough (07-02 vs 07-20) that the same-day-omission bug never
  actually triggered on this specific pair; that is reassuring for THIS
  pair, not a general guarantee the contract fix was inconsequential (see
  §7's rebuild-equivalence discussion for why the fix still matters).
- **Classes (a) and (c) are NOT comparable this round** — not because the
  candidate lost, but because it was not run against most of their
  oracles. See §5.

## 4. Direction-reversal / closure-expressibility observations

Carried forward from the step-2 local-model smoke (`docs/harness-design.md`
§7, "Extraction arm plumbing"), for continuity, PLUS the two step-3
measured sweeps (§3). **#1603 finding 6 note: the claims below are
reconciled against what each run's own record actually shows, not
extrapolated** — where a per-oracle result was not separately logged, that
is stated as a gap, not papered over with a stronger claim than the
evidence supports.

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
  **closure-expressibility gap**, not a mis-orientation. A step-3
  local-model re-check (same `google/gemma-4-e4b`, run alongside the
  offline suite this round) reproduced the SAME `O3_supersession` FAIL,
  and additionally showed `O5_conflicts_injected` FAIL where it had
  previously passed — the local model's results are not stable run to run
  either, not just the cloud model's (see the rebuild-equivalence
  discussion, §7).
- **Cloud model (`gpt-5-mini`) class-(c) results differ between the two
  canonical sweeps**: run 1 (contract-defective) recorded class (c) as
  1/15; run 2 (fixed contract) recorded it as 2/15. Per-oracle detail was
  NOT separately logged in either run (`run_measured_sweep.py` reports
  only the aggregate `ComparisonReport`, not a per-oracle breakdown) —
  **this ADR draft does not claim to know which of `O3_supersession` /
  `O5_conflicts_injected` passed in which run**, only that the aggregate
  moved from 1/15 to 2/15. An UNOFFICIAL, non-sweep diagnostic query run
  once between the two sweeps (not itself a recorded measurement) observed
  both oracles passing gpt-5-mini in that one query — consistent with,
  but not proof of, run 2's 2/15. The honest state is: gpt-5-mini's
  class-(c) pass rate across observed attempts ranges from 1/2 to 2/2 on
  these two oracles, and this ADR does not have enough runs to say more
  than that a real closure-expressibility capability exists but its
  reliability is unmeasured. **Fixing `run_measured_sweep.py` to log a
  per-oracle breakdown, not just the aggregate, is a concrete, low-cost
  improvement for whichever round does the class-(a)/(c) authoring
  round.**

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

**Measurement-integrity tightening this round (#1603 finding 5):**
`O2_blocking_observed`'s ATL-101 expectation now pins
`require_evidence_refs={"ev1_dep_101_110"}`, matching its `O2_blocking_valid`
twin (both read the same source document; ground truth already carried this
exact ref for both facts — `corpus/ground_truth.py`'s `gt_blocks_101_110`).
Without this pin, a single fact with ANY evidence ref (including a
fabricated one) satisfied the oracle, which is a real gap this trial's own
smoke tests exist to catch (an oracle that cannot fail is worse than no
oracle). This did not change class (b)'s 2/2 result — the extraction arm's
actual answer already cited the real ref.

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
3. **gpt-5-mini's run-to-run variance on closure-expressibility (§4, §7):**
   is this a real capability ceiling, prompt-sensitivity, or normal LLM
   variance that a majority-vote or higher-reasoning-effort scheme would
   resolve? Two sweeps (1/15, 2/15) plus one unofficial diagnostic query is
   still not enough evidence to say — and neither sweep logged which
   specific oracle passed, which itself needs fixing before more evidence
   accumulates usefully. See §7 for why this bears directly on the
   rebuild-equivalence choice.
4. **`native_control_holds()`'s rendered banner conflating "not measured"
   with "measured and lost" — CONFIRMED as a fix, not a question**, per
   orchestrator direction. Scoped for the next round (after this PR
   merges): unmeasured must render as `NOT MEASURED`, never as a held/lost
   verdict. Listed here for traceability only; not this round's work.
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

## 7. Rebuild-equivalence: evidence for CHAOS-3500, not a recommendation

CHAOS-3500's own "semantically equivalent" definition is not landed yet
(`docs/harness-design.md`'s bring-up step 9 explicitly flags this gap).
This section reports evidence this trial has produced that bears on that
definition's Option A vs Option B choice — **the choice itself belongs to
CHAOS-3500, not to this ADR.**

**The evidence.** Across this trial's runs to date:

- Cloud model (`gpt-5-mini`), class (c), two FULL measured sweeps: run 1
  (contract-defective prompt) scored 1/15; run 2 (fixed prompt, same
  model, same two oracles with source material) scored 2/15. One
  additional UNOFFICIAL diagnostic query (not a recorded sweep) between
  the two showed both oracles passing. Neither sweep logged which
  specific oracle passed in which run (§4 — a concrete gap to close before
  this evidence gets stronger).
- Local model (`google/gemma-4-e4b`), same two oracles, run alongside this
  round's offline suite: `O3_supersession` FAIL (consistent with step 2's
  original observation) and `O5_conflicts_injected` FAIL (a REGRESSION
  from step 2's original PASS, on the identical prompt/documents/model —
  see §4).
- No code change, no document change, and (for the gpt-5-mini case) no
  caller-selected `temperature` — the API does not accept one for this
  model family — separates any of these results from one another. Same
  inputs, same model, different outputs, minutes apart.

**Implication for Option A (extraction-cache).** If extraction results are
cached at the time of extraction and treated as the durable record,
rebuild-equivalence becomes exact BY CONSTRUCTION: a "rebuild" reads the
cache, never re-extracts, so there is no re-extraction variance to
reconcile against. The cost is that extraction quality freezes at cache
time — a later prompt or model improvement (like this round's
`recorded_at` fix) never surfaces for already-cached facts without an
explicit, separately-authorized cache-bust/re-extraction pass.

**Implication for Option B (bounded drift).** If rebuilds are allowed to
re-extract and are judged "equivalent" within some tolerance, that
tolerance needs a drift metric, and it needs calibration data. This
trial's observations are a concrete, if small, candidate input: one
oracle, one model, one prompt version, a NON-reproducing pass/fail on
immediate re-run of the SAME inputs (both cloud and local). Drift can
happen within minutes with no code or document change at all — a bounded-
drift definition that assumes drift only accumulates over model/prompt
VERSION changes would be measuring the wrong thing.

**What this ADR does NOT do:** pick between A and B. That is CHAOS-3500's
own deliverable, under its own "semantically equivalent" definition. This
ADR's job is to hand over evidence that the definition, whichever way it
goes, needs to account for same-input non-determinism — not just
version-to-version drift.
