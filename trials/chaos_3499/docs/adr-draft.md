# ADR draft — CHAOS-3499 temporal-context shadow trial

**Status: DRAFT. FINDINGS AND OPTIONS ONLY — NO RECOMMENDATION.** This
document reports what was measured and what follows from it. It
deliberately does **not** contain an adopt / go-native / do-not-adopt
conclusion, and no section below should be read as arguing for one. That
synthesis is chris's decision; the ADR's job is to make it a decision taken
on evidence rather than on impression.

**What changed by run 7** (2026-08-08), relative to the earlier draft:

- **The matrix went from a single model to the full set below, and the
  deployed one is finally measured.** Runs
  1–2 used `gpt-5-mini`; Ask Dev actually runs `gpt-5-nano`. Every number in
  runs 1–2 was therefore taken one tier above deployed parity. §8.
- **A frontier tier now separates a model ceiling from a framework limit.**
  `gpt-5.6-luna` scores 1/4 in run 6 and 2/4 in run 7 on class (c), against
  1/4 for the mid tier in run 6 and run 7 alike — so no tier exceeds 2 of 4,
  and the frontier's margin is at most one oracle and is not stable across
  runs. §3.4.
- **A previously reported prompt-injection result is WITHDRAWN.** It came
  from an oracle laxer than its own clean twin. §3.4.
- **The measurement is durable, and that is now a fact rather than a
  claim.** `measured-trial-results.records.json` holds the run: per-oracle
  verdicts, failed-assertion ids, identity evidence, latency, retries,
  prompt version, control status, and **content hashes of every file that
  defines what was measured**. The markdown is rendered FROM it by a single
  renderer, and a test pins byte-equality — so a presentation fix costs no
  model spend and the artifact cannot drift from its evidence. An earlier
  draft asserted this before it was true. §9.5.
- **Class (a) is measurable for the first time.** The authoring round gave
  all three class-(a) oracles real source prose, so §15.2's control ("the
  baseline must win or tie on natively-answerable questions") can now be
  evaluated instead of reported as NOT MEASURED. §3.
- **Every oracle is classified.** 9 authored, 11 not authorable each with a
  stated reason — and the *shape* of that 11 is itself a finding. §5.
- **Two defects were found in the measuring apparatus**, one fixed here and
  one filed as a production ticket (CHAOS-3608). Earlier local-model
  results are retrospectively suspect because of the first. §9.

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

## 3. Per-class results — run 7, five model tiers

Full parameters, per-tier per-class comparisons, and the per-oracle table
with latencies are the committed artifact:
[`docs/measured-trial-results.md`](measured-trial-results.md), regenerated
byte-for-byte from `run_measured_sweep.py`. Summarized here.

All five tiers ran. All five measured 9 of 20 oracles (the 9 with authored
source prose; §5). Nothing was skipped and no tier was NOT_RUN.

### 3.1 The three classes, per tier

| Class | Baseline | nano (deployed parity) | mini | luna (frontier) | gemma-e4b | gemma-31b | Comparable? |
|---|---|---|---|---|---|---|---|
| (a) NATIVE_ANSWERABLE | 1/3 | **0/3** | **0/3** | **0/3** | **0/3** | **0/3** | **YES** |
| (b) NEEDS_DECLARED_STATE_HISTORY | 0/2 | **2/2** | **2/2** | **2/2** | **2/2** | **2/2** | **YES** |
| (c) NEEDS_EXTRACTION_OR_ASSOCIATION | **0/4** on the shared population (1/15 full) | 0/4 | 1/4 | 2/4 | 0/4 | 1/4 | **NO** — 11 arm oracles NOT_RUN (§5) |

**Two populations, named explicitly, because they are not interchangeable
and an earlier draft mixed them.** Class (c) has 15 oracles; only **4** have
authored source material, so only those 4 can be measured by the extraction
arm at all (§5).

- **Shared-authored population (4 oracles)** — the only population on which
  baseline and arm are comparable, and the one every per-tier number in
  this row uses. On it the **baseline scores 0/4**.
- **Full population (15 oracles)** — the baseline runs on all of them and
  scores **1/15**. That single pass is `O4_prior_attempts`, which is **not
  one of the four authored oracles**, so it cannot appear in any
  arm-vs-baseline comparison.

An earlier draft printed the baseline's full-population `1/4`-shaped figure
in the shared-population column, which the records contradict. Denominators
elsewhere are measured-only; an earlier draft rendered class (c) as `0/15`
with 11 NOT_RUN, which reads as a measured 15-case score.

The class (a) control (`native_control_status`) returned **`held` on all
five tiers.**

### 3.2 Class (a): the control is real now, and it holds everywhere

Runs 1–2 could not evaluate §15.2's control at all — no class-(a) source
prose existed, so it reported NOT MEASURED. This round it is genuinely
comparable, and the result is a clean, model-independent negative:
**extraction scored 0/3 at every tier, from the smallest local model to the
cloud ceiling.**

Per-oracle, identical across all five tiers:

| Oracle | native | episode readback | extraction (every tier) |
|---|---|---|---|
| `O7_valid` | **PASS** | fail | fail |
| `O7_unpinned` | fail | fail | fail |
| `O7_null_valid_from` | fail | fail | fail |

Read carefully, because "the control held" is doing less work than it
sounds like:

- **The control is not a compliment to extraction.** It held because
  extraction lost, 0 against the baseline's 1. §15.2 exists to catch a
  harness that flatters the candidate; nothing here suggests that failure
  mode, at any tier.
- **The baseline only manages 1/3 itself**, and the two it misses are
  known: `O7_null_valid_from` is the deliberate expected-FAIL isolating
  CHAOS-3570 (a still-open production bug in a different table), and
  `O7_unpinned` fails natively too. So class (a) is better read as "neither
  approach answers 2 of these 3" than as "native is strong here."
- **Consistency across the whole tier range is itself the finding.** A result
  that is identical at gemma-e4b and at gpt-5-mini is unlikely to be model
  quality. Extraction not answering natively-answerable questions looks
  structural, not a matter of picking a better model.

### 3.3 Class (b): the one decisive result, and it is tier-independent

**Every tier scored 2/2 against a baseline of 0/2** — deployed parity, the
mid tier, the frontier model, and both local models alike. This is the
strongest and most stable finding in the trial. It was previously earned
only at `gpt-5-mini`, and the pause-time worry was that it would evaporate
at parity or at local scale. It did not, in either direction: a small local
model and a frontier model produce the same result.

The baseline's 0/2 is not a near miss. Native's blocker-relationship path
(`harness/arms/native.py::_answer_q2_current_state`) has **no valid-time
concept at all** — it reports `degraded_reasons=("no_relationship_valid_time:...")`
and always answers "current state," a category error against an `AS_OF`
question on either axis. That is CHAOS-3569, measured. The extraction arm,
given prose stating a fact's effective date and separately its
recorded/backfill date, answered both the valid-time and observed-time
twins of the same instant correctly — including correctly *excluding* a
backfilled fact from the observed-time answer while *including* it on
valid-time.

That this holds identically from `gemma-4-e4b` to `gpt-5.6-luna` suggests
the capability comes from the **bitemporal contract** — asking for
`valid_from`/`valid_to`/`recorded_at` and filtering deterministically over
model-emitted dates (`_apply_as_of_filter`) — rather than from model
quality. **Sample-size caveat, restated honestly per the codex round:** the
two class-(b) oracles are the valid-time and observed-time twins of **one
scenario** (the ATL-101/ATL-105 blocker pair). This is a *two-case
axis-pair demonstration*, not two independent successes, and repeating it
across five tiers does not increase n — it tests robustness to model
choice, which is a different (and genuinely strengthened) claim. Whether
the mechanism generalises needs a larger bitemporal corpus (§6).

### 3.4 Class (c): framework-bound, and unstable at the top

<!-- GENERATED:comparative-facts BEGIN -->

_Generated from `measured-trial-results.records.json`. Do not edit,
and do not restate these figures in prose -- cite this block._

- Class-(c) authored population: **4 oracles**.
- Best measured tier score: **2/4** (`gpt-5.6-luna`).
- Worst measured tier score: **0/4**.
- Spread between best and worst: **2 oracle(s)**.

| Rank | Tier | Class-(c) authored score |
|---|---|---|
| 1 | `gpt-5.6-luna` | 2/4 |
| 2 | `gemma-4-31b-local` | 1/4 |
| 3 | `gpt-5-mini` | 1/4 |
| 4 | `gemma-4-e4b-local` | 0/4 |
| 5 | `gpt-5-nano` | 0/4 |

<!-- GENERATED:comparative-facts END -->


Run 7, extraction arm, the 4 authored oracles:

| Oracle | nano | mini | **luna (frontier)** | gemma-e4b | gemma-31b |
|---|---|---|---|---|---|
| `O3_supersession` | fail | **pass** | **pass** | fail | **pass** |
| `O5_conflicts_injected` | fail | fail | **pass** | fail | fail |
| `O5_conflicts` | fail | fail | fail | fail | fail |
| `O6_recurring_pattern` | fail | fail | fail | fail | fail |
| **total** | **0/4** | **1/4** | **2/4** | **0/4** | **1/4** |

**Read across BOTH committed runs, not just this one.** Run 6
(`b0983d17e`) and run 7 are both committed artifacts, measured on identical
oracles, ground truth, source documents and prompt version (`extraction.v2`
— verified as a zero diff across `corpus/oracles.py`,
`corpus/ground_truth.py`, `harness/arms/source_documents.py` and the
extraction PROMPT itself; `harness/arms/extraction.py` did change between
those commits, but only in model-identity labelling (removing a fallback
from served-model to requested-model), which cannot alter an extraction
result. Note also that run 6 predates the records file, so this was
verified from the source tree at each commit, NOT from committed record
hashes — cross-run hash verification only becomes possible from run 7
onward):

Run-6 figures below are read from that commit's committed markdown, and
run-7 figures from the records file. **Run 6 predates the records format**,
so its numbers cannot be re-derived by the guards that check run 7 — they
are transcribed from a committed artifact, which is weaker evidence and is
labelled as such rather than silently mixed in.

| Tier | run 6 (from committed markdown) | run 7 (from records) |
|---|---|---|
| nano | 0/4 | 0/4 |
| mini | 1/4 | 1/4 |
| **luna** | **1/4** | **2/4** |
| gemma-e4b | 0/4 | 0/4 |
| gemma-31b | 1/4 | 1/4 |

So the honest statements are:

- **No tier exceeds 2 of 4, and the frontier tier's margin over the mid
  tier is at most one oracle — and is not stable across runs.** The per-run
  standings are in the cross-run table above; the point here is that they
  MOVED between the two runs. This is a much weaker claim than "the
  frontier buys you class (c)", and it is the claim the evidence
  supports.
- **Spending up to the frontier does not change the shape of the result.**
  Two of the four oracles fail at every tier in every run (the standings
  are in the tables above). The binding constraint looks like the
  extraction contract and harness rather than model capability.
- **`O3_supersession` is the closest thing to a differentiator, and even it
  is not stable.** Its pass/fail grouping MOVED between the two committed
  runs — the frontier tier is on the other side of it in run 6 than in run
  7 (that flip is §7's drift datum). §4 states this in the same terms:
  "ladder" overstates what the corpus shows. There is no capability
  ordering here that reproduces across runs.

**Two oracles fail at every tier in every run**, and one of them now has a
cause recorded in the committed artifact rather than inferred. Every tier's
`O5_conflicts` failure carries the same failed assertion:

> `returned but failed qualifiers: claim_kind=observed, expected inferred`

The models return the root-cause attribution as `observed`; the corpus
demands `inferred`. That is a **contract-vocabulary disagreement, not a
retrieval failure** — and because it is identical at all five tiers, it is
better explained by our definition than by model quality. It is the
cheapest lead in this document, and §6 asks whether the corpus is the thing
that is wrong.

**The prompt-injection result.** `O5_conflicts_injected` passes at
`gpt-5.6-luna` only, in both runs. An earlier round reported it passing
everywhere except deployed parity; **that result is withdrawn** — it came
from an oracle strictly laxer than its own clean twin (no `INFERRED`
requirement, and a single hardcoded fake-approval exclusion that any other
fake id walked around). Under the corrected oracle the honest result is
1/5, and the direction of the original claim was close to inverted.

### 3.5 Latency, and the timeout machinery

Latency per extraction call, authored oracles only, run 6:

| Tier | Range |
|---|---|
| `gpt-5.6-luna` | ~2–7s (fastest measured) |
| `gpt-5-mini` | ~8–35s |
| `gpt-5-nano` | ~14–46s |
| `gemma-4-e4b` (local) | ~17–60s |
| `gemma-4-31b` (local) | ~70–1000s — expected for a local 31B model; see §8.1, this tier is a quality proxy and its latency is context, not a finding |

**On the timeout machinery, stated precisely.** An earlier run produced a
genuine live event: `O7_valid` on `gemma-4-31b` exceeded its configured
900s window, was classified as an infra `NOT_RUN`, and the single bounded
re-attempt recovered it. **Run 6 produced no timeout at all**, so in the
current artifact that path is **unit-verified and mutation-verified, not
field-observed.** The distinction is kept deliberately: a mechanism that
happens not to fire in the run you are reading is not the same as one
demonstrated in it. Per-attempt outcome, attempt count, timeout and
recovery are now persisted into the artifact itself (codex H2) rather than
surviving only in a run's stderr, so the next live event will be auditable
from the committed record.

### 3.6 Prior rounds, kept as history (runs 1 and 2 — `gpt-5-mini` only)

Retained for provenance and for the variance evidence §7 needs. **Neither
is the record any more**: `docs/measured-trial-results.md` now holds run 7,
and both earlier runs measured only `gpt-5-mini` — one tier above deployed
parity — against only 4 authored oracles.

| Run | Model | Class (a) | Class (b) | Class (c) | Notes |
|---|---|---|---|---|---|
| 1 (10:24Z) | gpt-5-mini | 0/3 NOT MEASURED | 2/2 | 1/15 | Contract-defective prompt (see below) |
| 2 (11:14Z) | gpt-5-mini | 0/3 NOT MEASURED | 2/2 | 2/15 | Fixed contract; was canonical until run 3 |
| 3 | nano/mini/e4b/31b | **0/3, comparable** | 2/2 all tiers | by tier | §3.1 |

Run 1 carried a prompt/filter contract defect (#1603 finding 1): the prompt
told the model to omit `recorded_at` for a same-day fact, which the `AS_OF`
observed-time filter would have read as "never observed". Run 1's class-(b)
2/2 worked only because the model did not follow that instruction. Run 2
re-earned every number under the fixed contract (`recorded_at` is now
requested whenever the text states one, same-day or not).

Class (b) has held 2/2 under both contract versions and at every tier of
every run since. The honest qualifier on that streak: the ATL-101/ATL-105
pair's two dates (07-02 vs 07-20) are far enough apart that the
same-day-omission bug never triggered on this specific pair — reassuring
for this pair, not a general guarantee the contract fix was inconsequential
(§7).

Class (a) in runs 1–2 is reported as NOT MEASURED, not as 0/3: no class-(a)
source prose existed, so the candidate was never asked. Run 3 is the first
round where that number means anything.

## 4. Closure-expressibility and the per-oracle record

Runs 1–2 logged only the aggregate `ComparisonReport`. That gap is closed:
per-oracle verdict, call timestamp, latency, attempt count, and
timeout/recovery are now written for every arm × oracle pair in every tier,
from a single `sweep()` call. §3.4's table is read off that record.

What the record shows about closure expressibility — emitting a `"closes"`
block naming which other extracted fact a new one ends, with a date drawn
from the source text:

- **`O3_supersession` passes at `gpt-5-mini`, `gemma-4-31b` and (in run 7)
  `gpt-5.6-luna`, and fails at `gpt-5-nano` and `gemma-4-e4b`.** It is the
  one reproducible capability ordering in the corpus — and even it moved
  for luna between runs, so "ladder" overstates it.
- **It is not stable within one tier.** `gpt-5.6-luna` FAILED
  `O3_supersession` in run 6 and PASSED it in run 7 — both committed
  artifacts, zero diff in oracles, ground truth, source documents or
  prompt between them. See §7; this is the trial's artifact-backed
  same-input non-determinism datum.
- **The step-2 local-model observation is superseded.** Step 2 recorded
  `O3_supersession` FAIL on `gemma-4-e4b`; run 6 reproduces that, and shows
  the same family at 31b passing. So the step-2 finding was about *that
  model*, not about extraction as a technique.
- **An earlier direction-REVERSAL defect** (ADR-021/ADR-014 extracted
  backwards) did not reproduce at any tier and remains superseded.

**Retrospective caveat on every pre-run-3 local-model observation.** All
were taken through a client whose request window was hardcoded and
cloud-sized (§9.1), and a genuine 900s-plus local call has since been
observed. Earlier local FAILs cannot be cleanly separated from timeouts and
should be read as *potentially understating* the local models. Run 6's
numbers do not carry that caveat.

## 5. Coverage: every oracle is now classified (9 authored / 11 not authorable)

**The corpus has no unclassified remainder.** Where earlier rounds had 4 of
20 oracles authored and 16 reporting a generic "not authored yet", the
authoring round closed that gap by deciding, for every one of the 20, either
to author source prose for it or to state why prose cannot express it. That
distinction is the point: "we have not gotten to this" and "there is nothing
to author here" are different facts about the trial, and only the second is
a statement about the technique.

**Authored — 9 oracles with real source prose (measurable by this arm):**

| Class | Oracles |
|---|---|
| (a) NATIVE_ANSWERABLE | `O7_valid`, `O7_unpinned`, `O7_null_valid_from` |
| (b) NEEDS_DECLARED_STATE_HISTORY | `O2_blocking_valid`, `O2_blocking_observed` |
| (c) NEEDS_EXTRACTION_OR_ASSOCIATION | `O3_supersession`, `O5_conflicts`, `O5_conflicts_injected`, `O6_recurring_pattern` |

This is what makes run 3 materially more informative than runs 1–2: **class
(a) now has authored material for the first time**, so the §15.2 control
("the baseline must win or tie on natively-answerable questions") can
actually be evaluated rather than reported as NOT MEASURED.

**Not authorable — 11 oracles, each with a stated reason.** All are class
(c). The reasons below are restated in prose for readability; the
authoritative machine-readable strings live in
`harness/arms/source_documents.py`'s `NOT_AUTHORABLE_REASONS` and are what
the generated artifact (`docs/measured-trial-results.md`) reproduces
verbatim — if the two ever disagree, the registry is right and this table
is stale.

| Oracle | Reason |
|---|---|
| `O1_ci_prior_attempts` | structured episode data has no natural prose form |
| `O4_prior_attempts` | structured episode data has no natural prose form |
| `O1_ci_prior_attempts_stale` | staleness is a projector-watermark concept; this arm has no equivalent — it always reads at call time |
| `O1_ci_prior_attempts_squash` | tests a declared PR/commit-linkage coverage gap — a property of the structured source, not of any document's content |
| `O3_supersession_extraction_down` | requires a self-declared coverage gap under a simulated provider outage — scenario detection, not prose content |
| `O3_supersession_deterministic_only` | requires a self-declared coverage gap under a simulated provider-policy-forbidden scenario — scenario detection, not prose content |
| `O4_prior_attempts_after_redaction` | redaction is a downstream deletion operation on already-extracted facts, not something source prose states |
| `O4_prior_attempts_after_revocation` | repo-visibility revocation is an authorization-scoped filter applied after extraction, not something source prose states |
| `O4_prior_attempts_graph_outage` | tests a graph-backend outage; this arm has no graph backend to go down — the scenario does not apply to its architecture |
| `O4_prior_attempts_manipulated` | needs `max_results` truncation logic this adapter does not implement (carried forward from step-2 scope) |
| `O5_conflicts_poisoned` | entity-linking poisoning is a distinct security dimension needing dedicated adversarial design; deferred |

**How to read the not-authorable set — and what may NOT be concluded from
it.** The registry now carries a category per oracle
(`NOT_AUTHORABLE_CATEGORIES`), because the flat list let all 11 be cited as
evidence about the technique when only some of them are. Totals:
**`structural` 7, `source_shape` 2, `deferred` 2.**

- **`structural` (7) — these ARE evidence.** The scenario tests a property
  an extraction arm architecturally cannot have (`O4_prior_attempts_graph_outage`
  needs a graph backend to fail; the two `O3_supersession_*` degradation
  oracles need self-declared coverage gaps under simulated provider
  conditions; `O1_ci_prior_attempts_stale` needs a projector watermark),
  or it lives downstream of extraction entirely
  (`O4_prior_attempts_after_redaction`, `_after_revocation`,
  `O1_ci_prior_attempts_squash`). The honest reading is: **extraction alone
  does not answer these, and whatever adopts it still has to.**
- **`source_shape` (2) — measurement artefact, not a finding.**
  `O1_ci_prior_attempts` and `O4_prior_attempts` are structured episode
  data with no natural prose form; authoring it would measure the trial
  author's writing rather than the model's extraction.
- **`deferred` (2) — scope, and NOT citable as a limitation of the
  technique.** `O4_prior_attempts_manipulated` (needs `max_results`
  truncation the adapter does not implement) and `O5_conflicts_poisoned`
  (a distinct adversarial dimension needing dedicated design). A future
  round could close both.

The consequence for §15.2's weighting, stated plainly: class (c) is
weighted ×5 because it is the largest and most important class, and **11 of
its 15 oracles are unreachable by this arm — 7 for structural reasons, 4
for reasons that are about this trial rather than about extraction.** Any
reading of class (c) evidence in this ADR is a reading of **4 oracles**.

`O7_null_valid_from` is authored as its own class-(a) oracle and is never
folded into `O7_valid`'s assertions — CHAOS-3570 credit isolation, so no
arm can claim credit for a different table's still-open bug. It is a
deliberate expected-FAIL (open-interval semantics); a PASS there would be
the surprising result.

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

## 6. Open questions — what run 3 answered, and what it did not

Four of the earlier draft's six open questions are now answered by
measurement. They are kept here with their answers rather than deleted, so
a reader can see which conclusions rest on evidence and which still rest on
judgement.

**ANSWERED by run 3:**

1. ~~Is class (b)'s +2 delta durable, or an artifact of measuring one tier
   above parity?~~ **Durable across the tier range.** 2/2 at all five
   tiers, including deployed parity and a small local model (§3.3). The
   small-N caveat is unchanged and still real — two oracles is the entire
   class-(b) sample — but the *model-tier* worry is retired.
2. ~~How much does class-(a)/(c) source authoring cost, and is it worth
   doing before the ADR?~~ **Done, and it was worth it.** Class (a) is now
   comparable and produced the trial's cleanest negative (§3.2); class (c)
   authoring exposed the deployed-parity zero (§3.4). Neither would have
   been visible otherwise.
3. ~~Is class-(c) variance a capability ceiling, prompt sensitivity, or
   normal variance?~~ **Answered, and it is at least two of the three.**
   Across the two committed runs the frontier tier's margin over the mid
   tier is at most one oracle and is not stable — level in run 6, one ahead
   in run 7 (see §3.4's generated block for the current standings). An
   unstable ≤1-oracle margin is evidence against a stable, purchasable
   ceiling; it is NOT a claim that the tiers score the same. And the same
   tier flipped an oracle between two runs on identical inputs (§7), so
   genuine non-determinism is present as well.
4. ~~The `native_control_holds()` banner conflating "not measured" with
   "measured and lost".~~ **Fixed** — `ControlStatus` is now three states
   and renders distinctly.

**STILL OPEN — these need chris, not more measurement:**

5. **What does "adopt" mean operationally, given class (b)'s shape?** The
   class-(b) result is "extraction beats a baseline that has no valid-time
   concept at all" (CHAOS-3569). Is the relevant comparison
   extraction-versus-native-today, or extraction-versus-whatever
   CHAOS-3563/3564/3565's increments support once they grow bitemporal
   query paths — a moving target this trial does not control?
6. **Do Graphiti and direct-store arms need to exist before anything is
   decided?** Both remain unbuilt. Can findings about
   extraction-as-a-technique stand on their own, separately from which
   storage backend eventually hosts it?
7. **NEW — what follows from class (c) being framework-bound?** No tier
   exceeds 2 of 4, and the frontier buys at most one oracle over the mid
   tier — not stably (§3.4). The live options are accept, attack the
   contract, or cache (§8.3) — a product decision, not a measurement one.
9. **NEW — is the observed/inferred contract vocabulary right?** Models
   consistently mark root-cause attribution `observed` where the corpus
   requires `inferred` (§3.4). If the corpus is the one that is wrong, a
   headline class-(c) failure is partly an artefact of our own definition.
8. **NEW — does contract fidelity belong in this trial's scope at all?**
   §8.2's evidence came from outside this harness, and this corpus has no
   oracle that would catch it. If it matters to the decision, something has
   to measure it deliberately rather than by accident.

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

**Run-7 update — the drift evidence is now artifact-backed, and one
earlier version of this claim was not.**

First, a correction of method that matters more than the number. An earlier
draft cited a same-input flip observed between run 5 and run 6. **Run 5 was
never committed** — it was overwritten before it entered git — so the
record could not support the claim, and the claim was made anyway. That
observation is retained here only as an **unverified historical observation
by the operating lane**, explicitly not artifact-backed, and nothing in
this ADR rests on it. It would have been possible to re-run until the flip
reappeared; that was deliberately not done, because **a drift observation
you went looking for on purpose is not the same evidence as one that
happened while you were measuring something else.**

Second, the claim now stands on committed records:

> **`gpt-5.6-luna` FAILED `O3_supersession` in run 6 (`b0983d17e`) and
> PASSED it in run 7.** Both are committed artifacts. Between the two
> commits, `corpus/oracles.py`, `corpus/ground_truth.py`,
> `harness/arms/source_documents.py` and the extraction prompt have a
> **zero-line diff**, and both artifacts record prompt version
> `extraction.v2`. Same model, same question, same expectation, different
> answer.

Third, the same-tier instability is visible in the aggregate too: luna's
class-(c) total moved 1/4 → 2/4 across those runs while every other tier
held (§3.4).

Second-order derivation, retained: the earlier `gpt-5-mini` "reproduced run
2 exactly" claim IS derivable rather than assumed — at run 2's commit
(`7edd169f6`) `SOURCE_DOCUMENTS` held exactly four authored oracles of
which exactly **two were class (c)**, and run 2 scored class (c) = 2, so
both passed. The derivation belongs in the document, not in the author's
head.

**Two independent hazards for CHAOS-3500's definition:**

1. **Model identity must be part of any cache key or equivalence
   predicate.** Class-(c) results differ by tier (§3.4), so a rebuild after
   a model change would otherwise compare two different capabilities and
   call the difference drift. This rests on tier sensitivity, which is
   artifact-backed across two runs.
2. **A bounded-drift tolerance cannot assume drift only accumulates across
   version changes.** The luna flip above is same-model, same-prompt,
   same-oracle, on committed evidence.

Option A (cache the extraction) makes both moot by construction, at the
cost of freezing quality at cache time.

**What this ADR does NOT do:** pick between A and B. That is CHAOS-3500's
own deliverable, under its own "semantically equivalent" definition. This
ADR's job is to hand over evidence that the definition, whichever way it
goes, needs to account for same-input non-determinism — not just
version-to-version drift.

## 8. Model tiers: parity, ceiling, and the cost regime

### 8.1 Why this round is a matrix and the earlier ones were not

Runs 1 and 2 measured `gpt-5-mini`, which was the repo's own
`DEFAULT_MODEL_BY_PROVIDER["openai"]` — a defensible pick, but **not the
model Ask Dev actually runs.** Ask Dev's deployed configuration
(`ops/.env`'s `LLM_MODEL`) is `gpt-5-nano`. Those two runs therefore
measured the technique one tier ABOVE deployed parity, and any conclusion
drawn from them about what the product would do was, strictly, unearned.

Run 3 removes that confound by measuring the same corpus, the same prompt
version (`extraction.v2`), and the same oracles against the
explicitly named tiers below:

The `tier key` column is the identifier the generated artifact
(`docs/measured-trial-results.md`) keys its summary table and section
headings on — given here so a reader can cross-reference this ADR against
the artifact without guessing.

| Model | Tier key | Role | Why it is in the matrix |
|---|---|---|---|
| `gpt-5-nano` | `gpt-5-nano` | **DEPLOYED PARITY — primary scored tier** | The configured model. Parity claims must be read from here and nowhere else. |
| `gpt-5-mini` | `gpt-5-mini` | Ceiling / comparative | One tier up. Shows what the technique does when model quality is not the binding constraint, and keeps runs 1–2 comparable to this round. |
| `google/gemma-4-e4b` | `gemma-4-e4b-local` | Local cost floor | A small locally-hosted model — the cheapest regime a cost-driven architecture could operate in. Not parity; no parity claim rests on it. |
| `google/gemma-4-31b` | `gemma-4-31b-local` | **Flash-class quality proxy** — excluded as a deployment candidate, and RETIRED from future runs, by the decision owner (see below) | Stands in for hosted mid-tier models (Gemini 3.5-flash / flash-lite, 3.6-flash) that this trial did not call directly. Its **answer quality** is the datum. Its measured latency is ~70–1000s per call (§3.5) — reported here as a fact. **Deployment candidacy is excluded by the decision owner (chris, 2026-08-08) as a stated constraint on latency grounds; that exclusion is his, not this document's.** The same decision owner has since **retired this tier from future runs** (chris, 2026-08-08), giving speed as part of the rationale and judging that it "adds little over e4b" — his words and his judgement, quoted rather than restated as a measured finding; the measured 31b-versus-e4b difference is in §3.4's generated block. Run 7's 31b column stays as measured history: nothing is deleted or re-run. The ADR itself recommends nothing, here or anywhere. |

**Reading the 31b row correctly matters**, because it is the row most
easily misread. It is in the matrix to answer *"what would flash-class
quality buy us?"* — not *"should we run gemma-31b?"* The answer to the
second question is no, and nothing below argues otherwise. Paired with
e4b it also separates *"small models cannot do this"* from *"local models
cannot do this"*, which are different facts.

A cost caveat belongs with that proxy, recorded here so a later reader does
not reconstruct an argument the evidence does not support: **the hosted
flash-class tiers this proxy stands for are not believed to be cheaper than
`gpt-5-nano` / `gpt-5.6-luna` / `gpt-5.4-mini`.** A provider switch argued
on cost grounds is therefore weak on its face. The 31b numbers are evidence
about a *quality class*, not about a cheaper bill.

The mechanical guarantee behind the labels: each tier's model is a literal
in `MODEL_TIERS` and is passed into the arm explicitly
(`extraction.make_answer(config)`). Nothing resolves a model from the
environment. This is not defensive style — before this round the arm
called the client with no config at all, so with `ops/.env` set to nano, a
run labelled `gpt-5-mini` would have been silently measured on nano. A
mislabelled result is worse than a missing one, because a reader stops
checking.

### 8.2 Contract fidelity: small local models violate the output contract

Separate from pass rates, and arguably more consequential for an
architecture decision: **the local model does not reliably honour the
response contract at all.**

Evidence, captured outside this harness during Ask Dev's own local-model
experiment (`/tmp/ask-dev-how-are-pipelines-running.json`, model
`google/gemma-4-e4b`, LM Studio). The model emitted a single `final_answer`
payload that is internally self-contradictory:

The `direct_summary` string, **verbatim** (one line, unwrapped — it is
quoted exactly so a reader can grep the source file for it):

> The repository saw 12 items completed in the current window, compared to a baseline of 10 items completed in the comparison window. One work item ('Implement contract baseline') is currently in progress.

And the payload structure it sits in. `arguments`, `call_id`, and `prompt`
are elided; `<SUMMARY>` marks a field whose value is **byte-identical** to
the string quoted above, verified by comparison rather than by eye:

```json
{
  "kind": "final_answer",
  "tool_id": "change_summary.v1",
  "value": {"direct_summary": "<SUMMARY>", "status": "insufficient_evidence"},
  "candidates": ["<SUMMARY>"],
  "code": "<SUMMARY>",
  "message": "<SUMMARY>"
}
```

Note the nesting: `status` sits *beside* `direct_summary` inside `value`,
while `candidates` / `code` / `message` are siblings of `value` itself.

The payload simultaneously asserts a confident, specific, quantified answer
(12 vs 10 completions, a named in-flight work item) **and** declares
`status: "insufficient_evidence"`. Those two fields cannot both be true.
The same answer string is additionally duplicated, byte-for-byte, into
three sibling fields (`candidates`, `code`, `message`) that have different
meanings in the schema — `code` in particular is being filled with prose.
The model is not populating a structured envelope so much as writing its
answer into every slot that will accept a string.

Why this matters more than a pass/fail count:

- **A status field is a control-plane signal, not decoration.** Any caller
  that gates on `status` — refusing to display, degrading, or asking a
  follow-up — will make the wrong call on this payload in one direction or
  the other. Trusting `status` discards a correct answer; trusting
  `direct_summary` displays a confident answer the model itself flagged as
  unsupported.
- **It is a failure mode the oracles in this trial would not catch.** This
  corpus measures extraction quality — did the right facts come out, with
  the right dates and provenance. It does not measure whether the model
  fills a structured envelope coherently. A tier could score respectably on
  this corpus and still be unusable behind a contract-dependent caller.
- **No equivalent contradiction was observed from `gpt-5-nano` or
  `gpt-5-mini`.** Stated as an observation, not a guarantee: this trial did
  not systematically fuzz contract adherence on any tier, so the honest
  claim is "not observed on the cloud tiers, observed on the local one",
  not "cloud tiers cannot do this."

**Input for the cost-architecture decision.** The cost case for a local
model rests on it being adequate at the job. This evidence says adequacy
has at least two independent dimensions — extraction quality *and* contract
fidelity — and that the local tier can fail the second while the first is
still being debated. A design that puts a small local model behind a
structured-output contract needs either a validating/repairing layer at
that boundary or a schema-constrained decoding mode; neither is free, and
both belong in the cost comparison rather than being assumed away.

### 8.3 What the tier matrix says about the cost regime

The matrix was built to answer a cost-architecture question: does the
capability this trial measures survive at the price points a cost-driven
design would actually choose? The measured answer separates into three
different shapes, and conflating them is the main way to misread this ADR.

**Capability that is tier-independent — class (b), 2/2 everywhere.** The
bitemporal axis-aware result held identically from `gemma-4-e4b` to
`gpt-5-mini`. This capability appears to come from the *contract* — asking
for `valid_from`/`valid_to`/`recorded_at` and filtering deterministically
over model-emitted dates — rather than from model quality. **For this
class, the cost regime is not a constraint at all.** That is a genuinely
useful thing to know: it is the one place where a cheap local model is not
a compromise.

**Capability that no tier buys — class (c).** Per-tier scores over the
authored class-(c) oracles are in §3.4 and are not restated here, so this
section cannot drift from them. The frontier tier exists precisely to tell
a model ceiling from a framework limit, and across the two committed runs
**its margin over the mid tier is at most one oracle and is not stable**
(§3.4). Reading:

- **No model purchase closes this gap.** Spending up to the frontier buys
  nothing on this corpus. The binding constraint is the extraction contract
  and harness.
- **The deployed-parity gap and the spread between best and worst are in
  §3.4's generated block; this section does not restate them.** What is
  worth saying here and cannot be read off a number: *which* oracle passes
  varies by tier and even between runs of the same tier (§4, §7), so the
  spread is not a stable ranking you can purchase against.
- **This is not a cost argument in either direction.** Per §8.1 the hosted
  flash-class tiers are not believed to undercut
  `gpt-5-nano`/`gpt-5.6-luna`/`gpt-5.4-mini`, and the frontier result says
  paying more buys nothing here anyway.
- **Two oracles fail at every price point** (`O5_conflicts`,
  `O6_recurring_pattern`), one of them for a legible contract-vocabulary
  reason (observed vs inferred, §3.4) that a prompt or contract change
  might move.

**Costs that are not priced per token**, applying to the genuine cost floor
(`gemma-4-e4b`):

- **Contract fidelity** — it is the tier that emitted the self-contradictory
  payload in §8.2. A small local model behind a structured-output contract
  needs a validating layer or schema-constrained decoding; neither is free.
- **Production cannot currently run the local regime at all** — every
  provider gets a 60s window (§9.2 / CHAOS-3608), shorter than several
  measured tiers need at their slowest. **The local regime has never been
  observed in production under a window that would let it finish.**

**The options this leaves, stated without a preference between them:**

1. **Accept class (c) as out of reach for now** and scope any capability
   claim to classes (a)/(b), where the tier question does not arise at all.
2. **Attack the framework, not the model** — the frontier result points
   here. The `O5_conflicts` failure is a contract-vocabulary disagreement
   (observed vs inferred), which is a prompt/contract experiment, not a
   procurement decision. Cheapest lead the data offers.
3. **Treat class (c) as an extraction-cache problem** (§7 Option A): a
   better or slower model runs once per fact rather than once per question,
   changing the shape of the quality/price trade — and requiring model
   identity in the cache key (§7).
4. **Do nothing on class (c) yet.** 11 of its 15 oracles are unreachable by
   this arm (§5), so the measured sample is 4, and 2 of those 4 fail
   everywhere. That may be too thin a base for any decision.

Two findings are independent of whichever option is chosen: **class (b)
needs no tier decision at all** (2/2 everywhere, §3.3), and **the
prompt-injection result is now 1/5, passing only at the frontier tier**
(§3.4) — which a purely cost- or quality-framed discussion would skip past.

**No recommendation is made among these.** Each trades a different
resource, and which resource is scarce is not a question this trial can
answer.

**Decision-owner position on future tiers (chris, 2026-08-08), recorded as
context rather than as a finding.** The Gemini API is believed to be
supported in the provider layer, so Gemini models could be measured
directly in a future round. The standing choice for now is a real frontier
model (`gpt-5.6-luna`) for the frontier tier, with `gemma-4-e4b` kept as
the local proxy for Gemini-Flash-class models. This is the decision owner's
stated position, not a measured result of this trial and not a
recommendation by it.

## 9. Measurement-integrity findings from run 3

Two defects were found in the measurement apparatus itself while wiring the
tier matrix. Both are recorded here because a reader assessing this ADR's
evidence needs to know what was wrong with the instrument, not only what
the instrument reported.

### 9.1 The trials client made a per-model timeout impossible (FIXED)

`trials/chaos_3499/harness/llm/client.py` constructed its SDK client as
`OpenAI(base_url=..., api_key=..., timeout=120.0, max_retries=0)` — the
window was a **literal at the construction site**, so no caller could give
a slow local model a longer one. Every tier necessarily shared a
cloud-sized window.

This matters beyond tidiness because of the direction in which it fails.
A local model that needs longer than the window does not return a wrong
answer; it returns no answer, and before this round that outcome was
reported through the same generic "could not reach" path as a dead
provider. **Any earlier local-model observation in this trial's history —
including the step-2 and step-3 gemma results reported in §4 and §7 — was
taken under a window that may have been too short for the workload, and
should be read as potentially understating the local model rather than as
a clean capability measurement.**

Fixed this round: `LLMConfig` carries a per-model `timeout` (`for_local()`
defaults to 900s, `for_cloud()` to 120s), the value reaches the SDK client,
and `APITimeoutError` is caught **before** `APIConnectionError`. That
ordering is load-bearing and easy to get wrong: `APITimeoutError` is a
*subclass* of `APIConnectionError`, so the natural ordering silently
collapses every timeout into the generic unreachable message and discards
the configured-window detail. An exceeded window now produces an
infra-marked `NOT_RUN` naming the window it exceeded — never a scored
`PASS` and never a scored `FAIL`, pinned end to end through
`oracle.evaluate`.

### 9.2 Production has the same defect, unfixed — CHAOS-3608

The production Ask Dev path carries the same class of hardcode, at a
*shorter* value, and it is **not** fixed by this changeset (`trials/` is
deliberately self-contained):

- `src/dev_health_ops/llm/providers/_http.py:10` and `:18` hardcode
  `timeout=60.0` in the two shared httpx client factories.
- Every provider uses them: `openai.py:564,579`, `anthropic.py:60`, and
  `local.py:163` — the LM Studio path.
- A sweep of `src/dev_health_ops/llm/` found no per-provider or per-model
  override anywhere; the other `timeout` occurrences are budget waiters,
  readiness probes, and agent step waiters, none of which set a transport
  window.
- `src/dev_health_ops/llm/errors.py:350` classifies transport timeouts into
  the timeout bucket, so a slow-but-correct local generation is recorded as
  a failure.

**Relevance to this ADR:** the deployed system gives a local model a 60s
window for a workload this trial measured as needing on the order of
minutes. Any operational impression that "the local model does not work
for Ask Dev" formed during the gemma cost experiment may be partly an
artifact of that window rather than of model capability. This does not
change any number reported above — the trial's own tiers ran under the
corrected client — but it does bear directly on §8's cost-regime
discussion, because it means the cost regime has never actually been
observed under a fair window in production. Filed as **CHAOS-3608** (High,
standalone, related to CHAOS-3499) with the fix framed as a per-provider or
per-model window, explicitly NOT a blanket raise: a cloud call hanging for
900s is its own defect.


### 9.3 Codex adversarial round (2026-08-08): four more measurement defects

An adversarial review of the run-3 harness raised nine findings. Eight were
confirmed and fixed or actioned; one was partially refuted (§7's derivation).
The four that had actually corrupted, or could have corrupted, measurement:

- **Retry-shopping through a substring match.** The bounded re-attempt
  policy decided "is this infra?" by searching for the phrase `could not
  reach` inside the NOT_RUN reason — and a parse-failure reason embeds up
  to 500 characters of **model-controlled output**. A model emitting that
  phrase could buy itself a re-roll of a genuine quality failure. Now a
  typed `ArmResponse.infra_failure` field; nothing substring-matches.
- **Served-model identity was never verified.** The tier table is a claim
  about which model produced each row, and nothing checked it: the response
  was discarded and the row labelled with the *requested* id. A local
  server with a different model loaded would have had its answers silently
  attributed to the requested one. The client now reads back the served id,
  accepts a dated snapshot that extends the requested alias, and raises
  `ModelIdentityMismatch` otherwise — recorded as NOT_RUN, never a score.
  The reachability probe additionally requires the requested model to be in
  the endpoint's own listing.
- **The timeout event was not auditable.** Run 3's live timeout survived
  only as a stderr line; the committed artifact showed a plain FAIL with no
  indication a window had been exceeded or a re-attempt made. Attempt
  count, timeout and recovery are now persisted per call.
- **`0.00s` latencies for calls that never happened.** The record was
  written unconditionally, so oracles the arm returned on *before* any
  provider call rendered as `0.00s` — which reads as "answered instantly".
  Now `n/a`, from a real `provider_attempts == 0` signal.

**Two defects in the fixes themselves, both caught by verification rather
than review, and both worth recording because they are the same class of
error:**

1. A tightened expectation cited an evidence ref (`ev_inc503_rootcause`)
   that **does not exist** — invented while hand-authoring. The
   golden-response test caught it immediately.
2. The first "generic exclusion" was **vacuous**: `object=None` on a
   positionally-compared field matched nothing, so the guard read as
   present while catching neither the shape it named nor any other. Found
   only by planting a differently-shaped injected fact and watching it
   pass. Replaced with an explicit `ANY_OBJECT` sentinel — deliberately not
   `None`, so a *forgotten* field cannot silently become a wildcard.

### 9.4 A corpus defect that caps what any oracle here can assert — CHAOS-3612

While tightening the injection oracle, a pre-existing corpus inconsistency
surfaced: for the C14 case, **ground truth and the authored source
documents cite different evidence ids for the same facts.** Ground truth
carries `ev1_postmortem_503a` / `_503b`; the documents the arm actually
reads are `ev1_incident_503_comment` / `ev1_incident_503_second_review`.
The intersection is empty.

Any evidence-ref requirement on that case is therefore satisfiable by the
golden (built from ground truth) and **unsatisfiable by every possible
arm** (reading the documents). A model citing a real, correct document id
fails — and in the artifact that failure reads as a model-quality finding
when it is a corpus bug. This is the most expensive defect shape available
to this trial: it manufactures false negatives that look like evidence.

Filed as **CHAOS-3612**. A corpus-wide guard now prevents new instances
(`test_authored_oracles_are_satisfiable_by_an_ARM_shaped_response`): no
`must_include` expectation on an arm-measurable oracle may require an
evidence ref the authored documents cannot supply. The guard prevents
recurrence; it does not reconcile the existing mismatch.

**The general lesson, which applies beyond this corpus: a golden-passes
check is not sufficient.** The golden and the arm speak different evidence
vocabularies, so an oracle can pass its golden test while being impossible
to satisfy in the measurement that actually matters.


### 9.5 Codex confirmation round: evidence fidelity

A second adversarial pass, scoped to whether every claim in this document
is backed by the committed artifact, raised five findings. All five were
confirmed; none was refuted.

- **Model identity verification failed OPEN in four places.** Absent
  response metadata was treated as agreement; the probe accepted a provider
  that enumerated no models; the arm fell back to the requested tier name;
  and nothing about identity was persisted. All four now fail closed, and
  each tier's artifact section carries the ids the provider enumerated plus
  the ids it reported as served. Run 7's evidence: `gpt-5-nano` served
  `gpt-5-nano-2025-08-07`, `gpt-5-mini` served `gpt-5-mini-2025-08-07`, and
  the local tiers served exactly their requested ids. **Unverifiable is not
  the same as verified**, and the earlier code could not tell them apart.
- **`NOT_RUN` rows sat inside score denominators.** Class (c) rendered as
  `0/15` while 11 of those 15 were never run — which a reader takes for a
  measured 15-case result. Denominators are now measured-only, unmeasured
  counts sit beside the score, and a NOT-COMPARABLE row carries no signed
  delta.
- **The artifact was not re-renderable.** The markdown was the only output,
  so correcting a presentation defect required paying for a fresh sweep.
  That is a durability defect in its own right, and it is what makes stale
  numbers tempting. Raw records are now the source of truth.
- **A diagnosis in this document was not in the artifact.** The
  observed-vs-inferred finding (§3.4) came from an ad-hoc probe. Failed
  assertion ids and details are now persisted per row, and that claim is
  re-earned from run 7's committed records at all five tiers.
- **This document cited deleted evidence** (§7). Corrected by downgrading
  the claim, not by re-running until it reappeared.

**A related process failure, recorded because the ADR should not look
cleaner than the work was.** This lane reported capturing LM Studio server
logs as corroboration for local-tier identity. On audit, the two capture
files were **byte-identical** — not independent captures — and **no capture
was taken during the run that was committed.** A measurement that did not
happen was described as having happened. It changed no number here, because
identity evidence is now in-band and persisted, but the reporting was
wrong and is corrected in the record.


### 9.6 Closure on stale numeric prose

Stale numbers survived into this document three times: two manual sweeps
and one exact-match guard each missed instances that were live. Per the
house rule, the third occurrence gets a closure argument rather than a
fourth sweep.

**Why the earlier attempts failed.** The diagnosis was not carelessness in
any single pass; it was that all three attempts matched *phrases*:

1. **Digit versus word** — patterns looked for the word form; the document
   used the digit form.
2. **Line wrapping** — `grep` is line-based, so a phrase split across a
   newline could not match any single-line pattern however it was spelled.
3. **Exact variants** — a longer noun phrase matched none of the shorter
   patterns.

The exact strings that escaped, quoted here so the failure is legible
(these are illustrations, and the guards strip fenced blocks for precisely
this reason):

```text
all 4 tiers            <- digit form, word-only pattern
4-tier range           <- digit form, hyphenated
No tier\nexceeds 1/4    <- wrapped across a newline
four explicitly named tiers   <- variant of "four tiers"
flat with the mid tier        <- number-free comparative, all guards blind
the best available tier reaches 1/4   <- comparative, derivable fraction
```

A fourth list of phrases fails the same way, because the failure is the
method, not the list.

**What is enforced now.** Numeric claims are checked *structurally*, from
the records, in
[`tests/test_adr_claims_match_records.py`](../tests/test_adr_claims_match_records.py):

- All comparisons run on **normalised text** (lowercased, whitespace
  collapsed), which removes case and wrapping as failure modes entirely.
- **Tier counts** are extracted by pattern in any spelling — digit or word,
  hyphenated or spaced — and must equal the count declared in the records.
- **Every fraction** in the document must be derivable from the records, or
  be listed in a small `_HISTORICAL_FRACTIONS` table with a stated reason.
  An undeclared historical number is indistinguishable from a stale one.
- **Superlatives** ("no tier exceeds N of M") are checked against the
  measured maximum. This one is in the list because the first version of
  this closure MISSED it: it is not a tier count, `1/4` was a derivable
  fraction, and no tier name sat beside it, so all three guards passed a
  false claim about the maximum. It was found by probing the document
  independently afterwards — which is why the closure below is stated as a
  class, and why the probe is worth repeating rather than trusting that the
  guards are complete.
- **Tier↔score pairings** are checked, because the set-based fraction check
  is not sufficient on its own: §8.3 paired the frontier tier with the mid
  tier's score after the frontier had moved, and that score is a legitimate
  fraction elsewhere in the document — so a set-based guard blesses it.
  What was stale was the pairing, not the number.
- The **artifact** carries no hand-written numbers at all: it is rendered
  from the records, byte-equality is pinned, and re-rendering **refuses**
  if the corpus hashes have moved.

**Round four, and the closure commit itself carried two instances.** The
guards above went green while the document still carried these two claims:

```text
the best available tier reaches 1/4      <- comparative around a fraction
                                            that is another tier's, so the
                                            set-based guard blessed it
the frontier tier scores the same as     <- NO DIGITS AT ALL, so every
the mid tier                                numeric guard was blind, and it
                                            flipped a conclusion the
                                            decision owner acts on
```

Neither was caught by the round-three guards. Two further stale statements survived alongside them: one naming the wrong
run as the artifact's contents, and one overstating which files were
unchanged between two commits (§3.4 now states that comparison in the same
accurate form §7 uses).

The response was to stop guarding phrasings and change what the document
is allowed to contain:

- **Comparatives are GENERATED.** §3.4 carries a
  `GENERATED:comparative-facts` block — best and worst tier score, the
  spread, and the full per-tier ranking — rendered from the records, with a
  test that it matches a fresh render.
- **Hand-written comparatives are scanned for and rejected — by a literal
  marker list, which is exactly the method this section calls broken.**
  Stated precisely, because overstating it is how the last three rounds
  ended: the test lowercases the prose, removes the generated block and
  fenced illustrations, and fails on any of a fixed list of comparative
  strings (superlatives, attainment verbs, equality and ordering
  phrasings). A comparative that NAMES its run is allowed through and
  checked instead against both runs' artifacts, so cross-run statements are
  verified rather than exempted.

  **The residual, named honestly: a number-free comparative phrased outside
  that list still slips through.** This is not hypothetical — the live
  example is the one this round caught, a claim that an oracle's pass/fail
  grouping was "reproducible across runs" when the frontier tier sits on
  opposite sides of it in the two runs. No numeric guard could see it, and
  no marker matched it. Full semantic comparative detection was considered
  and deliberately not attempted: it has no floor. The mitigation that
  actually works is the generated block — a claim that is *generated*
  cannot go stale, and prose that points instead of characterising has
  nothing to get wrong.

**Round five, and the reviewing probe missed it once too.** The
"reproducible across runs" claim predated round four and survived a probe
that was looking for exactly this class. That is worth recording for the
same reason the rest of this section is: the guard set here was assembled by
repeatedly being wrong, not derived, and the next reader should treat it as
evidence of what has been caught rather than proof of what cannot happen.
Widened this round: a preposition variant of an ordering phrase, and the
tier↔score pairing check, which previously allowed only punctuation between
a tier name and its number and so could not see "`<tier>` scores N/4".

**The class that is now impossible:** a tier count, a class-score fraction,
a tier↔score pairing, a superlative, or ANY comparative claim about tier
performance cannot sit in this document contradicting the records — in any
spelling, wrapping, or digit/word form, and whether or not it contains a
number — without a test failing. The number-free case is the one that
needed round four. What remains possible, and is deliberately not claimed to be
closed, is stale *qualitative* prose carrying no number (an example is in
the fenced block above) that
carries no number; §8.3 was fixed by deleting its restated figures and
pointing at §3.4 instead, so that section can no longer drift, but the
general case of prose adjectives is not machine-checked. That residual is
stated rather than hidden.
