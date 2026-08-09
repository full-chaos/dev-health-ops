---
page_id: con-ask-dev-graph-safety-proof
summary: The CHAOS-3620 release-blocking safety proof for the graph-assisted investigation arm — what was proved on the composed arm, the four defects it found in merged code, and the two requirements that cannot be accepted yet.
content_type: architecture
owner: engineering
source_of_truth:
  - tests/context_fabric/chaos_3620_spine.py
  - tests/context_fabric/chaos_3620_dispositions.py
  - tests/context_fabric/test_chaos_3620_authorization.py
  - tests/context_fabric/test_chaos_3620_adversarial.py
  - tests/context_fabric/test_chaos_3620_provenance.py
  - tests/context_fabric/test_chaos_3620_semantic_safety.py
  - tests/context_fabric/test_chaos_3620_guard_harness.py
  - tests/context_fabric/test_chaos_3620_dispositions.py
  - scripts/chaos_3620_guard_injection.py
applicability: current
lifecycle: active
---

# Graph-assisted investigation: the safety proof (CHAOS-3620)

CHAOS-3620 asks whether any product-value gain the corrected trial measures
is *safe* — authorized, evidence-closed, semantically bounded, and
observable. This page records what was proved, on what, and what could not
be proved. Its headline is not a pass.
{: .fc-page-lede }

**The hard gate is not green.** Zero unauthorized leakage is measured and it
holds; the oracle that owns the measurement cannot yet run on this arm. Four
of the issue's requirements are violated by merged code. Both facts are
recorded as assertions rather than prose, so neither can close quietly.

## What is new here: the composition

Every earlier lane tested a stage.

* CHAOS-3617's suite reads back over synthetic `alpha`/`beta` fixtures whose
  authorized set is a hand-written tuple.
* CHAOS-3616's oracles score a witness packet the corpus itself builds.

Neither had ever been composed. `tests/context_fabric/chaos_3620_spine.py`
runs the whole thing as one path, with nothing stubbed:

```text
world.PRINCIPALS[...].visible_entity_ids     the true per-principal grant
  -> corpus_adapter.corpus_batch(tenant)     the real ingestion adapter
  -> build_projection                        the real projection
  -> ProjectionGraphReader.neighbourhood     the real bounded traversal
  -> discover_drivers                        the real driver discovery
  -> build_packet                            the real frozen-contract emitter
  -> audit_authorization                     the corpus's INDEPENDENT oracle
```

Three properties of that path carry the whole proof.

**The grant is the world's, not the arm's.** The corpus plants a restricted
project, `proj_quarry`, *inside the caller's own tenant*. Every tenant
comparison, org-id check and partition assertion in the arm says it is fine
to return. Only a check that knows the true grant catches it.

**The graph contains what the grant excludes.** `corpus_batch` is
tenant-scoped, so `proj_quarry` is ingested, is a node, and has a real edge
to a team the analyst owns. Its absence from a packet is a *filtering*
result, never an absence of data.

**Every negative claim is paired with its fault shape.** A tenant-derived
grant — `seed_ids_for_tenant`, exactly what an arm authorizing by tenancy
would compute — puts `proj_quarry` into four packet locations, and the
independent oracle names it. The frozen contract accepts that same packet
without complaint, which is the whole reason the independent oracle exists.

## The disposition ledger is the artifact

`tests/context_fabric/chaos_3620_dispositions.py` holds one entry per
requirement bullet, transcribed verbatim from the issue, with a status, the
tests that establish it, and — for anything not proven — a reason and a named
Linear blocker.

| Status | Count | Meaning |
| --- | --- | --- |
| `proven` | 33 | Holds, and the named tests are what establish it. |
| `defect` | 3 | Violated by merged code; the named tests pin current behaviour. |
| `not_accepted` | 2 | Blocked on another issue; never scored as a pass. |
| `unmeasured` | 6 | Not measured, with a stated reason rather than a proxy. |

The ledger is machine-checked by
`tests/context_fabric/test_chaos_3620_dispositions.py`: every named test is
resolved by import, totality is enforced against the issue's own bullet list
in both directions, each entry must quote its requirement rather than
paraphrase it, every non-proven status must state a substantive reason, every
defect must cite file and line, and the two blocked entries are asserted by
id. `render()` produces the authorization/adversarial and
provenance/deletion/revocation reports the issue requires, from the same data
the tests check.

## The two requirements that cannot be accepted

### A9 — zero unauthorized result leakage (blocked by CHAOS-3627)

The measurement holds. Across every project and team the analyst may see, no
packet the arm can produce discloses any entity outside the true grant, and
the same code path under a widened grant does leak.

The gate still cannot be signed off. `audit_authorization` — the independent
oracle that owns this dimension — cannot return clean for **any** graph-arm
packet, because three id vocabularies do not overlap:

1. the arm mints evidence handles with the platform signer
   (`packet_builder.py:836`) where the world mints its own (`world.py:158`);
2. the declared authorized set is widened with observation ids
   (`packet_builder.py:890-892`) that the oracle reads as entity claims;
3. evidence entries carry an evidence slug in `entity_id`.

This is the CHAOS-3612 defect shape — two id vocabularies that never overlap,
making an expectation unsatisfiable by every possible arm — recurring in the
authorization dimension. A gate whose oracle cannot pass is not a green gate.

### P4 — conflicts retain both source assertions (blocked by CHAOS-3612)

The packet's `conflicts` tuple is an empty literal
(`packet_builder.py:1220`), so nothing is retained and nothing is chosen
either. Acceptance is blocked on CHAOS-3612. The frozen contract *does* carry
the field, so CHAOS-3612 is the only blocker — asserted rather than assumed.

## Four defects in merged code

None were fixed in this lane; all are pinned so they cannot close silently.

| ID | Requirement | What is wrong |
| --- | --- | --- |
| A9 | zero unauthorized result leakage | Three id vocabularies disagree, so the owning oracle is unusable. `packet_builder.py:836`, `:890-892`, `:828` vs `world.py:158`. |
| P6 | withdrawn sources disappear from packets | REVOKED and DELETED evidence reaches the emitted packet. Nothing in `context_fabric` reads evidence state; the adapter carries it as a display attribute (`corpus_adapter.py:218`) and no branch reads it back. |
| P1 | every driver **or relationship** closes to evidence | Drivers close, and the check is shown rejecting an arm-shaped bad response. Relationships never close: `_lineage_path` emits `evidence_ref_ids=()` as a literal (`packet_builder.py:632`). |
| S5 | current versus historical stays explicit | A relationship that ended two months before the trial instant is emitted with `relevance = current`. `relevance` is a literal at eight sites in `packet_builder.py` (542, 618, 630, 751, 799, 868, 935, 982) and nothing computes it. |

**P6 was masked by A9.** The check that would have caught withdrawn evidence
is the oracle's `withdrawn_evidence_handles`
(`investigation_corpus/authorization.py:278-279`). It is dead on every
graph-arm packet, because the handle lookup two lines above it misses and
`continue`s. One defect was hiding the other — which is why an inaccurate
coverage claim is worse than an admitted gap.

## Guard injection: 15 mutations, 15 killed

`scripts/chaos_3620_guard_injection.py` disables one guard at a time by exact
source substitution, runs only the tests that claim to cover it, and requires
them to fail **for the reason the guard exists**. Credit comes only from the
pytest *failure region* — `E `-prefixed lines and the anchored `FAILED`
summary — so a phrase echoed from a docstring, a comment or a passing
assertion cannot buy a kill.

There is exactly **one** region checker in this repository: the 3620 script
imports `failure_region` and `_reason_is_proven` from
`scripts/chaos_3617_guard_injection.py` rather than copying them. The rule
that decides what counts as evidence has already had two holes found by
adversarial review; a copy would be the thing that drifts.

The harness refuted three claims the suite was making before they were
trusted:

1. **"The emitter refuses a readout whose paths escape the declared grant."**
   Disabling that check does not let the packet out — the frozen contract's
   `validate_paths_stay_inside_authorized_set` refuses it instead. The test
   now claims what is true, and a second test exercises the contract layer
   independently, so the redundancy is known to work rather than assumed.
2. **"An unauthorized cohort peer is withheld."** SURVIVED. `build_cohort`
   authorizes at two sites and only one was mutated; worse, the subject under
   test was one the restricted project could never have been a member of. Now
   there is a subject where it genuinely *is* a member, a test for the
   exclusion-list disclosure channel, and a separate test and mutation per
   site.
3. **"The measurement-only category guard is load-bearing."** SURVIVED, and
   structurally: no structural rule in this revision produces a
   measurement-only category, so the guard's condition can never be true. The
   mutation is deliberately absent with that reason written where it was, and
   a test asserts the unreachability and goes red when it changes.

## Reproduction

### The proof suites

```bash
uv run pytest tests/context_fabric/test_chaos_3620_authorization.py \
              tests/context_fabric/test_chaos_3620_adversarial.py \
              tests/context_fabric/test_chaos_3620_provenance.py \
              tests/context_fabric/test_chaos_3620_semantic_safety.py \
              tests/context_fabric/test_chaos_3620_guard_harness.py \
              tests/context_fabric/test_chaos_3620_dispositions.py -q
```

No live store, no optional extra and no network are required: every proof
runs over the in-memory `ProjectionGraphReader`, which is the arm's own
reader rather than a mock. Backend-outage and malformed-response cases drive
`LiveGraphReader` with a fake driver at the seam it actually reaches
(`store._driver`), so they need no FalkorDB either.

### Guard-injection RED evidence

```bash
uv run python scripts/chaos_3620_guard_injection.py            # all 15
uv run python scripts/chaos_3620_guard_injection.py --only ID  # one
```

Expect `GUARD PROOF PASSED: 15/15 guards observed failing`, about 2m35s.

**Run it single-process, and never during the standing gate.** It edits files
under `src/` and restores them; the unit tier runs under pytest-xdist with
four workers, and a sibling worker importing a module mid-mutation would fail
for an unrelated reason — or pass while reading a disabled guard. That is why
the full run is not wired into the suite, and why every way the harness could
silently stop proving anything is instead asserted as a *static* property in
`test_chaos_3620_guard_harness.py`: anchors present exactly once, named tests
still resolving, tokens discriminating, no token hiding inside its own node
id, no two mutations sharing an anchor.

### The rendered disposition report

```bash
uv run python -c "import sys; sys.path.insert(0, '.'); \
from tests.context_fabric.chaos_3620_dispositions import render; print(render())"
```

## What this lane deliberately did not do

* **No cross-runtime differential.** The differential leg needs CHAOS-3619's
  trial runner. Building a parallel runner here was forbidden and would have
  produced a second definition of what a run *is* — the drift CHAOS-3396
  exists to prevent. Recorded as `D1`, `unmeasured`, blocker CHAOS-3619.
* **No fixes to merged code.** Every defect is reproduced, cited to file and
  line, pinned by a test, and reported. Fixing arm code inside the lane that
  audits it would leave nobody auditing the fix.
* **No edits to the frozen CHAOS-3616 corpus or the CHAOS-3615 contract.**
  Adversarial material this lane needed beyond what the corpus plants is
  built in probe worlds inside the test modules.

## Related pages

* [Graph-assisted investigation arm](graph-investigation-arm.md) — the arm
  under test.
* [Ask Dev investigation corpus](ask-dev-investigation-corpus.md) — the world,
  the grants and the independent oracles.
* [Ask Dev native baseline](ask-dev-native-baseline.md) — the shadow seam
  every packet is recorded through.
