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

**The hard gate is not green.** 16 of 44 CHAOS-3620 requirements are not proven (4 defect, 2 not_accepted, 10 unmeasured).

That sentence is **generated from the ledger**, not written beside it — the
one place gate status is stated, so a correct table under a wrong headline is
not expressible. Zero unauthorized leakage is measured and it holds; the
oracle that owns the measurement cannot yet run on this arm. Requirements
violated by merged code are listed below. Both facts are recorded as
assertions rather than prose, so neither can close quietly.

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
| `proven` | 28 | Holds, and the named tests are what establish it. |
| `defect` | 4 | Violated by merged code; the named tests pin current behaviour. |
| `not_accepted` | 2 | Blocked on another issue; never scored as a pass. |
| `unmeasured` | 10 | Not measured, with a stated reason rather than a proxy. |

Three of the ten `unmeasured` entries were `proven` before adversarial
review and were downgraded rather than defended: `A1` (the
cross-*repository* half is not constructible — the corpus plants no
repository near-duplicate), `O3` (four of the six named counts reach
telemetry; candidate and result counts do not) and `P7` (no corpus entity
has partly-restricted multi-source evidence, so the case cannot be built
here). Each downgrade names what *is* established, so the work is not lost —
only the claim is corrected.

`X5` went the other way. It was downgraded on the same pass, then **built**
on the orchestrator's ruling that the ADR cannot leave a core bullet open:
a flood world of 28 competing paths against a citation cap of 10, with the
fault shape planted — disabling the ordering displaces the required one-hop
path entirely, all ten slots taken by three-hop routes.

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

The measurement holds **at base SHA `1ab76d955`, pre-CHAOS-3627 vocabulary,
and must be re-derived after the rebase onto that fix.** `entity_sightings`
reads an evidence entry's `entity_id` as a sighting, and pre-fix that field
is an observation slug or measurement key on every slug-bearing entry — so
the attributions the measurement runs over are known-unsound, and the masking
direction (leaked evidence attributed to a permitted entity) is the dangerous
one. A source pin forces the re-derivation at rebase.

Within that scope: across every entity the analyst may see, no packet the arm
can produce discloses any entity outside the true grant, and the same code
path under a widened grant does leak.

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
authorization dimension. A release check whose oracle cannot return a clean verdict cannot be signed off.

### P4 — conflicts retain both source assertions (blocked by CHAOS-3612)

The packet's `conflicts` tuple is an empty literal
(`packet_builder.py:1220`), so nothing is retained and nothing is chosen
either. Acceptance is blocked on CHAOS-3612. The frozen contract *does* carry
the field, so CHAOS-3612 is the only blocker — asserted rather than assumed.

## Four defects in merged code

None were fixed in this lane; all are pinned so they cannot close silently.
**A9 is deliberately not in this table** — it is `not_accepted`, not a
defect, and listing it here contradicted the ledger until adversarial review
caught it.

| ID | Requirement | What is wrong |
| --- | --- | --- |
| X1 | prompt injection must not reach a consumer | Source-controlled **titles** arrive verbatim. The adapter copies an evidence record's `display_label` onto the observation (`corpus_adapter.py:210`), the emitter copies the title onto the packet's evidence entry (`packet_builder.py:829`), and nothing inspects title text. Document *bodies* are contained; titles are not, and the packet feeds Ask Dev synthesis. |
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

**If you also run the CHAOS-3617 harness, arm the live store first.** The
3627 lane found that `scripts/chaos_3617_guard_injection.py` needs

```bash
export CONTEXT_FABRIC_GRAPH_STORE_URI=falkor://127.0.0.1:6389
export CONTEXT_FABRIC_GRAPH_REQUIRE_LIVE=1
```

or its projection-flag mutation **SURVIVES via skip** — the tests that would
have caught it skip themselves for want of a store, the harness sees green,
and reports a guard as proven that was never exercised. The 3620 harness has
no live-store mutations and is unaffected, but the two are usually run
together and a survived-by-skip result is exactly the unearned green both
harnesses exist to prevent.

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

## Prompt injection: what is actually holding

The corpus's injected documents never reach a packet — and that result is
weaker than it looks, because **every corpus document is unapproved**. It
measures a property of the corpus, not of the arm.

The case that decides the question is one the corpus cannot produce, so the
suite builds it: an approved document carrying an injection. Its payload
still never reaches a packet — but **not because approval works**.
`projection.approved_documents` has **zero consumers** in `src/`: it is
written and never read. The structured path never reads document bodies at
all.

That makes containment strong now and fragile later. The moment an extraction
pass exists, approval becomes the *only* gate, and the unapproved-by-accident
property that currently masks everything disappears. A structural test goes
red the instant anything reads the approved set, which is exactly when the
approval gate stops being decorative and needs a proof of its own.

## Seam authority is not authorization

The shadow seam refuses a packet whose cited evidence is not in the native
frame's canonical set. That refusal is about **authority** — whether a
canonical service admitted the evidence — not **authorization** — whether the
caller may see it. Both produce a rejected packet and mean opposite things
about safety.

Once the graph arm cites source-issued world handles, the seam can reject a
packet citing *authentic* evidence the graph legitimately discovered and
canonical services never admitted to the frame. That is a measured
architectural fact — the seam contract has no admission path for
graph-discovered evidence — and it **does not count toward the
zero-unauthorized-leakage gate**. Counting it there would report an
architecture boundary as a security failure, which is how a real leak later
gets dismissed as "one of those".

The suite asserts the separation on a single packet: refused by the seam,
and clean to the entity-sighting audit at the same time. The trial reports
seam verdict and oracle evaluation as two independent columns; anything built
against the seam mirrors that.

## Inherited CHAOS-3617 invariants, and whether they transfer

Where a 3617 proof ran only on the synthetic `alpha`/`beta` fixtures — whose
authorized set is a hand-written tuple — "proven" does not automatically
transfer to the corpus world under real per-principal grants. Every 3617
result this lane leans on instead of re-proving carries an explicit
disposition in `INHERITED_INVARIANTS`, machine-checked like everything else.

| Invariant | Transfer |
| --- | --- |
| authorized traversal never routes through an unauthorized entity | `re_proven` |
| cross-tenant near-duplicates stay distinct and unreachable | `re_proven` |
| budget truncation is bounded and disclosed with a per-flag reason | `re_proven` |
| the watermark's freshness state reaches every consumer surface | `re_proven` |
| no caller-supplied partition; the partition is server-derived | `world_independent` |
| the arm registers no router, task, tool or telemetry surface | `world_independent` |
| a semantic match claim requires an attested semantic embedder | **`synthetic_only`** |

The last one is the honest exception. It is **not exercised on the corpus
world**, and the reason is structural: the corpus path runs the
`DeterministicEmbedder`, `ProjectionGraphReader` attests no embedder, and
every corpus subject resolves by `EXACT_CANONICAL_ID`. No semantic claim is
ever made, so the guard that refuses one is inert on this path. A test
observes both halves of that on a real readout, so if a later revision
resolves corpus subjects by similarity the disposition goes red rather than
staying quietly stale.

## The cross-runtime differential (D1) — spec of record

Deferred until CHAOS-3619's runner merges; axes A, B and C land as one
package. This is the approved design, recorded here so the spec is not a
message.

**Two of the five runtimes are negative legs.** ACR and acr-mcp carry no
graph surface today. The correct differential for them asserts that
graph-derived material is **absent** *and* that the corresponding native
material is **present** — otherwise two empty result sets compare equal and
report agreement, which is a green light meaning "neither runtime returned
anything".

**Three real axes.** A: `ProjectionGraphReader` vs `LiveGraphReader` over the
same batch (backend representation). B: native vs graph packet over the same
case (product) — needs the runner. C: packet → shadow record → frame facts
(presentation).

**Derive the comparison contract; never hand-list it.** Walk
`AskDevInvestigationPacket.model_fields` recursively and require every leaf
field to be classified `MUST_MATCH` / `MAY_DIFFER(reason)` / `DERIVED`, with a
totality test whose failure **names the field path** so classification is a
one-line fix rather than an investigation. A hand-written list of compared
fields is how a differential ends up covering three of them.

`MUST_MATCH`: committed subject ids; cohort member and exclusion ids; driver
ids, standings, roles, categories, assertion bases; lineage path endpoints and
`(source, relationship, direction, target)` hop triples; evidence **entity**
ids; authorization-filtered counts; outcome.

`MAY_DIFFER`, recorded: `packet_id`, `run_id`, `produced_at`, `path_id`
numbering. Evidence **handle values** differ today because the signer is
run-scoped — but CHAOS-3627 switches corpus-originated evidence to
source-issued world handles, so handles are **scheduled for promotion to
`MUST_MATCH`** in the same changeset that builds axis B (which is post-3627 by
construction).

**Every `MAY_DIFFER` that exists because of a missing capability must key on
the declared capability, never on reader identity.** `LiveGraphReader`
declares `observation_attachment_available=False`, so driver exclusion reasons
differ. Allowlisting "exclusion reasons may differ between readers" would
silently excuse real standing drift forever afterwards. Keying on
`readout.observation_attachment_available` makes the allowance evaporate the
moment the capability flips — and H3 is already implemented on the 3619
branch, so by axis-B time attachment will be `True` and that allowance should
never fire. If it does, that is drift.

**Compare model objects, not a JSON round-trip** (a round-trip collapses ints
to floats and equates large integers, and the packet carries measurement
values). **Build every case from the real producer**, never hand-authored
fixtures. **Keep an acceptance set** of known defects the comparator must keep
rediscovering — a reversed hop, a cohort member present in one arm only, a
driver standing downgraded, an evidence entity id dropped, a committed subject
swapped for the corpus's near-duplicate decoy. If one stops being found, the
change broke what made the tool worth building.

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
