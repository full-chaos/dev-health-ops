# Canonical evidence admission for graph-discovered evidence

**CHAOS-3646, design brief. Flag-off by default.** Sits beside
[the graph-assisted investigation arm](graph-investigation-arm.md) and
[the Ask Dev native baseline](ask-dev-native-baseline.md), which describe the
two things this brief joins.

## The measured fact this exists to answer

The CHAOS-3619 trial recorded a **canonical bypass** as an architectural fact,
not as a defect and not as an artifact of the trial's own wiring:

> The graph arm mints its own evidence refs. A post-3627 rejection means the
> graph cited authentic world evidence the native frame's canonical set does
> not contain — the seam contract cannot accept graph-DISCOVERED evidence
> until canonical services admit it to the frame.

Two independent columns, and the trial deliberately declined to recommend a
path across them. The consequence for CHAOS-3614 is narrow and total: the
trial proved the graph arm constructs valid investigation packets, and proved
nothing about whether an Ask Dev **answer** is better for them, because no
graph-discovered evidence can reach an answer at all.

`answer_usefulness_beyond_dashboard` therefore has no result through the real
frame — not a failing one, no result.

## What the boundary actually is, in code

Three checkpoints, each of which independently rejects a graph-discovered
handle today. All three are correct and none is relaxed by this design.

| # | Checkpoint | File | What it rejects |
|---|---|---|---|
| 1 | `canonical_bypass_offenders` | `api/dev/investigation_shadow.py:364` | a packet citing any handle whose **whole payload digest** is absent from `FinishedRunContext.canonical_evidence` |
| 2 | `validate_answer_candidate` | `api/dev/answer_validator.py:452` | an answer whose evidence is not an **identical object** in `_canonical_objects(tool_results)` |
| 3 | `validate_structural_closure` | `contracts_v2/validators.py:1226` | a frame fact citing a handle outside `frame.evidence` |

And the frame's evidence has exactly one source:

```
DevToolResult.evidence  →  Orchestrator._canonical_answer_data  (orchestrator.py:4305)
                        →  DevAnswer.evidence
                        →  wrap_legacy_answer_as_frame          (terminal_frames.py:787)
                        →  DevAnswerFrame.evidence
                        →  FinishedRunContext.canonical_evidence (orchestrator.py:3877)
```

Checkpoint 2 is the one that decides the shape of this design. It compares
**objects**, not handles, against what canonical *tool results* carried. So
"the handle enters the frame" cannot be implemented by writing into the frame.
The admitted ref has to be minted by the canonical evidence service and travel
the existing route. Nothing else is admissible, and nothing else needs to
change.

## The design

**One new call on the canonical service, and a graph-side candidate that
structurally cannot carry authority.**

```
graph traversal ──emits──▶ EvidenceCandidate      (a POINTER: no handle, no claim)
                                  │
                                  ▼
                    EvidenceService.admit(...)     ← the canonical service
                       1. entitlement
                       2. scope resolution, re-resolved PER CANDIDATE
                       3. resolve  → the SOURCE's own record
                       4. mint     → the existing _to_ref / signer.issue
                       5. authorize→ the existing _authorize_expansion
                                  │
                    ┌─────────────┴─────────────┐
                    ▼                           ▼
             DevEvidenceRef                 refusal
          (canonical, authorized)   (existing EvidenceAvailability
                    │                + existing warning strings)
                    ▼
          DevToolResult.evidence ──▶ frame ──▶ synthesis may cite it
```

### 1. `EvidenceCandidate` — discovery without authority

```python
@dataclass(frozen=True, slots=True)
class EvidenceCandidate:
    source_system: str
    entity_type: str
    entity_id: str
    locator: str                       # the SOURCE's own record identity
    repository_ids: tuple[str, ...] = ()
```

The type is the guarantee. There is **no** `evidence_ref_id` field, no
`display_label`, no `citation_text`, no `confidence`, no `observed_at`. A
discovery layer cannot hand across a handle it minted, or a claim about the
record, because the dataclass has nowhere to put one. Every field a reader
would take as canonical comes back from the source, not from the candidate.
This is asserted by a field-set test, not left to review.

`locator` is the source's record identity, distinct from `entity_id` (the
entity the record is *about*). That distinction is CHAOS-3633 restated: two
records of one kind about one entity are two records, and a candidate that
could not tell them apart would admit the wrong one.

### 2. `EvidenceCandidateResolver` — resolution stays with the source

```python
class EvidenceCandidateResolver(Protocol):
    source_system: str
    async def resolve(
        self, *, org_id: str, scope: ScopeResolution, candidate: EvidenceCandidate
    ) -> EvidenceRecord | None: ...
```

Added to `EvidenceService.__init__` as `candidate_resolvers: Sequence[...] = ()`.

The default is empty and that is the **structural** off-switch: with no
resolver configured, `admit()` can only refuse. No existing construction of
`EvidenceService` changes, and no deployed process can admit anything until
someone deliberately supplies a resolver. The feature flag is the second lock,
not the only one.

Resolution is `EvidenceRecord | None` — the source either has the record or it
does not. The graph's belief about it is never consulted.

### 3. `EvidenceService.admit()` — the admission call

```python
async def admit(
    self, *, org_id, permission_fingerprint, scope_request,
    candidates: Sequence[EvidenceCandidate],
) -> EvidenceAdmissionResult
```

Per candidate, in order:

1. `await self._entitlement.require(org_id)` — once, up front, as `search`
   and `expand` both do.
2. **Re-resolve scope for every candidate.** Copied deliberately from
   `expand`, including its comment: one denied locator must not inherit
   another's successful authorization decision.
3. **Resolve** through the resolver for `candidate.source_system`. No
   resolver → refuse. `None` → refuse.
4. **Mint** the ref with the existing `self._to_ref(...)`, over the **record
   the source returned**. Because the mint is over the returned record, a
   candidate that pointed at one record and got another back still yields a
   truthful handle: the handle describes what the source actually had.

   `valid_entity_ids` is minted as **the record's own entity**, and that one
   choice is what makes the next step real. `search` mints with the whole
   authorized set, which makes step 5's containment check compare a set to
   itself — harmless there, because an adapter is handed the scope and returns
   records within it. On the admission path the same tautology would leave the
   record's own entity unchecked, and a resolver bug would admit a record about
   an entity the principal cannot see. Narrowing the minted set turns the
   existing, unmodified check into the admission path's entity authorization,
   with no second authorization surface written anywhere.
5. **Authorize** with the existing `self._authorize_expansion(...)`,
   unmodified and uncopied. It runs the signature check, the
   `valid_entity_ids ⊆ allowed` check, and the separate repository-scope
   re-resolution with `allow_organization_fallback=False`. Anything other
   than `AVAILABLE` is a refusal and the ref is discarded.

Order matters and is deliberate: **mint then authorize**, so authorization
runs against the same object the caller would receive, through the same code
path expansion uses. Authorizing a candidate and then minting would authorize
a *pre-mint representation* and then hope the mint preserved it — a second,
parallel authorization surface, which is exactly the class of defect the
trial keeps finding.

### The property mint-then-authorize depends on

**The mint must leave no residue.** A refused candidate is minted and then
thrown away, so the ordering is only safe if minting creates nothing that
outlives the call — no registry entry, no store write, no counter, nothing a
later caller could present. Were that false, a refusal would still have
*created* something.

The production signer has the property: `EvidenceReferenceSigner.issue` is a
pure HMAC over `_payload` with no persistence of any kind, and `_to_ref`
around it only constructs a model. This is asserted on the object's own state
rather than read off the source
(`test_the_production_signer_mints_without_persisting_anything`), so a later
edit that adds a cache fails.

Recorded because the trial's own signer got it wrong first: an earlier
`CorpusEvidenceSigner` cached every issued payload so `verify` could compare
against it, which left a verifiable handle behind for every *refused*
candidate. It is now stateless, verifying as a pure function of the world.
The planted caching mint is kept as a test, and it immediately earned its
place — it exposed that the first version of the residue assertion used a
*shallow* state snapshot, which shares nested containers with the live object
and therefore could not fail.

### 4. Refusal uses the vocabulary that already exists

No new refusal words. Every refusal is an existing
`EvidenceAvailability` member with an existing warning string:

| Cause | State | Warning |
|---|---|---|
| no resolver for `source_system` | `UNCONFIGURED` | `source_unconfigured` |
| resolver raised | `UNAVAILABLE` | `source_unavailable` |
| source has no such record | `NO_MATCHES` | `evidence_deleted_or_unavailable` |
| scope, signature, entity or repository check failed | `UNAUTHORIZED` | `not_found` |

`not_found` for every authorization failure is the existing behaviour of
`_authorize_expansion` and is retained rather than "improved": a caller
learning *why* it was denied learns something about a tenant it cannot see.

### 5. The graph side

`context_fabric/graph_arm/admission.py`:

* `EVIDENCE_ADMISSION_FLAG = "CONTEXT_FABRIC_GRAPH_EVIDENCE_ADMISSION_ENABLED"`,
  default off, `== "1"` convention, registered in `tests/_env_isolation.py`'s
  scrub list and the `tests/context_fabric` autouse fixture.
* `candidates_from_readout(readout)` — pure, no I/O. One candidate per
  observation that describes its own evidence, keyed by the record's own id
  (`source_evidence_id`, falling back to the observation's canonical id).

`packet_builder.build_packet` gains one optional keyword,
`admitted_evidence: Mapping[str, DevEvidenceRefV2] | None = None`:

* `None` (the default, the flag-off path) — today's behaviour, byte for byte.
  `_mint_handle` still runs. Nothing about the merged trial changes.
* supplied — the evidence entry is the **admitted ref's own field values**,
  and `_mint_handle` is not called. A locator with no admitted ref is
  **dropped and disclosed**, never self-minted. This is where "the graph never
  mints authority" stops being a claim about intent and becomes a branch.

The entry must be the admitted ref *verbatim* because checkpoint 1 digests the
whole payload. That is a feature: if the arm alters any field of an admitted
record on its way into the packet, the seam rejects the packet. The check
still bites, and a planted single-field mutation is the test that proves it.

## What this design refuses to do

* **It does not relax any authorization check.** `_authorize_expansion` is
  called, not reimplemented, not parameterised, not bypassed for a "trusted"
  source.
* **It does not let the frame be written into.** Admitted refs travel the
  `DevToolResult` → `_canonical_answer_data` → frame route that every other
  canonical ref travels.
* **It does not feed the packet back to the seam.** `canonical_evidence`
  still comes from the frame, and the frame still comes from the canonical
  service, before the packet exists. Comparing the packet to itself remains
  impossible.
* **It does not widen the seam Protocol.** `InvestigationPacketProducer` is
  untouched.
* **It does not ship.** The flag is off, and with no resolver configured the
  call cannot succeed even with the flag on.

## What it deliberately leaves unmeasured

* **Production enablement.** Out of scope by instruction. No wiring in
  `production_runtime.py`, no new tool in the tool registry.
* **A canonical resolver for the real ClickHouse-backed sources.** The
  measurement supplies a resolver over the CHAOS-3616 corpus world — which is
  the same world both arms draw from, read through the canonical service's
  own path rather than through the graph's copy of it. Nothing here is
  evidence about how a `native_evidence` adapter would resolve a locator, and
  the records artifact says so.
* **The mint substitution the corpus forces — and its CHAOS-3633 lineage.**
  `world.evidence_handle(slug)` is the corpus's sole mint and the frozen
  authorization oracle audits cited handles against it, so the trial's
  canonical service signs with the world's mint rather than the platform
  HMAC. `world.py:158` already documents the substitution and why the corpus
  cannot key the platform HMAC.

  These are **one story, not two**. CHAOS-3633 is the platform-side defect:
  `EvidenceReferenceSigner._payload` identifies a record by `(org,
  source_system, source_version, entity_type, entity_id, repositories)`, and
  `entity_id` on the wire is the entity the evidence is *about* — so two
  distinct records of one kind about one entity mint the SAME handle. The
  graph arm already works around it on its own side
  (`packet_builder._mint_handle` signs over the record's canonical id, and
  says so), and the corpus works around it on the other side by deriving a
  handle from the slug. This lane's `EvidenceCandidate.locator` is the third
  appearance of the same distinction: the source's record identity is not the
  entity the record is about.

  A future reader should see that the corpus mint stops being a substitution
  when CHAOS-3633 lands — the ticket carries the removal steps, and this
  design needs no change when they are taken, because it never depends on
  *which* mint the service holds, only that the service holds it.
