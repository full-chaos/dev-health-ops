---
page_id: con-graph-investigation-arm
summary: The Graphiti-backed shadow investigation arm — structured-only ingestion, deterministic identity, the isolated FalkorDB trial store, and how the arm is verified.
content_type: architecture
owner: engineering
source_of_truth:
  - src/dev_health_ops/context_fabric/graph_arm/
  - tests/context_fabric/
  - scripts/chaos_3617_guard_injection.py
  - compose.yml (profile graph-trial)
applicability: current
lifecycle: active
---

# Graph-assisted investigation arm (CHAOS-3617)

<!-- The page slug avoids the string `context-fabric`: docs-data/ia's
     validator reserves every URL containing it for the canonical top-level
     marketing page at /context-fabric/. -->

The Graphiti-backed **shadow investigation arm** for the corrected CHAOS-3614
trial. It ingests canonical Dev Health and ACR structured records into an
isolated trial graph, reads a bounded, authorized neighbourhood back out, and
emits the frozen CHAOS-3615
[`ask_dev_investigation_packet.v1`](ask-dev-investigation-packet.md).

It is the first implementation in this epic that tests the **graph
hypothesis itself** rather than one-shot extraction reliability. It is not a
product feature, and nothing about it is on a user-visible path.

## What this revision does and does not do

| Capability | This revision |
| --- | --- |
| Structured ingestion of canonical records | Yes |
| Deterministic identity and partitioning | Yes |
| Bounded, authorized neighbourhood traversal | Yes |
| Related entities, lineage paths, evidence index | Yes |
| Packet emission through the canonical validator | Yes |
| Alias / acronym / renamed-entity candidate search | Yes — by lookup, never retrieval |
| Semantic retrieval | Seam + guard in place; search not yet |
| Cohort construction | Peer shapes yes; exhaustive shapes still refused |
| Driver synthesis | Structural yes — reaches a supported outcome |
| Canonical measurements | Cited, never computed |
| Approved-unstructured extraction | Boundary in place, extraction not yet |

The outcome is **derived from what was produced**, never passed in. A packet
with no asserted driver is, by the frozen contract's own
`validate_supported_outcome_asserts_a_judgment`, a redirect rather than an
answer — so for as long as the arm synthesized no drivers it could only emit
`unsupported`, and it did.

With structural drivers it now emits `supported_with_gaps` for
`proj_identity_rewrite`, credited to `drv_block_wu_authcore_release`, and the
packet revalidates through the canonical validator. Two things keep that
honest. The outcome is still derived rather than asserted, so a run that
finds nothing still says so — a control test builds the *same packet* with
drivers withheld and gets `unsupported` back. And the outcome tests name
**which** driver earned it, its standing, its category and its mechanism: a
supported outcome is a claim about a specific driver, and an assertion on the
enum alone stays green under driver substitution. A mutation that promotes a
different driver has to fail those tests, and does.

## Hard boundaries

- **Shadow-only.** Two flags, both default off and **independent**:
  `CONTEXT_FABRIC_GRAPH_PROJECTION_ENABLED` (may write) and
  `CONTEXT_FABRIC_GRAPH_READ_ENABLED` (may read). Independent so the arm can
  be brought up write-first with no read path reachable, and so switching
  reads off does not stop the projection and reset the watermark.
- **Graphiti is under evaluation, not approved.** It lives in the
  `context-graph-trial` optional extra, pinned exactly, and is imported
  lazily — never at module scope. An environment without the extra imports
  the whole package and runs the whole non-live suite.
- **No graph-native surface.** The arm registers no router, no Celery task,
  no Ask Dev tool and no MCP tool. There is no generic query executor:
  `readback.READ_ONLY_QUERIES` is the entire Cypher surface, and it is
  read-only. Graphiti's own MCP server is not exposed.
- **Removable.** Deleting `src/dev_health_ops/context_fabric/` breaks exactly
  two registration points: the optional extra in `pyproject.toml` and the
  `EXTERNAL_DERIVED_STORES` entry in `api/services/derived_store_registry.py`.
- **Telemetry off.** Graphiti's upstream default is *enabled* and posts to
  PostHog on init. `backend.require_graphiti` forces
  `GRAPHITI_TELEMETRY_ENABLED=false` unconditionally — including over an
  environment that said `true` — and the arm offers no way to turn it on.

## Backend choice: FalkorDB

graphiti-core 0.29.3 ships drivers for Neo4j, FalkorDB, FalkorDB-lite, Kuzu,
Neptune and Neo4j+OpenSearch. The choice was made against the installed
source and a real install probe, not from memory:

- **Kuzu** — the embedded option, and otherwise the most attractive for a
  trial (no container, trivially disposable). **Ruled out:** it has no wheel
  for this repository's Python 3.14 and its sdist build fails
  (`subprocess.CalledProcessError: Command '['make', 'clean']' returned
  non-zero exit status 2`). It is not installable here at all.
- **Neptune** — a managed AWS service. Not a local trial datastore.
- **Neo4j + OpenSearch** — two servers for a shadow trial.
- **Neo4j** — one server, but heavier than needed and with no per-tenant
  keyspace primitive.
- **FalkorDB** — chosen. One container; installs clean on Python 3.14
  (`graphiti-core[falkordb]`); first-class in Graphiti's own driver set and
  dev extra; and, decisively, **one graph keyspace per organization**, which
  turns org deletion into a keyspace drop rather than a traversal that could
  miss a node.

## Structured records stay structured

The issue and the corrective plan both forbid converting structured provider
or ACR records into hand-authored prose. The claim below is the **corrected,
narrower** one: an earlier draft of this note said "there is nowhere for a
sentence to live", and adversarial review demonstrated that false in four
places at once.

**What is true: the arm authors nothing.** Every textual value it stores is a
verbatim copy of a field the source record supplied, or a rejection. Nothing
formats, concatenates, templates or summarises. That is the property the
issue's rule actually needs — no adapter can "help" by writing a nice summary
of a structured record — and it is enforced by a compose guard that scans the
arm's own AST (`test_chaos_3617_no_authored_text.py`), splitting fields into
*source copies* (`display_label`, `title`, `outcome`, `name`, `matched_text`)
which may never be composed, and *arm disclosures* (`detail`, `impact`,
`inclusion_reason`, …) which the frozen contract requires the arm to write
and which may never interpolate source text.

**What is NOT true: prose cannot transit.** `display_label` and observation
`title` are source-supplied free text bounded only at 256 characters, so a
project whose real name is a sentence — or a review title containing a
person's name — is stored and carried into the packet as **untrusted
evidence**. Narrowing those to an identifier grammar would reject legitimate
provider labels like `fullchaos/auth-gateway` and `Nightfall Migration`, so
the claim is narrowed instead of the data.

Three mechanisms enforce the *authoring* half rather than describing it.

**The write path makes no model call.** `add_episode` runs extraction (even
for `EpisodeType.json`) and `add_triplet` calls the LLM for edge resolution.
Structured records instead go through
`graphiti_core.utils.bulk_utils.add_nodes_and_edges_bulk`, a direct driver
write, with a deterministic hash embedder supplying vectors. Projection is
therefore reproducible, offline and free.

**The structured attribute map is not a second free-text channel.**
Attribute values are bounded at 256 characters and attribute keys must be
`snake_case` tokens. `EntityNode.summary` — Graphiti's slot for a
model-written regional description — is always empty. (This heading used
to read "there is nowhere for a sentence to live", which the paragraph
sixteen lines above had already retracted. A heading a reader skims is
exactly where a withdrawn claim does its damage.)

**Edge facts are triples, not sentences.** `EntityEdge.fact` is a required
string whose intended value is natural language; it is precisely where a
well-meaning adapter would write "The Nightfall Migration project is owned by
the Platform team". The arm writes
`"<source_canonical_id> <relationship> <target_canonical_id>"` — three
tokens, none of them authored — and `parse_triple_fact` refuses anything else
on read, so a prose fact is detected rather than presented as evidence.

Unstructured documents travel a separate path
(`records.UnstructuredDocumentRecord`), are the only record type carrying a
body, and are dropped before extraction can see them unless `approved` is
true.

### Embedders, and the guard that keeps claims honest

Two embedders sit behind the `EmbeddingBackend` protocol.

`DeterministicEmbedder` derives vectors from BLAKE2b. It is reproducible,
offline and free, and it carries **no semantic similarity whatsoever** —
nearest-neighbour search over it returns a confident, arbitrary ordering.
`CloudEmbedder` wraps Graphiti's OpenAI embedder and is the one a retrieval
trial must use, or the trial measures a bare graph store rather than
Graphiti's actual value-add. It reads the repo's existing credential
convention (`LLM_API_KEY`, then `OPENAI_API_KEY`) and **refuses to fall back**
when no key is present: a run that silently used hash vectors would look
semantic in every artifact and score like noise, which is strictly worse
than failing.

The danger with the hash embedder is not that it fails — it is that it
*succeeds convincingly*. So the rule is a guard, not a doc note. The arm
tracks a `MatchMechanism` alongside each subject match (`EXACT_LOOKUP`,
`ALIAS_LOOKUP`, `LEXICAL_FUZZY`, `EMBEDDING_SIMILARITY`, `MODEL_INFERENCE`),
and `build_packet` **refuses** to emit a match whose mechanism needs
semantics — or whose signal is inherently semantic, like
`CONVERSATIONAL_REFERENCE` — while the active embedder reports
`semantic=False`. The mechanism never reaches the wire: the frozen contract
has no field for it and forbids extras, and it is an integrity concern about
how the arm produced a claim rather than something a consumer should branch
on.

The guard deliberately does *not* ban `FUZZY_LABEL` outright. Levenshtein
over stored labels is honest lexical work that needs no model; banning it
would push a future implementation toward mislabelling its own mechanism to
get past the check.

**`semantic=False` was necessary and was not sufficient.** `embedder` is an
*argument* to `build_packet`. `GraphArmStore` embeds at write time with
whatever embedder it was constructed with, and the packet is built later with
an unrelated object — so asking "does this embedder carry semantics" answered
a different question from the one that matters: *were the vectors this
readout was searched over produced by something semantic?* Adversarial review
turned that gap into three reproductions, and the fix is that the embedder's
identity travels with the **projection** rather than with the call:

- `to_graphiti_nodes` records the writing embedder's `model_id` on every node
  (`cf_projection_embedder`), and `LiveGraphReader` reads it back once per
  partition into `readout.embedder_model_id`. A semantic claim now needs
  **both** a semantic embedder and a partition attesting the vectors came
  from it;
- `CloudEmbedder()` with no API key used to report `semantic=True`. It
  refuses to *embed* without a key — but the guard never asks it to embed, it
  reads the flag — so a bare, unusable instance unlocked semantic claims.
  `semantic` is keyed on the key;
- a partition attesting **two** embedders raises rather than reading back
  whichever won: a mixture is not one projection.

A readout that attests nothing — the in-memory reader, which walks a
projection holding no vectors at all, or a partition written before the
attestation existed — still builds a packet and still stamps the only
embedder anyone offered. What it cannot do is carry a semantic match. That
costs no capability the arm has: `discovery._SIGNAL_MECHANISM` is exact and
lexical throughout, so the arm produces no semantic mechanism today and the
guard is a seam being held honest rather than a capability being restricted.

**The embedder is part of the projection's identity.** A store embedded with
one model is not the same projection as a store embedded with another, so the
embedder's `model_id` is folded into the emitted `projection_version` — the
frozen contract has no separate field for it, and that is where it belongs
anyway: a version that called those two runs the same would make
incomparable results look comparable. Because the stamp is derived from an
argument, a caller whose embedder disagrees with the partition's attestation
is refused (`EmbedderProvenanceMismatchError`) rather than silently
restamped: "I meant the other partition" and "I meant the other embedder" are
the caller's to resolve, and guessing would make a recorded run's provenance
depend on which one the builder picked.

**Embedding cost is bounded pre-flight.** `max_embedding_calls` is checked
before the first call, against the node+edge count the writer already knows,
so an over-budget run costs nothing rather than paying for most of the budget
and then stopping half-written. A non-semantic embedder makes no calls and is
not charged.

## Identity, partitioning and authorization

**Canonical IDs are the identity; Graphiti mints nothing.** Node UUIDs are
`uuid5(namespace, org_id ‖ discriminator ‖ kind ‖ canonical_id)` — storage
addresses derived from the canonical id, never competing product identities.
Consequences: re-projection is idempotent; nothing downstream ever quotes a
graph-native identifier; and two organizations holding the same canonical id
get different addresses *arithmetically*, not by a filter that could be
forgotten.

**The partition is server-derived, and derivation is injective.**
`identity.partition_for_org` is the only source of partition strings, it takes
the server-known organization id, and every read re-derives and asserts it
rather than trusting the one travelling with the results. A caller never
supplies a `group_id` and could not use one if they did.

Injective is load-bearing and was not free: the validator originally accepted
mixed case while derivation lowercased, so `Org_A` and `org_a` — both
accepted — derived the *same* partition and therefore shared one keyspace.
One organization's purge would have dropped the other's data. The fix narrows
what is accepted (lowercase only) rather than normalising it, because
normalising is precisely what collides them.

**The authorized-entity set is caller-declared and the arm does not verify
it.** Stated plainly because it is the security boundary: a caller that
includes a restricted entity receives it, and every downstream check only
proves the packet is internally consistent with a claim nobody validated.
`readback.derive_authorized_entity_ids` marks where the real derivation
belongs and raises rather than existing as a permissive stub. Until the
principal-grant adapter lands, correctness of the supplied set is scored
externally by CHAOS-3616's authorization oracle, which knows the true
per-principal grants. The partition bounds *what was searched*; this set
bounds *what may be returned*. Graph membership never grants access.

**Authorization applies to intermediate hops.** A path that merely routes
through a restricted entity still discloses that the entity exists and that
it links two things the caller can see, so the traversal never walks
*through* an unauthorized node. Filtering is counted and the count reaches
the packet. The packet builder then re-checks that every emitted hop endpoint
is an authorized *entity* — defence in depth against a readback bug or a
future second reader.

**There are no person entities, and no person-derived rankings.** Also
narrowed after review. No graph kind names an individual, so a person can
never be a node, a traversal endpoint, a cohort member or a ranked subject;
contribution and membership are team-level association with an aggregate
`contributor_count`. What is *not* claimed is that a person's name cannot
appear — it transits inside source-supplied titles and labels, as untrusted
evidence, and the corpus's `zero_person_level_ranking` oracle scores the
downstream behaviour this vocabulary alone cannot guarantee.

## Entities versus observations

`LineageHop`'s endpoints are typed `InvestigationSubjectKind`, so a node kind
the packet cannot name is a node kind that can never appear in a path.
Therefore:

- **Entities** are exactly the ten wire subject kinds, plus an
  `ORGANIZATION` partition root that never reaches the wire.
- **Observations** — reviews, CI outcomes, deployments, releases, incidents,
  status changes, decisions, documents and the ACR agent
  episode/task/artifact/outcome family — attach to entities and surface as
  `InvestigationEvidenceEntry` items. "Incident context association" means a
  path between entities whose evidence is an incident, not a hop that lands
  on an incident.

Decision supersession and ACR prior-attempt chains are ingested as explicit
`supersedes` / `prior_attempt_ids` links on observations, so "which decision
is current" and "what was tried before" are graph reads rather than
heuristics over timestamps.

## Operational controls

| Control | Where |
| --- | --- |
| Independent projection/read flags, default off | `flags.py` |
| Bounded rows, nodes, paths, bytes, time | `budgets.py`, applied in `projection.py`, `readback.py`, `packet_builder.py`. Every bound that removes work sets a flag and a reason; the ingest bound **refuses** rather than annotating |
| Every bound paired with the contract's `TruncationReason` | `budgets.py` |
| Indexed-through watermark, stale / partial / never-projected | `watermark.py` |
| Canonical writes never wait on graph indexing | `projection.py` is pure and synchronous; the store write happens strictly after |
| Deterministic cleanup | `store.purge_org` drops the keyspace |
| Read-only deletion preview | `store.partition_exists_for` — no driver, so no keyspace is created |
| Org-deletion registration | `EXTERNAL_DERIVED_STORES` (CHAOS-3566) |
| Content-safe logs | `IndexWatermark.detail_for` — timestamps and counts only |
| Exact dependency/projection/query versions | `versions.*` on every packet; `backend.graphiti_version()` reads installed metadata |

**Every bound that removes work discloses, and each flag carries its own
reason.** The trigger for hop-depth disclosure is "the walk declined to
expand a prefix that still had edges", not "the budget undercut the caller" —
the earlier form left the *default* read path silently truncating, because
the reader's default depth (3) sits below the budget ceiling (6) and nobody
chose either number. A walk that simply ran out of graph is complete and does
not flag. `entities_truncation_reason`, `paths_truncation_reason` and
`evidence_truncation_reason` are separate fields because a single shared one
reported whichever bound fired last, telling a consumer that partial lineage
was caused by the evidence budget.

**Values carrying a storage join byte are refused, not escaped.** Multi-value
attributes are joined with US (0x1f) for aliases and `,` for repository ids,
supersession and prior-attempt chains. A source alias containing US came back
from a real store as *two* aliases, one of which no source supplied — worse
than losing a value, because a later alias search would match a string nobody
wrote. Escaping would be a second encoding to keep in sync whose first drift
looks like data, so the refusal follows the same rule as organization ids.
The fixture world deliberately carries two aliases of one kind: with a single
value per kind the separator never appears in a stored attribute and this
whole class round trips invisibly.

`max_nodes_visited` counts *dequeued path prefixes*, not reached entities:
the traversal enumerates simple paths, so a dense neighbourhood can expand
for a long time while reaching nothing new. `max_wall_seconds` backstops it
for shapes a count cannot predict, and is tested through an injected clock
rather than a real sleep. A packet over `max_result_bytes` is **refused, not
trimmed** — the packet is a web of internal references the frozen contract
checks, so there is no field the builder could drop without either breaking
closure or silently changing what the arm claims to have found.

`max_output_tokens` is declared and **not enforced by anything in this
revision**, because the structured path makes no model call and there is no
model output to bound yet. That gap is pinned by a test rather than left to
be discovered: `TestOutputTokenBudget` asserts both that the checker works
and that no module in the arm references a model-calling entry point, so the
control cannot start reading as enforced without the test failing first.

A never-projected store reports `unavailable`, checked **before** staleness,
so an empty store can never read as "current with nothing in it".

### Two deletion behaviours worth knowing about

**Constructing a store creates the organization's keyspace.**
`FalkorDriver.__init__` schedules `build_indices_and_constraints()` as a
background task, so a dry-run deletion that constructed a store would create
an empty keyspace for every organization it previewed. `org_deletion_visit`
therefore checks existence through `partition_exists_for`, which opens a bare
FalkorDB client and never touches the Graphiti driver. The test asserts this
*structurally* (no store is constructed) rather than by looking for the
keyspace afterwards — the creation races `close()`'s cancellation, so a
"keyspace absent" assertion could pass with the defect present.

**Once the store is configured, an unverifiable deletion fails visibly.** A
missing graphiti-core does not make the data disappear, and an unreachable
endpoint is an unknown rather than an absence — both raise. `0` is returned
only after a positive existence check proved the partition absent.
Propagating does not block deletion:
`OrganizationDeletionService._purge_external_stores` catches, records
`"Derived store '…' deletion failed: …"` in `result.warnings`, and carries
on, so the choice is only between a *visible* incomplete deletion and an
invisible one.

**The one exception is a store that is not configured at all**, which returns
`0` with a logged warning. Adversarial review argued this should raise too,
and the full gate showed why it must not: raising made every org deletion in
every unconfigured environment record a warning about an optional trial
store, breaking the property CHAOS-3566's registry requires — that a
deployment without the trial store sees no behaviour change — and a warning
channel with a permanent entry is one nobody reads. The residual (a
deployment that once *had* the store configured) is carried in the log, and
its remedy is to point `CONTEXT_FABRIC_GRAPH_STORE_URI` at the store for the
deletion run.

## Running the trial store

```bash
# Bring up the isolated store (profile-gated; a bare `up` creates nothing).
# Compose interpolates EVERY service before it filters by profile, so this
# needs the repo's usual root .env present (e.g. BUGSINK_SECRET_KEY) even
# though none of those services start.
docker compose --profile graph-trial up -d graph-trial-store

export CONTEXT_FABRIC_GRAPH_STORE_URI=falkor://127.0.0.1:6389

# Install the optional extra.
uv sync --extra context-graph-trial
```

Port 6389, not 6379: `valkey` already owns 6379, and a trial store sharing a
port with the production cache would be the "isolated datastore" requirement
violated at the first hop. There is no named volume — the store holds only
derived, rebuildable projection data, so `down` genuinely removes it.

## Reproduction

### The full arm suite, with the live half required

```bash
export CONTEXT_FABRIC_GRAPH_STORE_URI=falkor://127.0.0.1:6389
export CONTEXT_FABRIC_GRAPH_REQUIRE_LIVE=1     # required: see below
uv run pytest tests/context_fabric -q -p no:randomly
```

**Both variables are required, and the second is not optional polish.**
`CONTEXT_FABRIC_GRAPH_STORE_URI` is a *conditional keep* in
`tests/_env_isolation.py` whose lane sentinel is
`CONTEXT_FABRIC_GRAPH_REQUIRE_LIVE`: the suite scrubs the URI unless the live
lane has announced itself. Setting the URI alone therefore yields the loud
skip rather than a live run — deliberately, so a URI that merely happened to
be in someone's shell can never turn the unit tier into an unannounced live
run that writes real projections.

`CONTEXT_FABRIC_GRAPH_REQUIRE_LIVE=1` is the load-bearing part. Every live
test routes its availability decision through
`tests/context_fabric/live_gate.require_live_store`, which skips with an
actionable reason by default and **fails** when this flag is set — so a
recorded reproduction cannot be a green suite in which no live assertion
executed. `test_chaos_3617_live_gate.py` tests the gate's own two branches,
and one test in it always runs and records what the environment offered.

### Guard-injection RED evidence

```bash
uv run python scripts/chaos_3617_guard_injection.py
```

For each of the **80** guards the arm relies on, the harness disables
**that guard alone** by
an exact source substitution, runs the tests that claim to cover it, requires
them to FAIL, restores, and requires them to PASS again. Three rules it
follows:

1. a mutation whose anchor does not match verbatim (or matches more than
   once) is **INVALID**, not KILLED, and aborts the run — an anchor that
   silently stopped matching after a refactor would otherwise become a
   permanently green "the guard is proven" line;
1. every mutation declares the failure it must produce, and a run that goes
   red for a *different* reason is **WRONG-REASON**, not KILLED. This was
   itself a review finding: disabling the prose-fact guard used to let `None`
   reach `match.group(...)`, so the test failed with `AttributeError` from a
   downstream dereference rather than because prose was accepted — and the
   harness said KILLED. Making the rule executable then caught a second
   over-claim of my own, where the packet builder's authorization re-check
   turned out to be backed by the frozen contract's own validator, so that
   mutation now claims only what it proves;
2. the restore is verified by **re-running the tests**, never by a git check
   — a disabled guard still compiles and `git diff` calls a restored file
   clean whatever it contains;
3. **where the mutation died** is recorded, so a reader can check the failure
   is the one the guard exists to prevent rather than an unrelated collapse
   in setup.

## What the readout carries, and why the list is closed

The traversal returns a **declared subset** of each node's structured
attributes (`READBACK_ATTRIBUTE_KEYS`) plus each edge's observed validity
interval. Both matter for driver work and neither was free:

- **Attributes** carry an observation's trust level and an entity's declared
  status. Without them the arm cannot tell a canonical work-item record from
  an untrusted note asserting the same link, which is exactly the poisoned-
  linkage case the corpus plants.
- **Edge validity** (`valid_from` / `valid_to`) is what distinguishes a
  dependency that *is* there from one that *was*. `PathStep.is_current_at`
  treats an absent interval as in force — most providers assert none, and
  reading silence as "expired" would erase most of a real graph — but never
  the reverse.

The readout also carries two **declarations about the reader itself**, which
are deliberately not traversal results: `observation_attachment_available`
(can this reader say what a record is about?) and `embedder_model_id` (what
does the store attest produced its vectors?). Both are excluded from the
differential oracle with written reasons — the two readers are *supposed* to
differ, and demanding agreement would demand that the live reader lie about
what it can do — and both are instead asserted directly against what that
reader actually returns, on the live store.

The attribute list is closed and declared rather than "whatever properties
the node has", because the live reader names its Cypher columns. Generating
those columns from the declared list was tried and **reverted inside the same
commit**: the containment guard reads the arm's entire Cypher surface out of
the AST's string constants, so an f-string query is not statically comparable
and the guard silently stops being able to see it. The queries stay literal;
`test_every_declared_attribute_has_a_column_in_both_queries` catches the
duplication drifting in either direction, and
`test_the_adapter_writes_no_attribute_the_readers_cannot_return` makes a
stored-but-unreadable attribute a build failure rather than a capability that
quietly returns nothing.

## Structural drivers, and what the graph is entitled to assert

The capability the correction hinges on. The native arm cannot assert a
driver at all; whether this one earns **principal standing** under the frozen
rules — a real cause, on a real path, with real evidence, currently relevant
— is the trial's live question. It does, structurally:

| Subject | Principal driver | Why it is one |
| --- | --- | --- |
| `proj_identity_rewrite` | `wu_authcore_release` | declared blocker, open, canonical CI + work-item records |
| `proj_ledger_migration` | `wu_ledger_backfill` | open child under a parent declared complete |
| `proj_pulse` | `wu_pulse_runbook` | release-incomplete, not implementation-incomplete |

None of that is a number. The governing rule is "the graph determines what is
relevant; canonical services determine what is measurable", so
`MEASUREMENT_ONLY_CATEGORIES` (cycle time, review load, capacity, investment
mix) are refused by name rather than approximated from graph shape — a test
asserts no structural rule ever emits one, which is a claim that can fail
today rather than a promise.

**Only a record about a linkage may vouch for it.** Support is scoped to the
asserting *edge* rather than to the cause entity — a canonical record on a
neighbouring edge must not vouch for a fabricated one — and scoping alone
turned out not to be enough. An edge that merely **cites** a canonical record
about neither of its endpoints inherited that record's trust, which put the
corpus's planted false dependency back at principal standing. Trust says a
record's own content can be relied on; it says nothing about an edge that
names it. So the cited records are split by what each may establish:

- **vouching** — trusted and about an endpoint. Only these make a linkage
  canonically asserted;
- **corroborating** — trusted and about a third entity. They ride along with
  a linkage something else vouched for and establish nothing alone. Kept
  rather than dropped because the corpus has real ones: the identity
  rewrite's blocker cites a CI run recorded against `repo_identity`, and
  deleting genuine evidence would trade a false claim for a false absence;
- **untrusted** and **withheld**, as before.

`LiveGraphReader` cannot recover observation attachment, so it cannot say
what any record is about. That capability is **declared on the readout**
(`observation_attachment_available`) rather than inferred from whether
attachments happen to be present — inferring it would make the endpoint rule
a silent no-op on exactly the reader that cannot perform it, which is the
original defect with a smaller blast radius. `discover_drivers` attributes
nothing on such a readout and says so in the exclusion's own words; it does
**not** report the support as withheld, which would be an authorization claim
about the caller's grant that nothing supports.

**`blocked_by` and `depends_on` are treated asymmetrically on purpose.**
`blocked_by` *is* the provider asserting that something blocks, so the far end
needs no status of its own. `depends_on` needs the far end declared open,
because what makes a dependency a *pressure* is that it is unfinished.
Collapsing the two either loses real blockers or makes every dependency a
driver, and the corpus has a case for each.

**Symptom versus driver is decided before standing.** An incident or a status
change observed on the subject is an effect, and a symptom can never hold
asserted standing — pinned by a whole-tenant sweep, because a per-subject
test passes happily while some other subject promotes one. A symptom whose
cause is also a candidate is excluded `SYMPTOM_OF_ANOTHER_CANDIDATE`; a
symptom with nothing explaining it stays a candidate, because deleting the
one observation a reader had would not be honesty.

### Exclusion reachability, stated exactly

Three of the frozen contract's six exclusion reasons are earned by real
corpus shapes: `EVIDENCE_CONFLICT_UNRESOLVED` (the planted false dependency),
`NOT_CURRENTLY_RELEVANT` (a dependency closed before the window), and
`SYMPTOM_OF_ANOTHER_CANDIDATE`. `UNAUTHORIZED_EVIDENCE` is reachable and
earned by a **constructed** world in the arm's own tests — the corpus belongs
to CHAOS-3616 and was not touched for it.

`NO_SUPPORTING_PATH` **cannot be produced by the structural rules at all, and
that is a positive property rather than a coverage gap.** Every candidate is
derived from a step on a discovered path, so a driver without lineage is
unconstructable — which is precisely what an arm without a graph cannot say
about its own output. It is asserted, not described: a sweep checks that
every attribution candidate in the tenant carries a path.
`INSUFFICIENT_MEASUREMENT` belongs to the measurement commit.

### Four defects, and why none of them were visible to a tool

Every defect found building this module passed the type checker, the linter
and the existing suite. All four showed up only when real corpus output was
printed:

1. **support scoped to the cause entity rather than the asserting edge** —
   `dep_authcore` is a genuine dependency of four real projects, so a
   canonical record on one of those *true* edges vouched for the *fabricated*
   one. This promoted the corpus's planted false claim to **principal
   driver**;
2. **child candidates taken from any `parent_of` step on any path** — a
   portfolio became an "open child" of a project it merely co-occurred with;
3. **`not _is_complete(...)`** — a service has no completion concept, so
   reading that silence as "unfinished" made every dependency a blocker;
4. **a trust lookup defaulting to `canonical`** — which is what kept (1)
   invisible.

Two of the fixes then **passed for the wrong reason** before being chased
down: the false claim and the historical dependency both vanished via the
status rule rather than via the trust and currency guards that own them. Both
now reach the guard that is supposed to reject them, and each has a test
asserting the candidate is *present and excluded* rather than absent.

**Orientation coverage, stated exactly rather than as "every family".** Of
the contract's twelve relationship types, four reach a role-deciding site:
`blocked_by` and `depends_on` through the blocking rule, `parent_of` and
`contributes_to` through the child rule. The other eight never reach
`_canonical_endpoints`, so they have no orientation to get backwards. Those
four are seeded from **both ends**. `parent_of` was the last to get that:
adversarial verification found its arm of the child rule mutation-survivable
— collapsing both branches onto the `contributes_to` reading passed the whole
suite, because the corpus reaches the child rule only through
`contributes_to` and the corpus-wide orientation sweep filtered to blocking
findings. Probes for the family and a sweep widened to the child rule close
it, and the collapse is now a mutation that dies from either seeding end.

The mutation harness caught two more of the same kind: a test naming the
wrong leaked identifier passed while its guard was disabled, and the
adjacency guard turned out to be redundant with the status rule on every
corpus shape — so its case is now constructed, isolating adjacency as the
only thing rejecting the candidate.

## Canonical measurements: cited, never computed

Struggling-teams and capacity are the families the real questions live in, so
leaving them out would have handed the ADR a **scope artifact dressed as a
capability result** — a softer rerun of the failure the correction exists to
fix. Measurements are in scope, in exactly one shape.

The arm ingests each `WORLD_MEASUREMENT` as an observation inside the trial
partition — authorized, deletable with the keyspace, citable through the same
evidence handle — carrying the value, unit, cohort median and originating
evidence slug **verbatim**. It reads two numbers a canonical service already
produced and compares them. It never adds, scales, divides or averages
anything: `31 against a cohort median of 14` cites two canonical numbers,
while `2.2× the median` would invent a third, and that third number is the
arm measuring.

That is enforced structurally rather than by inspecting outputs.
`TestTheArmPerformsNoArithmetic` bans every deriving operator outright in the
two modules a measurement passes through, so a derivation cannot be
reintroduced through a local variable either. A name-based scan was tried
first and fired on the hash embedder's vector normalisation while still
missing anything assigned to a local — scope plus operator is both narrower
and stronger.

**A cited measurement is capped at `CANDIDATE_ONLY` and can never become the
judgment.** A number being high is a correlate, not a cause. This is the
sharpest form of "the graph determines what is relevant; canonical services
determine what is measurable": measurements enrich the packet, and the
judgment still has to come from structure. `StandingMechanism` keeps the two
tellable apart so CHAOS-3619 can report per family.

Three corpus cases pin the behaviour:

| Case | Result |
| --- | --- |
| `team_atlas` | five metrics cited with a `MEASURED` basis, each through the handle the world issued for the record that evidences it (CHAOS-3627 — two metrics evidenced by one record cite one handle, because they are one piece of evidence) |
| `proj_solstice` | demand measurable, no cohort comparison → `INSUFFICIENT_MEASUREMENT`, disclosed rather than dropped |
| `proj_tidal` | no measurement at all → nothing asserted in either direction |

`proj_tidal` is a **positive control**, not a gap: the confidence machinery is
only trustworthy if it produces silence where there is no evidence either way.

`proj_lattice` carries the person-level trap — eleven contributors ever, two
in window, with the corpus stating outright that the raw roster is the
misleading number. Aggregate counts are ingested and readable, because
dropping them would hide from a reader that the two differ by nine; what the
arm never builds is a driver *about* a count of people, since a claim whose
subject is a headcount is one inference away from naming them. The filter is
proved in isolation: the test gives a person metric a category so the person
filter is the only thing left refusing it.

### Two guards that turned out to be defence in depth

Both were caught by the harness reporting SURVIVED, and neither is claimed as
proven on its own:

- the person-metric filter overlaps the category map, which also rejects
  those metrics — so its test now patches a category in to isolate it;
- the `CONTEXTUAL_CORRELATE` role overlaps the lineage rule. A cited
  measurement carries no path, so a mislabelled one is refused for having no
  lineage even before its role is read. Both had to be disabled together
  before the fault appeared, which is what defence in depth looks like when
  it is real.

## Comparison cohorts, and the refusal that did not open

`cohort.build_cohort` answers "which subjects belong in this comparison, and
why" from edges already in the graph. Two relationship families, kept
deliberately separate:

- **anchor edges** (`owned_by_team`, `belongs_to_portfolio`,
  `contributes_to`) — subject → anchor ← peer. The shared team, portfolio or
  initiative is named in the member's rationale, so "shares an owning team"
  becomes "shares owning team `team_atlas`", which a reader can check;
- **peer edges** (`shares_dependency_with`) — the edge already *means* the
  two are comparable, so there is no anchor to name.

`depends_on` is in neither. A project depending on a database is *related* to
it, not a peer of it, and a cohort built from that comparison would compare a
project with its own dependency.

Splitting the two families was not tidiness. The first version read
`shares_dependency_with` as an anchor edge, which made the peer an *anchor* —
and anchors are excluded from their own cohort, so the single most obviously
comparable subject in the graph silently vanished from it. The CHAOS-3616
corpus caught it: `proj_beacon` was missing from `proj_identity_rewrite`'s
cohort while every test still passed, because no test had yet asserted it
should be there.

Three things hold this capability honest:

- **authorization is applied to the anchor as well as the peer.** A peer
  reached only through a team the caller cannot see is a peer whose
  membership discloses that team. Withheld subjects are counted, never named
  — including as *exclusions*, since naming a subject in order to say it was
  left out still tells the caller it exists;
- **dimensions are derived from the members that survived the size bound.**
  A dimension whose only supporting member was truncated away is a comparison
  the packet cannot make;
- **the shape opening is partial and deliberately so.** `discovered_cohort`
  and `explicit_cohort` are "peers of a committed subject", which is what the
  builder derives. `portfolio_wide` and `organization_wide` assert an
  *exhaustive* enumeration this arm cannot prove it achieved, and a partial
  sweep presented as complete is a stronger false claim than refusing. They
  still raise `UnsupportedComparisonShapeError`.

A cohort that cannot compare raises `IncomparableCohortError` instead —
deliberately a different exception from the shape refusal, because "this arm
cannot do that kind of work" and "the arm did the work and the world has no
comparison here" must not score identically.

**A cohort does not make a packet an answer.** This is the easiest thing in
the packet to mistake for one: it is populated, structured, and looks like a
comparison was performed. `derive_outcome` evaluates the frozen contract's
own rule — a supported outcome needs a driver with asserted standing and a
non-empty evidence index — and this revision synthesizes no drivers, so every
cohort-bearing packet it emits is still `unsupported`. The function exists
rather than a constant precisely so both branches can be observed; a constant
would make "the arm cannot over-claim" unfalsifiable.

## What the differential oracle found

The traversal exists twice — an in-memory walk over the projection
(`ProjectionGraphReader`) and a Cypher fetch from FalkorDB
(`LiveGraphReader`). No type checker, linter or code index can tell you
whether two implementations of the same logic agree; only running both over
the same world and comparing can. The in-memory reader is therefore not a
mock, it is the **oracle** the live reader is measured against, and
`test_chaos_3617_live_store.py::TestReaderDifferential` is the comparison.

It immediately found a real defect that review had not. With
`max_paths_per_entity` capping how many chains an entity keeps, *which*
chains survived depended on the order edges happened to arrive in: the
in-memory reader walked projection order, FalkorDB returned rows in its own.
Same entities, same counts, **different explanations** for why an entity was
included — which would have made recorded trial runs irreproducible in
exactly the dimension the trial is scoring.

The fix is a total order on adjacency (`readback._ordered_edges`), sorted on
`(relationship, other id, direction)` — deliberately not on `observed_at`,
where a tie would put the order back at the mercy of row order.
`test_chaos_3617_determinism.py` is the regression, and it runs with no live
store: it shuffles the ingestion order and requires an identical traversal,
including with the per-entity cap set to 1 so the cap is actually exercised.

The comparator now derives its field set from `InvestigationReadout` itself,
so a new field is compared by default and every exclusion carries a written
reason — the standing differential-oracle rule. Broadening it that way
immediately found a **second** real defect: the live reader was not reading
aliases back at all, which would have surfaced later as the capability
revision's alias/acronym search finding everything in the reference and
nothing in the live store.

The scope that remains is stated honestly rather than implied: both readers
share `_traverse`, so the comparison measures two **fetch** strategies rather
than two search algorithms, and observation-to-entity attachment is a *known
gap* (not a permitted difference) because `add_nodes_and_edges_bulk` writes
entity edges only — a Graphiti evidence/readback defect can still pass this
differential while changing the packet.

## Cross-repository ownership

This arm lives entirely in `dev-health-ops`. It reads the CHAOS-3615 contract
from `api/dev/investigation_contract/`. Evidence handles are **carried, not
re-minted**: a record's handle is its identity, so where a source issues one
the arm cites exactly that (CHAOS-3627 — re-signing it made every citation
un-joinable to the record it names, and the CHAOS-3616 oracle reported all 31
on the reproduction path as fabricated). Where a source issues none, the
platform's own `EvidenceReferenceSigner` mints it, so that handle verifies
against the service that issues it rather than against a parallel scheme.
Nothing in `dev-health-acr` or `dev-health-web` changes, and no contract is
duplicated.
