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
| Alias / acronym / renamed-entity candidate search | Not yet |
| Cohort construction | Not yet — refused loudly, never faked |
| Driver synthesis | Not yet — the packet's outcome says so |
| Approved-unstructured extraction | Boundary in place, extraction not yet |

Because it synthesizes no drivers, `build_packet` **never** emits a supported
outcome. A packet with no asserted driver is, by the frozen contract's own
`validate_supported_outcome_asserts_a_judgment`, a redirect rather than an
answer; claiming `supported` for one would be exactly the
"dashboard redirect without a direct judgment" fault mode. The outcome is
derived from what was produced, not passed in, so the arm cannot over-claim
even by accident.

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
or ACR records into hand-authored prose. Three mechanisms enforce it rather
than describing it.

**The write path makes no model call.** `add_episode` runs extraction (even
for `EpisodeType.json`) and `add_triplet` calls the LLM for edge resolution.
Structured records instead go through
`graphiti_core.utils.bulk_utils.add_nodes_and_edges_bulk`, a direct driver
write, with a deterministic hash embedder supplying vectors. Projection is
therefore reproducible, offline and free.

**There is nowhere for a sentence to live.** Attribute values are bounded at
256 characters and attribute keys must be `snake_case` tokens.
`EntityNode.summary` — Graphiti's slot for a model-written regional
description — is always empty.

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

### The honest limitation of the deterministic embedder

`DeterministicEmbedder` derives vectors from BLAKE2b. It is reproducible and
needs no API key, and it carries **no semantic similarity whatsoever** —
nearest-neighbour search over it is meaningless. `semantic` is `False` on the
protocol so a future candidate-search path must consult it before claiming a
semantic match signal. Exact, alias, acronym, previous-name and
provider-identifier matching are all exact lookups and are unaffected.

## Identity, partitioning and authorization

**Canonical IDs are the identity; Graphiti mints nothing.** Node UUIDs are
`uuid5(namespace, org_id ‖ discriminator ‖ kind ‖ canonical_id)` — storage
addresses derived from the canonical id, never competing product identities.
Consequences: re-projection is idempotent; nothing downstream ever quotes a
graph-native identifier; and two organizations holding the same canonical id
get different addresses *arithmetically*, not by a filter that could be
forgotten.

**The partition is server-derived.** `identity.partition_for_org` is the only
source of partition strings, it takes the server-known organization id, and
every read re-derives and asserts it rather than trusting the one travelling
with the results. A caller never supplies a `group_id` and could not use one
if they did. The partition bounds *what was searched*; the authorized-entity
filter bounds *what may be returned*. Graph membership never grants access.

**Authorization applies to intermediate hops.** A path that merely routes
through a restricted entity still discloses that the entity exists and that
it links two things the caller can see, so the traversal never walks
*through* an unauthorized node. Filtering is counted and the count reaches
the packet. The packet builder then re-checks that every emitted hop endpoint
is an authorized *entity* — defence in depth against a readback bug or a
future second reader.

**There is no person entity kind, and adding one is a contract change.**
Contribution and membership are ingested as team-level association with an
aggregate `contributor_count`. That makes "no person-level ranking" a
property of what the graph *contains* rather than of what a ranking function
happens to do.

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
| Bounded rows, nodes, paths, bytes, time, output tokens | `budgets.py` |
| Every bound paired with the contract's `TruncationReason` | `budgets.py` |
| Indexed-through watermark, stale / partial / never-projected | `watermark.py` |
| Canonical writes never wait on graph indexing | `projection.py` is pure and synchronous; the store write happens strictly after |
| Deterministic cleanup | `store.purge_org` drops the keyspace |
| Org-deletion registration | `EXTERNAL_DERIVED_STORES` (CHAOS-3566) |
| Content-safe logs | `IndexWatermark.detail_for` — timestamps and counts only |
| Exact dependency/projection/query versions | `versions.*` on every packet; `backend.graphiti_version()` reads installed metadata |

A never-projected store reports `unavailable`, checked **before** staleness,
so an empty store can never read as "current with nothing in it".

## Running the trial store

```bash
# Bring up the isolated store (profile-gated; a bare `up` creates nothing).
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
export CONTEXT_FABRIC_GRAPH_REQUIRE_LIVE=1     # a skipped live test now FAILS
uv run pytest tests/context_fabric -q -p no:randomly
```

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

For each guard the arm relies on, the harness disables **that guard alone** by
an exact source substitution, runs the tests that claim to cover it, requires
them to FAIL, restores, and requires them to PASS again. Three rules it
follows:

1. a mutation whose anchor does not match verbatim (or matches more than
   once) is **INVALID**, not KILLED, and aborts the run — an anchor that
   silently stopped matching after a refactor would otherwise become a
   permanently green "the guard is proven" line;
2. the restore is verified by **re-running the tests**, never by a git check
   — a disabled guard still compiles and `git diff` calls a restored file
   clean whatever it contains;
3. **where the mutation died** is recorded, so a reader can check the failure
   is the one the guard exists to prevent rather than an unrelated collapse
   in setup.

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

The differential's scope is stated honestly in the test itself: both readers
share `_traverse`, so the comparison measures two **fetch** strategies rather
than two search algorithms, and observation-to-entity attachment is excluded
because `add_nodes_and_edges_bulk` writes entity edges only.

## Cross-repository ownership

This arm lives entirely in `dev-health-ops`. It reads the CHAOS-3615 contract
from `api/dev/investigation_contract/` and mints evidence handles with the
platform's own `EvidenceReferenceSigner`, so a packet handle verifies against
the service that issues it rather than against a parallel scheme. Nothing in
`dev-health-acr` or `dev-health-web` changes, and no contract is duplicated.
