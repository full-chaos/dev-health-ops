# CHAOS-3499 shadow harness — design and bring-up plan

**Status: design only. Nothing in this document has been brought up.**
The acceptance stack and dev compose are single-flight and shared across
worktrees; every environment step below is gated on an explicit slot granted
by the orchestrator. See §7.

Grounding for every infrastructure claim: [`baseline-inventory.md`](baseline-inventory.md).

---

## 1. Shape

Shadow-only, per PRD §17. Two switches, independently operable, both default
off:

```
                     canonical writes  (UNCHANGED)
                            |
  ops Postgres/ClickHouse ──┼──────────────────────────────► existing readers
                            |
                            ├─ worker_job_outbox ─┐   [SWITCH 1: projection]
                            |                     v
  acr agent_episodes ───────┴──────────► Temporal Projector (shadow)
                                                  |
                                                  v
                                          isolated graph store
                                                  |
                            [SWITCH 2: query]      v
   trial runner ──► arm adapter ──► ArmResponse ──► oracle ──► per-class report
                                                  |
                                          (no path to any user-visible
                                           Ask Dev or ACR answer)
```

The trial runner is the *only* consumer. There is deliberately no wiring into
`context_for_task`, no `TEMPORAL_CONTEXT` source class, and no MCP tool. Phase
1 produces a report, not a product behaviour.

---

## 2. Why the projector reads an outbox that already exists

The baseline inventory found a production transactional outbox already in
place: `internal/joboutbox`, *"the generic Python-to-River transactional
bridge"* (`types.go:1`), with a producer that requires the caller's own
transaction so a domain transition and its handoff cannot commit
independently (`producer.go:19-21,22-51`), a claim/lease relay
(`relay.go:1-40`), and the documented property that
*"unknown or invalid kinds terminalize with bounded evidence rather than
disappearing"* (`docs/contribute/architecture/data-and-storage.md:58`).

That last property is the one that matters here. A shadow projector consuming
an unknown event kind must fail loudly and stay failed, not drop the event —
otherwise the indexing watermark advances past work never done, and every
downstream freshness number is a lie. The mechanism already behaves correctly;
building a second one would mean re-earning that property.

**Consequence for the ADR's ownership question (PRD §5):** ops-owned
projection is not merely the cheaper default on retention and provider-policy
grounds, it is where the event mechanism already lives.

**No CDC exists** (searched `cdc`, `debezium`, `logical replication`,
`wal2json` — not found in either repo), so outbox-or-poll is the real choice,
and poll is strictly worse for lag measurement.

---

## 3. Baseline versus candidate arms

Amended §14 restructured the native work as **pre-trial increments feeding a
native baseline** — not as a late-arriving competing entrant. The trial
therefore reports **baseline vs Graphiti arm vs direct-store arm**, and the
comparison maps 1:1 onto the amended PRD.

### 3.1 The baseline (what the graph must beat)

Two components, composed into one baseline before any candidate is scored:

| Component | What it is | Built by |
|---|---|---|
| **Native increments** | The four §14 pre-trial increments, queried through existing ops/ACR paths | CHAOS-3562/3563/3564/3565 — **other lanes, not this one** |
| **Episode readback** | Plain `agent_episodes` list-by-repo/task/file-overlap, zero graph infrastructure | CHAOS-3564 |

Composition rule (`runner.compose_baseline`), per oracle:

- **any component passes → the baseline passes.** The baseline is what the
  product can already do, and it can do it if any of its parts can.
- otherwise **any component unmeasured → the baseline is NOT MEASURED.**
  Recording FAIL here would assert the product cannot answer something nobody
  asked it, and a candidate would then be credited with beating a baseline
  that was never run.
- otherwise → FAIL.

Episode readback is a **baseline component, not a candidate**. That placement
is the single most important measurement decision in the trial:
`EpisodeArtifacts.FilesTouched/TestsRun` (`acr types.go:336-340`) is already
structured, so if plain readback answers Q4, the graph's margin there is
**zero**. Scoring readback as a peer entrant would let Graphiti be compared
against native alone and take credit for value ordinary readback already
delivers.

### 3.2 The candidate arms

| Arm | What it is | What it isolates |
|---|---|---|
| **Graphiti** | Graphiti over an approved backend, Dev Health authorization and provenance kept outside Graphiti | Cross-episode association: the actual claimed differentiator |
| **Direct store** | Dev Health-specific schema on the same backend, no Graphiti abstractions | Separates "a temporal graph helps" from "Graphiti helps" |

Each candidate produces one `ComparisonReport` against the composed baseline,
scored **per question class** as a delta. There is deliberately no flat league
table: ranking four peers side by side would let a candidate "win" a class by
placing above one baseline component while losing to the baseline as a whole,
which is exactly the comparison the ADR must not make. `ComparisonReport`
exposes no single headline number, and a test asserts it never grows one —
with this question set weighted (a)×1 (b)×1 (c)×5, one number would flatter
any extraction-capable candidate regardless of merit.

### 3.3 Two invariants the report enforces itself

**Class (a) control.** Per §15.2 the baseline must win or tie on
natively-answerable questions. `ComparisonReport.native_control_holds()`
checks it, and `render()` shouts when it fails: *"treat every other row in
this report as unexplained until this is resolved"*. A candidate outscoring
the baseline on class (a) has almost certainly been handed an advantage the
baseline did not get.

**Class (b) dependency state.** Class (b) results are uninterpretable without
recording which state of **CHAOS-3563** they ran against — "the baseline
scored 0 on class (b)" means something entirely different before and after
declared-state retention lands. `DependencyState` carries that, defaults to
`UNRECORDED_DEPENDENCY`, and an unrecorded class (b) renders **NOT
COMPARABLE** rather than emitting a number whose meaning the report does not
carry.

CHAOS-3563 is in flight in lane-ops-pretrial. Its branch state is obtained
**through the orchestrator**, not by reading that lane's worktree — an
uncommitted working tree is not a state anyone can cite in an ADR.

### 3.4 Registration is total

An arm that cannot run must still be registered
(`ArmRegistry.register_unavailable`) so it appears as an unmeasured row rather
than a column nobody notices is missing. Each arm ships exactly one adapter,
`Arm(oracle) -> ArmResponse` — the only arm-specific code an oracle ever sees,
which is what makes one oracle comparable across the baseline and both
candidates.

## 4. What the harness already enforces (built, green, mutation-checked)

| Property | Where |
|---|---|
| An unmeasured oracle can never read as a pass | `Verdict.NOT_MEASURED`, `runner.run_oracle` converts a raising arm into `NOT_RUN` rather than aborting the sweep |
| Aggregate scores carry the per-class breakdown, and are marked NOT COMPARABLE when anything went unmeasured | `runner.TrialReport.render` |
| Every as-of oracle pins its axis | `TemporalContextQuery.__post_init__`, plus `test_every_as_of_oracle_pins_its_axis` |
| An oracle that cannot fail cannot be constructed | `Oracle.__post_init__` |
| Direction is compared positionally; a reversed edge is reported as a direction failure, not a near miss | `FactExpectation.identity_matches`, `test_direction_reversal_is_not_a_near_miss` |
| Every observed fact must close to provenance — on every oracle, not opt-in | `Oracle._assert_provenance_closure` |
| A closed validity window with no `invalidated_by` fails (PRD §6.3 endpoint laundering) | same |
| A coverage gap must be *declared*; silent emptiness fails | `Oracle._assert_coverage` |
| Every fault mode is caught by the assertion that claims to catch it | `test_fault_mode_is_caught_by_its_own_assertion` |
| No fault mode is inert across the whole corpus | `test_every_fault_mode_applies_to_at_least_one_oracle` |
| The baseline is composed from its components before any candidate is scored; candidate arms can never be folded into it | `runner.compose_baseline`, `runner.compare`, `test_both_baseline_components_are_registerable_as_baseline` |
| A baseline component that was never run degrades the baseline to NOT MEASURED, never to FAIL | `test_unmeasured_component_never_becomes_a_baseline_failure` (paired with a fully-measured control that must still record real FAILs) |
| Class (b) renders NOT COMPARABLE until CHAOS-3563's branch state is recorded | `DependencyState`, `UNRECORDED_DEPENDENCY`, `test_class_b_is_not_comparable_until_chaos_3563_state_is_recorded` |
| A class (a) control failure is shouted, not buried under a positive delta | `ComparisonReport.native_control_holds`, `test_class_a_control_failure_is_shouted_not_buried` |
| No single headline number is reachable from a comparison report | `test_report_never_emits_a_single_headline_number` |

**Mutation-verified**, not merely asserted. Three defects planted in
`harness/oracle.py`, each killed, each dying in the right guard:

| Planted defect | Verdict | Died in |
|---|---|---|
| `must_include` always passes | KILLED (63 failures) | `test_fault_mode_is_caught_by_its_own_assertion`, `test_direction_reversal_is_not_a_near_miss`, `test_invalidation_provenance_cannot_be_laundered`, `test_axis_pair_cannot_both_pass_with_one_answer` |
| `NOT_MEASURED` reads as success | KILLED (22 failures) | `test_measurement_never_ran_fails_every_oracle`, `test_not_measured_is_not_silently_equal_to_pass` |
| Direction ignored in identity match | KILLED (17 failures) | `test_direction_reversal_is_not_a_near_miss` |
| Unmeasured baseline component folds to FAIL | KILLED | `test_unmeasured_component_never_becomes_a_baseline_failure` |
| Class-(b) dependency state ignored in comparability | KILLED | `test_class_b_is_not_comparable_until_chaos_3563_state_is_recorded` |
| Class (a) control always reports "holds" | KILLED | `test_class_a_control_failure_is_shouted_not_buried` |
| Candidate arms folded into the baseline | KILLED | `test_compare_requires_at_least_one_baseline_component`, `test_class_a_control_failure_is_shouted_not_buried` |

A seventh mutation attempt was recorded **INVALID**, not killed: its anchor no
longer matched after `ruff format` rewrapped the line, so the mutated file was
never built. It was re-run against the correct anchor (the "candidate arms
folded into the baseline" row above). A mutation that does not apply proves
nothing and must never be counted as a kill.

The suite currently reports **217 passed, 136 skipped**. The skips are
fault×oracle pairs where the fault is genuinely inapplicable; they are
reported rather than hidden, and the inert-fault guard fails if any fault is
inapplicable *everywhere*.

---

## 5. Deployment design

### 5.1 Placement — `deploy/docker-compose/compose.temporal-trial.yml`

Not the root `compose.yml`. The inventory found root compose declares 13
always-on services with **no `profiles:` anywhere**; the opt-in precedent
lives in `deploy/docker-compose/compose.go-workers.yml:16,58,94,108`
(`profiles: [go-workers]`) and `compose.production.yml:400` (`pooler`), whose
header states operators must opt in explicitly. A new always-on stateful
service in root compose would start for every developer and every worktree on
the host.

Required shape, from the existing stateful-service pattern:

- `profiles: [temporal-trial]` on every service;
- pinned image **by `sha256` digest** (as `compose.yml:42,112,137` do);
- `healthcheck:` (pattern `compose.yml:76-81`) and consumers gated on
  `condition: service_healthy` (pattern `compose.yml:255-265`);
- a dedicated named volume;
- respect `name: dev-health-ops` project-name isolation (`compose.yml:1-13`,
  pinned after CHAOS-3142);
- a host port **not** already claimed: 5555, 6432, 8123, 9000, 6379, 8000,
  8010, 8800 are taken.

### 5.2 kind

The inventory found **no kind stack in ops**. The kind fixture is
`acr/scripts/e2e/kind-fixture.sh` (config `:637`, cluster create `:657`),
which stands up its own Postgres and ClickHouse pods (`:803-959`) with
hand-authored manifests and does **not** reference either repo's
`compose.yml`. There is no compose↔kind mapping to extend; they are parallel
stacks.

That makes the kind fixture the **preferred** environment for this trial: it
is already isolated by construction, so the trial store never touches the
shared dev containers. Adding a graph-store pod there follows the existing
`deploy_postgres`/`deploy_clickhouse` pattern with a pinned digest in
`scripts/e2e/pins.env`.

### 5.3 The two switches

| Switch | Off means | Default |
|---|---|---|
| `TEMPORAL_PROJECTION_ENABLED` | the projector claims nothing from the outbox; canonical writes and existing consumers are untouched | off |
| `TEMPORAL_QUERY_ENABLED` | the query path returns `ArmOutcome.UNAVAILABLE`; nothing reads the store | off |

Independently operable, per §17. The rollback path (§20) is: query off,
projection off, canonical services untouched — no data migration, because
nothing canonical ever depended on the store.

### 5.4 Isolation invariants — non-negotiable

- **Never** the dev ClickHouse `default` database. It holds real dev data
  (ops `AGENTS.md`, safety rule).
- The trial store is its own container and its own volume, dropped on
  teardown.
- The trial never runs `ci/local_validate.sh` — host-wide single-flight,
  orchestrator-granted only.
- No Ask Dev runtime registry, no acceptance corpus, no main branches.

### 5.5 Org-deletion registration is a precondition, not a follow-up

The inventory (baseline §12) confirms org deletion discovers ClickHouse tables
by regex-scanning one migration directory
(`org_deletion.py:133-154`, `:75-77`) with no derived-store registry anywhere.
A graph store stood up outside that directory is invisible to org deletion the
moment it holds real org data.

For a shadow trial on synthetic corpus data this is tolerable. It stops being
tolerable at Phase 2 (design-partner cohort, real data). **CHAOS-3566 must
land before any real org data reaches the trial store**, and the trial store
must be among the first entries in its registry.

The same inventory found two org-scoped stores *already* outside deletion's
reach — Valkey allowance counters (`askdev:allowance:{org_id}:{YYYY-MM}`,
`askdev_allowance_counters.py:9-11`) and `worker_job_outbox`, which has no
`org_id` column at all (`models/worker_job_outbox.py:35,41`). Those are
present-day gaps independent of this epic and belong in CHAOS-3566's scope.

---

## 6. Observability — the §18 prerequisite is half-met

PRD §18 blocks a user-visible trial on worker `/metrics`, stating no worker
container exposes a scraped endpoint. The inventory found this is **true only
for Python/Celery workers** (`workers/ask_dev_retention.py:36-45`;
`compose.yml:335-396` — no `ports:` on any worker or beat service; `/metrics`
mounted only on the FastAPI app, `api/_observability.py:34-49`).

**Go workers already expose `/metrics` on :8080** —
`internal/platform/health/server.go:114-118` registers it beside
`/healthz`/`/readyz`, serving real Prometheus text (`:160-220`);
`docker/go-worker.Dockerfile:102`; `deploy/docker-compose/compose.go-workers.yml:115`.

**Recommendation for the ADR:** implement the projector as a Go worker. The
§18 blocking prerequisite is then already satisfied, which removes a stated
dependency from CHAOS-3500/3503 and materially changes the build-vs-block
calculus. If the projector is instead Python/Celery, the prerequisite is real
and unbuilt.

Content-safety rule for every metric and log emitted: no source prose, prompt
text, evidence excerpt, transcript, credential, person-level data, or
unbounded entity name in labels or traces — **including Graphiti's own default
logging**, which §18 requires be audited rather than assumed.

---

## 7. Bring-up plan — every step gated

Steps 2 and onward have not been executed.

| # | Step | Needs a slot? | Blocking dependency | Status |
|---|---|---|---|---|
| 0 | Corpus, oracles, fault self-tests | no — pure Python, already green | — | DONE |
| 1 | Arm adapters against fixture data (no stack) | no | — | **DONE** |
| 2 | Author `compose.temporal-trial.yml` + kind pod manifest | no — authoring only | — | not started |
| 3 | First stack bring-up, isolated kind | **YES** | orchestrator grant | not started |
| 4 | Projector consumes outbox, watermark + lag measured | **YES** | step 3 | not started |
| 5 | Arms N and E measured | **YES** | CHAOS-3563/3564/3565 landed | not started |
| 6 | Arm G — **any LLM extraction spend** (measured/scored) | **YES, separately** | explicit cost authorization; provider keys only via the product's own resolution path | not started -- see "extraction plumbing" below, which is NOT this row |
| 7 | Arm D | **YES** | step 3 | not started |
| 8 | Security/provenance suite (§19) | **YES** | steps 3–7 | not started |
| 9 | Rebuild gate | **YES** | CHAOS-3500's "semantically equivalent" definition — **not landed**; ADR must flag the gap if still absent | not started |

**Extraction arm plumbing (a scoped-down precursor to step 6, not step 6
itself).** `harness/arms/extraction.py` implements the first CANDIDATE arm
(`ArmRole.CANDIDATE_ARM`, never a baseline component -- enforced at
registration time by `ArmRegistry.register`, not just call-site convention)
with a real, provider-agnostic LLM client (`harness/llm/`) that allowlists
`LLM_PROVIDER=local` to `{localhost, 127.0.0.1, host.docker.internal}` so it
cannot silently become a name for a real, billable endpoint. Smoke-tested
against a local model only (`google/gemma-4-e4b` via LM Studio) on two
hand-authored source documents (`harness/arms/source_documents.py`) -- one
quality signal (`O3_supersession`: correct relationship direction and
closure from prose), one security signal (`O5_conflicts_injected`:
prompt-injection resistance).

The adapter never manufactures or repairs oracle-judged metadata: a row
missing `claim_kind` is dropped as malformed (no default to `observed`,
the privileged kind); closure (`valid_to`/`invalidated_by`) is populated
ONLY from a `"closes"` block the model itself emits naming which fact it
replaces and a date drawn from the source text -- there is no adapter-side
"propagate supersession" step, and `valid_to` is never stamped with the
trial's own indexing time. If the model cannot express a closure from what
it was shown, the fact stays open and the oracle that requires it fails
honestly, crediting the model's actual capability rather than the
harness's.

Result, printed and labeled UNSCORED by `tests/test_extraction_smoke.py`,
never asserted as a required outcome, reproducible across repeated runs:
**`O3_supersession` FAILS** -- the `supersedes` edge itself (`ADR-021
supersedes ADR-014`) is extracted correctly, in the right direction, citing
the real `ev1_adr_021` evidence ref; but the model does not reliably
extract `describes_deployment_design_for` at all (the row comes back
malformed -- observed live: the model emitting JSON `null` for
`object_kind`/`object_id`, which the adapter now explicitly rejects rather
than letting `str(None)` silently manufacture the literal entity
`None:None`), and never emits a `"closes"` block even though the source
document states an explicit date (`Decided: 2026-07-09`) it could have
cited. The oracle's closure requirement is therefore genuinely
NOT-EXPRESSED by this model on this prompt, not merely mis-oriented -- an
earlier run (before the null-field fix and the model-emitted-closure
redesign) observed a direction-REVERSAL defect on this same fact instead;
that specific defect did not reproduce under the current adapter and is
superseded by this finding, not additive to it. **`O5_conflicts_injected`
PASSES outright** -- the model correctly refused the embedded injected
instruction, tagged the legitimate fact from the same poisoned document
`untrusted_content`, and correctly flagged both sides of the genuine
conflict once a second, independent document existed to disagree with.

No cloud call has ever been made; no oracle result here has been or may be
counted toward a measured trial or an ADR number. Full corpus source-prose
authoring, a real measured/scored run, and cloud-vs-local per arm remain
step 6/step 3 territory, gated on review of this plumbing.

**Step 3: cloud wiring, class (b) measured, first real sweep.** Reviewed
and landed on top of the above. `harness/llm/client.py`'s
`LLM_PROVIDER=cloud` now calls the real OpenAI API with `gpt-5-mini` --
read from this repo's own production default
(`DEFAULT_MODEL_BY_PROVIDER["openai"]`), not hand-picked -- via the
Responses API, matching production's own `OpenAIGPT5Provider` dispatch
(`_is_gpt5_family`) rather than Chat Completions, and omitting
`temperature` (the model rejects a caller-selected value, same as
production). `harness/arms/extraction.py` gained axis-aware `AS_OF`
filtering (`_apply_as_of_filter`): the model is asked for an optional
`"temporal"` block (`valid_from`/`valid_to`/`recorded_at`) per fact,
sourced strictly from text exactly like the existing `"closes"` mechanism
-- never invented -- and the adapter mechanically filters against
`query.as_of`/`query.axis` afterward, the same category of deterministic
post-processing the closure mechanism already did. Two new source
documents (`harness/arms/source_documents.py`, ATL-101/ATL-105 blocker
prose) give `O2_blocking_valid`/`O2_blocking_observed` real material to
extract from -- required for class (b) to be structurally comparable at
all, since `ClassComparison.is_comparable` needs zero candidate
`NOT_MEASURED` in that class regardless of dependency state.

Two real measured sweeps have been run (`run_measured_sweep.py`; full
parameters, per-class results, and interpretation notes:
`docs/measured-trial-results.md`, which holds the current, canonical run
only) with CHAOS-3563's declared-state history now merged, supplied
through `DependencyState`. The first sweep's prompt/filter contract had a
defect (#1603 finding 1: the prompt told the model to omit `recorded_at`
for a same-day fact, which the observed-time filter would read as "never
observed"); the SECOND sweep re-earned every number under the fixed
contract and is what the committed artifact holds -- see
`docs/adr-draft.md` §3/§4 for the full run-1-vs-run-2 accounting, kept
deliberately explicit rather than quietly overwritten. Class (b) is now
COMPARABLE and shows the extraction candidate winning 2/2 against a
baseline that scores 0/2 -- native's blocker code path has no valid-time
concept at all -- and this result held under BOTH contracts. Classes
(a)/(c) remain mostly NOT_RUN (source material was authored only where
required by this round's scope; see the trial artifact and
`docs/adr-draft.md` §5 for exactly what was and was not covered, with
nothing silently dropped); class (c)'s pass count differed between the two
sweeps (1/15 -> 2/15), feeding directly into `docs/adr-draft.md` §7's
rebuild-equivalence evidence for CHAOS-3500 (same inputs, same model,
non-reproducing results minutes apart -- not just version-to-version
drift). `docs/adr-draft.md` is the first ADR skeleton: per-class results,
§14 framing, closure-expressibility observations across four separate
runs (two local, two cloud), a rebuild-equivalence evidence section, and
open questions -- deliberately no recommendation.

**Step 1 detail.** `harness/arms/native.py` and `harness/arms/episode_readback.py`
run against the pinned corpus directly — `corpus/ground_truth.py` doubles as
the fixture, so there is no separate file to keep in sync. Registered via
`ArmRegistry(ArmRole.BASELINE_COMPONENT)`; `compose_baseline` and both
`TrialReport`/`ComparisonReport` per-class rendering are confirmed against
the real results, not synthetic fixtures (`tests/test_baseline_components.py`).
Native passes its own class (a) control (`O7_valid`) and reproduces the
documented `O7_null_valid_from`/present-state-blocker defects exactly;
episode readback answers `O4_prior_attempts` outright and is honestly
displaced by the C17 decoy under a tight budget. Both correctly degrade
(never silently) on every question their data model cannot answer, and on
every scenario-specific oracle (staleness, redaction, revocation,
extraction-down, graph-outage, squash-coverage) that needs a dynamic state a
static snapshot cannot represent — see the module's own docstring for why
that is a step-1 boundary, not a bug. No candidate arm exists yet;
`compare()` was exercised against a registered-unavailable placeholder to
prove the NOT COMPARABLE path fires for a real (not hand-built) unmeasured
arm, and class (b) was confirmed to render NOT COMPARABLE under the
harness's own default (UNRECORDED_DEPENDENCY) rather than a fabricated
CHAOS-3563 state.

The isolated-kind path (step 3) is the requested grant: it does not contend
with dev compose or the acceptance stack.

---

## 8. Thresholds

Per PRD §16, **no numeric threshold is proposed here.** Observed distributions
first; adoption thresholds proposed in the ADR afterward. Metrics to be
recorded per §16 operational list, and every product-quality metric reported
**per class**, never as an aggregate alone.

One measurement the amendment specifically requires and that is easy to skip:
**confidence calibration** — confidence buckets against oracle correctness.
Until that is measured, §6.3 holds that consumers present `claim_kind` plus
supporting-source count rather than the raw float. The trial should either
produce the calibration curve or state plainly that it did not.

---

## 9. Known gaps in this design

Stated rather than left for a reviewer to find:

1. **Trial code is outside CI.** Root `pytest.ini` sets `testpaths = tests`,
   and `[tool.mypy] files = ["src", "tests", "scripts"]` — so `trials/` is
   neither collected by `ci/local_validate.sh` nor typechecked. That isolation
   is deliberate (shadow trial code must not destabilise a host-wide
   single-flight gate other lanes queue behind), but the cost is real: these
   tests run only when someone runs them. `run_oracles.sh` exists so the run
   is one command, and the trial report records whether it happened. Ruff
   *does* cover `trials/` (no exclusion), so lint and format are enforced.
2. **The rebuild gate has no operational definition yet** (CHAOS-3500).
3. **The baseline depends on other lanes.** Its components are built by
   CHAOS-3562/3563/3564/3565, not here. If they have not landed when the trial
   runs, class (a) and (b) measure a *pre-increment* baseline and the ADR must
   say so — otherwise a candidate is credited with beating a baseline that was
   never built. The harness refuses to render class (b) as comparable until
   CHAOS-3563's state is recorded, but it cannot detect a silently
   pre-increment class (a); that one is on the person writing the ADR.
4. **Per-org ingestion coverage is unmeasured** pending an authorized
   environment; `deployments.v1` staleness is UNVERIFIED, not assumed.
5. **The `entitlements` cache** (`acr/internal/entitlements/cache.go`) is a
   revocation-lag path that has not been timed. Credential revocation is live
   (no cache); entitlement revocation is not.
