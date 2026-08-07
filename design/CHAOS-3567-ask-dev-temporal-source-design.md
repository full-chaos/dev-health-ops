# CHAOS-3567 — Ask Dev temporal source: intent taxonomy, registry impact, flag-off scaffold design

Status: proposed, awaiting main lane approval before any stub code is written.
Scope: CHAOS-3567 only (design + flag-off scaffolding). Full recognizer/plan/source
implementation stays blocked on the CHAOS-3499 ADR and CHAOS-3500/3501 contracts, per
CHAOS-3502 (amended 2026-08-07) and the CHAOS-3498 plan-of-record comment
(2026-08-07, "partial override").

This is not published under `ops/docs/` because that tree is the customer-facing
MkDocs site and this is an internal, pre-adoption design record for unshipped,
flag-off scaffolding on a feature branch.

## 0. Inputs read

* CHAOS-3567 (this issue), CHAOS-3502 (parent, amended 2026-08-07), CHAOS-3498
  (epic) plan-of-record comment (2026-08-07) and PRD-review comment (2026-08-07).
* PRD/TRD: *Context Fabric temporal graph and episodic context*
  (`prdtrd-context-fabric-temporal-graph-and-episodic-context-5dc3685f6548`),
  amended sections §2, §5, §6, §7.2, §9, §10, §12, §14, §15.
* Live repo state at HEAD of `feature/chaos-3498-context-fabric` in this worktree
  (`dev-health-ops`), plus read-only inspection of sibling `dev-health-web` for
  wire-schema-union evidence (no edits made there — MUST NOT touch web/acr repos).

## 1. Temporal intent taxonomy → interpreter recognizer requirements

CHAOS-3502's authorized intent families (its "Approved initial intent families",
mirrored in PRD §12 and §10's `query_mode` enum) map to five new
`QuestionIntentID` members. None of these exist today —
`src/dev_health_ops/api/dev/contracts_v2/base.py:105-119` defines exactly twelve
Wave 3.1 launch intents, and the closest two (`OBSERVED_CHANGE`,
`BOUNDED_INVESTIGATION`) are structurally different questions, confirming the
CHAOS-3502 amendment's own claim ("only `observed_change` and
`bounded_investigation` come close").

| Proposed intent (`QuestionIntentID` member) | PRD family (§10 `query_mode`) | Recognizer signal shape | Why existing recognizers don't already match it |
|---|---|---|---|
| `TEMPORAL_AS_OF` | `as_of` | Date/time anchor phrase ("as of", "on <date>", "back on") + a committed subject. Must also decide `axis` (§10: `valid_time` vs `observed_time`) — the recognizer cannot infer axis from wording alone in the general case; axis defaults to `observed_time` per PRD §10's own open point ("no server default today" is called out as something CHAOS-3499 must define) and is overridable only through the classifier-fallback path, never guessed by the deterministic recognizer. | `OBSERVED_CHANGE` (`change.observed.v1`) is a *recency* window query ("what changed recently"), not an arbitrary historical-instant query; it has no `as_of` anchor concept at all. |
| `TEMPORAL_PRIOR_ATTEMPTS` | `prior_attempts` | "tried before" / "previous attempt(s)" / "last time this happened" phrasing, optionally anchored to a CI-failure or incident-like noun. | No existing intent reads `agent_episode.v1` at all (§14.c pre-trial increment — episode read path — does not exist yet; ties this intent to CHAOS-3564 landing first). |
| `TEMPORAL_SUPERSEDED_DECISIONS` | `superseded_decisions` | "replaced", "superseded", "which decision changed X" anchored to a decision/ADR-shaped noun. Must not collide lexically with `agent_episode.v1`'s existing `superseded` **outcome** enum value (PRD §6.3 explicitly separates the `supersedes` **predicate** from that outcome — the recognizer and any downstream narrative copy must keep the two nouns apart). | Nothing today models "decision B replaced decision A" as a queryable relationship; `DATA_TRUST`/`OBSERVED_CHANGE` don't touch relationship supersession. |
| `TEMPORAL_RECURRING_PATTERNS` | `recurring_patterns` | "keeps happening", "recurring", "again" plus a repeated-failure/incident-shaped noun, evaluated across more than one committed subject or episode. | Requires cross-episode association — PRD §14.c's explicit Graphiti-differentiator class; no deterministic native equivalent exists pre-trial. |
| `TEMPORAL_CONFLICT_INVESTIGATION` | `conflicts` (bounded historical investigation) | "conflict(s) with", "disagrees with", "contradicts" plus a fact/claim-shaped noun; also the catch-all bounded-historical-investigation shape CHAOS-3502 names. | `BOUNDED_INVESTIGATION` today is the *no-recognizer-matched* fallback intent (`question_interpreter.py:887`, `intent_id = QuestionIntentID.BOUNDED_INVESTIGATION` when no `_RECOGNIZERS` entry fires) — it carries no temporal semantics of its own; a temporal variant needs its own recognizer so a real conflict-investigation question doesn't silently fall through as generic bounded investigation with no temporal evidence attached. |

Recognizer registration point: `_RECOGNIZERS: tuple[_Recognizer, ...]` at
`src/dev_health_ops/api/dev/question_interpreter.py:695` — a flat tuple of
`(recognizer_id, intent, matches: Callable[[_Signals], bool])`, evaluated in
order, first match wins (`question_interpreter.py:890-895`). Adding five entries
here is mechanically cheap; the design risk is recognizer *precedence* and
*false-positive rate* against the existing twelve (a temporal-sounding word
inside an otherwise-ordinary status question must not misroute the whole
answer), which is exactly why CHAOS-3567 stops at flag-off stubs and defers real
recognizer logic to the post-ADR implementation issue.

Amended-semantics constraints these recognizers must honor once implemented
(not exercised by the flag-off stub, but binding on the eventual recognizer
logic and recorded here so the design survives to that issue):

* **Time axis naming** (PRD §10): `axis: "valid_time" | "observed_time"` is
  mandatory whenever `query_mode == "as_of"`; a recognizer that emits
  `TEMPORAL_AS_OF` without deciding axis is an incomplete implementation, not a
  don't-care.
* **Renamed ingestion unit** (PRD §6.1): the graph-side ingestion unit is
  `source_event` (`sevt_...`), never "episode" — `agent_episode.v1` keeps its
  existing name and outcome vocabulary unchanged. Any new code, docstring, or
  corpus note this design's follow-on issues write must keep the two nouns
  distinct, mirroring `TEMPORAL_SUPERSEDED_DECISIONS`'s predicate/outcome split
  above.
* **Closed entity-kind vocabulary** (PRD §6.2, `entity_kind_vocabulary.v1`):
  person/individual is explicitly excluded. A recognizer's subject-kind
  inference must never propose a person-shaped subject for a temporal query —
  this is a stricter subset of `EntityKind` (`contracts_v2/base.py:84-102`,
  which already has no person kind, so no widening is needed here; it is a
  forward-looking constraint on the *new* `entity_kind_vocabulary.v1` PRD §6.2
  introduces for the graph layer, not on today's `EntityKind`).

## 2. Registry impact enumeration (file:line evidence)

CHAOS-3502's amendment is explicit that adding `TEMPORAL_CONTEXT` touches four
closed, totality-checked registries plus schema regen, the sources.json
terminal-state matrix, and web vocabulary — "not one source class." Traced here
against live HEAD.

### 2.1 `SourceClass` (`contracts_v2/base.py:226`) + schema regeneration

* **Registry:** `class SourceClass(StrEnum)`,
  `src/dev_health_ops/api/dev/contracts_v2/base.py:226-284`. Sixteen closed
  members (`STATUS_CHANGE` … `DEFICIENCY_INVENTORY`). The class docstring
  (`base.py:240-243`) states the contract directly: *"Adding a source class is
  therefore a deliberate contract change, not an incidental string: a new
  adapter must add its member here (and regenerate the exported schemas)
  before it can appear on the wire."* Adding `TEMPORAL_CONTEXT` is one new
  member here.
* **Schema regeneration:** generator is
  `src/dev_health_ops/api/dev/export_contracts_v2.py` (`_schema()` at line 38,
  reads `CONTRACT_MODELS_V2[name].model_json_schema(...)`), writing to
  `contracts/ask-dev/v2/schemas/*.json`. `SourceClass` is `$ref`'d from (grep
  confirmed, live HEAD):
  * `contracts/ask-dev/v2/schemas/dev_source_requirement.v1.schema.json:4-25`
    (the `SourceClass` `$defs` enum itself — this is the master copy every
    other schema's `$ref` resolves against) and `:133-135` (`source_class`
    field).
  * `dev_source_observation.v1.schema.json`, `dev_investigation_plan.v1.schema.json`,
    `dev_investigation_result.v1.schema.json`, `dev_answer_frame.v1.schema.json`
    (`DevCoverageV2.unavailable_required_sources`/`stale_required_sources` at
    `dev_answer_frame.v1.schema.json:678-729` — this is the exact "coverage
    counts stay answerable for a denial" channel the `SourceClass` docstring
    warns about), `dev_answer.v2.schema.json`, `dev_stream_event.v2.schema.json`.
  * Drift is caught at PR time the same way v1 already is:
    `tests/api/external_ingest/test_schema_export_no_drift.py` is the pattern
    (compares generated bytes to committed source); the v2 equivalent is
    `export_contracts_v2.py`'s own `main()`/verify path — any stub member added
    to `SourceClass` without a regen run fails that check, which is exactly the
    "totality check" this registry enforces mechanically rather than by review.

#### 2.1.1 Two more `SourceClass`-keyed totality tables (found during stub implementation, not in the original survey above)

`SourceClass` is closed *and* exhaustively consumed in more than the schema
layer §2.1 covers above. Implementing the flag-off stub (adding
`TEMPORAL_CONTEXT` to the enum) immediately surfaced two more import-time
completeness checks in `src/dev_health_ops/api/dev/investigation_plans/relationship_matrix.py`,
neither of which this design's original registry survey listed. Recorded
here so this doc stays the accurate registry-impact map for the post-ADR
implementation issue, rather than silently relying on the reader to
rediscover them the same way — same discipline as the corrections already
threaded through the amended PRD itself.

* **`RELATIONSHIP_MATRIX: dict[SourceClass, RelationshipMatrixEntry]`**
  (`relationship_matrix.py:126-311`) — built as a dict comprehension over one
  `_entry(...)` call per `SourceClass` member, then asserted complete at
  import time (`relationship_matrix.py:313-317`):
  ```python
  _missing = set(SourceClass) - set(RELATIONSHIP_MATRIX)
  if _missing:
      raise RuntimeError(f"relationship_matrix.v1 is missing entries for: {sorted(_missing)}")
  ```
  Every `SourceClass` member not yet wired to a real adapter (`CODE_CHANGE`,
  `REVIEW`, `TEST_REPORT`, `OPERATIONAL_CONTROL`, `COGNITIVE_LOAD`,
  `INVESTMENT_ALLOCATION` — `relationship_matrix.py:226-267`) already carries
  this exact "not yet landed, honest empty vocabulary" placeholder shape:
  `role="supporting", requirement="not_applicable", freshness_policy="unversioned",
  evidence_expansion_capability=False`, empty `approved_relationship_types`.
  `TEMPORAL_CONTEXT` gets the identical placeholder — implemented in this
  issue's PR, not deferred, because the module fails to *import at all*
  otherwise (this is a harder gate than a test: nothing that imports
  `relationship_matrix` — which is most of `investigation_plans/`, transitively
  most of the orchestrator — can load).
* **`APPROVED_CONTENT_SLOTS: dict[SourceClass, frozenset[str]]`**
  (`relationship_matrix.py:169-380` region) — the same posture, one entry per
  `SourceClass` naming which `DevSourceContent` fields that class's steps may
  populate, with its own import-time completeness assertion
  (`relationship_matrix.py:449-453`, `_missing_content_slots = set(SourceClass)
  - set(APPROVED_CONTENT_SLOTS)`). `TEMPORAL_CONTEXT` gets `frozenset()` —
  no step mints content under it, matching every other not-yet-landed class.

**Checked and confirmed out of scope — `persistence/service.py`'s
`_SOURCE_CLASSES` allowlist (CHAOS-3337).** This is a *third*
`SourceClass`-related table (`persistence/service.py:264`,
`if source_class not in _SOURCE_CLASSES` at `:3000`), but its own totality
check (`wave_3_1_plans._source_classes_missing_from_persistence_allowlist`,
`wave_3_1_plans.py:93-124`, invoked at that module's own import time,
`wave_3_1_plans.py:886-895`) is scoped differently: it diffs the allowlist
against `{requirement.source_class for plan in registered_plans.values() for
requirement in plan.source_requirements}` — source classes an actually
*registered plan* emits — never against the full `SourceClass` enum. Because
the flag-off stub registers no plan or step against `TEMPORAL_CONTEXT`
(§4), this table's own totality check never sees the new member and needs
no edit. Confirmed two ways: by reading `wave_3_1_plans.py`'s check directly,
and empirically — the flag-off stub's fault-injection proof (§4) planted a
`DevSourceRequirement` against `SourceClass.TEMPORAL_CONTEXT` on a real
registered plan specifically to verify this table's guard *would* fire if
the source class were ever actually wired, and it did (this exact
`RuntimeError`, before the stub's own guard test even reached collection) —
strong first-hand confirmation of the boundary this note describes, not an
inference from reading code alone.

### 2.2 `ToolID` (`contracts.py:187`) + executor exact-set-equality

* **Registry:** `class ToolID(StrEnum)`, `src/dev_health_ops/api/dev/contracts.py:187-196`.
  Nine members (`RESOLVE_SCOPE` … `DATA_HEALTH`).
* **Exact-set-equality enforcement:** `AskDevToolRegistry.__init__`,
  `src/dev_health_ops/api/dev/tool_registry.py:198-219`:
  * line 203: `expected = set(ToolID)`
  * line 204: `if set(definitions) != expected: raise RuntimeError(...)` — every
    `ToolID` member must have a `TOOL_DEFINITIONS` entry (metadata/manifest).
  * line 212: `if set(executors) != expected: raise ValueError(..., missing=..., extra=...)` —
    every `ToolID` member must have a registered executor callable; the caller
    cannot construct `AskDevToolRegistry` with a partial or superset mapping.
  * `build_wire_tool_name_map(item.value for item in ToolID)` (called
    unconditionally in `__init__`) additionally asserts no two `ToolID` values
    collide once sanitized into OpenAI-compatible wire function names —
    relevant if a future `TEMPORAL_CONTEXT`-reading tool ID is added later
    (CHAOS-3567 itself does not need a new tool — the PRD's `TEMPORAL_CONTEXT`
    is a *source class* consumed by existing plan steps, not a new
    model-invokable tool; documented here so a later lane doesn't assume one
    is needed without re-checking).
* **Scope call for this issue:** no new `ToolID` member is required for the
  flag-off scaffold (see §4) — this section is inventory, not a change list.

### 2.3 `StepRegistry` (`investigation_plans/steps.py:128`) + plan documents

* **Registry:** `class StepRegistry`, `src/dev_health_ops/api/dev/investigation_plans/steps.py:128-167`.
  `register()` (line 142) raises `DuplicateStepError` on a `(plan_id, step_id)`
  collision; construction-time cross-plan totality is proven separately by
  `registry_validation.validate_registry`
  (`src/dev_health_ops/api/dev/investigation_plans/registry_validation.py:59`),
  which takes `core_intents: frozenset[QuestionIntentID]` (line 63) and raises
  `MissingCorePlanError` (line 47, raised at line 71) for any core intent with
  no registered plan.
* **Plan-ID vocabulary:** `PLAN_REGISTRY: frozenset[str]`,
  `src/dev_health_ops/api/dev/contracts_v2/plan.py:49-76` — thirteen entries
  (twelve intent-mapped plan IDs plus the `legacy.tool_choice.v1` compatibility
  marker). The docstring (`plan.py:39-44`) states the same contract shape as
  `SourceClass`: *"A new plan is added by adding a member here."* Five new
  `PLAN_REGISTRY` string members would be needed (one per new intent in §1),
  each requiring:
  * a `DevInvestigationPlan` document (shape defined `plan.py:125-179`,
    `validate_plan_invariants` enforces no duplicate/self-referencing steps,
    unique `(source_class, adapter_id)` per source requirement, and dependency
    references resolve to known steps);
  * step registration via `register_builtin_steps`
    (`src/dev_health_ops/api/dev/investigation_plans/builtin_steps.py:1028`) or
    an equivalent registration function, so `StepRegistry` actually has a
    runner for every step the plan document declares — a plan with no matching
    registered step is `MissingStepImplementationError`, not a silent no-op
    (per `registry_validation.py`'s module docstring, lines 1-18).
  * Existing plan documents for reference: `wave_3_1_plans.py:703` (`PROJECT_HEALTH`),
    `:732` (`TEAM_HEALTH`), `:761` (`TEAM_WORKLOAD_BALANCE`), `:790`
    (`OPERATIONAL_DEFICIENCY_INVENTORY`), `:831` (`PORTFOLIO_STATUS`);
    `plan_documents.py:43` (`ENTITY_STATUS`), `:107` (`REMAINING_WORK`), `:150`
    (`OBSERVED_CHANGE`), `:201` (`REGISTERED_STATISTICS`), `:244`
    (`METRIC_COMPARISON`), `:274` (`DATA_TRUST`). Notably `BOUNDED_INVESTIGATION`
    (`investigation.bounded.v1`) has **no** `DevInvestigationPlan` document at
    all today — it is referenced only in `PLAN_REGISTRY` (`plan.py:62`) and
    `preflight_outcomes.py`, and is the fallback `intent_id` the interpreter
    assigns when no recognizer matches (`question_interpreter.py:887`). This
    means "the closest existing intent" for
    `TEMPORAL_CONFLICT_INVESTIGATION`/bounded historical investigation has no
    existing plan-authoring precedent to copy structurally — the new plan
    document is greenfield work for the post-ADR implementation issue, not a
    small diff against an existing one.

### 2.4 `native_evidence._SPECS` / `DataHealthService`

* **`_SPECS`:** `src/dev_health_ops/api/dev/native_evidence.py:103`, a
  `dict[str, _SourceSpec]` keyed by source string
  (`work_units`, `work_items`, `projects`, `pull_requests`, `reviews`,
  `commits`, `ci_runs`, `deployments`, `incidents` — confirmed present at HEAD).
  Each entry is a pair of parameterized ClickHouse SQL templates
  (search + get-by-id). `TEMPORAL_CONTEXT` has **no** ClickHouse table of its
  own to query this way (PRD §5: the temporal graph is a separate derived
  store, not a ClickHouse projection queried through `native_evidence`'s
  search/get contract) — so this registry needs **no new `_SPECS` entry** for
  CHAOS-3567 or its eventual implementation. Recorded here to close the
  enumeration honestly rather than silently mark it "not applicable" without
  showing the check.
* **`DataHealthService`:** `class DataHealthService`,
  `src/dev_health_ops/api/dev/data_health_service.py:127-273`.
  `NATIVE_EVIDENCE_SOURCES` (line 42-51) is the *default* `required_sources`
  tuple for `inspect()` (line 149) — eight mandatory native sources. The
  **precedent to follow**, exactly as CHAOS-3502's amendment names it, is the
  existing `acr` optional-source special case in
  `NativeDataHealthReader.read` (`data_health_service.py:398-406`):
  ```python
  if source == "acr":
      observations.append(
          SourceHealthObservation(
              source, False, False, warning="acr_optional"
          )
      )
      continue
  ```
  `required=False` (second positional arg) means `DataHealthService.inspect`'s
  `complete_eligible = all(not item.required or item.state is COMPLETE ...)`
  (line ~269-273) never gates completeness on it — org-wide, unconditional, no
  fixture rows needed. `TEMPORAL_CONTEXT` (or a `temporal_context` health-check
  key, naming TBD by the post-ADR issue) follows the identical shape:
  `required=False`, a distinct warning token (not `acr_optional` — reusing that
  token would make the two sources' unavailability indistinguishable in the
  `data_health.v1` tool's disclosed warnings), added as another `if source ==
  "temporal_context":` branch, not a change to `NATIVE_EVIDENCE_SOURCES` itself
  (that tuple stays exactly the eight mandatory sources it is today — temporal
  is optional by construction, matching PRD §12: *"It is not mandatory for
  normal present-state ... plans"*).
  * Corpus precedent already exists for this exact pattern and is directly
    reusable as an authoring template: `tests/acceptance/world/ask-dev-world.v1/corpus/case-deg.source-state.not-applicable.json`
    and `case-deg.acr-unavailable-native-usable.json`, both grounded in the
    same `acr_optional` mechanism (see §2.5).

### 2.5 `sources.json` terminal-state matrix

* `tests/acceptance/world/ask-dev-world.v1/sources.json` — `source_classes`
  array (line ~8, nine entries including `"acr"`) and the `matrix` array's
  dedicated `"not-applicable"` row (mechanism text quotes
  `data_health_service.py`'s `acr_optional` branch verbatim, `realized_by`
  deliberately `null`/org-wide — no fixture rows). Adding `temporal_context` to
  `source_classes` and a matrix row reusing this same `not-applicable`
  pattern (or a new `unavailable`/`stale` row, once §2.4's watermark plumbing
  exists) is required *only when the source class is actually wired* — **not**
  for the flag-off stub, since a disabled-flag `TEMPORAL_CONTEXT` member that
  no plan ever declares as a `DevSourceRequirement` is never reachable by
  `DataHealthService.inspect` or any corpus run (see §4's zero-behavior-change
  argument). Recorded here as the exact file the post-ADR issue must touch,
  and as the reason the *acceptance corpus itself* (§4) is provably untouched
  by this issue's scaffolding.

### 2.6 Web schema-union impact (`dev-health-web`, read-only evidence)

Read-only inspection of the sibling `dev-health-web` worktree (no edits — MUST
NOT touch web repos) to ground CHAOS-3502's claim that this is "a coordinated
ops+web changeset, not an ops-only change":

* `dev-health-web/scripts/ask-dev-contracts.mjs:16` currently pins
  `SOURCE_PREFIX = "contracts/ask-dev/v1/"` — the web repo's contract-sync
  script has **not yet been repointed to `contracts/ask-dev/v2/`** (the v1
  generator's own docstring at
  `src/dev_health_ops/api/dev/export_contracts_v2.py:1-9` calls this out as
  "CHAOS-3298, out of scope" at the time v2 export was built). This means
  `SourceClass` — a v2-only enum — is **not currently synced to web at all**
  through that script. Any `TEMPORAL_CONTEXT` member added to the v2 enum is
  invisible to web until CHAOS-3298 (or an equivalent v2 sync) lands; this is
  a real gap the post-ADR implementation issue must either close or explicitly
  route around, not an incidental detail.
  Note the amended CHAOS-3502 comment continues to describe `SourceClass` as
  something a temporal source-class change touches even before v2 sync exists
  — this is why the same amendment lists **"web-side vocabulary" as its own,
  separate bullet** from the four ops registries: it is additive, coordinated
  work, not a mechanical consequence of the ops-side enum, and is not
  contradicted by this gap.
* The totality-check pattern web already uses for pinned enums —
  `dev-health-web/src/components/ask-dev/AskDevAnswer.test.tsx:457-470` —
  is a `Record<DevAnswer["status"], true>` "known members" map plus an
  `Object.keys` comparison against the shipped `ANSWER_STATUS_LABELS` map, so
  a new enum member fails TypeScript compilation (missing `Record` key) *and*
  fails this test (label-map/reference-list drift) until both are updated.
  This is the exact "exhaustiveness tests and total label maps" mechanism
  CHAOS-3502's amendment names. Any new wire-visible enum member introduced by
  the eventual temporal implementation (new `QuestionIntentID` members if ever
  surfaced to web, `SourceClass.TEMPORAL_CONTEXT` once v2 sync exists, any new
  public-facing warning/degradation token) needs the matching `Record<...,
  true>` + label map updated in the same web PR, mirrored from this pattern.
* `dev-health-web/src/lib/dev/contracts/schemas/dev_capabilities.v1.schema.json:57-66`
  carries a **separate, coarser** `QuestionClass` enum (six members: `status`,
  `remaining_work`, `observed_change`, `registered_statistics`, `data_trust`,
  `investigation`) — this is the client-supplied *hint* class
  (`client_question_class_hint`, read-and-recorded-never-planned-on per
  `question_interpreter.py:937-938`), not the twelve-member server-owned
  `QuestionIntentID`. Whether a temporal question class needs a new
  `QuestionClass` member (an eighth "temporal" hint bucket) or folds under
  `investigation` is a product decision for the post-ADR issue, not something
  this design resolves — flagged here so it isn't missed.

### 2.7 Corpus impact

None for this issue. §4 shows why the flag-off stub is unreachable by any
existing corpus run; the *acceptance corpus is UNTOUCHED* per hard constraint.
New corpus cases (independent expected-evidence oracles per PRD §7.2/§15.1,
following the `case-deg.source-state.not-applicable.json` /
`case-deg.acr-unavailable-native-usable.json` authoring pattern already in
§2.4) are the post-ADR implementation issue's responsibility once the source
class is actually wired into a plan.

## 3. Summary: what CHAOS-3567 touches vs. what it doesn't

| Registry / artifact | Touched by this issue's flag-off stub? | Touched by post-ADR implementation |
|---|---|---|
| `SourceClass` | Yes — one new disabled member, `TEMPORAL_CONTEXT` | schema regen already covers it |
| Schema regen (`contracts/ask-dev/v2/schemas/*`) | Yes — mechanical consequence of the enum add | — |
| `RELATIONSHIP_MATRIX` / `APPROVED_CONTENT_SLOTS` (§2.1.1) | Yes — one inert placeholder entry each, mandatory (module fails to import otherwise) | Yes — real entries once a plan/step wires the class |
| `persistence/service.py` `_SOURCE_CLASSES` allowlist (§2.1.1) | No — confirmed both by reading its narrower totality check and empirically via the fault-injection proof (§4) | Yes — once a registered plan actually emits `TEMPORAL_CONTEXT` |
| `ToolID` + executor | No | Only if a dedicated tool is later justified (§2.2) |
| `PLAN_REGISTRY` / `StepRegistry` / plan documents | No | Yes — five new intents, five new plan docs, new steps |
| `native_evidence._SPECS` | No (never applicable, §2.4) | No |
| `DataHealthService` optional-source branch | No | Yes — `required=False` branch, acr precedent |
| `sources.json` terminal-state matrix | No | Yes |
| Web schema unions | No | Yes (coordinated ops+web changeset) |
| Acceptance corpus | No — untouched | Yes — new independent-oracle cases |

## 4. Flag-off stub design: zero runtime behavior change

**Flag mechanism:** reuse the `EXPLICIT_PURCHASE_FEATURES` opt-in pattern PRD
§9 names for `agent_context_runtime` (`temporal projection is an explicit
per-org opt-in ... never default-on"`) rather than an ad hoc boolean or env
var — this keeps the eventual per-org opt-in semantics (§9's retention-posture
composition requirement) consistent with how the codebase already gates
comparable optional capabilities. For CHAOS-3567 the flag is never read at
runtime by any live code path (see below) — it exists purely as the seam the
post-ADR issue wires a real gate onto.

**What "flag-off stub" means concretely for this issue:**

1. Add `TEMPORAL_CONTEXT = "temporal_context"` to `SourceClass`
   (`contracts_v2/base.py:226`). This satisfies §2.1's registry totality
   requirement (a `SourceClass` member always exists in the schema) without
   creating any reachable code path: `SourceClass` membership alone does not
   cause any plan, step, or tool to reference it.
2. Regenerate `contracts/ask-dev/v2/schemas/*.json` so the schema-drift test
   passes with the new enum member present (§2.1).
3. Add **no** new `PLAN_REGISTRY` entry, **no** new `DevInvestigationPlan`, and
   **no** `StepRegistry.register()` call. A `SourceClass` member with no plan
   ever declaring a `DevSourceRequirement` against it is inert by construction:
   nothing in `orchestrator.py`, `executor.py`, or `registry_validation.py`
   iterates `SourceClass` itself looking for coverage — they iterate
   `PLAN_REGISTRY`/registered steps, which are unchanged.
4. Add **no** `DataHealthService`/`NativeDataHealthReader` branch. Until a
   step declares a `temporal_context` requirement, `DataHealthService.inspect`
   never receives `"temporal_context"` in its `required_sources` argument, so
   the acr-shaped `required=False` branch (§2.4) is genuinely unneeded for a
   flag-off stub — it would be dead code today, and dead code with a disabled
   flag is a worse totality story than no code at all. It is deferred to the
   post-ADR issue alongside the plan wiring that gives it a caller.
5. **Zero `sources.json` / corpus changes** — nothing to add a matrix row or
   corpus case for, because nothing reachable changed (§2.5, §2.7).

**Why this is provably zero runtime behavior change, not just "should be
fine":** every existing runtime path that could observe a `SourceClass`
member (`DataHealthService.inspect`'s `source_systems` argument, any
`DevSourceRequirement.source_class` on a registered plan, `native_evidence`
dispatch, `production_runtime.py` tool execution) is driven by *plan
documents and explicit source-system lists*, never by enumerating
`SourceClass` itself. Adding a sixteen-plus-one'th member to a closed
`StrEnum` that nothing iterates is inert. This is also why the guard test
below plants the specific defect that would falsify that claim, rather than
asserting the (unsurprising) absence of change.

**RED-first guard test (plant the defect, watch it fail, then restore):**

The test proves the specific claim above — "a new inert `SourceClass` member
changes nothing reachable" — by doing the one thing that *would* break it and
observing the existing/new test distinguish the two states:

1. Write the guard test first, against **today's** `SourceClass` (before
   `TEMPORAL_CONTEXT` is added): assert that
   `DataHealthService.inspect(..., required_sources=NATIVE_EVIDENCE_SOURCES)`'s
   observed `source_systems` set is exactly the eight
   `NATIVE_EVIDENCE_SOURCES` members (no more, no less), and that
   `set(SourceClass) - {classes referenced by any DevSourceRequirement in
   PLAN_REGISTRY's plans}` is non-empty only for members already known to be
   unreferenced today (there may be pre-existing unreferenced members;
   the test pins the *current* unreferenced set, not zero) — this fails RED
   if run against a broken checkout where `TEMPORAL_CONTEXT` were, say,
   accidentally wired into `NATIVE_EVIDENCE_SOURCES` or a real plan's
   `source_requirements` by mistake.
2. Add `TEMPORAL_CONTEXT` to `SourceClass` and regenerate schemas (§4 steps
   1-2).
3. Re-run the same test: it must still pass, because the new member joins the
   *unreferenced* set, not `NATIVE_EVIDENCE_SOURCES` or any plan's
   requirements.
4. **Plant the defect this guard exists to catch:** temporarily add
   `"temporal_context"` to `NATIVE_EVIDENCE_SOURCES` (or register a throwaway
   `DevSourceRequirement` against `SourceClass.TEMPORAL_CONTEXT` on an
   existing plan) in a scratch/local diff — confirm the guard test **fails**
   red, proving it actually detects the enablement it exists to prevent, not
   just green-by-construction. Revert the scratch diff; it never reaches a
   commit.
5. Commit the guard test green against the real stub (`TEMPORAL_CONTEXT`
   present in `SourceClass`, absent from every plan/required-source list).

This satisfies "observe every guard failing": step 4 is the fault-mode
self-test proving the assertion rejects the bad behavior it claims to catch,
per this project's verification discipline — not merely "the stub compiles
and existing tests stay green," which would be a measurement that could pass
even with the source class silently wired live.

## 5. Open items for the post-ADR implementation issue (not resolved here)

* Recognizer precedence/false-positive risk against the twelve existing
  intents (§1) — needs its own corpus-driven measurement, not a design-time
  guess.
* `entity_kind_vocabulary.v1` (PRD §6.2) is a new, separate closed vocabulary
  from today's `EntityKind` (§1) — confirm whether it's implemented as a
  distinct enum or as a documented subset/superset relationship before intent
  recognizers reference it.
* `axis` default behavior for unpinned `as_of` queries (PRD §10, §15.1 item 20)
  is explicitly undefined pre-trial; `TEMPORAL_AS_OF`'s recognizer cannot ship
  without it.
* Web `QuestionClass` (§2.6): whether temporal questions get a new hint bucket
  or fold under `investigation`.
* `contracts/ask-dev/v2` → web sync gap (§2.6, CHAOS-3298) — a real
  prerequisite this design surfaced, not previously flagged against this
  effort.
* `DataHealthService` `temporal_context` warning token naming (§2.4) — pick a
  name distinct from `acr_optional` before wiring the real branch.
