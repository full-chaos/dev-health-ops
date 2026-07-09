# Implementation Brief: CHAOS-2692 — External ingest schema discovery and JSON Schema export

> **SYNTHESIZER RECONCILIATION (authoritative — see master-spec.md; overrides body below):**
> 1. **Ownership split resolved**: CHAOS-2691 lands FIRST (wave 1) and owns
>    `schemas.py`/`router.py`/`main.py` registration and ALL Pydantic model definitions.
>    This issue (wave 2) NEVER re-declares models — import `RECORD_KIND_MODELS`,
>    `BatchEnvelope`, etc. from `api/external_ingest/schemas.py`. The model sketches below
>    (RepositoryRecordV1 with name/url/defaultBranch, embedded `kind` literals, required
>    `producer`) are VOID — 2691's field sets are canonical (wrapper record envelope
>    `{kind, externalId, payload}`, versioned kinds, optional producer,
>    `externalId` = repo full name not URL).
> 2. D4 stands (versioned `kind` everywhere) and is now also binding on CHAOS-2700.
> 3. ADR path/number: `docs/architecture/adr-005-external-ingest-schema-discovery.md`
>    (flat naming; 003=2691's boundary ADR, 004=2715's webhook ADR).
> 4. `examples/*.json` under `api/external_ingest/examples/` is the epic's SINGLE canonical
>    fixture home (CC18): 2700's `push sample` calls `schema_registry.load_example(kind)`;
>    2701 copies to docs with a drift test; 2702 asserts fixture equality.
> 5. Discovery bundle also exposes `"limits"` (maxRecordsPerBatch/maxBodyBytes) — values from
>    the same env vars 2691's router enforces.
> 6. D2 (public discovery endpoints, `schema:read` reserved/no-op for GETs) is RATIFIED;
>    note `/validate` (2691) does require `schema:read`.
> 7. **POST-CRITIQUE (CC15)**: GET /schemas + GET /schemas/{version} carry
>    `INGEST_READ_LIMIT="120/minute"` (IP-keyed; constant landed by 2691) — keep the
>    decorators when reworking the handlers; 429 uses the external-ingest envelope.
>    Rationale: public + ETag'd but still a registry-render per request → cheap
>    anonymous DoS surface without a limiter.

Epic: CHAOS-2690 External customer-push ingestion API
Repo: `dev-health-ops`, worktree `chaos-2690-integration`
Sibling issues referenced: CHAOS-2691 (REST contract/schemas — **overlaps this issue, see Design Decision D1**), CHAOS-2696 (source registration/token scopes), CHAOS-2700 (dev-hops push CLI), CHAOS-2712 (auth model)

As of this recon, `src/dev_health_ops/api/external_ingest/` and `src/dev_health_ops/external_ingest/` **do not exist yet** on the integration branch — only the plan doc has landed (`git log` shows `0622bb04b docs: external customer-push ingestion plan` as the latest external-ingest-related commit, no code). This brief assumes CHAOS-2692 may land before, after, or interleaved with CHAOS-2691 and is written to be self-sufficient either way (see D1).

---

## Scope

1. `src/dev_health_ops/api/external_ingest/schemas.py` — Pydantic v2 models for the batch envelope, source descriptor, ingest window, and the 9 record kinds (`repository.v1`, `identity.v1`, `team.v1`, `work_item.v1`, `work_item_transition.v1`, `work_item_dependency.v1`, `pull_request.v1`, `review.v1`, `commit.v1`). These are the **single source of truth** — JSON Schema is generated from them, never hand-written.
2. `src/dev_health_ops/api/external_ingest/schema_registry.py` — builds the versioned schema bundle (JSON Schema $defs, per-record-kind index, examples) once at import time from the Pydantic models above, with an ETag.
3. `src/dev_health_ops/api/external_ingest/router.py` — `GET /api/v1/external-ingest/schemas` and `GET /api/v1/external-ingest/schemas/{schema_version}` (only these two routes; POST /batches, POST /validate, GET /batches/{id} are CHAOS-2691's routes — see D1 for what to do if that module doesn't exist yet).
4. `src/dev_health_ops/api/external_ingest/examples/*.json` — one canonical example payload per record kind, loaded by the registry and validated by tests.
5. `src/dev_health_ops/api/external_ingest/export_schemas.py` — CLI-invokable static export of the schema bundle to `docs/api/external-ingest/v1/*.schema.json`, mirroring the existing `graphql/export_schema.py` precedent.
6. Checked-in static artifacts under `docs/api/external-ingest/v1/` (generated, not hand-edited) + a no-drift test.
7. `docs/architecture/adr/003-external-ingest-schema-discovery.md` — record the versioning/caching/static-export decisions in-tree (house rule: document decisions in the same changeset).
8. Tests: schema generation unit tests, discovery-endpoint API tests, example-validates-against-model tests, ETag/caching tests, drift test.

## Out of scope

- `POST /batches`, `POST /validate`, `GET /batches/{id}` handler *logic* (CHAOS-2691) — this issue only needs the Pydantic models those routes will also use, and only implements the two `GET /schemas*` routes.
- Ingest-token auth / scopes enforcement (CHAOS-2696/2712) — schema discovery endpoints are public in this design (see D2); no token model is created here.
- Worker normalization, sinks, idempotency, source registration (CHAOS-2693/2695/2696/2697/2698).
- `dev-hops push validate` / `dev-hops push sample` CLI commands (CHAOS-2700) — this issue only needs to produce artifacts (`schema_registry.py` API + static JSON files) that CHAOS-2700 can import/fetch; do not implement CLI subcommands here.
- Any GitHub Actions/GitLab CI workflow files (CHAOS-2713) — the no-drift check added here is a **pytest**, not a new CI workflow.
- Web-side consumption (CHAOS-2714) — no `dev-health-web` changes.
- Deprecating/reconciling the legacy `/api/v1/ingest` router — flagged repeatedly in recon as a cross-cutting gap; out of scope for this issue, do not touch `src/dev_health_ops/api/ingest/`.

---

## Design decisions

**D1. Ownership split with CHAOS-2691 (both issues list `router.py`/`schemas.py` and both list `GET /schemas`+`GET /schemas/{schema_version}` in scope).**
Decision: whichever issue lands first creates `src/dev_health_ops/api/external_ingest/{__init__.py,router.py,schemas.py}` and registers the router in `main.py`. CHAOS-2692's implementer must check for the module's existence first:
- If it does **not** exist: create it. Populate `schemas.py` with the full envelope + 9 record models (this issue's scope) since POST /batches/POST /validate need the exact same models — CHAOS-2691 extends `router.py` with the write-path routes and `schemas.py` with only the response models it uniquely needs (`AcceptedResponse`, `ValidationResponse`), importing the record/envelope models from this issue's `schemas.py`. Leave a `# CHAOS-2691: implement POST /batches and POST /validate here` comment block in `router.py`.
- If it already exists (CHAOS-2691 landed first): do not recreate `schemas.py`; add the registry/export/discovery-route code on top of the existing envelope/record models, matching field names exactly as CHAOS-2691 defined them.
Rationale: avoids duplicate/conflicting Pydantic model definitions; the schema-generation code has a hard dependency on the record models regardless of which ticket writes them first. Flagged as `decisionsNeeded` for epic-owner visibility since it's a real scheduling/merge-conflict risk between two Backlog issues with identical file-path scope.

**D2. Discovery endpoints are public (no ingest-token auth).**
JSON Schema is not org-specific or sensitive in v1 (no per-org schema customization exists or is planned for v1). Public discovery lets a prospective customer, CI pipeline, or `dev-hops push validate` (offline mode) fetch/validate without first minting a token — consistent with `docs_url`/`openapi_url` being unconditionally public today. The plan's `schema:read` token scope is reserved for a **future** state (e.g., per-org schema customization or rate-limiting schema fetches per customer) and is a no-op scope in v1; do not gate these two routes behind `Depends(get_current_user)` or an ingest-token dependency. Flagged as `decisionsNeeded` for CHAOS-2696/2712 owners to confirm since it's the first place `schema:read` is defined without being enforced anywhere.

**D3. `schema_version` path/route semantics: it is the envelope-level API version (`external-ingest.v1`), not a per-record-kind version.**
`GET /schemas/{schema_version}` returns **one bundled JSON Schema document** containing the envelope schema plus all 9 record-kind schemas (via `$defs` + a `recordKinds` index of `$ref`s), for the single currently-supported value `external-ingest.v1`. This matches how the plan's CLI usage (`dev-hops push validate payload.json --schema external-ingest.v1`) and the `POST /batches` envelope's `schemaVersion` field both use `external-ingest.v1` as a single top-level version, not a per-kind one. Individual record-kind versions (`commit.v1`, `pull_request.v1`, ...) are named entries inside that bundle, not separate URL-addressable schema-version resources. Rationale: one version = one wire contract; bumping envelope shape (e.g., a required field added/removed) requires an `external-ingest.v2`; bumping a single record kind's shape independently is expressed by renaming that kind's Pydantic model / `kind` literal (`commit.v2`) while the envelope schema_version stays `v1` — both are representable in one bundle document without inventing two independent versioning axes.

**D4. `kind` field on records and in validation-error diagnostics is always the fully versioned string (e.g. `"pull_request.v1"`), never the bare `"pull_request"`.**
The core plan's `POST /validate` example response shows `"kind": "pull_request"` (unversioned) in the errors array, which contradicts the plan's own "Record kinds for v1" list (`pull_request.v1`). Decision: use the versioned string everywhere (payload records, validation errors, registry keys) because a batch could in principle mix `commit.v1` and a future `commit.v2` record before `commit.v1` is deprecated, and an unversioned `kind` string would make it ambiguous which schema an error refers to. This is a deliberate deviation from the plan doc's example JSON — call it out explicitly in the CHAOS-2691 implementer's PR description so the `POST /validate` response matches this issue's registry keys exactly. (If CHAOS-2691 already shipped with bare `kind` before this lands, reconcile by aliasing: registry lookup accepts both `"pull_request"` and `"pull_request.v1"`, but discovery responses only ever emit the versioned form.)

**D5. JSON Schema is generated via Pydantic v2's `pydantic.json_schema.models_json_schema()`, not per-model `model_json_schema()` calls stitched together by hand.**
Verified: `pydantic>=2.7.0` is already pinned in `pyproject.toml` (no new dependency needed) and no `model_json_schema`/`models_json_schema` usage exists anywhere in the codebase today — this is genuinely new code, not an extension of an existing helper. `models_json_schema()` takes `Sequence[tuple[type[BaseModel], JsonSchemaMode]]` and returns `(refs_by_model, definitions)` where `definitions["$defs"]` already has **collision-free** names across all 9+ models — this is the correct idiomatic way to combine multiple Pydantic models into one JSON Schema document and avoids hand-rolled `$ref` merging bugs.

**D6. Caching: in-process singleton computed once at import time, ETag = sha256 of canonical JSON, `Cache-Control: public, max-age=3600, must-revalidate`, `If-None-Match` → 304.**
Confirmed via grep: **no** ETag/Cache-Control/If-None-Match handling exists anywhere in the app today (only GraphQL has a query-size-limit middleware; no generic caching middleware). This is new code, implemented **inside the router handlers**, not as new app-wide middleware — schemas are the first genuinely cacheable, static-per-deploy REST resource in this codebase, so a narrow, local implementation is more appropriate than a generic caching middleware (which would need scope decisions for every other route). Compute the schema bundle + ETag once per process at module import (`schema_registry.py` top-level), not per-request — `model_json_schema()`/`models_json_schema()` calls are non-trivial reflection and there's no reason to redo them on every GET. If a future issue needs multiple envelope versions (`external-ingest.v2`), extend `_REGISTRY: dict[str, SchemaBundle]` keyed by version; this issue only populates `"external-ingest.v1"`.

**D7. Static artifact export: YES, checked into `docs/api/external-ingest/v1/`, generated via `export_schemas.py`, enforced by a no-drift pytest (not a new CI workflow).**
Precedent: `src/dev_health_ops/api/graphql/export_schema.py` already does exactly this pattern for GraphQL SDL (`python3 -m dev_health_ops.api.graphql.export_schema --out <path>`), consumed by `dev-health-web`'s exact-diff CI check. Mirror the module shape (argparse, `--out`, stdout fallback) for `external_ingest/export_schemas.py`. Rationale for doing this (vs. "customers just call the live endpoint"): (a) lets customers/CI vendor a schema file into their own repo for offline `ajv`/`jsonschema` validation without network access to FullChaos, (b) gives docs a stable linkable artifact, (c) the no-drift test catches accidental schema drift between the registry code and committed docs at PR time — cheap insurance since generation is deterministic and fast. Do **not** add a new GitHub Actions workflow for this (out of scope, and the existing gate — `bash ci/run_tests.sh unit` — already runs all pytest files, including the new no-drift test, for free).

**D8. Examples live as standalone JSON fixture files, not inline in Python, and are shared with CHAOS-2700.**
`src/dev_health_ops/api/external_ingest/examples/<kind>.json` (e.g. `repository.v1.json`), one realistic example object per record kind. Loaded by `schema_registry.py` and attached under each record-kind schema's `"examples"` array (standard JSON Schema keyword) in the discovery response and static export. `dev-hops push sample --kind <kind>` (CHAOS-2700, not this issue) should import `dev_health_ops.api.external_ingest.schema_registry.load_example(kind)` rather than duplicating example payloads — note this as an interface CHAOS-2700 depends on.

**D9. `GET /schemas/{schema_version}` does NOT support a `?kind=` narrowing query param in v1** — the full bundle is always returned; standard JSON Schema `$ref` resolution (ajv, jsonschema, etc.) lets customers validate a single record against `$defs.RepositoryRecordV1` from the one bundled document. Adding a narrowing param is low-cost but unnecessary complexity for a v1 with only 9 kinds and no acceptance-criteria requirement for it — explicitly deferred, not a gap.

---

## API / schema sketches

### Envelope + record Pydantic models (`schemas.py`)

```python
from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

# --- Source / window --------------------------------------------------------

class SourceDescriptor(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    type: Literal["customer_push"] = "customer_push"
    system: Literal["github", "gitlab", "jira", "linear", "custom"]
    instance: str = Field(..., min_length=1, description="e.g. github.com/acme")
    producer: str = Field(..., min_length=1, description="e.g. dev-hops-cli")
    producer_version: str | None = Field(default=None, alias="producerVersion")


class IngestWindow(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    started_at: datetime = Field(..., alias="startedAt")
    ended_at: datetime = Field(..., alias="endedAt")


# --- Record kinds (each is independently JSON-Schema-exportable) ----------

class RepositoryRecordV1(BaseModel):
    """repository.v1"""
    model_config = ConfigDict(populate_by_name=True)

    kind: Literal["repository.v1"] = "repository.v1"
    external_id: str = Field(..., alias="externalId", description="Customer-stable repo identifier, e.g. 'acme/api'")
    name: str
    url: str | None = None
    default_branch: str | None = Field(default=None, alias="defaultBranch")
    tags: list[str] = Field(default_factory=list)


class IdentityRecordV1(BaseModel):
    """identity.v1"""
    model_config = ConfigDict(populate_by_name=True)

    kind: Literal["identity.v1"] = "identity.v1"
    external_id: str = Field(..., alias="externalId")
    display_name: str | None = Field(default=None, alias="displayName")
    emails: list[str] = Field(default_factory=list)
    usernames: list[str] = Field(default_factory=list)


class TeamRecordV1(BaseModel):
    """team.v1"""
    model_config = ConfigDict(populate_by_name=True)

    kind: Literal["team.v1"] = "team.v1"
    external_id: str = Field(..., alias="externalId")
    name: str
    member_external_ids: list[str] = Field(default_factory=list, alias="memberExternalIds")


WorkItemProviderLiteral = Literal["jira", "github", "gitlab", "linear", "custom"]
WorkItemTypeLiteral = Literal[
    "story", "task", "bug", "epic", "issue", "incident", "chore", "unknown",
]
WorkItemStatusLiteral = Literal[
    "backlog", "todo", "in_progress", "in_review", "blocked", "done", "canceled", "unknown",
]


class WorkItemRecordV1(BaseModel):
    """work_item.v1 — mirrors dev_health_ops.models.work_items.WorkItem field names."""
    model_config = ConfigDict(populate_by_name=True)

    kind: Literal["work_item.v1"] = "work_item.v1"
    external_id: str = Field(..., alias="externalId")
    provider: WorkItemProviderLiteral
    title: str
    type: WorkItemTypeLiteral = "unknown"
    status: WorkItemStatusLiteral = "unknown"
    status_raw: str | None = Field(default=None, alias="statusRaw")
    description: str | None = None
    repository_external_id: str | None = Field(default=None, alias="repositoryExternalId")
    project_key: str | None = Field(default=None, alias="projectKey")
    assignees: list[str] = Field(default_factory=list)
    reporter: str | None = None
    created_at: datetime = Field(..., alias="createdAt")
    updated_at: datetime | None = Field(default=None, alias="updatedAt")
    started_at: datetime | None = Field(default=None, alias="startedAt")
    completed_at: datetime | None = Field(default=None, alias="completedAt")
    labels: list[str] = Field(default_factory=list)
    story_points: float | None = Field(default=None, alias="storyPoints")
    priority_raw: str | None = Field(default=None, alias="priorityRaw")
    url: str | None = None


class WorkItemTransitionRecordV1(BaseModel):
    """work_item_transition.v1 — mirrors WorkItemStatusTransition."""
    model_config = ConfigDict(populate_by_name=True)

    kind: Literal["work_item_transition.v1"] = "work_item_transition.v1"
    work_item_external_id: str = Field(..., alias="workItemExternalId")
    provider: WorkItemProviderLiteral
    occurred_at: datetime = Field(..., alias="occurredAt")
    from_status: WorkItemStatusLiteral | None = Field(default=None, alias="fromStatus")
    to_status: WorkItemStatusLiteral = Field(..., alias="toStatus")
    from_status_raw: str | None = Field(default=None, alias="fromStatusRaw")
    to_status_raw: str | None = Field(default=None, alias="toStatusRaw")
    actor: str | None = None


class WorkItemDependencyRecordV1(BaseModel):
    """work_item_dependency.v1 — mirrors WorkItemDependency."""
    model_config = ConfigDict(populate_by_name=True)

    kind: Literal["work_item_dependency.v1"] = "work_item_dependency.v1"
    source_work_item_external_id: str = Field(..., alias="sourceWorkItemExternalId")
    target_work_item_external_id: str = Field(..., alias="targetWorkItemExternalId")
    relationship_type: Literal["blocks", "blocked_by", "relates_to", "duplicates", "unknown"] = Field(
        ..., alias="relationshipType"
    )
    relationship_type_raw: str = Field(..., alias="relationshipTypeRaw")


class PullRequestReviewV1(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    review_id: str = Field(..., alias="reviewId")
    reviewer: str
    state: Literal["APPROVED", "CHANGES_REQUESTED", "COMMENTED", "DISMISSED"]
    submitted_at: datetime = Field(..., alias="submittedAt")


class PullRequestRecordV1(BaseModel):
    """pull_request.v1 — mirrors GitPullRequest."""
    model_config = ConfigDict(populate_by_name=True)

    kind: Literal["pull_request.v1"] = "pull_request.v1"
    repository_external_id: str = Field(..., alias="repositoryExternalId")
    number: int
    title: str | None = None
    body: str | None = None
    state: Literal["open", "closed", "merged"]
    author_name: str = Field(..., alias="authorName")
    author_email: str | None = Field(default=None, alias="authorEmail")
    created_at: datetime = Field(..., alias="createdAt")
    merged_at: datetime | None = Field(default=None, alias="mergedAt")
    closed_at: datetime | None = Field(default=None, alias="closedAt")
    head_branch: str | None = Field(default=None, alias="headBranch")
    base_branch: str | None = Field(default=None, alias="baseBranch")
    additions: int | None = None
    deletions: int | None = None
    changed_files: int | None = Field(default=None, alias="changedFiles")


class ReviewRecordV1(BaseModel):
    """review.v1 — standalone (also embeddable via PullRequestRecordV1.reviews in v1 payloads,
    but exists as its own record kind for reviews reported independently of a full PR object,
    e.g. a review posted on a PR FullChaos already knows about)."""
    model_config = ConfigDict(populate_by_name=True)

    kind: Literal["review.v1"] = "review.v1"
    repository_external_id: str = Field(..., alias="repositoryExternalId")
    pull_request_number: int = Field(..., alias="pullRequestNumber")
    review_id: str = Field(..., alias="reviewId")
    reviewer: str
    state: Literal["APPROVED", "CHANGES_REQUESTED", "COMMENTED", "DISMISSED"]
    submitted_at: datetime = Field(..., alias="submittedAt")


class CommitRecordV1(BaseModel):
    """commit.v1 — mirrors GitCommit."""
    model_config = ConfigDict(populate_by_name=True)

    kind: Literal["commit.v1"] = "commit.v1"
    repository_external_id: str = Field(..., alias="repositoryExternalId")
    hash: str
    message: str | None = None
    author_name: str | None = Field(default=None, alias="authorName")
    author_email: str | None = Field(default=None, alias="authorEmail")
    author_when: datetime = Field(..., alias="authorWhen")
    committer_name: str | None = Field(default=None, alias="committerName")
    committer_email: str | None = Field(default=None, alias="committerEmail")
    committer_when: datetime | None = Field(default=None, alias="committerWhen")
    parents: int = 1


# --- Discriminated union + envelope ----------------------------------------

RecordV1 = Annotated[
    RepositoryRecordV1
    | IdentityRecordV1
    | TeamRecordV1
    | WorkItemRecordV1
    | WorkItemTransitionRecordV1
    | WorkItemDependencyRecordV1
    | PullRequestRecordV1
    | ReviewRecordV1
    | CommitRecordV1,
    Field(discriminator="kind"),
]


class BatchEnvelopeV1(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    schema_version: Literal["external-ingest.v1"] = Field(..., alias="schemaVersion")
    idempotency_key: str = Field(..., alias="idempotencyKey", min_length=1, max_length=256)
    source: SourceDescriptor
    window: IngestWindow
    records: list[RecordV1] = Field(..., min_length=1, max_length=5000)
```

Notes:
- `repository_external_id` / `work_item_external_id` etc. are **customer-supplied external identifiers**, not FullChaos-internal UUIDs — the worker (CHAOS-2697) is responsible for deriving `Repo.id` via `get_repo_uuid_from_repo()` from `f"{source.system}:{source.instance}:{repository_external_id}"` or similar, so the customer never needs to know FullChaos's UUID scheme. This issue only defines the wire shape; do not invent a UUID field on these models.
- `max_length=5000` on `records` is a placeholder max-batch-size; CHAOS-2691 owns the authoritative limit — keep this in sync via a shared constant `MAX_RECORDS_PER_BATCH` in `schemas.py` if CHAOS-2691 lands after this issue (put the constant here since this file is the shared source of truth per D1).
- All field aliases use camelCase (`populate_by_name=True` allows both) to match the plan's JSON examples (`schemaVersion`, `idempotencyKey`) and the existing `product_telemetry` module's alias convention (`ConfigDict(populate_by_name=True)` + `Field(alias=...)`) — reuse that exact idiom, don't introduce a different casing convention.

### Schema registry (`schema_registry.py`)

```python
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from functools import lru_cache
from importlib import resources
from typing import Any

from pydantic import BaseModel
from pydantic.json_schema import models_json_schema

from . import schemas as s

SUPPORTED_SCHEMA_VERSIONS = ("external-ingest.v1",)

_RECORD_MODELS: dict[str, type[BaseModel]] = {
    "repository.v1": s.RepositoryRecordV1,
    "identity.v1": s.IdentityRecordV1,
    "team.v1": s.TeamRecordV1,
    "work_item.v1": s.WorkItemRecordV1,
    "work_item_transition.v1": s.WorkItemTransitionRecordV1,
    "work_item_dependency.v1": s.WorkItemDependencyRecordV1,
    "pull_request.v1": s.PullRequestRecordV1,
    "review.v1": s.ReviewRecordV1,
    "commit.v1": s.CommitRecordV1,
}


@dataclass(frozen=True)
class SchemaBundle:
    schema_version: str
    document: dict[str, Any]  # full JSON Schema doc incl. $defs
    etag: str
    record_kinds: tuple[str, ...]


def _load_example(kind: str) -> dict[str, Any]:
    data = resources.files("dev_health_ops.api.external_ingest.examples").joinpath(
        f"{kind}.json"
    ).read_text()
    return json.loads(data)


@lru_cache(maxsize=1)
def _build_v1_bundle() -> SchemaBundle:
    models_and_modes = [(s.BatchEnvelopeV1, "validation")] + [
        (model, "validation") for model in _RECORD_MODELS.values()
    ]
    refs, definitions = models_json_schema(
        models_and_modes,
        ref_template="#/$defs/{model}",
    )

    record_index: dict[str, Any] = {}
    for kind, model in _RECORD_MODELS.items():
        ref = refs[(model, "validation")]
        record_index[kind] = {**ref, "examples": [_load_example(kind)]}

    document: dict[str, Any] = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "https://api.fullchaos.dev/api/v1/external-ingest/schemas/external-ingest.v1",
        "title": "external-ingest.v1",
        "description": "FullChaos external customer-push ingestion contract, v1.",
        "envelope": refs[(s.BatchEnvelopeV1, "validation")],
        "recordKinds": record_index,
        **definitions,  # contributes "$defs": {...}
    }

    canonical = json.dumps(document, sort_keys=True, separators=(",", ":"))
    etag = hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    return SchemaBundle(
        schema_version="external-ingest.v1",
        document=document,
        etag=f'"{etag}"',
        record_kinds=tuple(_RECORD_MODELS),
    )


def get_bundle(schema_version: str) -> SchemaBundle | None:
    if schema_version == "external-ingest.v1":
        return _build_v1_bundle()
    return None


def list_versions() -> list[dict[str, Any]]:
    bundle = _build_v1_bundle()
    return [
        {
            "schemaVersion": bundle.schema_version,
            "recordKinds": list(bundle.record_kinds),
        }
    ]


def load_example(kind: str) -> dict[str, Any]:
    """Public accessor for CHAOS-2700 (`dev-hops push sample --kind`)."""
    if kind not in _RECORD_MODELS:
        raise KeyError(f"Unknown record kind: {kind!r}")
    return _load_example(kind)
```

Notes:
- `@lru_cache(maxsize=1)` gives the "compute once" behavior (D6) without needing a manual module-level global + import-order concerns; call `_build_v1_bundle()` once eagerly at router import time too (not lazily on first request) so schema-generation errors surface at app startup, not on a customer's first request — add a `_build_v1_bundle()` call inside `router.py`'s module body (bare statement, result discarded) to force this.
- `models_json_schema`'s `ref_template` param — verify the exact signature against the installed pydantic version during implementation (`python -c "from pydantic.json_schema import models_json_schema; help(models_json_schema)"` in the venv) before treating the sketch above as final; the parameter exists in pydantic 2.x but double-check the default ref_template format matches `#/$defs/{model}` before assuming it needs overriding — omit the `ref_template=` kwarg if the default already produces `#/$defs/...` refs (do not blindly keep an unnecessary override).

### Router (`router.py`)

```python
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request, Response, status

from .schema_registry import get_bundle, list_versions

router = APIRouter(prefix="/api/v1/external-ingest", tags=["external-ingest"])


@router.get("/schemas")
async def list_schemas() -> dict:
    return {"schemaVersions": list_versions()}


@router.get("/schemas/{schema_version}")
async def get_schema(schema_version: str, request: Request, response: Response) -> dict:
    bundle = get_bundle(schema_version)
    if bundle is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Unknown schema version")

    if request.headers.get("if-none-match") == bundle.etag:
        response.status_code = status.HTTP_304_NOT_MODIFIED
        response.headers["ETag"] = bundle.etag
        return {}

    response.headers["ETag"] = bundle.etag
    response.headers["Cache-Control"] = "public, max-age=3600, must-revalidate"
    return bundle.document
```

Mount in `main.py` (follow the existing `from .ingest import router as ingest_router` / `app.include_router(ingest_router)` idiom exactly):

```python
# alongside the other .api submodule imports, alphabetically near .ingest:
from .external_ingest import router as external_ingest_router
...
app.include_router(external_ingest_router)   # add near app.include_router(ingest_router)
```

If `src/dev_health_ops/api/external_ingest/__init__.py` doesn't yet exist per D1, create it as `from .router import router  # noqa: F401` (matches `ingest/__init__.py`'s shape — verify by reading it before copying).

### `GET /schemas` response shape

```json
{
  "schemaVersions": [
    {
      "schemaVersion": "external-ingest.v1",
      "recordKinds": [
        "repository.v1", "identity.v1", "team.v1",
        "work_item.v1", "work_item_transition.v1", "work_item_dependency.v1",
        "pull_request.v1", "review.v1", "commit.v1"
      ]
    }
  ]
}
```

### `GET /schemas/external-ingest.v1` response shape (abbreviated)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://api.fullchaos.dev/api/v1/external-ingest/schemas/external-ingest.v1",
  "title": "external-ingest.v1",
  "envelope": { "$ref": "#/$defs/BatchEnvelopeV1" },
  "recordKinds": {
    "commit.v1": {
      "$ref": "#/$defs/CommitRecordV1",
      "examples": [ { "kind": "commit.v1", "repositoryExternalId": "acme/api", "hash": "abc123", "authorWhen": "2026-06-25T10:00:00Z", "authorName": "Alice", "authorEmail": "alice@example.com", "message": "fix: resolve login bug" } ]
    }
  },
  "$defs": {
    "BatchEnvelopeV1": { "type": "object", "...": "..." },
    "CommitRecordV1": { "type": "object", "...": "..." }
  }
}
```

Response headers on the version-scoped endpoint: `ETag: "<sha256>"`, `Cache-Control: public, max-age=3600, must-revalidate`.

---

## Files to create/modify

Create:
- `src/dev_health_ops/api/external_ingest/__init__.py` (only if not present per D1)
- `src/dev_health_ops/api/external_ingest/schemas.py` (only if not present per D1; otherwise add to existing file)
- `src/dev_health_ops/api/external_ingest/schema_registry.py`
- `src/dev_health_ops/api/external_ingest/router.py` (only if not present per D1; otherwise add the two GET routes to existing file)
- `src/dev_health_ops/api/external_ingest/export_schemas.py`
- `src/dev_health_ops/api/external_ingest/examples/__init__.py` (empty, makes it a package for `importlib.resources`)
- `src/dev_health_ops/api/external_ingest/examples/repository.v1.json`
- `src/dev_health_ops/api/external_ingest/examples/identity.v1.json`
- `src/dev_health_ops/api/external_ingest/examples/team.v1.json`
- `src/dev_health_ops/api/external_ingest/examples/work_item.v1.json`
- `src/dev_health_ops/api/external_ingest/examples/work_item_transition.v1.json`
- `src/dev_health_ops/api/external_ingest/examples/work_item_dependency.v1.json`
- `src/dev_health_ops/api/external_ingest/examples/pull_request.v1.json`
- `src/dev_health_ops/api/external_ingest/examples/review.v1.json`
- `src/dev_health_ops/api/external_ingest/examples/commit.v1.json`
- `docs/api/external-ingest/v1/schema.json` (generated by `export_schemas.py`, checked in)
- `docs/architecture/adr/003-external-ingest-schema-discovery.md`
- `tests/api/external_ingest/__init__.py`
- `tests/api/external_ingest/test_schema_registry.py`
- `tests/api/external_ingest/test_schemas_api.py`
- `tests/api/external_ingest/test_examples_validate.py`
- `tests/api/external_ingest/test_schema_export_no_drift.py`

Modify:
- `src/dev_health_ops/api/main.py` — add import + `app.include_router(external_ingest_router)` (only if D1 requires this issue to create the module; if CHAOS-2691 already wired it, skip).
- `pyproject.toml` — likely **no change** (pydantic>=2.7.0 already covers `models_json_schema`); only touch if the exact pydantic minor needed for a `models_json_schema` kwarg used above is higher than 2.7.0 — verify in venv first (`python -c "import pydantic; print(pydantic.VERSION)"`), constrain the range in `pyproject.toml` if a bump is genuinely required (never edit `uv.lock` to fix this).

Do not modify: `src/dev_health_ops/api/ingest/*` (legacy module, out of scope), `src/dev_health_ops/api/product_telemetry/*`, any `dev-health-web` files, any Alembic/ClickHouse migration files (this issue has no persistence).

---

## Test plan

All new tests are **pure-Python / in-process ASGI** — this issue has no ClickHouse or Postgres dependency, so no `@pytest.mark.clickhouse` tests are needed here (the schema registry is computed from Pydantic classes only). Confirm this remains true during implementation; if a future revision adds org-scoped schema storage, that would need clickhouse-marked tests, but v1 as designed does not.

Unit tests (`tests/api/external_ingest/test_schema_registry.py`):
- `get_bundle("external-ingest.v1")` returns a non-None bundle whose `document["$defs"]` contains all 9 record model names + `BatchEnvelopeV1`.
- `get_bundle("external-ingest.v2")` (or any unsupported string) returns `None`.
- `list_versions()` returns exactly one entry with all 9 record kinds present and no duplicates.
- `bundle.etag` is stable across repeated calls (`_build_v1_bundle()` is idempotent thanks to `lru_cache`) and changes if a model's field set changes (parametrize with a locally-defined dummy model swapped via monkeypatch, or simply assert etag is a 64-hex-char sha256 wrapped in quotes — don't over-engineer a mutation test here).
- `load_example("commit.v1")` returns a dict; `load_example("nonexistent.v1")` raises `KeyError`.

API tests (`tests/api/external_ingest/test_schemas_api.py`), using the `ASGITransport` + `AsyncClient` pattern from `tests/test_ingest_api.py`:
- `GET /api/v1/external-ingest/schemas` → 200, body lists `external-ingest.v1` and all 9 record kinds.
- `GET /api/v1/external-ingest/schemas/external-ingest.v1` → 200, `ETag` header present and matches `^"[0-9a-f]{64}"$`, `Cache-Control` header present, body has `$defs`, `envelope`, `recordKinds` keys.
- `GET /api/v1/external-ingest/schemas/external-ingest.v1` with `If-None-Match: <etag from previous response>` → 304, empty body.
- `GET /api/v1/external-ingest/schemas/bogus-version` → 404.
- Confirm the routes are **not** gated by `Depends(get_current_user)` — call with no `Authorization` header and assert 200 (locks in D2; a future PR that accidentally adds auth should fail this test loudly).
- Confirm the routes appear in `/openapi.json` (`GET /openapi.json` → assert `/api/v1/external-ingest/schemas` key present) since D2 says they're public REST, so should show up like any other public-ish route (mirrors the api-app recon note that `docs_url`/`openapi_url` are always on).

Examples validate against models (`tests/api/external_ingest/test_examples_validate.py`), parametrized over `_RECORD_MODELS` (import the private dict directly, or add a small public `iter_record_kinds()` helper to `schema_registry.py` if a leading-underscore import feels wrong per repo lint conventions — check `ruff` config for private-import rules before deciding):
- For every `(kind, model)` pair, `model.model_validate(load_example(kind))` does not raise. This directly satisfies the acceptance criterion "Examples pass validation" and will automatically cover any record kind added later.
- Every example's `"kind"` field value equals the registry key exactly (locks in D4 — versioned kind everywhere).

No-drift test (`tests/api/external_ingest/test_schema_export_no_drift.py`):
- Call the same document-building function `export_schemas.py` uses (import it, don't shell out) to produce the bundle in-memory, `json.dumps(..., indent=2, sort_keys=True)`, and compare byte-for-byte against `docs/api/external-ingest/v1/schema.json` read from disk. Fail with a clear message ("run `python -m dev_health_ops.api.external_ingest.export_schemas --out docs/api/external-ingest/v1/schema.json` and commit the diff") if they differ.

mypy: all new modules must type-check cleanly under `mypy --install-types --non-interactive .` — pay particular attention to `models_json_schema`'s return type (pydantic ships types; avoid `# type: ignore` unless genuinely needed after checking the installed pydantic's `.pyi`).

---

## Gate commands

Ops (from the worktree root, using its own `.venv`):

```bash
cd /Users/chris/projects/full-chaos/dev-health/ops/.claude/worktrees/chaos-2690-integration
# Full local-validate gate (ruff format/check, mypy, full unit tier, isolated live-CH stage).
# This issue adds NO ClickHouse-touching code, so the live-CH stage should be a no-op pass-through,
# but still run it — it's the standing pre-push gate for this repo, not opt-in per-issue.
SCRATCH_DB=ci_local_validate_chaos2692 bash ci/local_validate.sh

# If you want to skip the live-CH stage explicitly while iterating (schema code has no CH dependency):
SKIP_CLICKHOUSE=1 bash ci/local_validate.sh

# Targeted fast loop while iterating:
.venv/bin/ruff format --check src/dev_health_ops/api/external_ingest tests/api/external_ingest
.venv/bin/ruff check src/dev_health_ops/api/external_ingest tests/api/external_ingest
.venv/bin/mypy --install-types --non-interactive src/dev_health_ops/api/external_ingest
.venv/bin/pytest tests/api/external_ingest -v
```

No web-side changes in this issue, so the web gate (`ci/run_tests.sh format/quality/unit` + Playwright e2e) is **not applicable** to CHAOS-2692 — do not run it unless CHAOS-2714 (web setup screens) later needs to fetch this schema for client-side display, which is out of this issue's scope.

---

## Live verification procedure

This issue has no database dependency, so live verification is just "start the API, hit the two new routes, confirm shape + caching headers." Uses the existing dev compose stack (already running per project convention; do not `docker compose up` fresh unless it's down — check first).

```bash
cd /Users/chris/projects/full-chaos/dev-health/ops/.claude/worktrees/chaos-2690-integration

# Confirm the API container is up (port 8000 per compose.yml); if not, this issue's routes can
# also be verified without any container via a local uvicorn run against the venv (no DB needed
# since these routes touch no DB):
.venv/bin/uvicorn dev_health_ops.api.main:app --port 8000 &
sleep 2

# 1. List versions
curl -sS http://localhost:8000/api/v1/external-ingest/schemas | python3 -m json.tool

# 2. Fetch the v1 bundle, inspect headers
curl -isS http://localhost:8000/api/v1/external-ingest/schemas/external-ingest.v1 | head -20

# 3. Confirm ETag round-trip: re-request with If-None-Match, expect 304
ETAG=$(curl -sS -D - -o /dev/null http://localhost:8000/api/v1/external-ingest/schemas/external-ingest.v1 | grep -i etag | cut -d' ' -f2 | tr -d '\r')
curl -isS -H "If-None-Match: $ETAG" http://localhost:8000/api/v1/external-ingest/schemas/external-ingest.v1 | head -5
# expect: HTTP/1.1 304 Not Modified

# 4. Confirm unknown version -> 404
curl -isS http://localhost:8000/api/v1/external-ingest/schemas/external-ingest.v2 | head -5

# 5. Confirm each record kind's schema validates its own example via an external tool
#    (belt-and-suspenders vs. the in-repo pytest — uses a genuinely independent JSON Schema
#    validator, not pydantic itself, to catch any accidental $ref-generation regression):
python3 -c "
import json, urllib.request
import jsonschema  # pip install jsonschema if not already a dev dep; check pyproject first
doc = json.load(urllib.request.urlopen('http://localhost:8000/api/v1/external-ingest/schemas/external-ingest.v1'))
for kind, entry in doc['recordKinds'].items():
    ref = entry['\$ref']
    example = entry['examples'][0]
    schema = {**doc, '\$ref': ref}
    jsonschema.validate(example, schema)
    print('OK', kind)
"

kill %1  # stop the ad-hoc uvicorn
```

If `jsonschema` isn't already a dev dependency, either add it to `pyproject.toml`'s `[project.optional-dependencies].dev` (constrain a range, don't pin exact-equals per house rule) or skip step 5 and rely on the in-repo pytest (`test_examples_validate.py`) which validates via Pydantic directly — Pydantic validation is a legitimate stand-in for JSON-Schema validation since the JSON Schema is derived from the same models, but an independent validator is stronger proof the *exported* JSON Schema (not just the Python model) is well-formed. Prefer adding `jsonschema` as a dev dep if the CI budget allows; otherwise document this as a manual-only verification step.

Static export verification:

```bash
.venv/bin/python -m dev_health_ops.api.external_ingest.export_schemas --out docs/api/external-ingest/v1/schema.json
git diff --stat docs/api/external-ingest/v1/schema.json   # expect no diff if already committed correctly
```

---

## Dependencies on other sub-issues

- **CHAOS-2691** (External ingest REST contract and schemas) — shares `router.py`/`schemas.py` file paths and the envelope/record Pydantic models; see D1 for the concrete resolution. Coordinate landing order or rebase carefully — this is a real merge-conflict risk, not just a modeling nicety.
- **CHAOS-2700** (dev-hops push CLI) depends on this issue's `schema_registry.load_example()` (for `dev-hops push sample`) and the static export artifact / registry API (for `dev-hops push validate` running offline without hitting the live API) — CHAOS-2700 should NOT duplicate example payloads.
- **CHAOS-2696 / CHAOS-2712** (source registration, ingest token scopes, authorization model) — own the `schema:read` token scope's actual enforcement (if ever implemented); D2 makes it a no-op for v1's public discovery routes. Flag this brief's D2 to those issue owners so `schema:read` isn't silently assumed to gate `/schemas*` elsewhere.
- **CHAOS-2701 / CHAOS-2711 / CHAOS-2713** (customer examples/docs, dev docs, CI/CD examples) will link to `docs/api/external-ingest/v1/schema.json` and the example payloads produced here — no code dependency, just content reuse.

## Risks

- **Ownership overlap with CHAOS-2691 (D1)** is the single biggest risk — if both issues are picked up by different agents/PRs concurrently without reading this brief, expect duplicate/conflicting `schemas.py` definitions and a router registration collision in `main.py`. Mitigate by checking `git log`/`ls src/dev_health_ops/api/external_ingest/` before starting, exactly as this brief did.
- **`models_json_schema()` API signature drift across pydantic 2.x minor versions** — the sketch above is based on the documented 2.x API but the exact kwarg names (`ref_template`, mode literal values `"validation"` vs `"serialization"`) should be confirmed against the *installed* pydantic version before implementation, not assumed from memory (recon confirmed pydantic>=2.7.0 is pinned but did not confirm the exact resolved version in the venv).
- **D4's versioned-`kind`-everywhere decision may conflict with whatever CHAOS-2691 ships first** if that issue's implementer copies the plan doc's example verbatim (bare `"kind": "pull_request"`). Needs an explicit cross-PR review comment, not just this brief, since the plan doc itself is the thing being deviated from.
- **`importlib.resources` example-loading requires the `examples/` directory to be packaged** — confirm `pyproject.toml`'s package-data / `[tool.setuptools.package-data]` (or equivalent for whatever build backend is configured) includes `*.json` under `src/dev_health_ops/api/external_ingest/examples/`, or the examples will work in a source checkout but silently fail to load once the package is built as a wheel (relevant for the `dev-hops` console-script distribution). Check `pyproject.toml`'s packaging section before assuming this "just works."
- **Static export drift test false negative on dict key ordering** — `json.dumps(..., sort_keys=True)` must be used consistently both when writing `docs/api/external-ingest/v1/schema.json` (in `export_schemas.py`) and when comparing in the no-drift test, or spurious ordering-only diffs will flake the gate.
