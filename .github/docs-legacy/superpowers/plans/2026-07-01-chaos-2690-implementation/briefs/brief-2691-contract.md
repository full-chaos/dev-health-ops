# CHAOS-2691 Implementation Brief: External Ingest REST Contract and Schemas

> **SYNTHESIZER RECONCILIATION (authoritative — see master-spec.md; overrides body below):**
> 1. **D9 is CORRECTED**: `repository.v1.externalId` is the provider **full name**
>    (`owner/repo` / `group/subgroup/project`), NOT a canonical URL. Verified in code:
>    `processors/github.py:1572` passes `repo=repo_info.full_name`, `processors/gitlab.py:1815`
>    passes `path_with_namespace`; `get_repo_uuid_from_repo` lowercases/strips. For
>    `system="custom"`, the UUID seed is `custom:{source_instance}:{externalId}`.
>    `source.instance` == `repository.v1.externalId` for git systems (repo-grain, CC5).
> 2. **WorkItemV1**: replace `work_item_id` ("jira:ABC-123") with `external_key`
>    (alias `externalKey`, provider-native key: `ABC-123`, `CHAOS-123`, issue/PR number).
>    The namespaced `work_item_id` (`jira:`/`gh:`/`ghpr:`/`gitlab:#`/`gitlab:!`) is derived
>    SERVER-SIDE in `external_ingest/ids.py` (CHAOS-2698). `WorkItemTransitionV1` and
>    `WorkItemDependencyV1` likewise take `externalKey`(s) + optional `workItemType`
>    (disambiguates issue vs pr/merge_request namespaces for github/gitlab).
> 3. `BatchEnvelope.records`: `min_length=1` (empty batches are a 400, per 2694 D5).
> 4. Rate limiting: do NOT create a second `Limiter`. Add `get_ingest_token_key`
>    (sha256(token)[:16], IP fallback) + `INGEST_BATCH_LIMIT="60/minute"` /
>    `INGEST_VALIDATE_LIMIT="60/minute"` to `api/middleware/rate_limit.py` (this issue,
>    wave 1) and apply via the shared `limiter` singleton. 429 uses the external-ingest
>    error envelope (`rate_limited`).
> 5. `GET /schemas` response also includes
>    `"limits": {"maxRecordsPerBatch": <env>, "maxBodyBytes": <env>}` (CLI pre-check reads it).
> 6. Interim `enqueue_batch()` signature gains `record_count`, `window_started_at`,
>    `window_ended_at` kwargs (pointer fields, CC9). CHAOS-2693 later drops the
>    `payload_json` kwarg and moves the payload to Postgres (`external_ingest_batch_payloads`),
>    updating the router call site in its own PR — an approved, planned call-site change
>    (the "must not change signature" sentence below is softened accordingly).
> 7. Deliverables += `docs/architecture/adr-003-external-ingest-rest-boundary.md`
>    (the epic's backend ADR: REST-vs-GraphQL, one-active-owner, token scopes).
> 8. Idempotency-replay: final behavior (2695, wave 4) returns **200 OK with full status
>    envelope** on REPLAY; this ticket's interim flow always 202s (no idempotency store yet).
> 9. Canonical error-code vocabulary and batch-status enum: master-spec CC16/CC12
>    (adds `invalid_token`, `source_not_registered`, `source_mismatch`, `not_found`,
>    `rate_limited`; batch statuses `accepted|stream_unavailable|processing|completed|partial|failed`).
> 10. Interim auth.py is INTEGRATION-BRANCH-ONLY; merging to main is gated on CHAOS-2712.
> 11. **POST-CRITIQUE (CC14): interim auth is MECHANICALLY gated** — the interim
>     dependency HARD-FAILS `503 auth_not_configured` unless env
>     `EXTERNAL_INGEST_INSECURE_AUTH=1` (set only in local compose/test env; never
>     deployed). Loud WARNING log per request stays. 2712 deletes flag + body.
>     `auth_not_configured` joins the 503 row of the error vocabulary.
> 12. **POST-CRITIQUE (CC17): this issue CREATES `external_ingest/validate.py`
>     COMPLETE in wave 1** (deep per-record validation over schemas.py models; powers
>     POST /validate). 2697 imports it UNCHANGED in wave 4 — single owner, no divergence
>     between endpoint and worker validation.
> 13. **POST-CRITIQUE (CC15): + `INGEST_READ_LIMIT="120/minute"`** in rate_limit.py,
>     applied to GET /schemas and GET /schemas/{version} (public → IP-keyed via the
>     token-or-IP key func). 2694 applies the same constant to its batch GETs.
> 14. **POST-CRITIQUE (CC5): adr-003 gains the ownership-matching residual-risk note**
>     (Linear team-UUID vs team-key cannot be equated without an API call; org-wide
>     `"linear"` placeholder owns all teams; docs instruct disabling managed Linear
>     before enabling customer push for the same team).

Repo: `ops` (`/Users/chris/projects/full-chaos/dev-health/ops`, worktree
`chaos-2690-integration`, branch `chaos-2690-external-ingest`).

This brief resolves every open design question left by the plan docs and the
Linear issue text for CHAOS-2691 specifically. It is scoped tightly to what
CHAOS-2691 actually owns; sibling sub-issues (2692-2699+) own the rest of the
epic and are called out explicitly wherever this ticket's code creates a seam
for them.

---

## Scope

CHAOS-2691 delivers the REST contract layer only:

1. `src/dev_health_ops/api/external_ingest/` package: `__init__.py`,
   `router.py`, `schemas.py`, `errors.py`.
2. Pydantic v2 models for: batch envelope, source descriptor, ingest window,
   the 9 versioned record-kind payloads, validation response/errors,
   accepted-batch response, and a standard external-ingest error envelope.
3. Four endpoints, wired into `main.py`:
   - `POST /api/v1/external-ingest/batches`
   - `POST /api/v1/external-ingest/validate`
   - `GET /api/v1/external-ingest/schemas`
   - `GET /api/v1/external-ingest/schemas/{schema_version}`
4. Structural + semantic validation of envelopes and per-kind records
   (unknown kind, missing required fields, bad literals, oversized batch,
   oversized body).
5. A **minimal, real (not a mock) interim implementation** of two seams the
   router depends on, so the endpoints are actually testable end-to-end
   without waiting on sibling tickets:
   - `streams.enqueue_batch()` — thin Valkey XADD writer, raise-on-failure
     (CHAOS-2693 hardens this into the full DLQ/consumer-group system; it
     must preserve this function's signature and stream-naming convention).
   - `auth.require_ingest_scope()` — interim bearer-token + `X-Org-Id`
     dependency (CHAOS-2696/CHAOS-2712 replace the body with real DB-backed
     `IngestToken` validation; the `IngestAuthContext` shape and
     `Depends(...)` call sites in `router.py` must not need to change).

## Out of scope (owned by sibling sub-issues — do not implement here)

- `GET /api/v1/external-ingest/batches/{ingestion_id}` and all status/
  rejected-record persistence — **CHAOS-2694**.
- Idempotency conflict enforcement against durable storage (payload-hash
  comparison, 409 semantics beyond the response *shape*) — **CHAOS-2695**.
- Real ingest-token issuance, scopes, source registration, one-active-owner
  policy enforcement — **CHAOS-2696** (data model) / **CHAOS-2712**
  (authorization model/credential lifecycle).
- Durable stream hardening: DLQ, consumer-group wiring, `StreamConsumer`
  subclass, Celery beat schedule, `compose.yml` worker wiring — **CHAOS-2693**.
- Worker normalization into `WorkItem`/`Repo`/`GitCommit`/etc., sink writes —
  **CHAOS-2697** / **CHAOS-2698**.
- Bounded metric recomputation — **CHAOS-2699**.
- `dev-hops push` CLI — **CHAOS-2700**.
- Schema-registry abstraction, per-kind JSON-Schema examples, "examples pass
  validation" CI check — **CHAOS-2692** (this ticket ships a working
  `GET /schemas*` pair using `model_json_schema()` directly off the Pydantic
  classes defined here; CHAOS-2692 is additive polish, not a rewrite — see
  Design Decision D8).
- Web onboarding screens, CI/CD example docs, webhook relay — CHAOS-2713/
  2714/2715.
- The legacy `/api/v1/ingest` router — untouched. No deprecation/merge in
  this ticket (flag only; see Risks).

---

## Design decisions

**D1. Idempotency-Key: body field is canonical; header is an optional alias
that must match.**
The core plan's envelope JSON shows `idempotencyKey` in the body; the
webhooks/setup addendum's cURL example sends an `Idempotency-Key` HTTP
header. Both are real, in-tree documents — resolve, don't pick one silently.
Decision: `BatchEnvelope.idempotencyKey` (body) is **required** and
authoritative (it participates in the batch-identity tuple
`org_id + source_system + source_instance + idempotencyKey`, which needs to
be inside the payload the worker/CLI can replay offline). If the HTTP
`Idempotency-Key` header is also present, it must equal the body value or
the request is rejected with `400 idempotency_key_mismatch`. This lets the
generic cURL/CI examples keep using the header (matches Stripe-style
ergonomics customers expect) while keeping a single source of truth for the
hashing/business key. `dev-hops push batch` (CHAOS-2700) should send both.

**D2. 400 vs 422 for malformed envelopes — bypass FastAPI's automatic body
parsing.**
The app-wide convention (`api/_errors.py`) maps `RequestValidationError` →
`422` with a `{"detail": {...}}` shape. The plan and the CHAOS-2691 issue
text both explicitly require `400` for malformed envelopes/unsupported
schema versions. Rather than fight FastAPI's automatic-Pydantic-body
behavior (which is app-wide and other tickets/tests depend on), the
external-ingest endpoints take `Request` (not a typed Pydantic body param),
read raw bytes themselves (needed anyway for D3's byte-size check before
parsing), and call `BatchEnvelope.model_validate_json(raw)` inside a
try/except that maps `pydantic.ValidationError` → `ExternalIngestError(400,
code="invalid_envelope", ...)`. This keeps the generic-validation-422
convention intact for every other router in the app and gives
external-ingest exact control of its own documented status codes.

**D3. Error envelope: a dedicated, customer-facing shape — not the app's
`{"detail": ...}` convention.**
External-ingest responses are consumed by customer SDKs/CI scripts, not just
the web app, so they need a stable, documented, machine-parseable error
shape distinct from the internal-app convention (which is not documented as
a public contract). New exception type + handler, registered from
`main.py` alongside `register_exception_handlers`:

```python
# api/external_ingest/errors.py
class ExternalIngestError(Exception):
    def __init__(self, status_code: int, code: str, message: str, *, errors: list[dict] | None = None):
        self.status_code = status_code
        self.code = code
        self.message = message
        self.errors = errors or []

def register_external_ingest_error_handlers(app: FastAPI) -> None:
    async def _handler(request: Request, exc: ExternalIngestError) -> JSONResponse:
        body = {"error": {"code": exc.code, "message": exc.message}}
        if exc.errors:
            body["error"]["errors"] = exc.errors
        return JSONResponse(status_code=exc.status_code, content=body)
    app.add_exception_handler(ExternalIngestError, _handler)
```
Starlette dispatches on exact exception type first, so this handler wins
over the generic `Exception` catch-all without any registration-order
dependency — safe to call `register_external_ingest_error_handlers(app)`
anywhere after `register_exception_handlers(app)` in `main.py`.
Error `code` values (stable, documented, snake_case): `invalid_envelope`,
`unsupported_schema_version`, `unknown_record_kind`,
`idempotency_key_mismatch`, `payload_too_large`, `batch_too_large`,
`source_disabled`, `insufficient_scope`, `idempotency_conflict` (409, wired
by CHAOS-2695 later — reserve the code now), `stream_unavailable` (503).

**D4. Max batch size: 1000 records/batch, 10 MB body — both env-overridable.**
No precedent gives a number (product_telemetry caps at 500 *events*/batch as
a bound for its distinct anonymized-analytics use case; legacy `/api/v1/ingest`
has no cap at all). Decision: `EXTERNAL_INGEST_MAX_RECORDS` (default `1000`)
enforced on `len(envelope.records)` after parse (→ `400 batch_too_large`,
not 413 — it's a semantic limit, not a transport limit), and
`EXTERNAL_INGEST_MAX_BODY_BYTES` (default `10_000_000`, i.e. 10 MB) checked
against `Content-Length` (reject fast) and against actual streamed byte
count if `Content-Length` is absent/chunked (→ `413 payload_too_large`).
`POST /validate` enforces the same two limits (same envelope shape).
Rationale for the numbers: 1000 records keeps a single batch well under the
stream's `maxlen~100000`-per-org backpressure budget even at moderate
throughput, and 10 MB keeps batches an order of magnitude under GitHub's own
25 MB webhook cap while comfortably fitting a 1000-record batch of the
richest kind (`pull_request.v1` with nested reviews, see schemas below).

**D5. Router module layout matches the plan's proposed tree, with explicit
interim-vs-final ownership per file.**
```
src/dev_health_ops/api/external_ingest/
  __init__.py         # exports `router` (mirrors product_telemetry/__init__.py convention)
  router.py            # 4 endpoints (this ticket)
  schemas.py            # envelope + 9 record-kind Pydantic models (this ticket)
  errors.py              # ExternalIngestError + handler (this ticket)
  streams.py               # enqueue_batch() — MINIMAL impl this ticket, HARDENED by CHAOS-2693
  auth.py                   # require_ingest_scope() — INTERIM impl this ticket, REPLACED by CHAOS-2696/2712
```
Do not create `status.py` in this ticket — it belongs to CHAOS-2694 and its
absence is intentional (`GET /batches/{id}` is not implemented here).

**D6. `streams.py` interim implementation — real, not mocked; raises, never
accept-and-warns.**
Per the plan's explicit stricter-than-precedent requirement, and confirmed
against both existing precedents (`api/ingest/streams.py` accept-and-warns,
`api/product_telemetry/streams.py` raises `ConnectionError` which the plan
requires becomes a 503 for external-ingest). Stream naming: use
**`external-ingest:{org_id}:batches`** and DLQ key
**`external-ingest:{org_id}:dlq`** — this is CHAOS-2693's issue text, which
supersedes the core plan doc's older placeholder name
(`external-ingest:<org_id>:events`); use the issue-text convention since it
is the more recently authored, more specific source and CHAOS-2693 owns the
file long-term.

```python
# api/external_ingest/streams.py
import json, logging, os
logger = logging.getLogger(__name__)

class StreamUnavailableError(Exception):
    """Raised when the durable ingest stream cannot accept a write."""

def stream_name(org_id: str) -> str:
    return f"external-ingest:{org_id}:batches"

def get_redis_client():
    redis_url = os.getenv("REDIS_URL")
    if not redis_url:
        return None
    try:
        import valkey as redis
        return redis.from_url(redis_url, decode_responses=True)
    except Exception:
        logger.warning("Redis unavailable for external-ingest streams")
        return None

def enqueue_batch(
    *, org_id: str, ingestion_id: str, source_system: str, source_instance: str,
    schema_version: str, idempotency_key: str, payload_json: str,
) -> str:
    """Write one batch to the durable stream. Returns the stream key.

    Raises StreamUnavailableError if Redis/Valkey is unavailable or the
    write fails — CALLERS MUST map this to HTTP 503, never accept-and-warn.
    """
    client = get_redis_client()
    if client is None:
        raise StreamUnavailableError("Redis/Valkey unavailable")
    stream = stream_name(org_id)
    try:
        client.xadd(
            stream,
            {
                "ingestion_id": ingestion_id,
                "org_id": org_id,
                "source_system": source_system,
                "source_instance": source_instance,
                "schema_version": schema_version,
                "idempotency_key": idempotency_key,
                "payload": payload_json,
            },
            maxlen=100000,
            approximate=True,
        )
    except Exception as exc:
        raise StreamUnavailableError(str(exc)) from exc
    return stream
```
CHAOS-2693 extends this file (adds `ensure_consumer_group`, DLQ writer,
subclasses `api/_stream_consumer.py::StreamConsumer`) but must not change
`enqueue_batch()`'s signature or the stream-name format without updating
`router.py`'s call site.

**D7. `auth.py` interim implementation — explicit, logged, safe-by-default.**
`GET /schemas` and `GET /schemas/{version}` are **unauthenticated** (matches
the plan's framing of schema discovery as customer-SDK tooling, and matches
the codebase's existing precedent that `/openapi.json`/`/docs` are already
fully public). `POST /batches` and `POST /validate` require
`Depends(require_ingest_scope("ingest:write"))`.

```python
# api/external_ingest/auth.py
import os, logging
from dataclasses import dataclass, field
from fastapi import Header, HTTPException
from dev_health_ops.api.services.auth import set_current_org_id

logger = logging.getLogger(__name__)

@dataclass
class IngestAuthContext:
    org_id: str
    scopes: set[str] = field(default_factory=set)
    token_id: str | None = None  # populated once CHAOS-2696 lands

def require_ingest_scope(required_scope: str):
    async def _dep(
        authorization: str | None = Header(default=None),
        x_org_id: str | None = Header(default=None, alias="X-Org-Id"),
    ) -> IngestAuthContext:
        # POST-CRITIQUE (CC14): mechanical guard — interim auth refuses to run
        # unless explicitly enabled for local/test use. Prevents the
        # any-bearer+X-Org-Id path from ever working in a deployed environment
        # (integration branches DO get deployed to shared envs; process gates slip).
        # CHAOS-2712 deletes this flag together with the interim body.
        if os.environ.get("EXTERNAL_INGEST_INSECURE_AUTH") != "1":
            raise ExternalIngestError(  # 503, code="auth_not_configured"
                status_code=503, code="auth_not_configured",
                message="external-ingest auth is not configured on this deployment",
            )
        if not authorization or not authorization.lower().startswith("bearer "):
            raise HTTPException(status_code=401, detail="Missing bearer token")
        if not x_org_id:
            raise HTTPException(status_code=400, detail="X-Org-Id header required")
        # INTERIM (CHAOS-2691): token value is opaque and NOT validated against
        # a DB-backed IngestToken/scope model yet. CHAOS-2696 replaces this
        # function body; the IngestAuthContext shape and Depends() call sites
        # in router.py must not change. Every interim-mode request is logged
        # at WARNING so this is visible in ops before 2696 lands.
        logger.warning(
            "external-ingest interim auth: unvalidated token accepted for org_id=%s",
            x_org_id,
        )
        ctx = IngestAuthContext(org_id=x_org_id, scopes={"ingest:write", "ingest:status", "schema:read"})
        set_current_org_id(ctx.org_id)  # keep ClickHouse auto-scoping consistent (api-app recon gotcha)
        return ctx
    return _dep
```
This is intentionally permissive ONCE the `EXTERNAL_INGEST_INSECURE_AUTH=1` flag is set
(any bearer token + any `X-Org-Id` is then accepted) because CHAOS-2691's own acceptance
criteria never test 401/403 —
only 202/400/413/openapi/tests-for-shapes. Ship it loud (WARNING log) rather
than silently insecure. **CHAOS-2696/2712 own closing this gap before any
external customer traffic is allowed** — call this out explicitly in the PR
description and in `docs/architecture/` (see Files section).

**D8. `GET /schemas` / `GET /schemas/{version}` — minimal real
implementation now, CHAOS-2692 extends.**
Both endpoints are in CHAOS-2691's own Scope text, but CHAOS-2692's Scope
("Add schema registry... Include examples... Ensure schema output can be
used by customer CI validation") is a superset. Resolve by building the
minimal but fully working version now, directly off the Pydantic classes
this ticket defines — no separate registry module:

```python
SCHEMA_VERSION = "external-ingest.v1"
RECORD_KIND_MODELS: dict[str, type[BaseModel]] = {
    "repository.v1": RepositoryV1,
    "identity.v1": IdentityV1,
    "team.v1": TeamV1,
    "work_item.v1": WorkItemV1,
    "work_item_transition.v1": WorkItemTransitionV1,
    "work_item_dependency.v1": WorkItemDependencyV1,
    "pull_request.v1": PullRequestV1,
    "review.v1": ReviewV1,
    "commit.v1": CommitV1,
}

@router.get("/schemas")
async def list_schemas():
    return {
        "schemaVersions": [SCHEMA_VERSION],
        "recordKinds": sorted(RECORD_KIND_MODELS),
    }

@router.get("/schemas/{schema_version}")
async def get_schema(schema_version: str):
    if schema_version != SCHEMA_VERSION:
        raise ExternalIngestError(404, "unsupported_schema_version", f"Unknown schema version: {schema_version}")
    return {
        "schemaVersion": SCHEMA_VERSION,
        "envelope": BatchEnvelope.model_json_schema(by_alias=True),
        "recordKinds": {
            kind: model.model_json_schema(by_alias=True)
            for kind, model in RECORD_KIND_MODELS.items()
        },
    }
```
CHAOS-2692 adds `examples` per kind and a dedicated
`external_ingest/schema_registry.py` if/when it needs versioned-history
support (v2 schemas later) — it should import `RECORD_KIND_MODELS` from
`schemas.py` rather than re-declaring the models, so the two tickets never
duplicate the field definitions. **Note this in the CHAOS-2692 PR
description as an explicit dependency on this ticket landing first**, and
list it in `decisionsNeeded`/dependencies for the synthesizer.

**D9. `repository.v1`'s `externalId` IS the deterministic-UUID seed — no
separate customer-supplied UUID field.**
`Repo.id` is derived via `get_repo_uuid_from_repo(repo_identifier)` where
`repo_identifier` is exactly the string stored in `Repo.repo` (SHA256-based,
`models/git.py:72-93`). To dedupe against a prior `fullchaos_sync` row for
the same logical repo, `repository.v1.externalId` must be **the same string
FullChaos's own GitHub/GitLab connector would have used** (verified
in-code: `processors/base_git.py` builds `Repo(repo=<clone/remote URL>)`).
Document this loudly in the JSON Schema description and the `dev-hops push
sample` payloads (CHAOS-2700): customers must send the canonical remote URL
(e.g. `https://github.com/acme/api` or `git@github.com:acme/api.git` —
**pick exactly one canonicalization and document it**, since
`get_repo_uuid_from_repo` does *not* normalize URL variants itself — recommend
documenting `https://{host}/{owner}/{repo}` lowercase, no `.git` suffix, no
trailing slash, as the required customer-facing format, and note this is a
genuine cross-cutting risk (see Risks) since a customer sending a
differently-formatted URL than FullChaos's connector used will silently
create a duplicate repo row instead of reconciling.

**D10. `Repo.provider` stores the source system, not the ingestion mode —
resolves the recon's flagged ambiguity.**
`repository.v1.sourceSystem` (mirrors `envelope.source.system`) is written
directly into `Repo.provider` (e.g. `"github"`, `"gitlab"`, `"custom"`) —
this is consistent with what `fullchaos_sync` already writes for the same
column. **No new `"customer_push"` provider value is introduced.**
Ownership/ingestion-mode (`fullchaos_sync` vs `customer_push`) is tracked
exclusively in the CHAOS-2696 source-registration table, keyed on
`(org_id, system, instance)` — not overloaded onto `repos.provider`. This
means `repos.provider` remains a clean "who authored this data" field for
both ingestion paths, and the one-active-owner policy check happens at the
source-registration layer (CHAOS-2696), not by inspecting written rows.

**D11. `team.v1` / `identity.v1` get real Pydantic dataclasses in this
ticket** (there are none in the codebase to reuse — recon confirmed this),
shaped to match `ClickHouseStore.insert_teams`/`insert_identities`'s exact
row keys (verified directly against `storage/clickhouse.py:1526-1645`) so
CHAOS-2697's normalizer can pass fields straight through with zero
translation.

**D12. `review.v1.state` is a validated free-string allow-list, not a
strict enum.**
`GitPullRequestReview.state` has no normalized enum in the internal model
(raw provider string). Decision: constrain the wire schema to
`Literal["APPROVED", "CHANGES_REQUESTED", "COMMENTED", "DISMISSED", "PENDING"]`
(matches the legacy `IngestPullRequestReview.state` docstring's known-values
comment) rather than accepting arbitrary strings — an external-ingest
customer sending garbage into a currently-untyped column is worse than a
sync provider doing so (sync providers are trusted/known-shape; customer
payloads are not). Unknown values → `422`-shaped per-record validation error
in `/validate`, not silently passed through.

**D13. Batch acceptance response includes `stream` even though the real
value comes from the D6 interim writer.**
Matches the plan's documented response shape exactly
(`ingestionId`/`status`/`itemsReceived`/`stream`) — `status` is always
`"accepted"` for a 202 (no partial-accept concept at the envelope level;
per-record accept/reject is a `/validate`-only and
status-endpoint-only (CHAOS-2694) concept, not something `POST /batches`
computes synchronously).

---

## API / schema sketches

### Envelope (`schemas.py`)

```python
from __future__ import annotations
from datetime import datetime
from typing import Annotated, Literal, Union
from uuid import UUID
from pydantic import BaseModel, ConfigDict, Field, field_validator

SCHEMA_VERSION = "external-ingest.v1"
MAX_RECORDS_DEFAULT = 1000

class SourceDescriptor(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    type: Literal["customer_push"] = "customer_push"
    system: Literal["github", "gitlab", "jira", "linear", "custom"]
    instance: str = Field(..., min_length=1, max_length=255)  # e.g. "github.com/acme"
    producer: str | None = None            # e.g. "dev-hops-cli"
    producer_version: str | None = Field(default=None, alias="producerVersion")

class IngestWindow(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    started_at: datetime = Field(..., alias="startedAt")
    ended_at: datetime = Field(..., alias="endedAt")

    @field_validator("ended_at")
    @classmethod
    def _ended_after_started(cls, v, info):
        started = info.data.get("started_at")
        if started and v < started:
            raise ValueError("window.endedAt must be >= window.startedAt")
        return v

class RecordEnvelope(BaseModel):
    """Generic wrapper: kind + externalId (for error correlation) + kind-specific payload."""
    model_config = ConfigDict(populate_by_name=True)
    kind: str                       # e.g. "pull_request.v1"
    external_id: str = Field(..., alias="externalId", min_length=1, max_length=512)
    payload: dict  # validated per-kind in router.py against RECORD_KIND_MODELS, not here

class BatchEnvelope(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    schema_version: str = Field(..., alias="schemaVersion")
    idempotency_key: str = Field(..., alias="idempotencyKey", min_length=1, max_length=255)
    source: SourceDescriptor
    window: IngestWindow | None = None
    records: list[RecordEnvelope] = Field(..., min_length=0)
```

Rationale for `RecordEnvelope.payload: dict` (not a discriminated union at
the outer level): the plan's per-record error shape
(`{"index":12,"kind":"pull_request","code":"missing_external_id", ...}`)
requires reporting **which record failed and why** without aborting parse
of the whole batch — a strict `Union[RepositoryV1, PullRequestV1, ...]`
discriminated on `kind` inside Pydantic would make one bad record's
`ValidationError` abort `BatchEnvelope.model_validate_json()` entirely
(can't distinguish "envelope malformed" 400 from "one record kind rejected"
per-item diagnostics). Router-level flow: parse `BatchEnvelope` first (only
envelope-level shape enforced structurally — `kind`/`externalId` presence,
not payload contents), then loop `records`, `RECORD_KIND_MODELS[kind].model_validate(payload)`
per item, collecting `ValidationResponse.errors` for `/validate` or
rejecting the whole batch with `400 unknown_record_kind` for `/batches` if
any `kind` is not in the allow-list (a batch containing an unsupported kind
is rejected entirely at accept-time — no partial-acceptance in v1, matches
"do not create a parallel analytics path" framing: partial silent drops
would be worse for a durability-focused ingest path).

**`/batches`-time validation policy** (resolves an ambiguity the plan
doesn't address): reject the *entire batch* with `400` if:
- `schemaVersion` unsupported,
- any `records[i].kind` not in the 9 allowed kinds,
- envelope-level shape invalid (missing required fields, bad literals).

Do NOT reject the whole batch for individual malformed record *payloads*
(e.g. missing `title` on one `pull_request.v1`) — those are accepted into
the stream and surface as per-record rejections in
`GET /batches/{id}` (CHAOS-2694) after the worker (CHAOS-2697) runs full
validation. This matches the plan's phase split ("worker...Run full
validation") and the plan's explicit two-tier design (`/validate` = client-
side pre-check with per-record errors; `/batches` = envelope+kind-allowlist
check only, deep-per-record validation happens durably in the worker so a
customer's momentary schema drift on 3 of 500 records doesn't drop the
other 497). **`/validate` performs the deep per-record check eagerly**
(that's its whole purpose) using the same `RECORD_KIND_MODELS[kind].model_validate()`
call the worker will eventually use — same validators, so "schemas match
API validation behavior" (CHAOS-2692's AC) holds by construction.

### Responses

```python
class ValidationErrorItem(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    index: int
    kind: str
    code: str                 # e.g. "missing_required_field", "invalid_literal", "unknown_kind"
    message: str
    path: str | None = None   # e.g. "records[12].payload.title"

class ValidationResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    valid: bool
    items_accepted: int = Field(..., alias="itemsAccepted")
    items_rejected: int = Field(..., alias="itemsRejected")
    errors: list[ValidationErrorItem] = Field(default_factory=list)

class BatchAcceptedResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    ingestion_id: str = Field(..., alias="ingestionId")
    status: Literal["accepted"] = "accepted"
    items_received: int = Field(..., alias="itemsReceived")
    stream: str
```

### The 9 record-kind payload schemas

All models: `model_config = ConfigDict(populate_by_name=True, extra="forbid")`
(reject unknown fields — customer typos should be loud validation errors,
not silently dropped data; `extra="forbid"` is a deliberate deviation from
the rest of the codebase's looser Pydantic configs, justified because this
is a public, versioned, customer-facing contract where silent field-drop
would hide integration bugs). All field names below use `camelCase` aliases
matching the envelope convention (`schemaVersion`, `idempotencyKey`).

```python
# --- repository.v1 -> models/git.py Repo (async ClickHouseStore.insert_repo) ---
class RepositoryV1(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="forbid")
    external_id: str = Field(..., alias="externalId", min_length=1, max_length=1024)
    # ^ RECONCILED (CC4): provider FULL NAME ("owner/repo" / "group/subgroup/project"),
    # NOT a URL — becomes Repo.repo AND the get_repo_uuid_from_repo() seed, matching
    # native sync (processors/github.py:1572 repo=repo_info.full_name). Must equal
    # source.instance for git systems. custom: seed = f"custom:{instance}:{externalId}".
    source_system: Literal["github", "gitlab", "custom"] = Field(..., alias="sourceSystem")
    # ^ becomes Repo.provider directly (D10) -- no "customer_push" value.
    default_ref: str | None = Field(default=None, alias="defaultRef")
    tags: list[str] = Field(default_factory=list, max_length=50)
    settings: dict[str, str | int | float | bool] = Field(default_factory=dict)

# --- identity.v1 -> ClickHouseStore.insert_identities row shape ---
class IdentityV1(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="forbid")
    canonical_id: str = Field(..., alias="canonicalId", min_length=1, max_length=255)
    display_name: str | None = Field(default=None, alias="displayName")
    email: str | None = None
    provider_identities: dict[str, list[str]] = Field(default_factory=dict, alias="providerIdentities")
    team_ids: list[str] = Field(default_factory=list, alias="teamIds")
    is_active: bool = Field(default=True, alias="isActive")
    updated_at: datetime = Field(..., alias="updatedAt")

# --- team.v1 -> ClickHouseStore.insert_teams row shape ---
class TeamV1(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="forbid")
    id: str = Field(..., min_length=1, max_length=255)   # slug, matches Postgres Team.id convention
    name: str
    description: str | None = None
    members: list[str] = Field(default_factory=list)     # canonical_ids, resolved by worker
    project_keys: list[str] = Field(default_factory=list, alias="projectKeys")
    repo_patterns: list[str] = Field(default_factory=list, alias="repoPatterns")
    is_active: bool = Field(default=True, alias="isActive")
    updated_at: datetime = Field(..., alias="updatedAt")
    native_team_key: str | None = Field(default=None, alias="nativeTeamKey")
    parent_team_id: str | None = Field(default=None, alias="parentTeamId")

# --- work_item.v1 -> models/work_items.py WorkItem ---
class WorkItemV1(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="forbid")
    external_key: str = Field(..., alias="externalKey", min_length=1, max_length=512)
    # ^ RECONCILED (CC7): provider-NATIVE key ("ABC-123", "CHAOS-123", issue/PR number).
    # The namespaced work_item_id (jira:/linear:/gh:/ghpr:/gitlab:#/gitlab:!) is derived
    # server-side in external_ingest/ids.py (CHAOS-2698) — customers never send it.
    provider: Literal["jira", "github", "gitlab", "linear"]
    title: str
    type: Literal["story", "task", "bug", "epic", "pr", "merge_request", "issue", "incident", "chore", "unknown"] = "unknown"
    status: Literal["backlog", "todo", "in_progress", "in_review", "blocked", "done", "canceled", "unknown"]
    status_raw: str | None = Field(default=None, alias="statusRaw")
    description: str | None = None
    repository_external_id: str | None = Field(default=None, alias="repositoryExternalId")
    native_team_key: str | None = Field(default=None, alias="nativeTeamKey")
    project_key: str | None = Field(default=None, alias="projectKey")
    project_id: str | None = Field(default=None, alias="projectId")
    project_name: str | None = Field(default=None, alias="projectName")
    assignees: list[str] = Field(default_factory=list)
    reporter: str | None = None
    created_at: datetime = Field(..., alias="createdAt")
    updated_at: datetime | None = Field(default=None, alias="updatedAt")
    started_at: datetime | None = Field(default=None, alias="startedAt")
    completed_at: datetime | None = Field(default=None, alias="completedAt")
    closed_at: datetime | None = Field(default=None, alias="closedAt")
    labels: list[str] = Field(default_factory=list)
    story_points: float | None = Field(default=None, alias="storyPoints")
    sprint_id: str | None = Field(default=None, alias="sprintId")
    sprint_name: str | None = Field(default=None, alias="sprintName")
    parent_id: str | None = Field(default=None, alias="parentId")
    epic_id: str | None = Field(default=None, alias="epicId")
    url: str | None = None
    priority_raw: str | None = Field(default=None, alias="priorityRaw")
    service_class: str | None = Field(default=None, alias="serviceClass")
    due_at: datetime | None = Field(default=None, alias="dueAt")

# --- work_item_transition.v1 -> models/work_items.py WorkItemStatusTransition ---
class WorkItemTransitionV1(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="forbid")
    work_item_id: str = Field(..., alias="workItemId")
    provider: Literal["jira", "github", "gitlab", "linear"]
    occurred_at: datetime = Field(..., alias="occurredAt")
    from_status_raw: str | None = Field(default=None, alias="fromStatusRaw")
    to_status_raw: str | None = Field(default=None, alias="toStatusRaw")
    from_status: Literal["backlog", "todo", "in_progress", "in_review", "blocked", "done", "canceled", "unknown"] = Field(..., alias="fromStatus")
    to_status: Literal["backlog", "todo", "in_progress", "in_review", "blocked", "done", "canceled", "unknown"] = Field(..., alias="toStatus")
    actor: str | None = None

# --- work_item_dependency.v1 -> models/work_items.py WorkItemDependency ---
class WorkItemDependencyV1(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="forbid")
    source_work_item_id: str = Field(..., alias="sourceWorkItemId")
    target_work_item_id: str = Field(..., alias="targetWorkItemId")
    relationship_type: Literal["blocks", "blocked_by", "relates_to", "duplicates", "parent_of", "child_of"] = Field(..., alias="relationshipType")
    relationship_type_raw: str | None = Field(default=None, alias="relationshipTypeRaw")

# --- pull_request.v1 -> models/git.py GitPullRequest ---
class PullRequestV1(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="forbid")
    repository_external_id: str = Field(..., alias="repositoryExternalId")  # -> Repo.id via D9 derivation
    number: int = Field(..., ge=1)
    title: str | None = None
    body: str | None = None
    state: Literal["open", "closed", "merged"]
    author_name: str | None = Field(default=None, alias="authorName")
    author_email: str | None = Field(default=None, alias="authorEmail")
    created_at: datetime = Field(..., alias="createdAt")
    merged_at: datetime | None = Field(default=None, alias="mergedAt")
    closed_at: datetime | None = Field(default=None, alias="closedAt")
    head_branch: str | None = Field(default=None, alias="headBranch")
    base_branch: str | None = Field(default=None, alias="baseBranch")
    additions: int | None = Field(default=None, ge=0)
    deletions: int | None = Field(default=None, ge=0)
    changed_files: int | None = Field(default=None, alias="changedFiles", ge=0)
    first_review_at: datetime | None = Field(default=None, alias="firstReviewAt")
    first_comment_at: datetime | None = Field(default=None, alias="firstCommentAt")
    changes_requested_count: int | None = Field(default=0, alias="changesRequestedCount", ge=0)
    reviews_count: int | None = Field(default=0, alias="reviewsCount", ge=0)
    comments_count: int | None = Field(default=0, alias="commentsCount", ge=0)
    url: str | None = None

# --- review.v1 -> models/git.py GitPullRequestReview ---
class ReviewV1(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="forbid")
    repository_external_id: str = Field(..., alias="repositoryExternalId")
    pull_request_number: int = Field(..., alias="pullRequestNumber", ge=1)
    review_id: str = Field(..., alias="reviewId", min_length=1)
    reviewer: str
    state: Literal["APPROVED", "CHANGES_REQUESTED", "COMMENTED", "DISMISSED", "PENDING"]  # D12
    submitted_at: datetime = Field(..., alias="submittedAt")

# --- commit.v1 -> models/git.py GitCommit ---
class CommitV1(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="forbid")
    repository_external_id: str = Field(..., alias="repositoryExternalId")
    hash: str = Field(..., min_length=7, max_length=64)
    message: str | None = None
    author_name: str | None = Field(default=None, alias="authorName")
    author_email: str | None = Field(default=None, alias="authorEmail")
    author_when: datetime = Field(..., alias="authorWhen")
    committer_name: str | None = Field(default=None, alias="committerName")
    committer_email: str | None = Field(default=None, alias="committerEmail")
    committer_when: datetime | None = Field(default=None, alias="committerWhen")
    parents: int = Field(default=1, ge=0)
```

Every `*ExternalId`/`*WorkItemId` cross-reference field (e.g.
`pull_request.v1.repositoryExternalId`) is a **string FK into the same
batch or a prior batch's already-ingested `repository.v1`/`work_item.v1`
record** — normalization/resolution of these cross-references into real
ClickHouse UUIDs happens in the worker (CHAOS-2697), not in this ticket.
`/validate` does **not** attempt to resolve these against ClickHouse (no DB
round-trip in the request-validation-only path) — it only checks shape.
Document this explicitly in the JSON Schema `description` fields so
customers don't expect `/validate` to catch dangling repo references.

### Router (`router.py`) — endpoint sketch

```python
router = APIRouter(prefix="/api/v1/external-ingest", tags=["external-ingest"])

@router.get("/schemas")
async def list_schemas(): ...  # D8, no auth

@router.get("/schemas/{schema_version}")
async def get_schema(schema_version: str): ...  # D8, no auth

@router.post("/validate", response_model=ValidationResponse)
async def validate_batch(
    request: Request,
    ctx: IngestAuthContext = Depends(require_ingest_scope("schema:read")),
):
    raw = await _read_body_enforcing_size_limit(request)  # D4, 413
    envelope = _parse_envelope_or_400(raw)                 # D2, 400
    _check_schema_version_or_400(envelope)                 # 400 unsupported_schema_version
    _check_batch_size_or_400(envelope)                     # D4, 400 batch_too_large
    errors = _validate_records(envelope.records)            # per-record, D2/discriminator note
    return ValidationResponse(
        valid=not errors,
        items_accepted=len(envelope.records) - len(errors),
        items_rejected=len(errors),
        errors=errors,
    )

@router.post("/batches", response_model=BatchAcceptedResponse, status_code=202)
async def accept_batch(
    request: Request,
    ctx: IngestAuthContext = Depends(require_ingest_scope("ingest:write")),
    idempotency_key_header: str | None = Header(default=None, alias="Idempotency-Key"),
):
    raw = await _read_body_enforcing_size_limit(request)     # 413
    envelope = _parse_envelope_or_400(raw)                    # 400
    _check_idempotency_header_matches_body(envelope, idempotency_key_header)  # D1, 400
    _check_schema_version_or_400(envelope)                     # 400
    _check_all_kinds_known_or_400(envelope)                     # 400 unknown_record_kind
    _check_batch_size_or_400(envelope)                           # 400 batch_too_large
    ingestion_id = str(uuid4())
    try:
        stream = enqueue_batch(
            org_id=ctx.org_id,
            ingestion_id=ingestion_id,
            source_system=envelope.source.system,
            source_instance=envelope.source.instance,
            schema_version=envelope.schema_version,
            idempotency_key=envelope.idempotency_key,
            payload_json=raw.decode("utf-8"),
        )
    except StreamUnavailableError as exc:
        raise ExternalIngestError(503, "stream_unavailable", "Ingest stream unavailable") from exc
    return BatchAcceptedResponse(
        ingestion_id=ingestion_id, items_received=len(envelope.records), stream=stream,
    )
```
`429` is emitted by a `slowapi` rate-limit decorator keyed by
`ctx.org_id`/token (not `get_forwarded_ip` — see api-app recon gotcha #4);
since org_id is only known *after* the auth dependency runs, use a custom
`key_func` that reads `request.state`/header rather than the shared IP-keyed
`limiter` instance. Concretely: create a second `Limiter` instance in
`auth.py` or `router.py` with
`key_func=lambda request: request.headers.get("Authorization", "anonymous")`
(good enough for v1 — CHAOS-2696 can upgrade the key function to the actual
`token_id` once real tokens exist), and apply
`@ingest_limiter.limit("120/minute")` to `POST /batches` and
`@ingest_limiter.limit("60/minute")` to `POST /validate`. Document these as
placeholders; CHAOS-2696/2712 should revisit the actual limits.

`401`/`403` come from `require_ingest_scope` (D7) — `403` specifically for
`insufficient_scope` when `required_scope not in ctx.scopes` (always false
in interim mode since interim mode grants all scopes — this branch is dead
code until CHAOS-2696 lands, but the check must be written now so the
`Depends` seam doesn't change shape later).

---

## Files to create/modify

Create:
- `src/dev_health_ops/api/external_ingest/__init__.py`
- `src/dev_health_ops/api/external_ingest/router.py`
- `src/dev_health_ops/api/external_ingest/schemas.py`
- `src/dev_health_ops/api/external_ingest/errors.py`
- `src/dev_health_ops/api/external_ingest/streams.py` (D6, interim/real)
- `src/dev_health_ops/api/external_ingest/auth.py` (D7, interim)
- `tests/api/test_external_ingest_router.py` (or
  `tests/test_external_ingest_api.py` to match the flat
  `tests/test_ingest_api.py` precedent — prefer `tests/api/` since that's
  where `test_generic_exception_handler.py` and other newer API tests live;
  confirm the directory's existing convention with
  `ls tests/api/*.py | head` before deciding, don't assume)
- `docs/architecture/external-ingest-rest-contract.md` — record D1-D13
  here in the SAME changeset (house rule: document decisions in project
  docs, not just `.remember`). Cover: idempotency header-vs-body
  resolution, error envelope shape, the `Repo.provider` decision (D10), the
  `externalId` canonicalization requirement (D9), and the interim-auth/
  interim-stream seams with an explicit "replace before GA" callout.

Modify:
- `src/dev_health_ops/api/main.py`:
  - add `from dev_health_ops.api.external_ingest import router as external_ingest_router`
    (import block, alphabetical with the other `from dev_health_ops.api...`
    imports around line 25-26)
  - add `app.include_router(external_ingest_router)` next to
    `app.include_router(ingest_router)` (main.py:207) — keep the legacy and
    new routers visually adjacent in the mount list so future readers see
    both and ask the reconcile question (see Risks)
  - add `register_external_ingest_error_handlers(app)` call right after
    `register_exception_handlers(app)` (main.py:193)

Do NOT modify: `compose.yml`, `workers/config.py`, any alembic/ClickHouse
migration file, `api/_middleware.py`, `api/ingest/*` (legacy router
untouched), any file under `web/`.

---

## Test plan

**Unit (no live services, run in default `pytest` tier):**
- `tests/api/test_external_ingest_router.py` using the established
  `httpx.ASGITransport + AsyncClient` pattern (`tests/test_ingest_api.py:1-22`),
  with `app.dependency_overrides[require_ingest_scope] = lambda: IngestAuthContext(org_id="test-org", scopes={"ingest:write","schema:read"})`
  to bypass D7's interim auth entirely in tests (standard FastAPI
  dependency-override pattern — do not hit the real interim auth code path
  in unit tests, since that path deliberately WARNs).
  Cases (mirror the plan's "Testing" section + issue AC verbatim):
  1. Valid minimal batch (1 `commit.v1` record) → `202`, response matches
     `BatchAcceptedResponse` shape, `enqueue_batch` called once with correct
     kwargs (monkeypatch `enqueue_batch`, don't hit real Valkey).
  2. Valid batch with all 9 record kinds, one of each → `202`.
  3. Missing `schemaVersion` → `400 invalid_envelope`.
  4. Unsupported `schemaVersion` (`"external-ingest.v2"`) → `400 unsupported_schema_version`.
  5. Unknown record kind (`"deployment.v1"`, explicitly deferred per plan)
     → `400 unknown_record_kind`.
  6. `records` array with `1001` entries (over `EXTERNAL_INGEST_MAX_RECORDS`)
     → `400 batch_too_large`.
  7. Body over `EXTERNAL_INGEST_MAX_BODY_BYTES` → `413 payload_too_large`
     (construct via a large `body` string field, not via literal 10MB test
     fixture bloat — pad one record's optional string field).
  8. `Idempotency-Key` header present and equal to body `idempotencyKey` →
     `202` (no error).
  9. `Idempotency-Key` header present and NOT equal to body →
     `400 idempotency_key_mismatch`.
  10. `enqueue_batch` raises `StreamUnavailableError` (monkeypatch to raise)
      → `503 stream_unavailable`.
  11. `POST /validate` with 2 of 5 records malformed (e.g. `work_item.v1`
      missing `title`, `pull_request.v1` with `state: "bogus"`) →
      `200`, `valid: false`, `itemsAccepted: 3`, `itemsRejected: 2`, and
      `errors[]` entries carry correct `index`/`kind`/`code`/`path`.
  12. `POST /validate` with a fully valid batch → `valid: true`,
      `itemsRejected: 0`.
  13. `GET /schemas` → `200`, `recordKinds` contains exactly the 9 v1 kinds,
      sorted; no auth header required (test with no `Authorization` header
      at all).
  14. `GET /schemas/external-ingest.v1` → `200`, response's
      `recordKinds["commit.v1"]` round-trips as valid JSON Schema (assert
      via `jsonschema` if added as a dev dependency, or at minimum assert
      `"$defs"`/`"properties"` keys exist — do not add a new runtime dep
      just for this test if avoidable, check `pyproject.toml` `[project.optional-dependencies].dev`
      first).
  15. `GET /schemas/unknown-version` → `404 unsupported_schema_version`.
  16. Missing `Authorization` header on `POST /batches` → `401` (test the
      real `require_ingest_scope` dependency directly here, one dedicated
      test *without* the override, to prove D7's interim auth still enforces
      *something*).
  17. Missing `X-Org-Id` header (with `Authorization` present) → `400`.
  18. New route appears in `app.openapi()["paths"]` under
      `/api/v1/external-ingest/batches` etc. (acceptance criterion "API
      appears in OpenAPI docs" — assert programmatically, don't eyeball
      `/docs`).
  19. `tests/api/test_generic_exception_handler.py`-style check: an
      unexpected exception inside the router (e.g. monkeypatch
      `enqueue_batch` to raise a bare `RuntimeError`, not
      `StreamUnavailableError`) still falls through to the generic `500`
      handler with the sanitized body — confirms `ExternalIngestError`'s
      handler doesn't accidentally swallow unrelated exceptions.

**No `@pytest.mark.clickhouse` tests needed for this ticket** — CHAOS-2691
never touches ClickHouse (that's CHAOS-2697/2698's job); if a reviewer adds
one, it belongs in a later PR, not this one.

**Schema round-trip check** (cheap, worth adding even though it's not in
the AC): for every `RECORD_KIND_MODELS` entry, assert
`model.model_json_schema()` does not raise and produces valid JSON (guards
against a stray non-JSON-serializable default breaking CHAOS-2692's
consumer of this module).

---

## Gate commands

Ops (run from `/Users/chris/projects/full-chaos/dev-health/ops/.claude/worktrees/chaos-2690-integration`):

```bash
# Full local-validate gate, per-issue scratch DB to avoid clobbering other
# in-flight worktrees' ClickHouse state (this ticket has no CH work, but the
# gate still runs the full unit tier + argMax proof unconditionally):
SCRATCH_DB=ci_local_validate_chaos2691 bash ci/local_validate.sh

# If Docker/ClickHouse isn't available in this environment:
SKIP_CLICKHOUSE=1 SCRATCH_DB=ci_local_validate_chaos2691 bash ci/local_validate.sh

# mypy in isolation (matches CI's typecheck.yml exactly):
.venv/bin/mypy --install-types --non-interactive .

# Targeted test file during iteration (fast inner loop):
.venv/bin/pytest tests/api/test_external_ingest_router.py -v
```

No `dev-health-web` gate applies to CHAOS-2691 — this ticket touches only
`ops`. (If schema.graphql or any web fetcher references external-ingest
status later, that's CHAOS-2694/2714's gate, not this one.)

---

## Live verification procedure

This ticket has no durable persistence and no ClickHouse writes, so "live"
verification is scoped to proving the HTTP contract against a running API
process with a real Valkey behind it (proving D6's 503-on-unavailable
behavior specifically, which cannot be proven by a pure unit test with a
monkeypatched `enqueue_batch`).

```bash
# 1. Start the stack (or just the api+valkey services if compose supports partial up)
docker compose up -d valkey api

# 2. Confirm the route is live and documented
curl -s http://localhost:8000/openapi.json | jq '.paths | keys | map(select(startswith("/api/v1/external-ingest")))'

# 3. Schema discovery (no auth)
curl -s http://localhost:8000/api/v1/external-ingest/schemas | jq .
curl -s http://localhost:8000/api/v1/external-ingest/schemas/external-ingest.v1 | jq '.recordKinds | keys'

# 4. Valid batch (interim auth: any bearer token + a real-looking org id)
curl -s -X POST http://localhost:8000/api/v1/external-ingest/batches \
  -H "Authorization: Bearer dev-token" \
  -H "X-Org-Id: <a real org uuid from your local Postgres, e.g. via
       psql $POSTGRES_URI -c \"select id from organizations limit 1;\">" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: manual-verify-001" \
  -d '{
    "schemaVersion": "external-ingest.v1",
    "idempotencyKey": "manual-verify-001",
    "source": {"type": "customer_push", "system": "github", "instance": "github.com/acme"},
    "records": [{"kind": "commit.v1", "externalId": "abc123", "payload": {
      "repositoryExternalId": "https://github.com/acme/api",
      "hash": "abc1234567", "authorWhen": "2026-06-25T00:00:00Z"
    }}]
  }' | jq .
# expect: 202, ingestionId present, stream == "external-ingest:<org-id>:batches"

# 5. Prove the stream write actually happened (not just a 202 lie)
docker exec dev-health-valkey-1 valkey-cli -n 1 XRANGE "external-ingest:<org-id>:batches" - +

# 6. Prove 503-on-unavailable (D6's core requirement, cannot be skipped)
docker compose stop valkey
curl -s -o /dev/null -w "%{http_code}\n" -X POST http://localhost:8000/api/v1/external-ingest/batches \
  -H "Authorization: Bearer dev-token" -H "X-Org-Id: <org-id>" \
  -H "Content-Type: application/json" -H "Idempotency-Key: manual-verify-002" \
  -d '{"schemaVersion":"external-ingest.v1","idempotencyKey":"manual-verify-002","source":{"type":"customer_push","system":"github","instance":"github.com/acme"},"records":[]}'
# expect: 503
docker compose start valkey

# 7. Prove 413
python3 -c "import json; print(json.dumps({'schemaVersion':'external-ingest.v1','idempotencyKey':'x','source':{'type':'customer_push','system':'github','instance':'a'},'records':[{'kind':'commit.v1','externalId':'x','payload':{'repositoryExternalId':'r','hash':'a'*40,'authorWhen':'2026-06-25T00:00:00Z','message':'x'*(11*1024*1024)}}]}))" > /tmp/big.json
curl -s -o /dev/null -w "%{http_code}\n" -X POST http://localhost:8000/api/v1/external-ingest/batches \
  -H "Authorization: Bearer dev-token" -H "X-Org-Id: <org-id>" -H "Content-Type: application/json" \
  --data-binary @/tmp/big.json
# expect: 413
```

---

## Dependencies on other sub-issues

- **Blocks CHAOS-2692** (schema discovery/JSON Schema export) — 2692 should
  import `RECORD_KIND_MODELS`/`BatchEnvelope` from this ticket's
  `schemas.py` rather than redefining them (D8). Land 2691 first.
- **CHAOS-2693** (durable stream + DLQ) extends `streams.py`'s
  `enqueue_batch()`/stream-naming contract defined here (D6) — either
  ticket can land first in isolation (2691 ships a working minimal
  writer), but 2693's implementer must read D6 before touching the file to
  avoid an incompatible signature change breaking `router.py`.
  Recommend landing 2691 first for a cleaner base.
- **CHAOS-2694** (status/rejections) will implement
  `GET /batches/{ingestion_id}`, which is explicitly NOT part of this
  ticket — no code dependency, but the `BatchAcceptedResponse.ingestionId`
  this ticket returns is the join key 2694's status table will use.
- **CHAOS-2695** (idempotency/ownership policy) will replace the D1
  header/body-match check's downstream semantics (durable conflict
  detection, 409) — this ticket only defines the response *shape*
  (`idempotency_key_conflict` error code reserved in D3) and does the
  cheap in-request header/body consistency check; no code dependency but a
  clear "the interesting half of idempotency isn't here yet" callout for
  planning.
- **CHAOS-2696 / CHAOS-2712** (source registration, token scopes,
  authorization model) must replace `auth.py`'s D7 interim implementation
  before any external customer is allowed to call this API — treat this as
  a **hard pre-GA blocker**, not just a nice-to-have follow-up, and say so
  explicitly in the CHAOS-2691 PR description.
- **CHAOS-2697/2698** (worker normalization + sink writes) will need every
  field-level schema decision in this brief (D9-D12 especially) — those
  tickets should treat this brief's schemas.py as the frozen wire contract,
  not re-derive field sets from the plan doc (which has none).

## Risks

- **Legacy `/api/v1/ingest` router coexists, unreconciled.** Neither plan
  doc nor any CHAOS-2690 sub-issue assigns an owner for deciding its fate
  (deprecate/merge/keep). This ticket does not touch it, per scope, but the
  two routers will confuse support/on-call staff and API consumers (same
  202+ingestion_id shape, different auth, different URL prefix). Flag to
  epic owner; recommend a dedicated follow-up ticket (not yet filed) rather
  than silently deciding either way inside 2691.
- **Interim auth (D7) is genuinely insecure** (any bearer token + claimed
  org_id is accepted) and will be live in `main` as soon as this PR merges,
  even though the app-wide OpenAPI/`/docs` exposure means the route shape
  is publicly discoverable immediately. Mitigate by shipping loudly (PR
  description callout, `docs/architecture/` doc, WARNING-level log line per
  request) and by treating CHAOS-2696/2712 as a **release blocker**, not
  routine follow-up work — this needs to be surfaced to whoever schedules
  the epic's rollout.
- **`externalId` canonicalization (D9) is a real footgun** with no
  automatic enforcement in this ticket (Pydantic can validate string shape
  but not "is this the same URL FullChaos's connector would derive") — a
  customer using a different URL format than fullchaos_sync used for the
  same repo will silently create a second `repos` row instead of merging.
  CHAOS-2697's normalizer could add a best-effort canonicalization pass
  (strip `.git`, lowercase host, drop trailing slash) before calling
  `get_repo_uuid_from_repo` — flag this to CHAOS-2697's implementer
  explicitly, it's not something 2691 can fix in schema validation alone.
- **`extra="forbid"` on all 9 record models is a deliberate strictness
  choice (see schema section) that diverges from most Pydantic models in
  this codebase** — if reviewers push back expecting laxer validation
  (pattern-matching against `product_telemetry`'s `payload: dict[str, ...]`
  free-form field), hold the line: this is a versioned, external, customer
  SDK contract, not an internal analytics event, and the "schemas match API
  validation behavior" acceptance criterion on CHAOS-2692 depends on strict
  validation being meaningful.
- **Rate limiting placeholder (per-Authorization-header key) is weak**
  (multiple distinct interim "dev tokens" with the same literal string
  would share a bucket) — acceptable for v1 since real tokens don't exist
  yet (D7), but don't let this be mistaken for the final per-token limiter
  CHAOS-2696 should deliver.
