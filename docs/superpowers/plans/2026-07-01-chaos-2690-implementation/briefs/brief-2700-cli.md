# Implementation brief: CHAOS-2700 (dev-hops push CLI) + CHAOS-2701 (customer docs) + CHAOS-2713 (CI/CD examples)

> **SYNTHESIZER RECONCILIATION (authoritative — see master-spec.md; overrides body below):**
> 1. **D4 is REVERSED**: record `kind` is the VERSIONED string (`"pull_request.v1"`),
>    everywhere (adopts 2691/2692's convention; the plan doc's bare-noun error example is
>    corrected in 2691's PR). The record wrapper's payload field is named **`payload`**,
>    not `data`: `{"kind": "pull_request.v1", "externalId": "acme/api#123", "payload": {…}}`.
> 2. D5's import contract: the canonical model names are 2691's — `BatchEnvelope`,
>    `SourceDescriptor`, `IngestWindow`, `RecordEnvelope`, `ValidationErrorItem`,
>    `ValidationResponse`, `BatchAcceptedResponse`, `RECORD_KIND_MODELS`, `SCHEMA_VERSION`
>    (not the `ExternalIngest*` names sketched below). idempotencyKey max_length=255.
>    Offline validate = envelope parse + per-record `RECORD_KIND_MODELS[kind].model_validate`.
> 3. D6 limits corrected: `MAX_RECORDS_PER_BATCH = 1000`, `MAX_BATCH_BYTES = 10_000_000`
>    — and `GET /schemas` DOES expose a `limits` object from day one (2691), so the CLI
>    prefers server-reported limits immediately (no "until then" hedge).
> 4. D10 status enum amended: `accepted | stream_unavailable | processing | completed |
>    partial | failed` (terminal poll set unchanged: completed/partial/failed).
>    `stream_unavailable` → print "re-run `push batch` (same idempotency key re-enqueues)"
>    and exit 3. A REPLAY re-POST returns **200 OK with the full status envelope** (not
>    202) — `push batch` treats 200 as "already ingested, short-circuit polling".
>    Resubmitting a `failed` batch (same key+payload) is a fresh accept, same ingestion_id.
> 5. D13 amended: NO `src/dev_health_ops/push/samples/` directory. `push sample` loads the
>    canonical examples via `schema_registry.load_example(kind)` /
>    `importlib.resources` from `api/external_ingest/examples/` (CHAOS-2692's packaged
>    fixture home, CC18). `docs/examples/external-ingest/*.json` copies + byte-identity
>    drift test are CHAOS-2701's deliverable (not this issue's).
> 6. `source.instance` is repo/project-grain (CC5): `push batch` sends one batch per
>    source instance; document that multi-repo exports are split per repo full name.
> 7. Landing wave: 3 (2700). CHAOS-2701/2713 land in wave 5; 2713's scope additionally
>    absorbs the webhook-relay example tab + reconciliation-schedule guidance from 2715.
> 8. Errors from the API arrive in the `{"error": {code, message}}` envelope (CC16) —
>    parse that shape, not FastAPI's `{"detail": …}`.
> 9. **POST-CRITIQUE (CC16/CC29): retry ALL 503s regardless of error code** — the
>    server can return `stream_unavailable`, `ingest_temporarily_unavailable`
>    (concurrent same-key race), or `auth_not_configured` (interim-auth guard,
>    integration-branch only) at 503; the retry predicate keys on status class
>    (429/503/network), never on the code string.

Epic: CHAOS-2690 External customer-push ingestion API
Repo: `dev-health-ops` only (no `dev-health-web` changes required for these three issues)
Author: recon/brief agent, read-only pass, 2026-07-01

This brief assumes CHAOS-2691 (REST contract + `schemas.py`), CHAOS-2692 (schema
discovery/JSON Schema export), CHAOS-2694 (status/rejections store + `GET
/batches/{id}`), and CHAOS-2696/CHAOS-2712 (source registration + ingest tokens)
land the server-side pieces this CLI talks to. Where those issues are still
undesigned, this brief makes the calling contract concrete so CHAOS-2700 can be
implemented against a pinned interface — see "Dependencies" and
"decisionsNeeded" for what must be kept in lockstep.

---

## Scope

**CHAOS-2700 — `dev-hops push` CLI**
- New command group `dev-hops push` with subcommands `validate`, `batch`,
  `sample`, `status` (added beyond the literal issue text — see Design
  decisions §7), and a stubbed `export` (out of scope, extension point only).
- Local, offline payload validation (no network call) via the same Pydantic
  models the API uses.
- Batch submission over HTTP with retry/backoff, `--poll` status polling,
  JSON output mode for CI, and well-defined exit codes.
- Token/env handling: `--api-url`/`FULLCHAOS_API_URL`,
  `--token`/`FULLCHAOS_INGEST_TOKEN` (+ `FULLCHAOS_API_TOKEN` alias),
  `--org`/`FULLCHAOS_ORG_ID`.
- Sample payload generation per record kind, sourced from a single
  hand-authored fixture set shared with docs.

**CHAOS-2701 — Customer examples and docs**
- `docs/customer-push-ingestion.md`: REST vs GraphQL decision, auth, source
  registration, schema versions, idempotency, retries, status polling,
  rejected-record handling, conflict/ownership policy.
- `docs/customer-push-ingestion-cli-walkthrough.md`: end-to-end CLI walkthrough
  (install → token → sample → validate → push → poll).
- `docs/examples/external-ingest/*.json`: one example payload per v1 record
  kind + one combined multi-kind batch example.

**CHAOS-2713 — CI/CD examples**
- `docs/examples/ci/`: GitHub Actions (dev-hops flavor + cURL flavor), GitLab
  CI (dev-hops flavor + cURL flavor), generic Docker runner, local
  cron/systemd example — all as real, runnable files, not just doc snippets.
- Security guidance baked into every example: secret storage, least-privilege
  token scopes, no provider credentials sent to FullChaos, rotation guidance.

## Out of scope

- `dev-hops push export github|gitlab` (provider export helpers) — stub only,
  explicitly deferred per epic plan. Leave a registration extension point.
- Any server-side work: router, schemas, streams, status store, worker,
  token/source-registration model (CHAOS-2691/2692/2693/2694/2695/2696/2697/2698/2699/2712).
- Web UI (`/org/admin/integrations` setup screens, CHAOS-2714) — CHAOS-2713's
  file examples are a *source* CHAOS-2714 can embed/link later, this issue
  does not touch `dev-health-web`.
- FullChaos-hosted webhook ingestion (CHAOS-2715) — out of scope for CLI/docs.
- Auto-splitting oversized payloads into multiple batches — document as a
  known v1 limitation (customer must pre-batch by window/day).
- Non-Python client SDKs — `GET /schemas/{version}` (CHAOS-2692) is the
  extension point for those; this brief only covers the Python CLI + raw
  cURL examples.

## Design decisions

1. **HTTP client = `httpx.AsyncClient`.** `httpx>=0.28.1` is already a
   pyproject dependency and is the library used server-side
   (`api/services/oauth.py`, `api/admin/routers/*`). `dev-hops` CLI handlers
   are dispatched via `asyncio.run` when `inspect.iscoroutinefunction(func)`
   is true (`cli.py:main`), so `push batch`/`push status` should be `async
   def` — no new dependency, and it composes with the existing dispatch loop
   for free.

2. **Retry/backoff = reuse `dev_health_ops.connectors.utils.retry.retry_with_backoff`.**
   This decorator is generic (no connector-specific imports) and already
   implements exponential backoff + `Retry-After` honoring via a
   `retry_after_seconds` attribute duck-type on the raised exception. Define
   a local `IngestTransientError(Exception)` in the new `push` module
   carrying `retry_after_seconds` (parsed from the response's `Retry-After`
   header when present), and retry on: network/connection errors, timeouts,
   HTTP 429, and HTTP 503. Do **not** retry 400/401/403/409/413/422 — these
   are client/contract errors and retrying them wastes CI minutes and can
   flip a 409 idempotency-conflict into a false "it eventually worked."
   CLI-tuned params: `max_retries=5, initial_delay=1.0, max_delay=30.0,
   backoff_factor=2.0` (max_delay lowered from the connector default of
   60.0s — CI job time budgets are tighter than long-running sync jobs).

3. **Local `push validate` imports the server's Pydantic models directly —
   it does not re-implement validation and does not call the network.**
   Import path: `dev_health_ops.api.external_ingest.schemas` (owned by
   CHAOS-2691). This guarantees the CLI can never drift from what the API
   actually enforces, and it's genuinely offline (Pydantic validation has no
   I/O), matching the acceptance criterion "CLI validates payloads locally."
   Concretely: `ExternalIngestBatchEnvelope.model_validate(json.load(f))`,
   catch `pydantic.ValidationError`, and re-shape `.errors()` into the same
   `{index, kind, code, message, path}` diagnostic shape the `/validate`
   endpoint returns (see §5 below for the exact model this requires
   CHAOS-2691 to expose). **Consequence:** `dev-hops push validate` requires
   the `dev-health-ops` Python package be installed (it always is — this is
   the `dev-hops` CLI's own package), so this is not a burden, but it does
   mean `push validate` is *not* usable as a language-agnostic reference —
   that role belongs to `GET /schemas/{version}` (CHAOS-2692) for non-Python
   customers. Document this distinction in CHAOS-2701 docs.

4. **Record envelope shape — bare `kind`, batch-level `schemaVersion`
   carries the version.** The core plan's own `/validate` error example
   uses `"kind": "pull_request"` (no `.v1` suffix), while the record-kind
   catalog is named `pull_request.v1` etc. Resolve this by treating
   `schemaVersion: "external-ingest.v1"` on the *batch envelope* as pinning
   the per-kind schema version for every record in that batch; each record's
   `kind` field is the bare noun (`repository`, `identity`, `team`,
   `work_item`, `work_item_transition`, `work_item_dependency`,
   `pull_request`, `review`, `commit`). This avoids redundant
   per-record versioning and matches the one concrete example the plan
   docs give. **This is a cross-issue contract decision — flag to
   whoever implements CHAOS-2691/2692 so `schemas.py` and the JSON Schema
   export use the same convention** (see `decisionsNeeded`).
   Record shape:
   ```json
   { "kind": "pull_request", "externalId": "acme/api#123", "data": { "...": "..." } }
   ```

5. **Shared schema surface CHAOS-2700 needs from CHAOS-2691 (contract
   pinned here so CLI work isn't blocked on guessing).** `schemas.py` must
   expose these importable names (pydantic v2, `ConfigDict(populate_by_name=True)`,
   camelCase aliases — matches the existing `product_telemetry/schemas.py`
   convention):
   - `ExternalIngestSource` — `type: Literal["customer_push"]`, `system: str`,
     `instance: str`, `producer: str | None`, `producer_version: str | None`
     (alias `producerVersion`).
   - `ExternalIngestWindow` — `started_at: datetime` (alias `startedAt`),
     `ended_at: datetime` (alias `endedAt`).
   - `ExternalIngestRecord` — `kind: ExternalIngestRecordKind` (Literal of the
     9 bare kinds), `external_id: str` (alias `externalId`),
     `data: dict[str, Any]` (kind-specific validation happens one layer
     down, per-kind, in CHAOS-2691/2692 — not re-derived here).
   - `ExternalIngestBatchEnvelope` — `schema_version: Literal["external-ingest.v1"]`
     (alias `schemaVersion`), `idempotency_key: str` (alias `idempotencyKey`,
     `min_length=1, max_length=200`), `source: ExternalIngestSource`,
     `window: ExternalIngestWindow`, `records: list[ExternalIngestRecord]`
     (`min_length=1, max_length=5000` — see §6 for where 5000 comes from).
   - `ExternalIngestValidationError` — `index: int`, `kind: str | None`,
     `code: str`, `message: str`, `path: str | None`.
   - `ExternalIngestValidationResponse` — `valid: bool`, `items_accepted: int`,
     `items_rejected: int`, `errors: list[ExternalIngestValidationError]`.
   - `ExternalIngestAcceptedResponse` — `ingestion_id: str`,
     `status: Literal["accepted"]`, `items_received: int`, `stream: str`.
   If CHAOS-2691 lands with different names, the CLI import site is a single
   module (`src/dev_health_ops/push/validate.py`) — update the import there,
   not scattered call sites.

6. **Client-side size limits are conservative defaults, not authoritative.**
   Hardcode `MAX_RECORDS_PER_BATCH = 5000` and `MAX_BATCH_BYTES = 10_000_000`
   (10 MB) in `push/limits.py` as a fast client-side pre-flight check before
   sending (fail fast locally with a clear message instead of waiting for a
   round-trip 413). These numbers are a *recommendation to CHAOS-2691* for
   the server's actual enforced limits — **flag as decisionsNeeded**: the
   CLI's local pre-check must match whatever CHAOS-2691 finally enforces, or
   customers will see the CLI accept locally and then get a 413 from the
   server (or vice versa, be blocked locally under a server limit that's
   actually higher). Once CHAOS-2692's `GET /schemas` ships a `limits` field,
   `dev-hops push batch` should prefer the server-reported limit (fetch once,
   cache is not needed — it's a single extra `GET /schemas` call before
   `POST /batches`, only when `--skip-limits-check` is not passed); until
   then, use the hardcoded default.

7. **Add a `dev-hops push status <ingestion_id>` command beyond the literal
   issue bullet list.** The issue only lists `validate`, `batch`, `sample`,
   but the acceptance criterion "CLI can poll batch status" is under-specified
   for the common real case: a CI job runs `push batch --poll`, the job dies
   or times out mid-poll, and the customer needs to re-check status later
   without re-submitting the batch (which would also correctly 409 on
   idempotency-key reuse, but that's a worse UX than a direct status check).
   `push status <ingestion_id> [--poll] [--json]` calls
   `GET /api/v1/external-ingest/batches/{ingestion_id}` directly. This keeps
   `push batch --poll`'s polling loop and `push status`'s polling loop
   sharing one implementation (`push/poll.py`).

8. **JSON output mode: boolean `--json` flag, not `--output=json`.** The repo
   has two existing conventions (`recommendations compute --output-json`
   boolean, `workers inspect --output=json` value flag). Pick the boolean
   form (`--json`) for `push` because it's the more common flag in
   CI-oriented CLIs the target audience (customer CI pipelines) will
   recognize, and it avoids a third distinct spelling. When `--json` is set,
   all machine-readable output goes to stdout as a single JSON object per
   invocation (never NDJSON — batch/validate/status each produce one
   logical result); all human/progress logging goes to stderr via the
   existing `logging` handler, exactly like `recommendations compute`.

9. **Exit code contract (document verbatim in CHAOS-2701 docs — customers'
   CI will branch on these):**
   - `0` — success. `validate`: payload fully valid. `batch`/`status`
     (without `--poll`): batch accepted (202) / status fetched OK, whatever
     the batch's current status is — *not* poll-blocking, so "accepted" is
     success even if processing isn't done. `batch`/`status --poll`: reached
     terminal `completed` status with `items_rejected == 0`.
   - `1` — data-level failure. `validate`: payload invalid (errors printed/
     JSON). `batch --poll` / `status --poll`: reached terminal `completed`
     with `items_rejected > 0`, or terminal `failed`.
   - `2` — usage error (missing/invalid CLI args) — matches existing
     argparse convention (`parser.error()` → exit 2) used everywhere else
     in `dev-hops`.
   - `3` — transport/API error after retries exhausted (network failure,
     5xx after retry budget, 503 stream-unavailable, or any non-2xx the CLI
     doesn't have a specific code for). Distinct from `1` so CI can tell
     "your data was rejected" (fix the payload) apart from "FullChaos was
     unreachable" (retry the job / page on-call), which is the single most
     important UX distinction for unattended CI/cron usage.
   - `4` — poll timeout: batch is still non-terminal (`processing`/
     `partial`) when `--poll-timeout` elapses. Not a hard failure —
     document that customers should re-run `push status <id> --poll` rather
     than resubmitting.

10. **Batch status enum — pin it here since CHAOS-2694 owns the table but
    this CLI's polling loop needs concrete terminal states to check against.**
    `status ∈ {accepted, processing, completed, partial, failed}`. `partial`
    = terminal, `items_rejected > 0` and `items_accepted > 0` (some records
    landed, some didn't — distinct from `failed`, where nothing was
    processable, e.g. a normalization crash). Terminal set for polling:
    `{completed, partial, failed}`. **Flag as decisionsNeeded**: CHAOS-2694
    must adopt this exact enum (or tell the CLI owner what it actually
    shipped) — the webhook-status vocabulary in the addendum doc
    (`received/verified/normalized/enqueued/processed/partial/failed/ignored`)
    is a *different, wider* status model for webhook-assisted ingestion and
    should not be conflated with the plain batch-status enum used here.

11. **Env var names and precedence — resolves a real contradiction between
    the Linear issue text and the plan doc / this task's own GOAL.**
    CHAOS-2700's Linear acceptance criterion says "Auth token can come from
    `FULLCHAOS_API_TOKEN`"; the 2026-06-28 plan doc's CI examples and this
    task's FOCUS both say `FULLCHAOS_INGEST_TOKEN`. Resolution: support
    both. Precedence for token resolution: `--token` flag >
    `FULLCHAOS_INGEST_TOKEN` env > `FULLCHAOS_API_TOKEN` env (deprecated
    alias — emit one `logging.warning` when used, pointing at
    `FULLCHAOS_INGEST_TOKEN`). `FULLCHAOS_INGEST_TOKEN` is the name used in
    every shipped example (CHAOS-2713) and in customer-facing docs
    (CHAOS-2701); `FULLCHAOS_API_TOKEN` exists purely so the literal Linear
    acceptance criterion is satisfied without breaking the primary
    documented name. API URL: `--api-url` > `FULLCHAOS_API_URL` env
    (required, no default — do not silently default to a prod URL baked
    into the CLI). Org: `--org` > `FULLCHAOS_ORG_ID` env (required).

12. **`push` commands are excluded from `--org` auto-resolution and from the
    ClickHouse/Postgres preflight system entirely.** `_should_resolve_org`
    (`cli.py:360`) must add an exclusion clause for `ns.command == "push"`
    (mirroring the existing `audit planner-configs` / `migrate clickhouse
    repair` exclusions) — auto-resolving `--org` to "the first org in the
    local Postgres DB" is actively wrong for a CLI that's usually run from a
    customer's CI runner with no local DB at all, and would silently push
    to the wrong org if it ever did have DB access. Do **not** add `push`
    entries to `_COMMAND_REQUIREMENTS` (`_REQ_CLICKHOUSE`/`_REQ_POSTGRES`/
    `_REQ_ORG`/`_REQ_SINK_DB`) — those preflight tokens are DB-connection
    specific and `push` never touches ClickHouse/Postgres. Instead, declare
    `--api-url`, `--token`, `--org` as `required=True` (with env-var
    `default=`) directly on the `batch`/`status` subparsers — matching how
    `backfill run --sink` does its own required-arg validation rather than
    going through the DB preflight mechanism. `validate` and `sample` take
    no `--api-url`/`--token`/`--org` at all (fully offline).

13. **Sample payloads are static, hand-authored JSON fixtures — not
    generated from a JSON Schema.** Deriving "realistic" example data
    programmatically from a JSON Schema produces garbage (`"string"`,
    `123`, arbitrary enum picks) that's useless for customer onboarding and
    for the docs' "customer can validate and push a sample payload from
    docs" acceptance criterion. Single source of truth:
    `docs/examples/external-ingest/<kind>.v1.json` (9 files, one record
    example each, wrapped in a minimal single-record batch envelope) +
    `docs/examples/external-ingest/full-batch.v1.json` (one batch envelope
    with one record of every kind, for `push sample --all`). `dev-hops push
    sample --kind pull_request` and `--all` read these via
    `importlib.resources.files("dev_health_ops") /
    "push/samples/<kind>.v1.json"` — **package them under
    `src/dev_health_ops/push/samples/` and have `docs/examples/...` symlink
    or a small CI check assert byte-identical content**, so the CLI doesn't
    depend on `docs/` being present at runtime (docs/ is not part of the
    installed package; `src/dev_health_ops/push/samples/` must be added to
    `pyproject.toml`'s package-data / `[tool.setuptools.package-data]` or
    equivalent so it ships in the wheel — check current packaging config
    before assuming plain `.py`-only packaging works). A repo-root CI test
    (`tests/test_push_samples_match_docs.py`) asserts
    `src/dev_health_ops/push/samples/*.json ==
    docs/examples/external-ingest/*.json` byte-for-byte, so doc examples
    can never silently drift from what `push sample` actually emits.

14. **`push export` is a registered-but-stubbed subcommand, not omitted.**
    `dev-hops push export {github,gitlab}` parses and immediately exits 1
    with a clear message ("not implemented in v1 — see CHAOS-2690 plan; use
    `dev-hops push sample` + hand-written export, or the provider's native
    FullChaos sync instead") rather than being entirely absent from
    `--help`. Implementation: `src/dev_health_ops/push/export/__init__.py`
    exposes `EXPORT_PROVIDERS: dict[str, Callable[..., int]] = {}` and a
    `register_export_provider(name, fn)` decorator; the `export` subparser
    dispatches through this registry, falling through to the "not
    implemented" message for any name not yet registered. This gives a
    real, tested extension point without building the exporters themselves.

## API/DDL/schema sketches

### CLI-side Pydantic reuse contract (no new schema — imports from CHAOS-2691)

```python
# src/dev_health_ops/push/validate.py
from dev_health_ops.api.external_ingest.schemas import (
    ExternalIngestBatchEnvelope,
    ExternalIngestValidationError,
)

def validate_payload(raw: dict) -> tuple[bool, list[ExternalIngestValidationError]]:
    try:
        ExternalIngestBatchEnvelope.model_validate(raw)
        return True, []
    except pydantic.ValidationError as exc:
        return False, [_to_ingest_error(e) for e in exc.errors()]
```

### Batch envelope example (single source of truth for `push sample --all`)

```json
{
  "schemaVersion": "external-ingest.v1",
  "idempotencyKey": "acme-github-2026-06-25",
  "source": {
    "type": "customer_push",
    "system": "github",
    "instance": "github.com/acme",
    "producer": "dev-hops-cli",
    "producerVersion": "0.1.0"
  },
  "window": {
    "startedAt": "2026-06-25T00:00:00Z",
    "endedAt": "2026-06-26T00:00:00Z"
  },
  "records": [
    {
      "kind": "repository",
      "externalId": "github.com/acme/api",
      "data": {
        "name": "acme/api",
        "provider": "github",
        "url": "https://github.com/acme/api",
        "defaultBranch": "main",
        "tags": ["backend"]
      }
    },
    {
      "kind": "identity",
      "externalId": "github:acme-eng-1",
      "data": {
        "displayName": "Jordan Rivera",
        "primaryEmail": "jordan@acme.dev",
        "providerLogins": [{"provider": "github", "login": "jrivera"}]
      }
    },
    {
      "kind": "team",
      "externalId": "acme:platform",
      "data": {
        "name": "Platform",
        "memberExternalIds": ["github:acme-eng-1"]
      }
    },
    {
      "kind": "work_item",
      "externalId": "gh:acme/api#456",
      "data": {
        "title": "Fix login race condition",
        "type": "bug",
        "status": "in_progress",
        "statusRaw": "In Progress",
        "createdAt": "2026-06-20T10:00:00Z",
        "updatedAt": "2026-06-25T09:00:00Z",
        "assigneeExternalIds": ["github:acme-eng-1"],
        "repositoryExternalId": "github.com/acme/api",
        "labels": ["bug", "auth"]
      }
    },
    {
      "kind": "work_item_transition",
      "externalId": "gh:acme/api#456:2026-06-22T08:00:00Z",
      "data": {
        "workItemExternalId": "gh:acme/api#456",
        "fromStatus": "todo",
        "toStatus": "in_progress",
        "occurredAt": "2026-06-22T08:00:00Z",
        "actorExternalId": "github:acme-eng-1"
      }
    },
    {
      "kind": "work_item_dependency",
      "externalId": "gh:acme/api#456->gh:acme/api#450",
      "data": {
        "fromWorkItemExternalId": "gh:acme/api#456",
        "toWorkItemExternalId": "gh:acme/api#450",
        "type": "blocks"
      }
    },
    {
      "kind": "pull_request",
      "externalId": "github.com/acme/api#123",
      "data": {
        "repositoryExternalId": "github.com/acme/api",
        "number": 123,
        "title": "Fix login race condition",
        "state": "merged",
        "authorExternalId": "github:acme-eng-1",
        "createdAt": "2026-06-24T12:00:00Z",
        "mergedAt": "2026-06-25T08:00:00Z",
        "additions": 42,
        "deletions": 10,
        "changedFiles": 3
      }
    },
    {
      "kind": "review",
      "externalId": "github.com/acme/api#123:review:987654",
      "data": {
        "pullRequestExternalId": "github.com/acme/api#123",
        "reviewerExternalId": "github:acme-eng-2",
        "state": "approved",
        "submittedAt": "2026-06-25T00:00:00Z"
      }
    },
    {
      "kind": "commit",
      "externalId": "github.com/acme/api@abc123def456",
      "data": {
        "repositoryExternalId": "github.com/acme/api",
        "hash": "abc123def456",
        "message": "fix: resolve login race condition",
        "authorExternalId": "github:acme-eng-1",
        "authorWhen": "2026-06-25T07:45:00Z",
        "parents": 1
      }
    }
  ]
}
```
Notes: field names/types above are the CLI/docs-side proposal for
`data` per record kind, derived from the existing internal carrier models
(`models/work_items.py::WorkItem`/`WorkItemStatusTransition`/
`WorkItemDependency`, `models/git.py::Repo`/`GitCommit`/`GitPullRequest`/
`GitPullRequestReview`) translated to a customer-facing, `externalId`-keyed,
camelCase wire shape (no internal UUIDs exposed). **These are not binding on
CHAOS-2691/2692's per-kind JSON Schemas** — they are this brief's concrete
proposal so CHAOS-2700/2701/2713 have something real to validate/document
against; reconcile field-for-field with CHAOS-2691/2692 when that work
starts (flag in decisionsNeeded).

### CLI module layout

```
src/dev_health_ops/push/
  __init__.py
  cli.py                 # register_commands(subparsers) — argparse wiring
  http_client.py          # httpx.AsyncClient wrapper, retry_with_backoff usage
  validate.py             # imports api.external_ingest.schemas, local validation
  poll.py                 # shared polling loop for `batch --poll` and `status --poll`
  limits.py               # MAX_RECORDS_PER_BATCH, MAX_BATCH_BYTES
  output.py               # human vs --json rendering, exit-code mapping
  samples/                # packaged JSON fixtures (see decision 13)
    repository.v1.json
    identity.v1.json
    team.v1.json
    work_item.v1.json
    work_item_transition.v1.json
    work_item_dependency.v1.json
    pull_request.v1.json
    review.v1.json
    commit.v1.json
    full-batch.v1.json
  export/
    __init__.py            # EXPORT_PROVIDERS registry, stub dispatcher
```

## Files to create/modify

### CHAOS-2700 (CLI)
- Create `src/dev_health_ops/push/__init__.py`, `cli.py`, `http_client.py`,
  `validate.py`, `poll.py`, `limits.py`, `output.py`,
  `samples/*.json` (10 files), `export/__init__.py`.
- Modify `src/dev_health_ops/cli.py`:
  - add `from dev_health_ops.push import cli as push_cli` import inside
    `build_parser()` (lazy-import pattern, alongside the other
    `register_commands` imports).
  - add `push_cli.register_commands(sub)` call, placed after
    `backfill_cli.register_backfill_commands(sub)`, before
    `_propagate_global_args_to_subparsers(parser)` / `_attach_preflight_metadata(parser)` run.
  - extend `_should_resolve_org` with `and not (getattr(ns, "command", None) == "push")`.
  - do **not** touch `_COMMAND_REQUIREMENTS` (see decision 12).
- Modify `pyproject.toml`: confirm/add package-data inclusion for
  `src/dev_health_ops/push/samples/*.json` (check whether the project uses
  `[tool.setuptools.package-data]`/`MANIFEST.in`/`include-package-data` —
  read current packaging config before assuming plain source layout ships
  non-`.py` files; this repo's install path is `pip install .` per house
  rules, not `uv`, so verify against `pip`/`setuptools` semantics
  specifically).
- Create `docs/architecture/customer-push-cli-and-examples.md` (house rule:
  document architecture decisions in the same changeset) covering: httpx +
  retry_with_backoff reuse, offline-validation-via-shared-Pydantic-models
  design, exit-code contract, sample-payload SSOT + drift-check test,
  env var precedence/alias decision.

### CHAOS-2701 (docs)
- Create `docs/customer-push-ingestion.md`.
- Create `docs/customer-push-ingestion-cli-walkthrough.md`.
- Create `docs/examples/external-ingest/*.json` (10 files, byte-identical to
  `src/dev_health_ops/push/samples/*.json` — see decision 13 drift-check).
- Modify `docs/index.md` (or equivalent nav/TOC file — check current
  structure) to link the new docs.

### CHAOS-2713 (CI/CD examples)
- Create `docs/examples/ci/github-actions-dev-hops.yml`
- Create `docs/examples/ci/github-actions-curl.yml`
- Create `docs/examples/ci/gitlab-ci-dev-hops.yml`
- Create `docs/examples/ci/gitlab-ci-curl.yml`
- Create `docs/examples/ci/docker/Dockerfile`
- Create `docs/examples/ci/docker/entrypoint.sh`
- Create `docs/examples/ci/docker/README.md`
- Create `docs/examples/ci/cron-systemd/push-dev-health.sh`
- Create `docs/examples/ci/cron-systemd/dev-health-push.service`
- Create `docs/examples/ci/cron-systemd/dev-health-push.timer`
- Create `docs/examples/ci/cron-systemd/crontab.example`
- Create `docs/examples/ci/README.md` (index describing token storage +
  rotation guidance shared by all examples, security requirements checklist)

### Tests
- Create `tests/push/test_cli_validate.py`, `test_cli_sample.py`,
  `test_cli_batch.py`, `test_cli_status.py`, `test_http_client_retry.py`.
- Create `tests/test_push_samples_match_docs.py` (drift check, decision 13).
- Create `tests/test_cli_help.py` additions (or a new
  `tests/test_push_cli_help.py`) asserting `dev-hops push --help` and each
  subcommand's `--help` exits 0 (existing subprocess pattern).

## Test plan

### Unit (no network, no DB — run in default `unit_tests()` tier)
- `push validate <valid-sample>.json` → exit 0, `valid: true` in `--json`
  output.
- `push validate <payload with missing externalId>` → exit 1, errors array
  matches `{index, kind, code, message, path}` shape.
- `push validate -` (stdin) reads from stdin correctly (`sys.stdin.read()`
  gated on `payload == "-"`, matching the plan's `cat payload.json |
  dev-hops push batch -` convention — apply identically to `validate`).
- `push sample --kind pull_request` → prints the exact fixture JSON to
  stdout, exit 0.
- `push sample --all` → prints the combined `full-batch.v1.json`.
- `push sample --kind not_a_kind` → exit 2 (argparse choice validation).
- `push export github` → exit 1, stderr contains "not implemented".
- Env var precedence: `FULLCHAOS_INGEST_TOKEN` wins over
  `FULLCHAOS_API_TOKEN`; `--token` flag wins over both; using
  `FULLCHAOS_API_TOKEN` alone logs a deprecation warning (assert on
  `caplog`).
- `--org` auto-resolution is disabled for `push` subcommands (assert
  `_should_resolve_org` returns `False` when `ns.command == "push"` and
  `ns.org is None`, and that `push batch` without `--org`/`FULLCHAOS_ORG_ID`
  fails with exit 2, not a silent first-org resolution) — this directly
  tests decision 12 and prevents the "silently pushes to wrong org"
  regression.
- HTTP client retry behavior via `httpx.MockTransport` (built into `httpx`,
  **no new dependency needed** — confirmed no `respx`/`pytest-httpx` in
  `pyproject.toml`, and `MockTransport` is sufficient for request/response
  stubbing + simulating sequential failures then success):
  - 503 then 202 → succeeds after 1 retry, `Retry-After` header honored
    (assert the mocked clock/sleep was called with the header's value, not
    the exponential-backoff default).
  - 409 → no retry, returns immediately with the conflict body surfaced to
    the user, exit 3.
  - 400 → no retry, exit 3, response body's error message printed.
  - connection error 5x → exit 3 after exhausting `max_retries`.
- `push batch --poll` polling loop: `httpx.MockTransport` returns
  `accepted → processing → completed(items_rejected=0)` across 3 GETs →
  exit 0; `... → completed(items_rejected>0)` → exit 1; `... → processing`
  forever until `--poll-timeout` → exit 4.
- Exit-code contract table (decision 9) — one parametrized test per row.

### Live-DB / live-API (`@pytest.mark.clickhouse` or a new marker if this
needs a running API, not just ClickHouse — see note below)
- These CHAOS-2700 CLI tests genuinely don't need ClickHouse directly (they
  talk HTTP to the external-ingest API, which CHAOS-2691/2694 own). Do
  **not** mark them `@pytest.mark.clickhouse` — that marker is specifically
  for tests needing a live ClickHouse connection per repo convention. If a
  true end-to-end CLI→API→ClickHouse test is wanted, it belongs as an
  integration test *after* CHAOS-2691/2693/2694/2697/2698 land, using the
  existing `httpx.ASGITransport(app=app)` in-process pattern
  (`tests/test_ingest_api.py`) pointed at `dev-hops push batch` by
  monkeypatching the CLI's base URL to hit the ASGI transport directly, or
  (simpler) a subprocess test against a real `dev-hops api serve` process —
  **defer this to a follow-up once the server side exists**; note it in
  Risks, don't block CHAOS-2700 on it.

### Docs/examples correctness (CHAOS-2701/2713)
- `tests/test_push_samples_match_docs.py`: byte-diff
  `src/dev_health_ops/push/samples/*.json` vs
  `docs/examples/external-ingest/*.json`.
- Each `docs/examples/external-ingest/*.json` and `full-batch.v1.json`
  passes `push validate` (same unit test as CLI's own sample tests, just
  pointed at the docs/ copies too, to catch any future divergence even if
  the drift-check above is accidentally weakened).
- YAML lint the 4 CI example files (`ruff`/`yamllint` if configured, or at
  minimum `yaml.safe_load()` in a test to catch syntax errors) — these are
  the literal files a customer will copy-paste into their repo; a YAML
  syntax error here is a customer-facing embarrassment, not just an
  internal lint nit.
- `Dockerfile` in `docs/examples/ci/docker/` should be validated with
  `docker build --check` or at minimum a syntax-only parse if Docker isn't
  available in the test environment — do not require a live `docker build`
  in the ops repo's unit test tier (no docker daemon in CI unit tier per
  `ci/run_tests.sh`); a plain existence + basic-grep smoke test is
  sufficient for CI, and any live docker exercise should be manual (see
  Live verification procedure).

## Gate commands

Ops repo (all three issues are ops-only, no `dev-health-web` changes):

```bash
cd /Users/chris/projects/full-chaos/dev-health/ops/.claude/worktrees/chaos-2690-integration
SCRATCH_DB=chaos2700_scratch bash ci/local_validate.sh
.venv/bin/mypy --install-types --non-interactive .
```

If ClickHouse/docker isn't available in the implementing agent's sandbox,
run the pure-Python subset only:

```bash
SKIP_CLICKHOUSE=1 SCRATCH_DB=chaos2700_scratch bash ci/local_validate.sh
```

No `dev-health-web` gates apply — verify no web files were touched:

```bash
git -C /Users/chris/projects/full-chaos/dev-health/web/.claude/worktrees/chaos-2690-integration status --porcelain
```
(should be empty / unrelated to this changeset).

## Live verification procedure

These three issues are CLI/docs-only and largely mechanical to verify
without a live server, since CHAOS-2691/2693/2694/2696/2697/2698 (the
server side this CLI calls) are not yet implemented. Two verification
tiers:

**Tier 1 — fully offline (works today, no dependency on other sub-issues):**
```bash
cd /Users/chris/projects/full-chaos/dev-health/ops/.claude/worktrees/chaos-2690-integration
.venv/bin/dev-hops push sample --kind pull_request
.venv/bin/dev-hops push sample --all | .venv/bin/dev-hops push validate -
.venv/bin/dev-hops push validate docs/examples/external-ingest/work_item.v1.json --json
echo "exit=$?"
.venv/bin/dev-hops push export github --repo acme/api ; echo "exit=$?"   # expect 1, "not implemented"
.venv/bin/dev-hops push batch docs/examples/external-ingest/commit.v1.json ; echo "exit=$?"  # expect 2, missing --api-url/--token/--org
```

**Tier 2 — against a real (or locally-mocked) API, once CHAOS-2691/2693/2694
land.** Do NOT run against the shared dev compose stack's real org data —
use a disposable local API instance:
```bash
# once external_ingest router exists and is mounted:
.venv/bin/dev-hops api serve --port 8099 &
API_PID=$!
FULLCHAOS_API_URL=http://localhost:8099 \
FULLCHAOS_INGEST_TOKEN=<token minted via CHAOS-2712's issuance path> \
FULLCHAOS_ORG_ID=<disposable test org> \
  .venv/bin/dev-hops push batch docs/examples/external-ingest/full-batch.v1.json --poll
kill $API_PID
```
This tier cannot be exercised until the dependent sub-issues ship; the
implementing agent should not block CHAOS-2700's own merge on it, but
should re-run Tier 2 as a follow-up smoke test once CHAOS-2691/2693/2694
merge (track as a checklist item on CHAOS-2690 or a new "customer-push
end-to-end smoke test" issue).

## Dependencies on other sub-issues

- **CHAOS-2691** (blocking, hard): CLI's `push validate` imports
  `dev_health_ops.api.external_ingest.schemas` directly (decision 3/5).
  Without it, `push validate` cannot be implemented as offline validation —
  the implementing agent would have to either (a) stub a local copy of the
  Pydantic models temporarily and delete it once CHAOS-2691 lands, or (b)
  implement CHAOS-2691's `schemas.py` as a prerequisite/co-requisite of this
  work. Recommend sequencing CHAOS-2691 before or alongside CHAOS-2700.
- **CHAOS-2692** (soft): `GET /schemas`/`GET /schemas/{version}` is the
  long-term source of truth for record-kind field shapes and batch limits;
  CHAOS-2700 ships with hardcoded defaults (decision 6, 13) that should be
  reconciled once CHAOS-2692 exists, but is not blocked on it for v1.
- **CHAOS-2694** (blocking for `batch`/`status`, not for `validate`/`sample`):
  `GET /batches/{ingestion_id}` and the batch-status enum (decision 10) are
  owned there; `push batch --poll`/`push status` cannot be live-tested until
  it exists (see Live verification Tier 2).
  - **CHAOS-2696 / CHAOS-2712** (blocking for `batch`/`status` live use):
  ingest tokens with scopes must exist before `--token` has anything real
  to authenticate against.
- **CHAOS-2693** (blocking for `batch` end-to-end): without the durable
  stream, `POST /batches` can't actually 202-accept anything meaningfully
  (or must 503, exercising the CLI's exit-3 path only).
- **CHAOS-2714** (soft, downstream consumer): the web setup screens'
  "Setup examples" tab (Screen 4 in the webhooks/setup addendum) should
  source its GitHub Actions/GitLab/Docker/cURL snippets from
  `docs/examples/ci/*` (this issue's output) rather than duplicating them —
  note this as a cross-repo reuse opportunity for whoever picks up
  CHAOS-2714, not a blocking dependency in the other direction.

## Risks

- **Schema drift between CLI, docs, and server.** Mitigated by decision 3
  (CLI imports the server's own Pydantic models, not a copy) and decision
  13's byte-diff test (docs examples vs packaged CLI samples), but the
  per-kind `data` field shapes proposed in this brief are *not yet* what
  CHAOS-2691/2692 will actually enforce — until those land, `push
  validate`'s local validation is validating against a contract that
  doesn't exist yet server-side. Real risk: if CHAOS-2691 is implemented
  with materially different field names than this brief's sketch, the 10
  sample JSON files + docs walkthrough all need a follow-up pass. Flag this
  explicitly to whoever implements CHAOS-2691 (share this brief's schema
  sketch) rather than letting the two land independently.
- **Env var naming confusion for customers.** Two token env var names
  (`FULLCHAOS_INGEST_TOKEN` primary, `FULLCHAOS_API_TOKEN` deprecated alias)
  is a real support-burden risk if not documented crisply. Mitigation:
  decision 11 mandates a runtime deprecation warning + the docs (CHAOS-2701)
  must state the primary name in bold exactly once, with the alias
  mentioned only in a "legacy name" footnote.
- **Packaging risk for sample JSON files.** If `pyproject.toml` isn't
  configured to include non-`.py` package data, `pip install
  dev-health-ops` in a customer's CI runner will produce a working `dev-hops
  push validate/batch` but a broken `dev-hops push sample` (file not found)
  — this is exactly the kind of gap that only shows up in a clean-install
  test, not local dev (where `pip install -e .` from the repo root
  incidentally has the files on disk regardless of packaging config).
  **Explicitly test with a clean `pip install .` into a fresh venv**, not
  just `pip install -e .[dev]`, before calling CHAOS-2700 done.
- **Retry-on-429/503 vs idempotency-key reuse semantics.** If a `POST
  /batches` call times out client-side *after* the server actually accepted
  it (durably enqueued) but *before* the response reached the CLI, a naive
  retry would resubmit with the same `idempotencyKey` and correctly get a
  200/409-with-existing-status per CHAOS-2695's rules — but only if the
  payload hash matches byte-for-byte. Since the CLI retries the exact same
  request body, this is safe *as long as* CHAOS-2695's idempotency
  comparison is a payload-hash comparison, not a raw-bytes comparison with
  whitespace/ordering sensitivity. Flag to CHAOS-2695's implementer:
  the hash must be computed over parsed/canonicalized JSON, not raw request
  bytes, or the CLI's httpx retry (which resends the exact same bytes, so
  this is actually fine for CLI-originated retries) — the real risk is a
  *different* client (e.g. cURL example with a shell retry loop) re-encoding
  JSON differently between attempts. Document "send byte-identical retries"
  as a requirement in the cURL CI examples (CHAOS-2713).
- **`--poll` blocking CI job runtime.** Default `--poll-timeout` needs a
  sane default (proposed: 300s) that's long enough for typical bounded
  recompute (CHAOS-2699) but short enough not to eat a customer's CI
  minutes budget on a stuck job. This number is a guess without
  CHAOS-2699's actual recompute latency data — flag as a tuning follow-up
  once CHAOS-2699 ships and real latency numbers exist.
