# Implementation brief: CHAOS-2702 (E2E customer-push ingestion test) + CHAOS-2711 (Developer/user docs)

> **SYNTHESIZER RECONCILIATION (authoritative — see master-spec.md; overrides body below):**
> 1. §2's pinned contract is superseded by master-spec §2 where they differ (notably:
>    record wrapper `{kind, externalId, payload}` with VERSIONED kinds; batch statuses
>    `accepted|stream_unavailable|processing|completed|partial|failed`; limits 1000/10MB;
>    `source.instance` = repo full name; auth tables `external_ingest_sources`/`_tokens`;
>    migrations 0032/0033/0034; error envelope `{"error": {code, message}}`).
> 2. ADR renumbered: **adr-006** (`docs/architecture/adr-006-external-ingest-e2e-test-docs-
>    strategy.md`, flat naming). adr-003 = 2691's backend boundary ADR (decision 11's
>    "someone must own the backend ADR" is RESOLVED: CHAOS-2691 wave 1); adr-004 = 2715;
>    adr-005 = 2692.
> 3. Decision 5 amended: the canonical VALID examples live in
>    `src/dev_health_ops/api/external_ingest/examples/<kind>.json` (CHAOS-2692). This
>    issue's `tests/fixtures/external_ingest/v1/*.json` keeps the INVALID cases and
>    asserts byte/structural equality with the package examples for the valid ones
>    (no fourth fixture copy).
> 4. Decision 6 split RATIFIED: 2711 = mkdocs prose/nav; 2701 = docs example fixtures +
>    drift tests. **mkdocs.yml is edited ONLY by CHAOS-2711** (both the nav section AND
>    the pymdownx.snippets extension from decision 8 — drop them from 2702's file list).
> 5. Sequencing RATIFIED: 2702/2711/2701/2713 are wave 5 (last), no earlier partial ticket.
> 6. Legacy `/api/v1/ingest` fate = new follow-up issue (CC28); docs ship the
>    disambiguation note only (decision 12 stands).
> 7. Replay assertion for the e2e: re-POST same key+payload returns **200** with full
>    status envelope (not 202); resubmit-after-failed is a fresh accept with same
>    ingestion_id, attempts incremented.

Epic: CHAOS-2690 External customer-push ingestion API
Repo: dev-health-ops (worktree: `.claude/worktrees/chaos-2690-integration`)
Owner sub-issues: CHAOS-2702, CHAOS-2711
Prepared: 2026-07-01

This brief assumes the reader has NOT read the epic plan docs or recon files — everything
needed to implement is inlined or given as an exact file path.

---

## 0. Critical framing: these two issues are LAST in the epic, not parallel-buildable now

CHAOS-2702's E2E test drives `validate -> batch -> stream -> worker -> sinks -> status ->
bounded recompute enqueue`. Every one of those verbs is owned by a different, currently-
Backlog sub-issue (CHAOS-2691/2692/2693/2694/2695/2696/2697/2698/2699/2700). **None of that
code exists in the worktree yet** (confirmed: no `src/dev_health_ops/api/external_ingest/`,
no `src/dev_health_ops/external_ingest/` directory as of this recon). CHAOS-2711's docs
likewise describe endpoints, CLI commands, and screens that don't exist yet.

Do not attempt to implement CHAOS-2702/2711 in isolation against a fresh guess at the API
shape. This brief is written so that:

1. The **fixtures, docs skeleton, mkdocs wiring, and ADR** can be authored now (no code
   dependency) and are ready to receive real content.
2. The **E2E test and endpoint-reference docs** are written against a **pinned contract**
   (Section 2) that this brief treats as authoritative for CHAOS-2702/2711's own scope. If a
   sibling issue's implementation drifts from this contract, **update this brief's
   Section 2, not the other way around** — CHAOS-2702/2711 must track reality, but the
   contract must be pinned somewhere so the E2E test isn't rewritten from scratch every time
   a sibling PR lands. Whoever picks up CHAOS-2702/2711 must diff their implementation
   against Section 2 first and reconcile before writing test code.

---

## 1. Scope

### CHAOS-2702 (E2E test)

- One live-DB pytest module driving the full customer-push pipeline against **real**
  ClickHouse + Postgres + Valkey (not mocks, not FakeValkey) for at least one record kind
  per family, proving:
  - `POST /validate` catches invalid records without enqueueing.
  - `POST /batches` returns `202` + `ingestionId`, durably enqueues to the real Valkey
    stream.
  - The worker (driven synchronously in-test, see §5.3) reads the real stream, normalizes,
    and writes through the real sinks (ClickHouse rows visible via `FINAL`/argMax read).
  - `GET /batches/{id}` shows `itemsAccepted`/`itemsRejected` and per-record rejection
    diagnostics for a deliberately-invalid record.
  - Bounded metric recompute is **enqueued, not executed inline** (§5.4 defines the exact
    assertion).
- Fixture payloads for **all 9 v1 record kinds** (one valid + one invalid example each),
  shared with CHAOS-2701/2692/2711 (single source of truth, §4).
- Wiring this test into the existing `live-e2e` CI tier (extend, do not fork, the existing
  harness — §6).

### CHAOS-2711 (docs)

- Developer docs: API overview/lifecycle, REST endpoint reference, schema versioning model,
  local validation workflow, retry/idempotency behavior, rejected-record diagnostics, status
  polling, operational troubleshooting.
- User/customer docs: when to use customer push vs managed sync, source registration,
  credential create/rotate, test-before-production workflow, verify-data-landed workflow,
  common failure modes + remediation.
- mkdocs wiring (nav + new `docs/customer-push-ingestion/` section).
- One ADR in `docs/architecture/adr/` recording the test/docs-layer decisions made here
  (house rule: document decisions in the same changeset).
- Cross-links into `docs/webhooks.md` and `docs/ops/cli-reference.md` (existing files) so
  the two ingestion paths (provider webhooks vs customer push) aren't confused by a reader.

### Out of scope (explicitly deferred to sibling issues)

- Implementing the router, schemas, streams, worker, sinks, status store, token model,
  source-registration model, CLI, or bounded-recompute planner themselves — those are
  CHAOS-2691/2692/2693/2694/2695/2696/2697/2698/2699/2700.
- Web UI screens (CHAOS-2714), CI/CD example authoring beyond doc snippets (CHAOS-2713 owns
  the *tested, runnable* GitHub Actions/GitLab CI examples — CHAOS-2711 only needs to link
  to them, not duplicate them), webhook-relay evaluation (CHAOS-2715), authorization design
  doc (CHAOS-2712 — CHAOS-2711 documents the *resulting* credential UX, not the design
  rationale).
- The legacy `/api/v1/ingest` router — CHAOS-2702/2711 must not test or document it as if it
  were the new surface; if it's still mounted when this work lands, the docs must explicitly
  disambiguate (§3.6).

---

## 2. Pinned API/data contract this brief tests and documents against

This section synthesizes the two plan docs into one concrete contract. Treat every field
name here as the name to code against; if CHAOS-2691/2694/2696/2712's actual PRs differ,
reconcile before writing the test.

### 2.1 REST surface (`/api/v1/external-ingest`, router owned by CHAOS-2691)

| Method | Path | Auth scope | Success | Failure modes |
|---|---|---|---|---|
| POST | `/api/v1/external-ingest/validate` | `schema:read` (validation only, no write) | `200` | `400` malformed envelope/unsupported schema version |
| POST | `/api/v1/external-ingest/batches` | `ingest:write` | `202` + `{ingestionId, status:"accepted", itemsReceived, stream}` | `400`, `401`, `403` (disabled source/wrong org/missing scope), `409` (idempotency conflict), `413` (too large), `429`, `503` (stream unavailable — **must not accept-and-warn**) |
| GET | `/api/v1/external-ingest/batches/{ingestion_id}` | `ingest:status` | `200` + status/diagnostics body | `404` unknown id, `403` cross-org |
| GET | `/api/v1/external-ingest/schemas` | `schema:read` | `200` list of `{schemaVersion, recordKinds[]}` | — |
| GET | `/api/v1/external-ingest/schemas/{schema_version}` | `schema:read` | `200` JSON Schema bundle | `404` unknown version |

Auth: `Authorization: Bearer <ingest-token>` — **NOT** a JWT; resolved by a dedicated
dependency in `api/external_ingest/auth.py` (per plan), independent of
`get_current_user`/`OrgIdMiddleware`. The test must NOT reuse `generate_auth_token()` from
`ci/run_live_backend_e2e.sh` (that mints a user JWT) for ingest-token-scoped calls.

### 2.2 Batch envelope (request body of `POST /batches` and `/validate`)

```json
{
  "schemaVersion": "external-ingest.v1",
  "idempotencyKey": "acme-github-prs-2026-06-26T00:00:00Z",
  "source": {
    "type": "customer_push",
    "system": "github",
    "instance": "github.com/acme",
    "producer": "dev-hops-cli",
    "producerVersion": "0.12.0"
  },
  "window": {
    "startedAt": "2026-06-25T00:00:00Z",
    "endedAt": "2026-06-26T00:00:00Z"
  },
  "records": [
    {"kind": "pull_request.v1", "payload": { ... }}
  ]
}
```

### 2.3 Status response (`GET /batches/{id}`)

```json
{
  "ingestionId": "uuid",
  "status": "accepted|processing|completed|partial|failed",
  "itemsReceived": 500,
  "itemsAccepted": 492,
  "itemsRejected": 8,
  "source": {"system": "github", "instance": "github.com/acme"},
  "window": {"startedAt": "...", "endedAt": "..."},
  "errors": [
    {"index": 12, "kind": "pull_request", "code": "missing_external_id",
     "message": "externalId is required", "path": "records[12].externalId"}
  ]
}
```

Status vocabulary used by the test's polling loop: `accepted -> processing ->
completed|partial|failed`. `partial` = some records rejected but batch still processed;
this is the state the E2E test's "rejected-record diagnostics" scenario must reach.

### 2.4 Stream naming (owned by CHAOS-2693, `api/external_ingest/streams.py`)

```
external-ingest:<org_id>:batches   # accepted-batch entries the worker consumes
external-ingest:<org_id>:dlq       # poison entries after normalization/sink failure
```

Follows the `_stream_consumer.py` `StreamConsumer` base class (already in the codebase at
`src/dev_health_ops/api/_stream_consumer.py`) — CHAOS-2693's consumer MUST subclass it, not
hand-roll `XREADGROUP`. Confirmed gotcha from recon: blocking reads MUST use
`get_consumer_redis_client()` (`socket_timeout=None`), never `valkey.from_url()` directly.

### 2.5 Postgres persistence (owned by CHAOS-2694/2696/2712 — sketch only)

No migration exists yet. Pattern to follow: `ProviderRateLimitObservation`
(`src/dev_health_ops/models/rate_limit_observations.py` + migration `0031`) — Postgres
(not ClickHouse) for a durable, independently-retained operational log; `org_id` as `Text`;
no hard FKs to sync/integration tables; direct-SQL-friendly. Next Alembic revision is
`0032` (confirmed: latest committed revision is `0031_add_provider_rate_limit_observations.py`).

```sql
-- customer_push_sources (CHAOS-2696)
CREATE TABLE customer_push_sources (
  id UUID PRIMARY KEY,
  org_id TEXT NOT NULL,
  system TEXT NOT NULL,               -- github|gitlab|jira|linear|custom
  instance TEXT NOT NULL,             -- e.g. github.com/acme
  mode TEXT NOT NULL,                 -- fullchaos_sync|customer_push|disabled
  enabled BOOLEAN NOT NULL DEFAULT true,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  UNIQUE (org_id, system, instance)
);

-- ingest_tokens (CHAOS-2696/2712) -- hashed like RefreshToken, never reversible
CREATE TABLE ingest_tokens (
  id UUID PRIMARY KEY,
  org_id TEXT NOT NULL,
  source_id UUID NULL REFERENCES customer_push_sources(id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  token_hash TEXT NOT NULL UNIQUE,
  scopes TEXT[] NOT NULL,             -- subset of {schema:read, ingest:write, ingest:status}
  created_at TIMESTAMPTZ NOT NULL,
  expires_at TIMESTAMPTZ NULL,
  last_used_at TIMESTAMPTZ NULL,
  revoked_at TIMESTAMPTZ NULL
);

-- external_ingest_batches (CHAOS-2694)
CREATE TABLE external_ingest_batches (
  ingestion_id UUID PRIMARY KEY,
  org_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  source_system TEXT NOT NULL,
  source_instance TEXT NOT NULL,
  schema_version TEXT NOT NULL,
  payload_hash TEXT NOT NULL,         -- for idempotency 409-on-mismatch (CHAOS-2695)
  window_started_at TIMESTAMPTZ NOT NULL,
  window_ended_at TIMESTAMPTZ NOT NULL,
  status TEXT NOT NULL,
  items_received INT NOT NULL DEFAULT 0,
  items_accepted INT NOT NULL DEFAULT 0,
  items_rejected INT NOT NULL DEFAULT 0,
  error_summary TEXT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  completed_at TIMESTAMPTZ NULL,
  UNIQUE (org_id, source_system, source_instance, idempotency_key)
);

-- external_ingest_rejections (CHAOS-2694)
CREATE TABLE external_ingest_rejections (
  id UUID PRIMARY KEY,
  ingestion_id UUID NOT NULL REFERENCES external_ingest_batches(ingestion_id) ON DELETE CASCADE,
  org_id TEXT NOT NULL,
  record_index INT NOT NULL,
  record_kind TEXT NOT NULL,
  external_id TEXT NULL,
  code TEXT NOT NULL,
  message TEXT NOT NULL,
  path TEXT NULL,
  created_at TIMESTAMPTZ NOT NULL
);
```

Registered as SQLAlchemy ORM models attached to the shared `Base` (`models/git.py`) so that
`Base.metadata.create_all(engine, checkfirst=True)` (the exact bootstrap `ci/run_live_backend_e2e.sh`'s
`generate_auth_token()` already uses) picks them up automatically without a separate Alembic
step in the live-e2e harness — see §6.2.

### 2.6 ClickHouse sinks (owned by CHAOS-2697/2698 — no new tables, reuse existing)

Per recon (`recon-models-sinks`, verified against `metrics/sinks/clickhouse/` and
`storage/clickhouse.py`): `repository.v1`/`pull_request.v1`/`review.v1`/`commit.v1`/
`identity.v1`/`team.v1` go through the **async** `ClickHouseStore` (`insert_repo`,
`insert_git_pull_requests`, `insert_git_pull_request_reviews`, `insert_git_commit_data`,
`insert_identities`, `insert_teams`); `work_item.v1`/`work_item_transition.v1`/
`work_item_dependency.v1` go through the **sync** `ClickHouseMetricsSink`
(`write_work_items`, `write_work_item_transitions`, `write_work_item_dependencies`). The
E2E test's ClickHouse assertions must therefore instantiate **both** client types and read
back through the correct one per kind — do not assume a single "sink" abstraction.

`repository.v1`'s `Repo.id` is a deterministic SHA256 UUID
(`get_repo_uuid_from_repo`, `models/git.py:72`) — the test's expected-row lookup must
compute the same UUID from the pushed repo identifier, not assume a customer-supplied ID.

All 9 target tables are `ReplacingMergeTree` — every ClickHouse read in the test MUST use
`FINAL` (or `argMax`) and filter `org_id` in the predicate (house rule), never a raw
`SELECT *`.

### 2.7 Bounded recompute (owned by CHAOS-2699)

Per recon-celery-metrics, the closest reusable pattern is
`build_post_sync_dispatch_payload` / `_dispatch_post_sync_tasks`
(`src/dev_health_ops/workers/post_sync_dispatch.py`), which chains
`run_daily_metrics -> run_work_graph_build -> dispatch_investment_materialize_partitioned`
with `celery.chain(..., immutable=True)`. CHAOS-2699 should reuse this, scoped by the
batch's affected org/repos/teams/window rather than a full-org recompute.
`dispatch_investment_materialize_partitioned` already accepts `repo_ids`/`team_ids`/
`from_date`/`to_date`/`force` kwargs implementing exactly this scoped-vs-full distinction —
CHAOS-2699 should pass through the batch's affected scope into these existing kwargs, not
invent a new recompute planner abstraction.

**Test seam** (§5.4): whatever CHAOS-2699 names its dispatch entrypoint, the test asserts
via `unittest.mock.patch.object(celery_app, "send_task")` on the shared Celery app
singleton (`src/dev_health_ops/workers/celery_app.py:80`, `celery_app = create_celery_app()`)
— every `.delay()`/`.apply_async()` call in this codebase funnels through `send_task`
eventually, so patching it is naming-agnostic and survives CHAOS-2699 renaming its own
internal function.

---

## 3. Design decisions

1. **E2E test lives in `tests/test_external_ingest_customer_push_live.py`, marked
   `@pytest.mark.clickhouse`, and additionally skipif's on missing `POSTGRES_URI`/
   `REDIS_URL`.** Rationale: it's the only test in the suite needing ClickHouse *and*
   Postgres *and* Valkey simultaneously live; reusing the existing `clickhouse` marker
   (not inventing a new one) keeps it discoverable via the one documented opt-in convention
   (`pytest -m clickhouse`), per house rule.

2. **Drive the HTTP layer in-process via `httpx.ASGITransport` against
   `dev_health_ops.api.main.app`, not a spawned uvicorn process.** Rationale: matches the
   established `tests/test_ingest_api.py` convention for the *legacy* ingest router; faster
   and more deterministic than the subprocess-based `ci/run_live_backend_e2e.sh` pattern,
   and this test needs many small validate/batch/status round-trips, not one boot sequence.

3. **Drive the worker deterministically by invoking the Celery task's underlying function
   directly (`task.apply()` after seeding the real Valkey stream), not by polling a real
   Celery worker subprocess.** Rationale: the consumer is a *bounded-iteration* task by
   convention (per recon-celery-metrics — worker-ingest tasks have `max_iterations`, not an
   infinite loop), so `task.apply()` runs one bounded pass synchronously against the real
   `XADD`/`XREADGROUP` machinery — this proves genuine Valkey stream integration (unlike
   `FakeValkey`) while avoiding sleep-based polling flakiness for the worker side. Do NOT
   use `CELERY_TASK_ALWAYS_EAGER` for this — that changes broker semantics globally; call
   the specific task function/`.apply()` instead.

4. **"Recompute queued, not run inline" is asserted two ways, not one:** (a) patch
   `celery_app.send_task` and assert it was called with recompute-scope kwargs matching the
   ingested org/repo/window (naming-agnostic per §2.7); AND (b) immediately after the
   worker's status transitions to `completed`/`partial`, assert the ClickHouse metrics
   tables (`daily_metrics` or equivalent — confirm exact table name against whatever
   CHAOS-2699 lands with) still have **zero** rows for the test org. Two independent signals
   because (a) alone doesn't prove nothing executed synchronously, and (b) alone doesn't
   prove anything was actually queued (both could be silently absent).

5. **Fixture payloads for all 9 v1 record kinds live in
   `tests/fixtures/external_ingest/v1/<kind>.json`** (one file per kind, containing both a
   `valid` and an `invalid` example under top-level keys) as the **single source of truth**,
   consumed by: this E2E test, CHAOS-2692's JSON-Schema `examples` output (must literally
   validate against these files, not a hand-copied duplicate — "examples pass validation" is
   CHAOS-2692's own AC), and CHAOS-2711's docs (embedded via mkdocs snippet include, see
   decision 8). Rationale: house rule "mocks must mirror real backend vocabulary" — CHAOS-
   2225 already shipped a prod bug from drifted duplicated fixtures; do not let 3 different
   issues hand-copy the same JSON three times.

6. **CHAOS-2701 ("Customer examples and docs") and CHAOS-2711 ("Developer and user
   documentation") scopes overlap almost completely on paper** (both list: REST vs GraphQL,
   auth, source registration, schema versions, idempotency, retries, status polling,
   rejected-record handling, example payloads, CLI walkthrough). Decision (escalated further
   in `decisionsNeeded` since it crosses issue ownership): **CHAOS-2711 owns the mkdocs
   *structure and prose* — the actual `docs/customer-push-ingestion/*.md` pages, nav, and
   ADR. CHAOS-2701 owns *only* the raw example-payload fixtures (§3.5, shared with
   CHAOS-2702) and does not need its own prose pages** — its "docs" AC should be satisfied by
   linking into CHAOS-2711's pages rather than authoring a second doc tree. Flag this
   explicitly to whoever picks up CHAOS-2701 so it isn't done twice.

7. **Do not wire the new live-DB test into `ci/local_validate.sh`'s default run.**
   Rationale: `local_validate.sh` is deliberately ClickHouse-only (see its own docstring —
   "NEVER touches Postgres, isolated scratch ClickHouse DB only") and provisions no
   Postgres/Valkey scratch resources. Forcing a 3-service live test into it would either (a)
   silently skip (masking real regressions) or (b) require rearchitecting a gate that's
   explicitly scoped and trusted as-is. Instead: the new test rides the **existing
   `live-e2e` CI tier** (`.github/workflows/live-e2e.yml` + `ci/run_tests.sh live-e2e`),
   extended with a Valkey service container (§6.1) — this tier already provisions fresh
   ClickHouse+Postgres per run and is the one place all three services already coexist.

8. **Add `pymdownx.snippets` to `mkdocs.yml`'s `markdown_extensions`, scoped to
   `tests/fixtures/external_ingest/`, so docs pages embed the literal JSON fixture files
   instead of hand-copying them.** Rationale: directly enforces decision 5 (single source of
   truth) at the docs layer; `mkdocs.yml` currently has no snippet-include extension
   (confirmed — only `admonition`/`attr_list`/`footnotes`/`toc`/`pymdownx.details`/
   `pymdownx.superfences`/`pymdownx.tabbed`), so this is new but minimal config, not a
   restructure.

9. **New nav section `Customer Push Ingestion:` in `mkdocs.yml`**, placed after the existing
   `Webhooks: webhooks.md` entry and before `Alerting: alerting.md` — mirrors how `CLI:` and
   `Connectors:` already got dedicated top-level sections for similarly cross-cutting
   surfaces, rather than burying this under `API:` (which is GraphQL-heavy) or `Metrics:`.

10. **`docs/webhooks.md` gets an explicit disambiguation note, not a rewrite.** Rationale:
    the existing page documents FullChaos-hosted provider webhooks
    (`/api/v1/webhooks/{provider}`) — a *different*, pre-existing mechanism from customer
    push. Per the webhook addendum plan doc, v1 customer push explicitly does NOT use these
    endpoints as its source of truth. Without a cross-reference, a customer configuring a
    GitHub webhook per `webhooks.md` could reasonably (and wrongly) believe they're doing
    customer-push setup. Add one admonition block near the top linking to the new
    `customer-push-ingestion/overview.md`.

11. **ADR-003 records the test/docs decisions in this brief**, not the backend architecture
    decisions from the plan docs (those belong to whichever of CHAOS-2691/2695/2696 lands
    first as PR — flag in `decisionsNeeded` that *someone* on the epic needs to write the
    backend-architecture ADR; it does not yet exist despite the house rule, because the plan
    docs under `docs/superpowers/plans/` are design docs, not ADRs, and nothing has merged
    yet). CHAOS-2711's ADR should note this gap explicitly rather than silently duplicate or
    silently ignore it.

12. **The legacy `/api/v1/ingest` router is explicitly called out as a distinct, older
    surface in the docs overview page**, with a one-paragraph note (not a deprecation
    notice — that's not this issue's call) so a reader who finds both routers in
    `/openapi.json` isn't confused about which one is "the" customer-push API. This directly
    surfaces the cross-cutting gotcha every recon brief flagged independently.

---

## 4. API/DDL/schema sketches — concrete, copy-pasteable

### 4.1 Fixture file shape (`tests/fixtures/external_ingest/v1/pull_request.json`)

```json
{
  "kind": "pull_request.v1",
  "valid": {
    "kind": "pull_request.v1",
    "payload": {
      "externalId": "acme/api#4821",
      "repository": "github.com/acme/api",
      "number": 4821,
      "title": "Add login feature",
      "state": "merged",
      "authorName": "Bob",
      "createdAt": "2026-06-25T08:00:00Z",
      "mergedAt": "2026-06-25T14:00:00Z"
    }
  },
  "invalid": {
    "kind": "pull_request.v1",
    "payload": {
      "repository": "github.com/acme/api",
      "title": "Missing external id"
    },
    "expectedError": {"code": "missing_external_id", "path": "payload.externalId"}
  }
}
```

One file per kind: `repository.json`, `identity.json`, `team.json`, `work_item.json`,
`work_item_transition.json`, `work_item_dependency.json`, `pull_request.json`,
`review.json`, `commit.json`. Field names inside `payload` are **placeholders** — the
authoritative field names come from CHAOS-2691's Pydantic schemas / CHAOS-2692's JSON
Schema; whoever implements CHAOS-2702 must regenerate these fixture files against the real
schemas at that time (do not hand-guess field casing — confirm `camelCase` vs `snake_case`
against the actual Pydantic model's `alias_generator`, since the envelope-level example in
the plan doc uses `camelCase` (`schemaVersion`, `idempotencyKey`) while internal Python
dataclasses use `snake_case`).

### 4.2 Test batch envelope builder (test helper, not production code)

```python
# tests/_helpers/external_ingest_fixtures.py (new)
from __future__ import annotations
import json
from pathlib import Path
from typing import Any

FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "external_ingest" / "v1"
ALL_KINDS = [
    "repository", "identity", "team", "work_item", "work_item_transition",
    "work_item_dependency", "pull_request", "review", "commit",
]

def load_fixture(kind: str) -> dict[str, Any]:
    return json.loads((FIXTURE_DIR / f"{kind}.json").read_text())

def build_batch_envelope(
    *, idempotency_key: str, source_instance: str = "github.com/acme",
    kinds: list[str] | None = None, inject_invalid: bool = False,
) -> dict[str, Any]:
    kinds = kinds or ALL_KINDS
    records = []
    for kind in kinds:
        fx = load_fixture(kind)
        records.append(fx["valid"])
    if inject_invalid:
        records.append(load_fixture("pull_request")["invalid"])
    return {
        "schemaVersion": "external-ingest.v1",
        "idempotencyKey": idempotency_key,
        "source": {
            "type": "customer_push", "system": "github", "instance": source_instance,
            "producer": "pytest-e2e", "producerVersion": "0.0.0",
        },
        "window": {"startedAt": "2026-06-25T00:00:00Z", "endedAt": "2026-06-26T00:00:00Z"},
        "records": records,
    }
```

### 4.3 E2E test skeleton (`tests/test_external_ingest_customer_push_live.py`)

```python
"""Live E2E: validate -> batch -> stream -> worker -> sinks -> status -> bounded recompute.

Opt-in (filtered from unit/CI-unit runs): pytest -m clickhouse.
Requires CLICKHOUSE_URI, POSTGRES_URI (or DATABASE_URI), and REDIS_URL simultaneously —
the only test module in this repo that needs all three live services at once.
"""
from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone
from unittest.mock import patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

CLICKHOUSE_URI = os.environ.get("CLICKHOUSE_URI")
POSTGRES_URI = os.environ.get("POSTGRES_URI") or os.environ.get("DATABASE_URI")
REDIS_URL = os.environ.get("REDIS_URL")

pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.skipif(
        not (CLICKHOUSE_URI and POSTGRES_URI and REDIS_URL),
        reason="Requires CLICKHOUSE_URI, POSTGRES_URI/DATABASE_URI, and REDIS_URL "
        "(live customer-push E2E: run via ci/run_live_backend_e2e.sh, not local_validate.sh)",
    ),
]


@pytest.fixture(scope="module")
def org_id() -> str:
    # Fresh per test-session org_id -- avoids idempotency-key/source-instance collisions
    # across repeated local runs against the same shared Postgres/ClickHouse target.
    return f"e2e-customer-push-{uuid.uuid4()}"


@pytest.fixture(scope="module")
def clickhouse_sink():
    from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink
    sink = ClickHouseMetricsSink(CLICKHOUSE_URI)
    sink.ensure_schema(force=True)
    yield sink
    sink.close()


@pytest_asyncio.fixture
async def client():
    from dev_health_ops.api.main import app
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest_asyncio.fixture
async def registered_source_and_token(org_id):
    """Registers a customer_push source and mints a scoped ingest token directly
    against Postgres (bypassing any admin UI/API not yet in scope for this test) --
    use direct SQL per house rule, mirroring CHAOS-2696/2712's expected schema (see
    brief section 2.5). Update the INSERT statements here if the real migration's
    column names differ.
    """
    # ... INSERT INTO customer_push_sources / ingest_tokens via direct SQL,
    # return {"token": "<plaintext>", "source_id": "...", "org_id": org_id}
    ...


@pytest.mark.asyncio
async def test_validate_rejects_invalid_record_without_enqueue(client, registered_source_and_token):
    from tests._helpers.external_ingest_fixtures import build_batch_envelope
    envelope = build_batch_envelope(idempotency_key="e2e-validate-1", inject_invalid=True)
    resp = await client.post(
        "/api/v1/external-ingest/validate", json=envelope,
        headers={"Authorization": f"Bearer {registered_source_and_token['token']}"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["valid"] is False
    assert body["itemsRejected"] >= 1
    assert any(e["code"] == "missing_external_id" for e in body["errors"])


@pytest.mark.asyncio
async def test_batch_accept_process_status_and_bounded_recompute(
    client, registered_source_and_token, clickhouse_sink, org_id,
):
    from tests._helpers.external_ingest_fixtures import build_batch_envelope
    token = registered_source_and_token["token"]
    headers = {"Authorization": f"Bearer {token}"}

    envelope = build_batch_envelope(idempotency_key="e2e-batch-1", inject_invalid=True)

    with patch(
        "dev_health_ops.workers.celery_app.celery_app.send_task"
    ) as mock_send_task:
        resp = await client.post(
            "/api/v1/external-ingest/batches", json=envelope, headers=headers,
        )
        assert resp.status_code == 202
        ingestion_id = resp.json()["ingestionId"]

        # Drive the worker synchronously -- one bounded pass over the real Valkey
        # stream this batch was just XADD'd to (decision 3). Import path/task name
        # TBD against CHAOS-2693's actual module; expected shape:
        #   from dev_health_ops.workers.external_ingest_consumer import consume_external_ingest_batches
        #   consume_external_ingest_batches.apply(kwargs={"max_iterations": 1})
        from dev_health_ops.workers.external_ingest_consumer import (
            consume_external_ingest_batches,
        )
        consume_external_ingest_batches.apply(kwargs={"max_iterations": 1})

        status_resp = await client.get(
            f"/api/v1/external-ingest/batches/{ingestion_id}", headers=headers,
        )
        assert status_resp.status_code == 200
        status_body = status_resp.json()
        assert status_body["status"] in ("completed", "partial")
        assert status_body["itemsAccepted"] >= 1
        assert status_body["itemsRejected"] >= 1
        assert any(e["code"] == "missing_external_id" for e in status_body["errors"])

        # Assertion (a): recompute was enqueued, naming-agnostic (decision 4).
        assert mock_send_task.called, "expected a Celery task to be enqueued for bounded recompute"
        enqueued_orgs = [
            call.kwargs.get("org_id") or (call.args[1].get("org_id") if len(call.args) > 1 else None)
            for call in mock_send_task.call_args_list
        ]
        assert org_id in enqueued_orgs or any(org_id in str(a) for a in mock_send_task.call_args_list)

    # Assertion (b): recompute was NOT executed inline -- metrics tables still empty
    # for this org immediately after the worker's status flip. Confirm exact table
    # name against CHAOS-2699's landed implementation.
    result = clickhouse_sink.client.query(
        "SELECT count() FROM daily_metrics WHERE org_id = {org_id:String}",
        parameters={"org_id": org_id},
    )
    assert result.result_rows[0][0] == 0

    # ClickHouse sink assertions -- FINAL + org_id predicate (house rule).
    from dev_health_ops.storage.clickhouse import ClickHouseStore, get_repo_uuid_from_repo
    store = ClickHouseStore(CLICKHOUSE_URI, org_id=org_id)
    repo_uuid = get_repo_uuid_from_repo("github.com/acme/api")
    pr_rows = clickhouse_sink.client.query(
        "SELECT number FROM git_pull_requests FINAL "
        "WHERE org_id = {org_id:String} AND repo_id = {repo_id:String}",
        parameters={"org_id": org_id, "repo_id": str(repo_uuid)},
    )
    assert len(pr_rows.result_rows) >= 1
```

This skeleton is intentionally incomplete at the two integration seams that don't exist yet
(worker task import path, exact status-store SQL) — fill those in against the real
CHAOS-2693/2694 implementations, not by guessing further.

### 4.4 ADR skeleton (`docs/architecture/adr/003-external-ingest-test-docs-strategy.md`)

```markdown
# ADR-003: Customer-Push Ingestion — E2E Test and Documentation Strategy

**Status**: ACCEPTED
**Created**: 2026-07-01
**Parent Issues**: CHAOS-2702, CHAOS-2711 (epic CHAOS-2690)

## Context
[summarize section 0/1 of this brief]

## Decisions
1. Live E2E test rides the `live-e2e` CI tier (not `local_validate.sh`) because it is the
   only place ClickHouse+Postgres+Valkey coexist live. [rationale from decision 7]
2. Bounded-recompute "queued not inline" is asserted via `celery_app.send_task` patch +
   zero-rows-in-metrics-table, independent of CHAOS-2699's internal task naming.
   [rationale from decision 4]
3. Example payload fixtures for all 9 v1 record kinds live once, in
   `tests/fixtures/external_ingest/v1/`, and are the single source of truth for
   CHAOS-2702's test, CHAOS-2692's schema `examples`, and CHAOS-2711's docs (via mkdocs
   snippet include). [rationale from decisions 5, 8]
4. CHAOS-2701 and CHAOS-2711 doc scopes overlap; CHAOS-2711 owns the doc tree, CHAOS-2701
   owns only the fixtures. [rationale from decision 6]

## Open follow-up
No ADR yet exists for the *backend* architecture decisions (REST-vs-GraphQL, one-active-
owner conflict policy, token scope model) described in
`docs/superpowers/plans/2026-06-26-external-customer-push-ingestion-api.md` and
`docs/superpowers/plans/2026-06-28-customer-push-webhooks-and-setup-design.md`. Those plan
docs are design docs, not ADRs. Whichever of CHAOS-2691/2695/2696 merges first should add
that ADR in the same changeset per house rule; this ADR does not attempt to record decisions
outside its own (test/docs) scope.
```

---

## 5. Files to create/modify (ops repo)

### New

- `tests/fixtures/external_ingest/v1/repository.json`
- `tests/fixtures/external_ingest/v1/identity.json`
- `tests/fixtures/external_ingest/v1/team.json`
- `tests/fixtures/external_ingest/v1/work_item.json`
- `tests/fixtures/external_ingest/v1/work_item_transition.json`
- `tests/fixtures/external_ingest/v1/work_item_dependency.json`
- `tests/fixtures/external_ingest/v1/pull_request.json`
- `tests/fixtures/external_ingest/v1/review.json`
- `tests/fixtures/external_ingest/v1/commit.json`
- `tests/_helpers/external_ingest_fixtures.py`
- `tests/test_external_ingest_customer_push_live.py`
- `docs/customer-push-ingestion/overview.md`
- `docs/customer-push-ingestion/api-reference.md`
- `docs/customer-push-ingestion/schemas-and-idempotency.md`
- `docs/customer-push-ingestion/troubleshooting.md`
- `docs/customer-push-ingestion/setup-guide.md`
- `docs/architecture/adr/003-external-ingest-test-docs-strategy.md`

### Modify

- `mkdocs.yml` — add `pymdownx.snippets` (base_path incl. `tests/fixtures/external_ingest`)
  to `markdown_extensions`; add `Customer Push Ingestion:` nav section (5 pages above)
  after `Webhooks: webhooks.md`; add ADR-003 line under `Architecture:` nav, alongside the
  existing ADR-001 line.
- `docs/webhooks.md` — add a disambiguation admonition near the top linking to
  `customer-push-ingestion/overview.md`.
- `docs/ops/cli-reference.md` — append a `## push` section for `dev-hops push
  validate|batch|sample` once CHAOS-2700 lands (stub the section now with a "coming in
  CHAOS-2700" note if docs land first — do not invent CLI flags not yet implemented).
- `ci/run_live_backend_e2e.sh` — add a Valkey/Redis readiness wait + `REDIS_URL` export,
  and a final step invoking `pytest tests/test_external_ingest_customer_push_live.py -m
  clickhouse -q` after the existing curl-based checks (§6.1).
- `.github/workflows/live-e2e.yml` — add a `valkey` service container (image
  `valkey/valkey:8-alpine`, port `6379:6379`) alongside the existing `postgres`/`clickhouse`
  services; export `REDIS_URL: redis://localhost:6379/0` in the `Run live-e2e tier` step's
  `env:` block.

---

## 6. Test plan

### 6.1 Unit-tier coverage (runs in default CI, no live services)

These are NOT this brief's primary deliverable (they belong to CHAOS-2691/2693/2694/2697)
but the E2E test's own helper module needs unit coverage:

- `tests/_helpers/external_ingest_fixtures.py`: `load_fixture` for all 9 kinds parses valid
  JSON and every file has both `valid` and `invalid` keys (a cheap guard test,
  `tests/test_external_ingest_fixtures_shape.py`, unmarked, runs in the full unit suite).

### 6.2 Live-DB tier (`@pytest.mark.clickhouse`, needs CH+PG+Valkey)

`tests/test_external_ingest_customer_push_live.py` (§4.3), covering:

1. `POST /validate` with a batch containing one deliberately-invalid record → `valid:
   false`, `itemsRejected >= 1`, specific `code`/`path` present, and (separately) assert the
   Valkey stream key for this org has **zero** entries after `/validate` (proves validate
   never enqueues).
2. `POST /batches` happy path → `202` + `ingestionId`; assert the real Valkey stream
   (`external-ingest:<org_id>:batches`) has exactly one new entry via `XLEN`/`XRANGE`.
3. Idempotency replay: POST the same envelope+idempotencyKey again → same `ingestionId`,
   no second stream entry (CHAOS-2695 contract — write this test even if it's arguably
   CHAOS-2695's own scope, since it's cheap given the harness already exists here; if
   CHAOS-2695 lands its own test first, dedupe rather than delete either).
4. Idempotency conflict: same idempotencyKey, different payload hash → `409`.
5. Worker pass (`consume_external_ingest_batches.apply(...)`) → status flips to
   `completed`/`partial`; ClickHouse rows visible via `FINAL` + `org_id` predicate for at
   least: `repos` (via deterministic UUID), `git_pull_requests`, `git_pull_request_reviews`,
   `git_commits`, `identities`, `teams` (via `ClickHouseStore`), and `work_items`,
   `work_item_transitions`, `work_item_dependencies` (via `ClickHouseMetricsSink`) — one row
   each is sufficient, this is not a coverage test of every field.
6. `GET /batches/{id}` after worker pass → `itemsAccepted`/`itemsRejected` match, rejection
   entries have `index`/`kind`/`code`/`message`/`path`.
7. Bounded recompute: assertions (a)+(b) from decision 4.
8. Disabled-source rejection: register a source with `enabled: false`, POST a batch with a
   token scoped to it → `403` (cross-check against CHAOS-2696's own AC; again, cheap to
   include here, dedupe if CHAOS-2696 lands its own test first).
9. Stream-unavailable → `503`: monkeypatch the Valkey client factory to raise
   `ConnectionError`, POST a batch → assert `503`, **not** `202` (the plan's explicit,
   deliberate divergence from the legacy ingest router's accept-and-warn behavior — this is
   the single highest-value regression this test can catch, since a reviewer pattern-
   matching against the legacy router could "fix" this back to accept-and-warn without
   realizing it's an intentional divergence).

### 6.3 Docs verification (CHAOS-2711)

- `make docs:check` (runs `check_investment_docs_drift.py` + `check_docs_links.py`) must
  pass — catches broken relative links/anchors in the 5 new pages + ADR-003.
- Manual `mkdocs build --strict` locally (not in CI per current `docs-guards.yml`, which
  only runs `make docs:check` — but `--strict` catches nav entries pointing at missing
  files, which `check_docs_links.py` does NOT validate, per its source — confirmed it only
  checks in-content relative markdown links, not `mkdocs.yml`'s `nav:` tree). Run this
  locally before opening the PR since CI won't catch a typo'd nav path otherwise.

---

## 7. Gate commands

### ops (this repo)

```bash
# Standard pre-push gate (ruff format/check, mypy, FULL unit suite, ClickHouse argMax proof).
# Use a per-issue SCRATCH_DB to avoid colliding with other worktrees running this
# concurrently against the same shared dev-health-clickhouse-1 container.
SCRATCH_DB=ci_local_validate_chaos2702 bash ci/local_validate.sh

# Typecheck alone (matches CI's typecheck.yml exactly):
.venv/bin/mypy --install-types --non-interactive .

# Docs guard (matches CI's docs-guards.yml):
make docs:check
mkdocs build --strict   # local-only extra check; not run in CI, see 6.3
```

**Important**: `bash ci/local_validate.sh` does NOT exercise the new live E2E test (decision
7) — passing it is necessary but not sufficient for CHAOS-2702. The live E2E test only runs
under the `live-e2e` tier (§6.2, §8).

### web (not touched by CHAOS-2702/2711 — no web files in scope)

Not applicable. If a future pass decides docs need cross-linking from
`dev-health-web`'s admin UI (CHAOS-2714's job), that PR runs its own gates
(`ci/run_tests.sh format/quality/unit` + targeted Playwright e2e) — out of scope here.

---

## 8. Live verification procedure

Two tiers, matching decision 7's split:

### 8.1 Full live-e2e tier (proves the whole pipeline, matches CI exactly)

```bash
cd /Users/chris/projects/full-chaos/dev-health/ops/.claude/worktrees/chaos-2690-integration

# Requires: a FRESH standalone postgres (db "test_db") + clickhouse (db "default", ch/ch)
# + valkey, NOT the shared dev-health-* compose stack (that stack uses different
# ports/db names -- see ci/run_live_backend_e2e.sh's own defaults). Easiest local setup:
docker run -d --rm --name e2e-pg -p 5432:5432 \
  -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=test_db \
  postgres:17.4
docker run -d --rm --name e2e-ch -p 8123:8123 \
  -e CLICKHOUSE_DB=default -e CLICKHOUSE_USER=ch -e CLICKHOUSE_PASSWORD=ch \
  clickhouse/clickhouse-server:25.1
docker run -d --rm --name e2e-valkey -p 6379:6379 valkey/valkey:8-alpine

export DISABLE_DOTENV=1
export DATABASE_URI=postgresql+asyncpg://postgres:postgres@localhost:5432/test_db
export POSTGRES_URI=postgresql+asyncpg://postgres:postgres@localhost:5432/test_db
export CLICKHOUSE_URI=clickhouse://ch:ch@localhost:8123/default
export REDIS_URL=redis://localhost:6379/0
export ENVIRONMENT=test
export OTEL_ENABLED=false

chmod +x ci/run_tests.sh ci/run_live_backend_e2e.sh
./ci/run_tests.sh live-e2e

# Cleanup:
docker stop e2e-pg e2e-ch e2e-valkey
```

Expect the existing `/health`/`/api/v1/meta`/`/api/v1/home` curl checks to still pass
(unchanged), plus the new pytest step's PASS output for
`test_external_ingest_customer_push_live.py`.

### 8.2 Targeted iteration against the shared dev compose stack (faster inner loop)

For iterating on the E2E test itself without the full harness boot each time, point
directly at the running dev stack's services (verify container names first — confirmed live
via `docker ps`: `dev-health-postgres-1` (port 5555, db `devhealth`),
`dev-health-clickhouse-1` (port 8123), `dev-health-valkey-1` (port 6379, DB 1 for streams
per `REDIS_URL: redis://valkey:6379/1` in `compose.yml`)):

```bash
export CLICKHOUSE_URI=clickhouse://ch:ch@localhost:8123/ci_customer_push_e2e   # scratch DB, never /devhealth's ClickHouse default
docker exec dev-health-clickhouse-1 clickhouse-client --user ch --password ch \
  --query "CREATE DATABASE IF NOT EXISTS ci_customer_push_e2e"
export POSTGRES_URI=postgresql+asyncpg://devhealth:devhealth@localhost:5555/devhealth   # confirm real dev credentials before running -- do NOT write org_id='' or shared rows; the test's org_id is a fresh uuid4 (decision 3.7-equivalent) so it's additive-only against this shared DB
export DATABASE_URI="$POSTGRES_URI"
export REDIS_URL=redis://localhost:6379/1

.venv/bin/python -m pytest tests/test_external_ingest_customer_push_live.py -m clickhouse -q
```

**Caution**: 8.2 runs against the shared dev-health Postgres/Valkey containers (not a
scratch instance, since neither has one built yet — see gap G1 below). It is additive-only
(fresh `org_id` per run) and should be fine for iteration, but do NOT use this path in CI;
CI must use 8.1's fully isolated fresh containers.

### 8.3 Docs

```bash
cd /Users/chris/projects/full-chaos/dev-health/ops/.claude/worktrees/chaos-2690-integration
pip install -r requirements-docs.txt   # mkdocs-material==9.5.39, one-time
mkdocs serve   # visually verify new nav section renders, snippet-embedded JSON displays
```

---

## 9. Dependencies on other sub-issues

CHAOS-2702 and CHAOS-2711 are terminal in the epic's dependency graph — they depend on
essentially every other sub-issue:

- **CHAOS-2691** (REST contract/schemas) — router, Pydantic models, exact field
  casing/names (§4.1's placeholder fields must be reconciled against this).
- **CHAOS-2692** (schema discovery/JSON Schema) — `GET /schemas*` endpoints the docs
  reference; its `examples` should literally validate against `tests/fixtures/
  external_ingest/v1/*.json` (decision 5) — coordinate so CHAOS-2692 doesn't duplicate them.
- **CHAOS-2693** (durable stream/DLQ) — stream naming, consumer task import path (§4.3's
  `TBD` marker).
- **CHAOS-2694** (status/rejection persistence) — exact Postgres schema (§2.5 is this
  brief's sketch, not authoritative).
- **CHAOS-2695** (idempotency/ownership policy) — 409-conflict and one-active-owner
  behaviors the E2E test asserts (§6.2 items 3, 4, 8).
- **CHAOS-2696** (source registration/token scopes) — auth dependency, token issuance path
  the test's `registered_source_and_token` fixture needs (currently a direct-SQL stub in
  §4.3, pending the real model).
- **CHAOS-2697** (worker normalization) — normalization correctness for all 9 kinds.
- **CHAOS-2698** (sink writes) — the ClickHouse row assertions in §6.2 item 5.
- **CHAOS-2699** (bounded recompute planner) — exact dispatch entrypoint and metrics-table
  name for §6.2 item 7 / decision 4.
- **CHAOS-2700** (dev-hops push CLI) — CHAOS-2711's "copy/paste examples for ... dev-hops
  push" AC and the `docs/ops/cli-reference.md` update are blocked on this landing (or at
  minimum its final flag names).
- **CHAOS-2701** (customer examples and docs) — overlap resolved by decision 6; coordinate
  so fixtures aren't duplicated.
- **CHAOS-2712** (authorization model/credential lifecycle) — token scope semantics,
  rotation/revocation UX the setup-guide.md documents.
- **CHAOS-2713** (CI/CD examples) — CHAOS-2711 links to these rather than re-authoring them.

Practical sequencing recommendation: CHAOS-2702/2711 should be picked up **last**, after at
minimum CHAOS-2691/2693/2694/2696/2697/2698/2699/2700 have merged. If schedule pressure
forces earlier work, split as: fixtures + docs skeleton + ADR + mkdocs wiring (no code
dependency, can start immediately) now, defer the actual E2E test module and the
prose-heavy doc pages until the dependencies land.

---

## 10. Risks

1. **Contract drift.** §2 is a synthesis of two plan docs, not merged code. If
   CHAOS-2691/2694/2696 land with materially different field names/status vocabulary, the
   E2E test skeleton (§4.3) and API reference doc need rework, not just fixture edits. Treat
   §2 as provisional and re-verify before writing final test code (per §0).
2. **`send_task`-patch seam may not catch every dispatch path.** If CHAOS-2699 dispatches
   recompute via a Celery `chain(...).apply_async()` built from *already-bound* task
   signatures rather than `celery_app.send_task(...)` directly, the patch still works
   (chain's `apply_async` internally calls `send_task` for each step) — but if CHAOS-2699
   instead calls a task's `.delay()` on an object that was imported and bound BEFORE the
   patch is applied (e.g. a module-level `task = some_task.si(...)` cached at import time),
   patching `celery_app.send_task` still catches it, since `.si()`/`.apply_async()` don't
   bypass the app's `send_task`. Low residual risk, but reverify once CHAOS-2699 lands.
3. **No existing Postgres scratch-isolation tool** (unlike ClickHouse's
   `local_validate.sh` scratch-DB pattern). §8.2's shared-dev-Postgres iteration path is
   additive-only but not as strongly isolated as the ClickHouse path — a bug in the test's
   cleanup (or a crashed run leaving rows behind) pollutes the shared dev Postgres with
   `e2e-customer-push-*` org rows. Mitigate with an explicit teardown fixture (`DELETE FROM
   ... WHERE org_id = :org_id` for every new table) and recommend 8.1 (fully fresh
   containers) as the default, not 8.2.
4. **CI cost/flakiness of adding a third live service to `live-e2e.yml`.** Valkey startup is
   fast and lightweight (alpine image, no persistent volume needed) so risk is low, but this
   is the first CI job in the repo running Postgres+ClickHouse+Valkey together — watch the
   first few CI runs for startup-ordering flakiness (Valkey's healthcheck should gate the
   pytest step same as Postgres/ClickHouse already do).
5. **`docs/webhooks.md` cross-link could go stale** if CHAOS-2715 (webhook-assisted
   ingestion evaluation) lands a different recommendation than "customer-owned relay only,"
   after CHAOS-2711 has already shipped a "provider webhooks are not customer-push" note.
   Low risk (CHAOS-2715 is explicitly evaluate-only for v1), but flag for a docs follow-up
   if CHAOS-2715's recommendation changes.
6. **Legacy `/api/v1/ingest` disambiguation note (decision 12) could be interpreted as
   scope creep** on a test/docs issue making a product decision (deprecate vs coexist) that
   isn't this issue's call. Mitigated by explicitly scoping the note to *disambiguation only*
   ("this is a different, older API"), not a deprecation recommendation — but flag to epic
   owner since every recon brief independently surfaced this as needing an explicit decision
   somewhere in the epic.
