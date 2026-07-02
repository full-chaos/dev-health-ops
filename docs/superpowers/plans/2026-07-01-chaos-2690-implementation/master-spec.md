# CHAOS-2690 External Customer-Push Ingestion API — Master Implementation Spec

Status: AUTHORITATIVE. Where any brief-*.md, plan doc, or Linear issue text disagrees with
this spec, this spec wins. Each brief carries a "SYNTHESIZER RECONCILIATION" header listing
its deltas. Plan-doc corrections (`:events` stream name, envelope example, source_instance
examples) are made by the implementing PRs, not here.

Repos:
- ops: `/Users/chris/projects/full-chaos/dev-health/ops/.claude/worktrees/chaos-2690-integration` (branch `chaos-2690-external-ingest`)
- web: `/Users/chris/projects/full-chaos/dev-health/web/.claude/worktrees/chaos-2690-integration` (branch `chaos-2690-external-ingest`)

---

## 1. Cross-cutting decisions (with rationale)

**CC1. Record envelope = wrapper shape, versioned kinds.**
`records: [{ "kind": "pull_request.v1", "externalId": "...", "payload": { ... } }]`.
- `kind` is ALWAYS the versioned string (`pull_request.v1`), never bare (`pull_request`) —
  registry keys, validation errors, rejection rows, and docs all use the versioned form
  (adopts 2691/2692; overrides 2700's bare-kind choice and the plan doc's own /validate
  error example, which is corrected in 2691's PR).
- Field name is `payload` (not `data` — overrides 2697/2700 sketches).
- Wrapper `externalId` is the per-record correlation ID used in rejection diagnostics
  (`external_ingest_rejections.external_id`); kind-specific payload fields are authoritative
  for normalization.
- Rationale: uniform error correlation across 9 heterogeneous kinds; per-record validation
  without a discriminated-union abort of the whole batch parse.

**CC2. Idempotency-Key: body field canonical, header optional-but-must-match** (2691 D1).
`400 idempotency_key_mismatch` when both present and different. CLI sends both.

**CC3. Batch limits: 1000 records / 10 MB body.**
`EXTERNAL_INGEST_MAX_RECORDS=1000` (→ `400 batch_too_large`),
`EXTERNAL_INGEST_MAX_BODY_BYTES=10_000_000` (→ `413 payload_too_large`), both env-overridable.
`records` has `min_length=1` (empty batches rejected at parse). `GET /schemas` exposes
`"limits": {"maxRecordsPerBatch": N, "maxBodyBytes": N}` so the CLI pre-check reads live
values instead of hardcoding. Overrides 2693's 8 MiB and 2694/2700's 5000-record assumptions.

**CC4. Repo identity: `repository.v1.externalId` = provider full name, NOT a URL.**
VERIFIED in code (processors/github.py:1572 `repo=repo_info.full_name`;
processors/gitlab.py:1815 `repo=path_with_namespace`; `get_repo_uuid_from_repo` strips+lowers):
native sync seeds the deterministic repo UUID with `owner/repo` / `group/subgroup/project`.
Therefore `externalId` for github/gitlab is the full name (e.g. `acme/api`), stored in
`Repo.repo` and passed to `get_repo_uuid_from_repo()` unchanged. For `custom`:
seed = `custom:{source_instance}:{externalId}`. This OVERRIDES 2691 D9's canonical-URL claim
(mis-verified). Identity continuity across fullchaos_sync ↔ customer_push handoff depends on
this exact string.

**CC5. source_instance grain = repo/project level; one-active-owner uses PER-PROVIDER
matching, not bare external_id equality.** (REVISED post-critique: `integration_sources.
external_id` is `owner/repo` for GitHub but the NUMERIC project_id for GitLab
(sync/discovery.py:107 vs :123/:133; api/admin/routers/sync.py:~535), `sync_options.
project_id|project_key`-or-config-name for Jira, and a team UUID or the literal `"linear"`
org-wide placeholder for Linear (api/admin/routers/sync.py:775-820) — bare exact-match
fails open for 3 of 4 providers.)
- Instance grain (unchanged): github/gitlab repo full name `owner/repo` /
  `group/subgroup/project` (== `repository.v1.externalId`); jira: project key (e.g. `ABC`);
  linear: team key (e.g. `CHAOS`); custom: stable slug.
- Per-provider managed-source matching (owned by 2695's `ownership.py`, used at
  registration by 2696 and at accept by 2695); all queries org- AND provider-scoped:
  - github: `external_id == instance OR full_name == instance`
  - gitlab: `full_name == instance OR metadata_->>'path_with_namespace' == instance
    OR external_id == instance`
  - jira: `external_id == instance OR full_name == instance`
  - linear: `external_id == instance OR full_name == instance OR name == instance`;
    PLUS: any enabled Linear IntegrationSource with `metadata_.org_wide_placeholder == true`
    (or `external_id == 'linear'`) owns ALL linear instances for the org → conflict.
  - custom: no managed equivalent; never conflicts.
- Registration-time resolution is STORED: matching runs once at registration; the matched
  row's id is persisted as `external_ingest_sources.matched_integration_source_id UUID NULL`
  (column added to 0032). Accept-time check = (a) indexed is_enabled lookup on the stored
  id when present, plus (b) the two exact indexed matches (external_id/full_name == instance)
  and the linear org-wide check, to catch managed sources created AFTER registration.
- Plan-doc org-level examples (`github.com/acme`) are corrected in docs/UI copy.
  Consequence: a batch is scoped to ONE source instance; multi-repo customers register each
  repo and push per-instance batches (CLI handles splitting). Registration UX copy (2714)
  says "repository full name". Residual risk (documented in adr-003): a team-scoped managed
  Linear source stores a team UUID that cannot be equated to a team key without a Linear API
  call; matching falls back to full_name/name and the org-wide rule — docs instruct
  disabling managed Linear sync before enabling customer push for the same team.

**CC6. Kind × system matrix (worker + /validate enforce; violations are per-record
`unsupported_kind_for_system` / `record_outside_source_instance` rejections):**
| system | allowed kinds |
|---|---|
| github, gitlab | repository, pull_request, review, commit, work_item, work_item_transition, work_item_dependency, team, identity |
| jira, linear | work_item, work_item_transition, work_item_dependency, team, identity |
| custom | repository, pull_request, review, commit, team, identity (NO work_item family in v1 — avoids widening `WorkItem.provider` vocabulary) |
For github/gitlab/custom, every git-family record's `repositoryExternalId` MUST equal
`source.instance`. team.v1/identity.v1 are org-scoped and accepted from any enabled
customer_push source.

**CC7. work_item_id derivation is SERVER-SIDE (external_ingest/ids.py, owned by 2698).**
Customers send provider-native `externalKey` (jira `ABC-123`, linear `CHAOS-123`,
github/gitlab issue/PR number) — never the internal namespaced ID. VERIFIED formats:
`jira:{key}`, `linear:{identifier}`, `gh:{repo}#{n}` / `ghpr:{repo}#{n}` (type=="pr"),
`gitlab:{repo}#{iid}` / `gitlab:{repo}!{iid}` (type=="merge_request").
`work_item_transition.v1`/`work_item_dependency.v1` carry optional `workItemType` to
disambiguate issue vs PR/MR namespaces (default: issue). OVERRIDES 2691's
`workItemId`-supplied-by-customer field: `WorkItemV1.work_item_id` is replaced by
`external_key` (alias `externalKey`).

**CC8. Provider attribution: `provider` = source system; provenance = new nullable
`source_id UUID` ClickHouse column** (2698 D1 + 2691 D10 ratified; OVERRIDES 2697 D5's
`provider="customer_push"`, which would flip provider on RMT replace and break
provider-branching readers). CH migration `065_external_ingest_source_id.sql` adds
`source_id Nullable(UUID)` to the 9 target tables; pushed rows stamp the registered
source's UUID; native sync leaves NULL. `Repo.provider` gains `"custom"` as a legal value
for custom-system repos (readers must tolerate it — flagged in 2698 tests).

**CC9. Payload placement: stream carries a pointer; raw batch JSON lives in Postgres**
(`external_ingest_batch_payloads`) (2693 D2 ratified). Stream entry fields (pinned contract
2693↔2697): `ingestion_id, org_id, source_system, source_instance, schema_version,
idempotency_key, record_count, window_started_at, window_ended_at, enqueued_at`.
Worker fetches payload by `ingestion_id` (+org_id predicate). Worker deletes the payload row
on terminal status; beat prune sweeps orphans after `EXTERNAL_INGEST_PAYLOAD_MAX_AGE_HOURS`
(168). Wave-1 interim (2691 only): `enqueue_batch()` additionally XADDs `payload` inline
(nothing consumes it); 2693's PR removes that param and updates the router call site — an
approved, planned call-site change.

**CC10. Stream/queue naming (pinned):** streams `external-ingest:{org_id}:batches`,
DLQ `external-ingest:{org_id}:dlq` (per-org), consumer group `external-ingest-consumers`,
Celery queue `external-ingest`, compose worker `worker-external-ingest`
(`-Q external-ingest --concurrency=1`). Producer helper is
`api/external_ingest/streams.py::enqueue_batch()` (2691's name kept; 2693 hardens in place).
`:events` (plan doc) is dead; `external_ingest` (underscore, 2697) is dead.

**CC11. Fail-closed 503 + reclaim-based redelivery.**
`enqueue_batch` raises `StreamUnavailableError` → router returns `503 stream_unavailable`
(never accept-and-warn). Consumer subclasses `StreamConsumer` with the NEW additive,
default-off reclaim extension (2693 D5: `enable_reclaim/reclaim_idle_ms=900_000` (15 min —
raised from 60s post-critique: the in-process retry ladder alone sleeps 14s and a
1000-record batch can exceed 60s, risking duplicate concurrent processing)
`/max_deliveries=5` via XPENDING/XCLAIM). Deployment invariant (documented in brief-2693 +
compose comment): exactly ONE worker-external-ingest replica at concurrency=1; scaling
replicas requires revisiting reclaim semantics. Idempotent-skip guard: before processing
any entry (fresh or reclaimed), the consumer checks batch status — terminal
(completed/partial/failed) → ACK and skip without reprocessing. Retry ladder: worker does
bounded in-process retry (3×, 2s/4s/8s) on
sink-write transients; exhausted → raise transient → entry left UNACKED → reclaimed up to
max_deliveries → then DLQ + ACK + `mark_batch_failed()`. `PermanentProcessingError`
(defined in `external_ingest/errors.py`, owned by 2693) → immediate DLQ + ACK + failed.
This SUPERSEDES 2697 D9/D10's "no redelivery exists" analysis (true of the base class today;
2693 adds it). Ultimate durability = reclaim + RMT-upsert idempotency + persisted status +
customer resubmission.

**CC12. Batch status enum (single vocabulary, everywhere):**
`accepted → (stream_unavailable) → processing → completed | partial | failed`.
- `accepted` = row committed (server_default), enqueued or about to be.
- `stream_unavailable` = row committed but enqueue failed (client got 503; retryable).
- Terminal: `completed` (0 rejected), `partial` (some rejected), `failed` (0 accepted or
  system failure). `attempts` int column counts accept attempts.
Overrides: 2693's `received`, 2714's `rejected|ignored_unsupported_event` (webhook-addendum
vocabulary — display-only, not batch statuses), 2715's `processed` (→`completed`).

**CC13. Idempotency: 4-way outcome, hash over validated envelope** (2695 ratified).
`payload_hash = sha256(json.dumps(envelope.model_dump(mode="json"), sort_keys=True,
separators=(",",":")))`. Outcomes: NEW / REPLAY / CONFLICT (409) / RETRY.
`RETRYABLE_STATUSES = {"stream_unavailable", "failed"}` — resubmitting a failed batch with
the same key+hash is a fresh accept, SAME ingestion_id, attempts+=1, status reset to
accepted (2697 D11 amendment RATIFIED). STALE-ACCEPTED RECOVERY (post-critique — closes the
fail-open where a committed batch's stream entry is lost to an API crash-before-XADD or
stream trim): a same-key+hash resubmission against a batch still in `accepted`/`processing`
with `updated_at` older than `EXTERNAL_INGEST_ACCEPTED_STALE_MINUTES=15` is treated as
RETRY (attempts+=1, payload row refreshed per CC22, re-enqueue SAME ingestion_id); younger
than the threshold → REPLAY. REPLAY returns **200 OK with the full
GET-status-shaped envelope** (not 202) — ratified deviation from the plan's literal example;
CLI uses it to short-circuit polling. Idempotency DB primitives (payload_hash column +
unique `(org_id, source_system, source_instance, idempotency_key)`) live in 2694's DDL;
2695 ships ZERO migrations. (Defense-in-depth beat reconciler for never-resubmitted stale
batches stays a filed follow-up issue, out of v1.)

**CC14. Auth model** (2696/2712):
- Tables: **`external_ingest_sources`**, **`external_ingest_tokens`** (renamed from
  2696-brief's `ingest_sources`/`ingest_tokens` for feature-family consistency and to avoid
  legacy-`/api/v1/ingest` confusion). Model classes `IngestSource`/`IngestToken` in
  `models/ingest_auth.py` (token mint/hash helpers live here too, NOT in
  api/external_ingest/auth.py, so wave 1 has no file collision with 2691).
- Token format `fcpush_` + `secrets.token_urlsafe(32)`, sha256-hashed, `token_prefix` (12
  chars) stored for display. One-time plaintext on create/rotate only. (2714's `fchp_live_`
  sketch is wrong → `fcpush_`.)
- Scopes: `schema:read`, `ingest:write`, `ingest:status`; `ingest:write` requires
  source-bound token; provider scopes reserved/no-op.
- `IngestAuthContext(org_id: str, scopes: frozenset[str], token_id: str | None,
  source: IngestSource | None)` — frozen dataclass; single dependency factory
  `require_ingest_scope(scope)` in `api/external_ingest/auth.py`. 2691 ships the interim
  body; 2712 swaps the body, signatures and Depends() call sites unchanged.
  Interim auth has a MECHANICAL guard, not just a process gate (post-critique): the interim
  dependency HARD-FAILS with `503 auth_not_configured` unless env
  `EXTERNAL_INGEST_INSECURE_AUTH=1` is set (set only in local compose/test env files, never
  in any deployed environment). When enabled it accepts any bearer + X-Org-Id and logs a
  loud WARNING per request. 2712 deletes the flag and the interim body entirely. Merging
  the integration branch to main remains additionally gated on 2712 (hard pre-GA blocker).
- `webhook_mode TEXT NOT NULL DEFAULT 'disabled'` and `webhook_secret_id UUID NULL` are
  reserved columns in migration 0032 NOW (2715's must-not-foreclose contract); API accepts
  `webhook_mode ∈ {disabled, customer_relay}`, 400s on `fullchaos_hosted`.
- One-active-owner (PINNED post-critique; 2696 D8's warn-only stance OVERRULED): at
  registration, per-provider matching (CC5) runs against `integration_sources`; if a
  matching managed source is ENABLED → `409 source_owned_by_fullchaos_sync`; if matched
  but disabled → registration succeeds and the match id is stored in
  `matched_integration_source_id`. Accept-time re-check (403, via
  `resolve_effective_mode()` in 2695's ownership.py) is the authoritative guard and also
  catches managed sources created/re-enabled after registration. Wave note: 2696 (wave 1)
  ships the registration-time check inline with the CC5 matching rules; 2695 (wave 4)
  extracts/absorbs it into `ownership.py` without behavior change. Instance-level hard
  XOR + provider-level soft warning retained.

**CC15. Rate limiting**: shared slowapi `limiter` singleton +
`get_ingest_token_key` (sha256(token)[:16], IP fallback) added to
`api/middleware/rate_limit.py` **by 2691 in wave 1** (constants
`INGEST_BATCH_LIMIT="60/minute"`, `INGEST_VALIDATE_LIMIT="60/minute"`,
`INGEST_READ_LIMIT="120/minute"`); OVERRIDES 2691's
second-Limiter sketch and moves this out of 2712's file list (2712 verifies + owns audit of
failures). 429 uses the external-ingest error envelope. READ limits (post-critique):
`INGEST_READ_LIMIT` applies to GET /schemas + GET /schemas/{version} (public → keyed by IP
via the token-or-IP key func; applied by 2691, kept by 2692) and to GET /batches list +
detail (token-keyed; applied by 2694).

**CC16. Error envelope for ALL `/api/v1/external-ingest/*` responses** (2691 D3 ratified):
`{"error": {"code", "message", "errors"?: [...]}}` via `ExternalIngestError` + dedicated
handler. This INCLUDES auth failures and 2695's ownership/idempotency errors (2695's
HTTPException-detail shape is overridden; its `ingest_error()` helper raises
`ExternalIngestError`). Admin `/api/v1/admin/customer-push/*` keeps the house
HTTPException/snake_case conventions (two surfaces, two conventions — deliberate).
Canonical code vocabulary:
| status | codes |
|---|---|
| 400 | invalid_envelope, unsupported_schema_version, unknown_record_kind, idempotency_key_mismatch, batch_too_large |
| 401 | invalid_token |
| 403 | insufficient_scope, source_not_registered, source_disabled, source_owned_by_fullchaos_sync, source_mismatch |
| 404 | not_found (batch, schema version) |
| 409 | idempotency_conflict |
| 413 | payload_too_large |
| 429 | rate_limited |
| 503 | stream_unavailable, ingest_temporarily_unavailable (sub-ms concurrent same-key
  insert race, 2695), auth_not_configured (interim-auth guard, 2691; deleted by 2712) |
(2695's `idempotency_key_conflict`/`ingest_stream_unavailable` renamed to match. CLI and
customer docs: ALL 503s are retryable regardless of code.)
Per-record rejection codes: missing_required_field, invalid_literal, unknown_kind,
unsupported_kind_for_system, record_outside_source_instance, invalid_reference,
clickhouse_insert_failed.

**CC17. Validation single source of truth** = Pydantic models in
`api/external_ingest/schemas.py` (2691). 2692's registry imports `RECORD_KIND_MODELS` from
there (never re-declares); the worker re-validates by importing the same models.
`external_ingest/validate.py` (deep per-record validation over those models) has ONE
owner: CREATED COMPLETE by 2691 in wave 1 (it powers POST /validate); 2697 imports it
UNCHANGED in wave 4 — it does not create or extend it (post-critique fix; OVERRIDES 2697
D1's "validation lives in external_ingest/validate.py" as the model home AND the module
map's earlier 2697 attribution). `/batches` = envelope + kind-allowlist check only (400 on
any unknown kind);
deep per-record validation runs in `/validate` (eager) and in the worker (always re-run).
`/validate` requires `schema:read`; `GET /schemas*` is public; `POST /batches` requires
`ingest:write`; `GET /batches*` requires `ingest:status`.

**CC18. Canonical example payloads** live in
`src/dev_health_ops/api/external_ingest/examples/<kind>.json` (2692, packaged, loaded by the
registry, importlib-resources-readable). Consumers: `dev-hops push sample` calls
`schema_registry.load_example(kind)` (2700 — its separate `push/samples/` dir is DROPPED);
2701 copies to `docs/examples/external-ingest/` with a byte-identity drift pytest;
2702's e2e valid fixtures assert equality with the package examples (its invalid fixtures
live under `tests/fixtures/external_ingest/v1/`). One source of truth, three no-drift checks.

**CC19. Postgres migrations — fixed chain (collision resolved):**
- `0032_add_customer_push_ingest_auth.py` — CHAOS-2696 (wave 1): external_ingest_sources
  (+webhook_mode, webhook_secret_id), external_ingest_tokens.
- `0033_add_external_ingest_status_store.py` — CHAOS-2694 (wave 2): external_ingest_batches
  (incl. payload_hash, attempts, record_counts JSON, producer, producer_version, unique idem
  index), external_ingest_rejections (FK CASCADE), external_ingest_batch_payloads
  (2693's table — DDL+model hosted in 2694's migration/models file; 2693 owns the
  raw-SQL `payload_store.py` accessors).
- `0034_add_external_ingest_recompute.py` — CHAOS-2699 (wave 3): recompute status columns
  per brief-2699 DDL (renumbered from its 0033 sketch).
ClickHouse: `065_external_ingest_source_id.sql` — CHAOS-2698 (wave 3).
All models in `models/external_ingest.py` (2694) and `models/ingest_auth.py` (2696);
declarative classes are DDL/tests-only; runtime reads/writes are `text()` SQL (plan mandate,
sqlite-portable — no RETURNING/ON CONFLICT). **No new `postgres` pytest marker** in this
epic (2694 D4 convention wins over 2699's proposal); live-Postgres checks are runbook steps.

**CC20. Celery wiring has ONE owner: CHAOS-2693** (queue `external-ingest` in
`workers/config.py` task_queues, late-ack exclusions for BOTH
`run_external_ingest_consumer` AND `flush_external_ingest_recompute` (on 2699's behalf,
name pinned), beat entries for consumer(30s)+stream-health(60s), compose service). 2694 adds
only its own prune-task beat entry (wave 2, crontab 05:15, `sync` queue). 2699 adds NO
workers/config.py or compose edits (flush task runs on `default` queue via decorator, no
workers/tasks.py re-export — explicit task name instead). 2697 adds NO queue/compose wiring
(its brief's D2 wiring claim is reassigned to 2693).

**CC21. Bounded recompute** (2699 ratified): Valkey-SETNX debounce
(45s, key grain `(org_id, source_system, source_instance)`), pure-function planner over
PRIMITIVES (`schedule_or_coalesce(*, org_id, source_system, source_instance, ingestion_id,
repo_ids, team_ids, window_start, window_end, record_kinds)`) — no shared dataclass import
between 2698/2699 (2697 maps `AffectedScope` → kwargs). Fan-out: per-repo
`run_daily_metrics → run_work_graph_build` chains (immutable=True), caps
`EXTERNAL_INGEST_RECOMPUTE_MAX_FANOUT_REPOS=25` / `..._MAX_BACKFILL_DAYS=14`;
ONE `dispatch_investment_materialize_partitioned(repo_ids=…, team_ids=…, force=False)` per
flush, NEVER with both scopes empty (hard invariant D4). Work-item-only batches with zero
repo scope: day-bounded org-wide `run_daily_metrics` fallback ONLY within the caps, else
skip + `recompute_status="skipped_no_scope"`. NOT reusing
`dispatch_daily_metrics_partitioned` (no org filter) / `run_daily_metrics_batch`
(checkpoint-skip swallows same-day data). Investment/LLM recompute stays in the debounced
flush, scoped, force=False (same-day investment freshness best-effort until nightly run).
Dispatch failures never fail ingestion.
recompute_status enum PINNED EPIC-WIDE (post-critique — brief-2714's
`queued|running|completed|not_applicable` union was wrong):
`not_applicable | pending | dispatched | skipped_no_scope | failed`
(0034 server_default `not_applicable`). Surfacing ownership: 2699 (wave 3) owns BOTH
migration 0034 AND extending the GET batch-detail response block in 2694's
`api/external_ingest/status.py` (a deliberate cross-wave file touch; 2694 does not
reference recompute columns in wave 2).

**CC22. Router flow final owner = CHAOS-2695 (wave 4).** Pinned sequence for
`POST /batches`: auth → size/parse/kind checks (400/413) → ownership
(`resolve_effective_mode` → 403s) → `resolve_batch_idempotency` FIRST-write (409/replay-200)
→ UPSERT payload row (payload_store) same txn → COMMIT → `enqueue_batch` pointer (503 →
`mark_stream_unavailable`, commit-before-raise) → RETRY outcome re-enqueues with existing
ingestion_id → 202. payload_store semantics (post-critique — fixes the RETRY PK collision):
`upsert_payload()` is SELECT-by-PK-then-UPDATE-or-INSERT inside the same accept
transaction (sqlite-portable; no ON CONFLICT/RETURNING per CC19). The idempotency row —
already FIRST-written under its unique index — is the serialization point, so two
same-key accepts cannot interleave payload writes; the residual sub-ms concurrent-insert
race stays the `503 ingest_temporarily_unavailable` path (CC16). Covers both RETRY cases:
stream_unavailable (payload row exists, worker never ran → UPDATE refreshes it) and failed
(worker may have deleted it → INSERT recreates it). Until wave 4, 2691's simpler interim
flow (validate → uuid4 → inline-payload enqueue → 202) stands.

**CC23. Worker contract (pinned 2693↔2697):**
`external_ingest/processor.py::process_batch(*, ingestion_id, org_id, source_system,
source_instance, schema_version) -> int`, raises `PermanentProcessingError` (DLQ now) or
any other exception (transient → reclaim path); also exposes
`mark_batch_failed(ingestion_id, org_id, reason)` for the consumer's give-up path.
Processor: fetch payload → re-validate per record (kind×system matrix) → normalize via
2698's `ids.py`/`types.py` → `sinks.write_batch()` → status writes via `StatusReporter`
protocol (2697 D7; Postgres impl bound at task construction, wave 4) → delete payload row →
`schedule_or_coalesce(...)`. org_id stamped explicitly on every record (never rely on
store auto-inject). Statuses upsert-by-ingestion_id (replay-safe).

**CC24. Sinks** (2698 ratified): `sinks.write_batch(batch: NormalizedBatch, *, clickhouse_dsn)
-> SinkWriteResult`; two client lifecycles per invocation (async ClickHouseStore for git
family+team+identity; sync ClickHouseMetricsSink via to_thread for work_item family);
identities/teams pass through customer `updatedAt` (RMT version col — never stamp now() when
supplied), CLAMPED (post-critique): values more than 5 minutes in the future are replaced
with server now() and recorded as a per-record warning diagnostic (not a rejection) —
prevents a buggy/malicious `updatedAt=2100-01-01` from permanently pinning an RMT row;
work-item assignees/reporter through `resolve_identity()`; git-family
author/reviewer strings raw. `review.v1.state` allow-list Literal (2691 D12).
Live-CH round-trip tests for all 9 kinds are OWNED BY 2698 (2697 keeps fakeredis+mocked-sink
integration tests; 2702 owns the full-stack e2e).

**CC25. Admin surface (REST, snake_case, session-JWT + require_admin) —
`/api/v1/admin/customer-push/*`; NO GraphQL mutations for this epic.** (Correction
post-critique: the earlier "verified query-only schema" parenthetical was FALSE —
api/graphql/schema.py:814 defines a Mutation root with five saved-report mutations. The
REST-only decision stands, grounded in the epic's non-goals, not an absent Mutation root.)
Ownership split:
- 2696 (wave 1): sources CRUD + tokens issue/rotate/revoke + org-wide token list.
- 2694 (wave 2): `GET .../sources/{id}/batches` (filters: status, producer, from, to,
  limit/offset — record_kind filter DROPPED in v1), `GET .../batches/{ingestion_id}`
  (includes rejected_records + record_counts; recompute_status is NOT in 2694's wave-2
  response — 2699 adds that block in wave 3, see CC21), `GET .../schemas*` proxy.
- 2695 (wave 4): `POST .../sources/{id}/validate` ONLY. (PRODUCT DECISION post-critique:
  web Screen 5 is VALIDATE-ONLY in v1 — the console-push proxy
  `POST .../sources/{id}/batches` with producer="web-console" is CUT from v1 and moved to
  the v2/follow-up list; the ingestion write path stays exclusively token-authed.)
Data-plane `GET /api/v1/external-ingest/batches` (token-authed list) also ships in 2694.
Web token status (`active/revoked/expired/never_used`) and producer buckets are
CLIENT-DERIVED (2714 D8/D10); `last_result` field dropped in v1. Field names in 2714's TS
types mirror 2696/2694's Pydantic schemas exactly (`id`, not `source_id`, on source rows).

**CC26. ADR numbering (collision resolved):** adr-003 external-ingest REST boundary +
token/ownership model (CHAOS-2691's PR, wave 1 — this is "the backend ADR" 2702 flagged as
unowned); adr-004 webhook-assisted customer push (2715); adr-005 schema discovery/export
(2692); adr-006 e2e/docs strategy (2702). Flat `docs/architecture/adr-NNN-*.md` naming.
mkdocs.yml is edited ONLY by 2711 (nav + pymdownx.snippets); 2702 does not touch it.

**CC27. Docs split:** 2711 = mkdocs structure/prose (customer-push-ingestion/*.md, nav,
webhooks.md disambiguation admonition, legacy-ingest note); 2701 = docs/examples fixtures +
drift tests + per-kind walkthrough stubs linking into 2711 pages; 2713 = CI/CD example files
(GitHub Actions / GitLab CI / Docker / cron+systemd / cURL) PLUS the webhook-relay example
tab and reconciliation-schedule guidance (absorbs 2715 follow-ups #3/#5).

**CC28. Legacy `/api/v1/ingest` router**: untouched by this epic; disambiguation note in
docs (2711/2702 D12); reconcile/deprecate is a NEW ISSUE (filed by epic owner) — every
recon flagged it, no sub-issue owns it. PINNED (product decision post-critique): the new
issue does NOT block external-ingest GA.

**CC29. dev-hops push CLI** (2700 ratified with edits): commands
`push validate|sample|batch|status|export(stubbed)`; httpx.AsyncClient +
`retry_with_backoff` (retry 429/ALL 503s regardless of error code/network only); exit codes 0/1/2/3/4 as briefed; `--json`
boolean; env `FULLCHAOS_INGEST_TOKEN` (primary) / `FULLCHAOS_API_TOKEN` (deprecated alias) /
`FULLCHAOS_API_URL` / `FULLCHAOS_ORG_ID`; excluded from `--org` auto-resolution and DB
preflight; imports 2691's models for offline validate; sample payloads via
`schema_registry.load_example`; terminal poll set {completed, partial, failed};
`stream_unavailable` → print "re-run push batch" hint, exit 3.

---

## 2. Canonical interface contracts (quick reference)

### Batch envelope (wire, camelCase)
```json
{
  "schemaVersion": "external-ingest.v1",
  "idempotencyKey": "acme-github-prs-2026-06-26T00:00:00Z",
  "source": {"type": "customer_push", "system": "github", "instance": "acme/api",
             "producer": "dev-hops-cli", "producerVersion": "0.12.0"},
  "window": {"startedAt": "...", "endedAt": "..."},
  "records": [{"kind": "pull_request.v1", "externalId": "acme/api#123", "payload": {...}}]
}
```
Pydantic names (2691's, importable by 2700): `BatchEnvelope`, `SourceDescriptor`,
`IngestWindow`, `RecordEnvelope`, `RECORD_KIND_MODELS`, `ValidationErrorItem`,
`ValidationResponse`, `BatchAcceptedResponse`, `SCHEMA_VERSION`. idempotency_key
max_length=255. Records list min 1 / max `EXTERNAL_INGEST_MAX_RECORDS`.

### Endpoints
Data plane (`/api/v1/external-ingest`): POST /batches (ingest:write, 202/200-replay),
POST /validate (schema:read), GET /batches (ingest:status, list), GET /batches/{id}
(ingest:status), GET /schemas, GET /schemas/{version} (public, ETag/304, `limits` field).
Admin plane (`/api/v1/admin/customer-push`): see CC25.

### DDL summary (Postgres)
- `external_ingest_sources(id PK, org_id text idx, system, instance, display_name, mode
  default 'disabled', enabled bool, webhook_mode default 'disabled', webhook_secret_id null,
  matched_integration_source_id UUID null (CC5 registration-time ownership resolution),
  created_by_user_id, created_at, updated_at, UNIQUE(org_id, system, instance))`
- `external_ingest_tokens(id PK, org_id idx, source_id FK null, name, token_hash unique,
  token_prefix, scopes JSON, created_by_user_id, expires_at, revoked_at, last_used_at,
  last_used_ip, created_at, idx(org_id, revoked_at))`
- `external_ingest_batches(ingestion_id PK, org_id, idempotency_key, payload_hash,
  source_system, source_instance, producer, producer_version, schema_version,
  window_started_at, window_ended_at, status default 'accepted', attempts int default 1,
  items_received/accepted/rejected, record_counts JSON, error_summary JSON, created_at,
  updated_at, completed_at, UNIQUE(org_id, source_system, source_instance, idempotency_key),
  + 2699's 0034 recompute columns)`
- `external_ingest_rejections(id PK, org_id, ingestion_id FK CASCADE, record_index,
  record_kind, external_id, code, message, path, created_at)` — cap 1000 rows/batch stored;
  true totals + top_codes in error_summary.
- `external_ingest_batch_payloads(ingestion_id PK, org_id idx, schema_version,
  payload_json bytea, byte_size, created_at)` — transient, worker-deleted.
ClickHouse: `source_id Nullable(UUID)` on repos, git_commits, git_pull_requests,
git_pull_request_reviews, teams, identities, work_items, work_item_transitions,
work_item_dependencies.

### Module map (final ownership)
```
api/external_ingest/: __init__ router schemas errors streams auth   (2691; streams hardened by 2693, auth body by 2712)
api/external_ingest/: consumer stream_health                        (2693)
api/external_ingest/: schema_registry export_schemas examples/      (2692)
api/external_ingest/: status.py (own APIRouter, mounted in main.py) (2694; recompute
                      response block extended by 2699 in wave 3)
external_ingest/:     errors payload_store                          (2693)
external_ingest/:     types ids sinks                               (2698)
external_ingest/:     recompute recompute_status                    (2699)
external_ingest/:     validate                                      (2691; imported unchanged by 2697)
external_ingest/:     normalize processor status_reporter           (2697)
external_ingest/:     idempotency ownership                         (2695)
models/: ingest_auth.py (2696)  external_ingest.py (2694)
workers/: system_ops(+2693 tasks) config(2693,2694 lines) external_ingest_reconciler(2694)
          external_ingest_tasks(2699)
api/admin/routers/customer_push.py (2696 create; 2694/2695 append)
push/ + cli.py registration (2700)
```

---

## 3. Wave plan (max safe parallelism; no shared hot files within a wave)

**Wave 1 — foundations (parallel: CHAOS-2691, CHAOS-2696, CHAOS-2715)**
- 2691: contract layer + interim streams/auth + rate-limit key func + adr-003.
- 2696: auth tables (0032) + admin sources/tokens CRUD + audit enums + authz doc.
- 2715: adr-004 webhook evaluation + relay sketch (docs only).
Risks: none shared; 2691's interim auth hard-fails without EXTERNAL_INGEST_INSECURE_AUTH=1
(CC14) — set it in the local compose env for wave-1..3 live verification only.
Live verify: curl 202/400/413/503 flow per brief-2691 §Live (stop valkey for 503; includes
503 auth_not_configured with the flag unset); admin
source+token mint/rotate/revoke via curl with dev admin JWT; registration 409 against an
enabled managed source (per-provider matching, CC5); migration 0032 up/down on
scratch PG.

**Wave 2 — discovery, status store, real auth (parallel: CHAOS-2692, CHAOS-2694, CHAOS-2712)**
- 2692: registry/ETag/static export/examples + adr-005 (touches router.py GET handlers only).
- 2694: 0033 (3 tables), status.py (own router, main.py mount), data-plane GET batches
  list/detail, admin batches+schemas proxies, prune beat task.
- 2712: swap auth.py body (real token resolution, last-used tracking, failure audit).
Risks: 2692 vs 2712 both near router-adjacent code — disjoint files (router.py vs auth.py);
2694's main.py one-line mount is the only main.py touch this wave.
Live verify: token from wave 1 → 401/403 matrix per CC14 table; GET /schemas ETag/304;
create fake batch rows via status.py smoke script → GET /batches/{id} tenant-isolation 404;
prune dry-run.

**Wave 3 — transport, sinks, recompute, CLI, web (parallel: CHAOS-2693, CHAOS-2698,
CHAOS-2699, CHAOS-2700, CHAOS-2714)**
- 2693: pointer-based streams + consumer + StreamConsumer reclaim ext + payload_store +
  queue/beat/compose (incl. 2699's late-ack line) + stream-health + router call-site update.
- 2698: types/ids/sinks + CH 065 + live-CH round-trip tests (all 9 kinds).
- 2699: planner + debounce + flush task + 0034 + recompute block in status.py GET
  responses (cross-wave touch of 2694's file, pinned enum per CC21; no config/compose edits).
- 2700: push CLI (ops cli.py + push/).
- 2714: web screens (web repo; sources/tokens/batches live, validate/push against mocks).
Risks: 2693 rewires router.py enqueue call while 2700 exercises POST /batches — CLI tests
use mocked transport, fine; 2714's validate/console-push screens can't live-verify until
wave 4 (mock-verified only); ids.py derivations must match native (2698's live tests assert
UUID equality vs a native-sync-written row).
Live verify: valkey-cli XADD/XPENDING/XCLAIM cycle per brief-2693 §Live; sinks smoke script
0→N rows with FINAL dedup + source_id stamped; schedule_or_coalesce twice <45s → one guard
key; `dev-hops push validate/sample` offline + `push batch` against dev stack (interim
flow); web e2e suite against MSW.

**Wave 4 — full accept path + worker (parallel: CHAOS-2695, CHAOS-2697)**
- 2695: idempotency + ownership (absorbs 2696's registration-time check into ownership.py,
  CC5/CC14) + router rewire (CC22, incl. payload upsert + stale-accepted RETRY) + admin
  validate proxy (console-push CUT from v1, CC25).
- 2697: validate-ext/normalize/processor/status_reporter + recompute call.
Risks: both consume wave-3 modules; disjoint files (2695: idempotency/ownership/router.py;
2697: external_ingest worker modules). The 202→worker→completed path only becomes real when
BOTH merge — schedule the wave's live verification after both.
Live verify: end-to-end on dev compose: push batch → 202 → XADD pointer → consumer →
ClickHouse rows (FINAL) → status completed → payload row deleted → recompute task IDs in
flower/logs; replay → 200; mutate payload same key → 409; stop valkey → 503 →
status stream_unavailable → resubmit → RETRY same ingestion_id; kill CH mid-batch →
reclaim → max_deliveries → DLQ + failed → resubmit same key → fresh accept; stale-accepted:
XDEL a pending entry, backdate updated_at >15 min, resubmit same key+hash → RETRY (same
ingestion_id, attempts+1) not REPLAY.

**Wave 5 — proof + docs (parallel: CHAOS-2702, CHAOS-2711, CHAOS-2701, CHAOS-2713)**
- 2702: live e2e test (clickhouse marker + live-e2e tier + valkey service) + adr-006.
- 2711: mkdocs tree/nav/snippets + prose + disambiguation notes.
- 2701: docs/examples fixtures + drift tests.
- 2713: CI/CD + relay + cron examples.
Risks: doc-file adjacency only; mkdocs.yml single-owner (2711). 2702 confirms recompute
"queued not inline" two ways (send_task patch + zero metrics rows).
Live verify: `ci/run_tests.sh live-e2e` green with valkey; docs build; example curl scripts
executed verbatim against dev stack; `dev-hops push batch --poll` full happy path.

---

## 4. Per-issue scope deltas vs analyst briefs

- **2691**: repository.v1.externalId = full name not URL (CC4); WorkItemV1.work_item_id →
  external_key + workItemType on transition/dependency (CC7); records min_length=1;
  limits field on GET /schemas; shared-limiter rate limiting via rate_limit.py incl.
  INGEST_READ_LIMIT on GET /schemas* (CC15);
  adr-003 added to deliverables (incl. CC5 Linear-matching residual-risk note); interim
  enqueue_batch gains record_count/window kwargs
  (CC9); REPLAY-200 note (final behavior arrives with 2695); interim auth HARD-FAILS
  503 auth_not_configured unless EXTERNAL_INGEST_INSECURE_AUTH=1 (CC14); CREATES
  external_ingest/validate.py complete (CC17 — 2697 imports it unchanged).
- **2692**: model definitions deferred to 2691's schemas.py verbatim (its own divergent
  sketches void); adr-005 (flat path); examples/ is the canonical fixture home (CC18);
  keeps 2691's INGEST_READ_LIMIT on the GET /schemas* handlers it reworks (CC15).
- **2693**: 10MB/EXTERNAL_INGEST_MAX_BODY_BYTES (was 8MiB); status 'accepted' (was
  'received'); payload table DDL/model hosted by 2694's 0033 (2693 keeps payload_store.py,
  whose write primitive is `upsert_payload()` SELECT-then-UPDATE-or-INSERT, CC22);
  producer fn name enqueue_batch; owns late-ack line for 2699's flush task; router
  call-site update explicitly in scope; reclaim_idle_ms=900_000 (15 min, was 60s) +
  single-replica/concurrency=1 deployment invariant documented + terminal-status
  idempotent-skip guard in the consumer (CC11).
- **2694**: migration renumbered 0033; + payload-table DDL/model; + attempts +
  record_counts columns; max-records assumption 1000; + admin batches/schemas proxies;
  reconciler = retention prune only (orphan re-enqueue → new issue); GET batch detail does
  NOT surface recompute_status in wave 2 (2699 adds it, CC21); INGEST_READ_LIMIT on
  GET /batches list+detail (CC15).
- **2695**: zero migrations (primitives in 0033/0032); error shape → ExternalIngestError
  envelope + renamed codes (CC16); RETRYABLE += "failed" w/ attempts AND stale-accepted
  (>15 min) RETRY (CC13); + router rewire ownership (CC22, payload upsert); ownership.py
  implements CC5 per-provider matching incl. Linear org-wide placeholder rule; admin
  validate proxy only — console-push proxy CUT from v1 (CC25); table name
  external_ingest_sources confirmed.
- **2696/2712**: tables renamed external_ingest_sources/external_ingest_tokens (+
  matched_integration_source_id column, CC5); token
  helpers moved to models/ingest_auth.py; auth.py body swap is 2712/wave 2 (2691 ships
  interim behind EXTERNAL_INGEST_INSECURE_AUTH=1; 2712 deletes the flag); registration
  409s when the CC5 match hits an ENABLED managed source (D8 warn-only OVERRULED, CC14);
  + webhook_mode/webhook_secret_id reserved columns; rate-limit key func moved
  to 2691.
- **2697**: no queue/compose wiring (2693 owns); D5 provider="customer_push" REVERSED
  (CC8); D9/D10 superseded by reclaim ladder (CC11); RecordEnvelope field `payload` not
  `data`; validation models live in api schemas.py; external_ingest/validate.py is
  IMPORTED UNCHANGED, not created/extended here (CC17); kind×system matrix + instance
  match enforcement added; live-CH round-trip tests ceded to 2698; recompute called via
  primitives seam (CC21).
- **2698**: confirmed owner of types/ids/sinks + CH 065 + live-CH tests; source table name
  = external_ingest_sources; AffectedScope stays local (no cross-import with 2699);
  customer updatedAt clamped when >5 min in the future → server now() + per-record warning
  diagnostic (CC24).
- **2699**: migration renumbered 0034; no postgres pytest marker; no workers/config.py or
  compose edits; no workers/tasks.py re-export; primitives-based public seam; D8 fallback
  ratified within caps; investment recompute stays, scoped+force=False; + owns the
  recompute_status block in status.py GET responses (pinned enum
  not_applicable|pending|dispatched|skipped_no_scope|failed, CC21).
- **2700**: versioned kinds + `payload` field (D4 reversed); model import names = 2691's;
  limits 1000/10MB + server-reported limits; samples via load_example (push/samples/ dir
  dropped); status enum += stream_unavailable handling; retries ALL 503s regardless of
  error code (CC16/CC29).
- **2701/2711**: split ratified (CC27); 2701 fixtures+drift only.
- **2702**: adr-006 (not 003); valid fixtures assert equality with package examples;
  mkdocs.yml untouched (2711 owns).
- **2713**: + webhook-relay example tab + reconciliation-schedule guidance (from 2715).
- **2714**: token prefix fcpush_; status enum per CC12; recompute_status TS union
  corrected to CC21's pinned enum (its queued|running|completed|not_applicable union was
  WRONG); Screen 5 is VALIDATE-ONLY in v1 (console-push cut → v2 follow-up, CC25); field
  names mirror 2696/2694
  schemas (id, JSON error_summary, no last_result); record_kind list-filter dropped v1;
  wave-3 build with validate live wiring completed in wave 4.
- **2715**: ADR number 004; follow-ups #3/#5 absorbed by 2713; webhook_mode/secret columns
  pre-reserved in 0032; status vocab aligned to CC12.

## 5. Open items for the epic owner
See structured output: interim-auth-on-integration-branch confirmation, v1 limits, instance
grain UX, console-push product veto, legacy /api/v1/ingest fate (new issue).
Post-critique dispositions: interim auth now mechanically flag-gated (CC14); console-push
CUT from v1 (CC25); legacy-ingest issue pinned non-GA-blocking (CC28); limits and
repo/project instance grain CONFIRMED (CC3/CC5).

---

## Reconciliation delta (post-critique)

Adjudicated against the two adversarial critiques; each line = one applied change.
REFUTED findings (do not re-litigate):
- R1. "Org-blind RMT sorting keys on 8/9 tables" — FALSE. Live ClickHouse `system.tables.
  sorting_key` shows org_id FIRST on all 9 target tables (repos:(org_id,id),
  teams:(org_id,id), git_commits:(org_id,repo_id,hash), git_pull_requests:(org_id,repo_id,
  number), git_pull_request_reviews:(org_id,repo_id,number,review_id),
  work_items:(org_id,repo_id,work_item_id), work_item_transitions:(org_id,repo_id,
  work_item_id,occurred_at), work_item_dependencies:(org_id,source_work_item_id,
  target_work_item_id,relationship_type), identities:(org_id,canonical_id));
  migrations/clickhouse/027_add_org_id_to_sorting_keys.py did the shadow-table re-key the
  critic missed (it read only 024's warning comment).
- R2. brief-2695's "exact-match confirmed live at sync/discovery.py:249" — mis-verification
  the OTHER direction (see A1): line 249 is the generic INSERT, not evidence the match works.

APPLIED changes:
- A1. CC5 rewritten: one-active-owner = per-provider matching (github external_id/full_name;
  gitlab full_name/metadata path_with_namespace/external_id; jira external_id/full_name;
  linear external_id/full_name/name + org-wide `"linear"` placeholder owns ALL teams) +
  registration-time resolution persisted as external_ingest_sources.
  matched_integration_source_id (0032). Evidence: sync/discovery.py:107,123,133;
  api/admin/routers/sync.py:535,775-820 (Jira project_id|project_key|name fallback; Linear
  team UUID or org-wide placeholder — worse than the critics cited).
- A2. CC14 pinned: registration 409 source_owned_by_fullchaos_sync iff CC5 match is ENABLED;
  disabled match → allowed + stored; accept-time 403 authoritative. 2696 D8 warn-only
  OVERRULED.
- A3. CC22: payload_store primitive = upsert_payload() SELECT-then-UPDATE-or-INSERT in the
  accept txn; idempotency row is the serialization point; race → 503
  ingest_temporarily_unavailable. Fixes RETRY PK collision (both stream_unavailable and
  failed cases).
- A4. CC13: stale-accepted recovery — accepted/processing older than
  EXTERNAL_INGEST_ACCEPTED_STALE_MINUTES=15 + same key/hash → RETRY (same ingestion_id),
  else REPLAY. Closes crash-before-XADD / stream-trim fail-open. Beat reconciler stays a
  follow-up issue.
- A5. CC14: interim auth mechanically gated — 503 auth_not_configured unless
  EXTERNAL_INGEST_INSECURE_AUTH=1; 2712 deletes flag+body.
- A6. CC21/CC25: recompute_status enum pinned
  (not_applicable|pending|dispatched|skipped_no_scope|failed); 2699 owns 0034 AND the
  status.py GET response block (wave 3); 2694 surfaces nothing recompute in wave 2;
  brief-2714's TS union corrected.
- A7. CC17/module map: external_ingest/validate.py created COMPLETE by 2691 (wave 1);
  2697 imports unchanged.
- A8. CC25: "verified query-only schema" corrected — Mutation root exists
  (api/graphql/schema.py:814); REST-only decision unchanged (epic non-goals).
- A9. CC11: reclaim_idle_ms 60s → 900_000 (15 min); single-replica/concurrency=1 pinned as
  deployment invariant; consumer terminal-status idempotent-skip guard added.
- A10. CC16: + ingest_temporarily_unavailable and auth_not_configured at 503; CLI retries
  ALL 503s (CC29).
- A11. CC24: customer updatedAt clamped when >5 min future → server now() + warning
  diagnostic (RMT row-pinning defense).
- A12. CC15: INGEST_READ_LIMIT=120/minute on GET /schemas* (IP-keyed, public) and
  GET /batches list+detail (token-keyed); 2691/2692/2694 apply respectively.
- A13. Product decisions: web Screen 5 VALIDATE-ONLY in v1 (console-push → v2 follow-up);
  legacy /api/v1/ingest issue non-GA-blocking; batch limits stay 1000/10MB; source_instance
  stays repo/project grain.
