# Customer-push CLI (CHAOS-2700)

Part of the [customer-push ingestion epic](adr-003-external-ingest-rest-boundary.md)
(CHAOS-2690). This doc records the design decisions behind `dev-hops push` --
the `validate`/`batch`/`sample`/`status`/`export` command group customers use
to push data into the external-ingest API from their own CI/cron jobs.

## Module layout

```
src/dev_health_ops/push/
  cli.py         # register_commands(subparsers) -- argparse wiring, all 5 subcommands
  http_client.py # httpx.AsyncClient wrapper: retry/backoff, error-envelope parsing
  validate.py    # offline validation -- imports the server's own Pydantic models
  poll.py        # shared polling loop for `batch --poll` / `status --poll`
  limits.py      # client-side batch-size guardrails (server-reported, hardcoded fallback)
  output.py      # human vs --json rendering, exit-code constants
  export/        # `push export` provider registry -- stubbed, v1 has zero entries
```

Registered from `dev_health_ops/cli.py`'s `build_parser()` exactly like every
other command group (`push_cli.register_commands(sub)`, lazy-imported
alongside `backfill_cli`/`admin_cli`/etc.). `push batch`/`push status` are
`async def` handlers; `cli.py`'s dispatch loop already runs
`asyncio.run(func(ns))` whenever `inspect.iscoroutinefunction(func)` is true,
so no new dispatch machinery was needed.

## Offline validation reuses the server's own models -- never re-implemented

`push validate` and `push batch`'s pre-flight both import
`dev_health_ops.api.external_ingest.schemas.BatchEnvelope` and
`dev_health_ops.external_ingest.validate.validate_records` (CHAOS-2691,
master-spec CC17 -- the single owner of per-record deep validation, imported
UNCHANGED by both the worker and this CLI). This is genuinely offline
(Pydantic validation has no I/O) and can never drift from what
`POST /api/v1/external-ingest/validate` enforces server-side, since both call
the exact same function.

Two distinct entry points in `validate.py`, because `push validate` and
`push batch`'s pre-flight need different strictness:

- **`validate_payload`** -- full deep validation for `push validate`,
  mirroring `POST /validate` exactly: envelope parse -> schema-version check
  -> batch-size check -> per-record `RECORD_KIND_MODELS[kind]` validation.
- **`check_envelope_shape`** -- the *shallower* pre-flight `push batch` runs
  before a network call, mirroring what `POST /batches` itself checks
  (envelope parse, schema version, batch size, unknown-kind) and
  deliberately **not** per-record field validation. A batch with some
  invalid records is still legitimately submittable in v1 -- the worker
  reports those as a `partial` status with per-record rejections. Hard-
  blocking locally on a per-record error would prevent customers from ever
  exercising that normal partial-success path, so `push batch` only rejects
  locally for the same structural reasons the server itself would.

## HTTP client: httpx + the existing retry_with_backoff decorator

`httpx.AsyncClient` was already a pyproject dependency and is used
server-side (`api/services/oauth.py`, `api/admin/routers/*`), so no new HTTP
dependency. Retry/backoff reuses
`dev_health_ops.connectors.utils.retry.retry_with_backoff` -- generic, no
connector-specific imports, already implements exponential backoff and
`Retry-After` honoring via a `retry_after_seconds` duck-type attribute on the
raised exception. `push`'s local `IngestTransientError` carries that
attribute, parsed from the response's `Retry-After` header when present.

Retry predicate (master-spec CC16/CC29): retry on network/timeout errors,
HTTP 429, and **any** HTTP 503, regardless of the response body's `code`
field -- the server can legitimately return `stream_unavailable`,
`ingest_temporarily_unavailable` (concurrent same-key insert race), or
`auth_not_configured` (interim-auth guard on the integration branch) all at
503, and the retry decision keys on status class, never the code string. Do
**not** retry 400/401/403/404/409/413/422 -- these are client/contract
errors; retrying them wastes CI minutes and could flip a genuine 409
idempotency-conflict into a false "it eventually worked."

CLI-tuned retry params (`max_retries=5, initial_delay=1.0, max_delay=30.0,
backoff_factor=2.0`) lower `max_delay` from the connectors default of 60s --
CI job time budgets are tighter than long-running background sync jobs.

Every response is checked against the `{"error": {code, message, errors?}}`
envelope (master-spec CC16) via `parse_error_envelope`, with a defensive
fallback (generic code/message) if the body isn't that shape -- an upstream
proxy's HTML error page must never crash the CLI.

## The token never appears in logs or errors

`IngestClientConfig.token` is only ever read inside `auth_headers()` to build
the `Authorization: Bearer <token>` header value. No code path logs the
resolved token, includes it in an exception message, or echoes it in
`--json`/human output -- `IngestApiError`/`IngestTransientError` messages are
built exclusively from the *response* (status code, server-supplied
code/message), never from request state. `push`'s `--token` flag and the
`FULLCHAOS_INGEST_TOKEN`/`FULLCHAOS_API_TOKEN` env vars are the only places
the token value exists in the process; the deprecation warning for the
legacy env var name logs the *variable name*, never its value.

## Exit-code contract

Documented verbatim for CHAOS-2701 (customer docs) to copy -- customer CI
will branch on these:

| Code | Meaning |
| --- | --- |
| `0` | Success. `validate`: payload fully valid. `batch`/`status` (no `--poll`): accepted/fetched, whatever the batch's current status is -- not poll-blocking, so `accepted` counts as success even if processing isn't done. `--poll`: reached terminal `completed` with `itemsRejected == 0`. |
| `1` | Data-level failure. `validate`: payload invalid. `--poll`: reached terminal `partial`/`failed` (both terminal statuses always carry `itemsRejected > 0` per `terminal_status_for`'s invariant). |
| `2` | Usage error (missing/invalid CLI args, including an unresolved `--api-url`/`--token`/`--org` on `batch`/`status`) -- matches the argparse `parser.error()` -> exit 2 convention used everywhere else in `dev-hops`. |
| `3` | Transport/API error after retries exhausted (network failure, a 4xx contract error, or a 503 that survived the retry budget) -- including `stream_unavailable` polling result. Distinct from `1` so CI can tell "your data was rejected" (fix the payload) apart from "FullChaos was unreachable" (retry the job / page on-call). |
| `4` | Poll timeout: still non-terminal when `--poll-timeout` elapses. Not a hard failure -- re-run `push status <id> --poll` rather than resubmitting. |

`stream_unavailable` mid-poll prints a "re-run `push batch` (same
idempotency key re-enqueues)" hint and exits 3 immediately rather than
waiting out the full `--poll-timeout` -- the batch is durably accepted in
Postgres but never reached the stream, so polling it out would just hang.

A `200` response from `POST /batches` (the REPLAY outcome, master-spec
CC13/CC22, ships once CHAOS-2695 lands) already carries the full
status-envelope shape; `push batch --poll` short-circuits straight to that
body instead of paying for one more `GET /batches/{id}` round-trip when it's
already terminal.

## Sample payloads: schema_registry.load_example, not a packaged samples/ dir

Master-spec CC18/CC29 (post-dating brief-2700-cli.md's original design)
drops the brief's `push/samples/*.json` fixture directory entirely.
`dev-hops push sample --kind <kind>` reads
`dev_health_ops.api.external_ingest.schema_registry.load_example(kind)` --
the single canonical fixture home CHAOS-2692 packages under
`api/external_ingest/examples/`. This means there is exactly one place a
per-kind example payload can drift, with one consumer (`push sample`) and,
once CHAOS-2701 lands, one drift-check test comparing
`docs/examples/external-ingest/*.json` against the same package examples.

Each example file is the record's bare `payload` dict only (not a full
envelope). `push sample` wraps it into a complete single-record batch
envelope, deriving the wrapper's correlation `externalId` from the natural
key already present in that kind's payload (`_derive_correlation_
external_id` in `push/cli.py`) -- e.g. `pull_request.v1`'s
`repositoryExternalId`+`number` becomes `acme/api#482`, matching the
convention master-spec section 2's own canonical batch-envelope example
uses. `push sample --all` combines all 9 kinds under a single `github`
source (CC6: github supports every one of the 9 record kinds), even though
the packaged work-item examples happen to carry Jira-flavored data --
CHAOS-2701 owns polished, per-system customer-doc examples; this is a CLI
smoke fixture whose only job is to be schema-valid and pipeable
(`push sample --all | push validate -`).

`--kind` accepts both the versioned form (`pull_request.v1`, canonical
everywhere per master-spec CC1) and the bare form (`pull_request`, as shown
in the Linear issue's literal acceptance-criteria examples) -- normalized to
the versioned form before lookup, so neither the issue text nor the
authoritative spec's naming convention breaks.

## Env var precedence and the deprecated token alias

Resolves a real contradiction between the Linear issue text
(`FULLCHAOS_API_TOKEN`) and the plan doc's CI examples / this epic's other
docs (`FULLCHAOS_INGEST_TOKEN`). Precedence, applied in
`push/cli.py::_resolve_token`: `--token` flag > `FULLCHAOS_INGEST_TOKEN` env
> `FULLCHAOS_API_TOKEN` env (deprecated alias, logs one `logging.warning`
naming the variable -- never its value -- pointing at
`FULLCHAOS_INGEST_TOKEN`). `FULLCHAOS_INGEST_TOKEN` is the name used in every
shipped CI example and customer-facing doc; `FULLCHAOS_API_TOKEN` exists
purely so the literal Linear acceptance criterion is satisfied without
breaking the primary documented name.

`--api-url`/`FULLCHAOS_API_URL` has no default -- an unset API URL is a
usage error (exit 2), never a silently-baked-in production URL.
`--org`/`FULLCHAOS_ORG_ID` is likewise required, resolved the same
flag-over-env way.

## `--org` auto-resolution and DB preflight are both bypassed for `push`

`push` runs against a customer's own FullChaos org over HTTP, typically from
a CI runner with no local database at all. `dev_health_ops/cli.py`'s
`_should_resolve_org` gained a `ns.command == "push"` exclusion (mirroring
the existing `audit planner-configs` / `migrate clickhouse repair`
exclusions) -- auto-resolving `--org` to "the first org in the local
Postgres DB" would be actively wrong here, and would silently push to the
wrong org if the runner ever did have incidental DB access. `push` was
deliberately **not** added to `_COMMAND_REQUIREMENTS`: that preflight
mechanism is ClickHouse/Postgres-connection specific, and `push` never
touches either database. Instead `batch`/`status` resolve and validate their
own `--api-url`/`--token`/`--org` inside their handlers (`_resolve_client_
config`), returning exit 2 directly rather than going through
`parser.error()` -- deliberately *not* argparse's own `required=True`, which
would block the env-var fallback the acceptance criteria require (a
`required=True` optional argument must be given on the command line even
when a `default=` is set).

## Quiet telemetry for `push` invocations

`dev_health_ops/cli.py` already special-cased `workers inspect --output
json` (`_is_workers_inspect_json_invocation`) to set `OTEL_ENABLED=false` and
run `build_parser()` inside `_suppress_parser_construction_noise()`, because
this repo's `logging_config.py` attaches the root logger's `StreamHandler` to
**stdout** (a house convention for structured JSON logs), and Sentry/OTel's
lazily-imported init logging (plus their background exporters' async retry
chatter against unreachable local endpoints) would otherwise land on the
same stream as a JSON command's actual output -- corrupting anything piping
that output into a JSON parser.

`push` has the identical problem for every subcommand, not just a `--json`
flag: `push sample`'s primary output *is* raw JSON with no `--json` flag to
gate on, and the brief's own Tier-1 verification pipes it directly
(`push sample --all | push validate -`). `_is_push_invocation` extends the
same quiet-mode gate to any invocation whose first argument is `push`, and
the `log_rate_limit_configuration()` call (irrelevant to a CLI that never
touches the in-process rate limiter -- that's a server-side concern for the
API `push` talks to over HTTP) is skipped the same way. This is the only
change to shared `cli.py` beyond command registration and the `--org`
exclusion.

## `push export`: a real, tested extension point -- not a v1 feature

Provider export helpers (`push export github`, `push export gitlab`) are out
of scope for v1 per the epic plan. `push/export/__init__.py` exposes
`EXPORT_PROVIDERS: dict[str, Callable[[Namespace], int]] = {}` and a
`register_export_provider(name)` decorator; the `export` subcommand parses
normally and dispatches through this registry, falling through to a "not
implemented in v1" message and exit 1 for any name not registered --
`push export` is a real, `--help`-visible subcommand today, with zero
provider-specific code to remove when a real exporter eventually registers
itself here.

## Live verification

See CHAOS-2700's PR/Linear comment for the transcript. Tier 1 (fully
offline, works without any of CHAOS-2691/2693/2694/2696/2712's server pieces
running) exercises `push sample`, `push validate` (valid + invalid + stdin),
`push export`'s stub, and `push batch`'s exit-2 preflight. Tier 2 (against a
real API instance on a scratch Postgres DB + a minted `fcpush_` token)
exercises `push batch --poll`'s full accept/poll cycle, the 503-retry path
(pointed at a dead `REDIS_URL`, matching wave 1's fail-closed stream
behavior), and the auth-failure path against a bogus token.
