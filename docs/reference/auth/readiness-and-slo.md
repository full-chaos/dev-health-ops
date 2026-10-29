---
page_id: ref-auth-readiness-slo
summary: The auth-service operator surface - /healthz, /readyz and /metrics - the three required readiness checks and the reason codes behind them, the metric series, the ordered shutdown, and the initial service level objectives with the dashboard that does not exist yet.
content_type: reference
owner: engineering
source_of_truth:
  - internal/platform/health/server.go
  - internal/auth/authruntime/runtime.go
  - internal/auth/authstore/postgres.go
  - internal/auth/keystore/keystore.go
  - internal/audit/outboxloop.go
  - internal/auth/httpapi/accesslog.go
applicability: current
lifecycle: active
---

# Auth service readiness and SLO reference

The operator listener (default `:8096`, never the API port) serves three
read-only endpoints. Only `GET` and `HEAD` are accepted; anything else is `405`.
{: .fc-page-lede }

| Endpoint | Answers | Meaning |
| --- | --- | --- |
| `/healthz` | `200 {"status":"ok"}` or `503 {"status":"failed"}` | The process is alive. Independent of dependencies. |
| `/readyz` | `200 {"status":"ok"}` or `503 {"status":"not_ready","failed_checks":[...]}` | Every required check passes **right now**. |
| `/metrics` | Prometheus text | Runtime and request series. |

Liveness and readiness are separate signals on purpose: a database outage must
make the service not ready without making it restart.

## Readiness fails closed, live, and names only the check

Each required check is re-run on **every** `/readyz` request under a per-check
deadline (`AUTH_SERVICE_HEALTH_CHECK_TIMEOUT`, default 2 s), because a
dependency verified once at startup goes stale. The body names the failing
**checks** and nothing else (no DSN, no path); the bounded reason code is in the
service log (`readiness check failed`, with `check` and `reason`).

| Check | Passes when | Reason codes (log) |
| --- | --- | --- |
| `postgres` | The pool answers and the auth schema is visible to the runtime login. It does not check the migration version; use `auth-migrate --check` for that. | `postgres_unreachable`, `auth_schema_missing`, `postgres_config_invalid` |
| `signing_key` | The key file meets the custody contract and parses as Ed25519 (see the [configuration reference](configuration.md)). | `key_file_unreadable`, `key_file_not_regular`, `key_file_mode_too_permissive`, `key_file_too_large`, `key_file_not_pem`, `key_file_ambiguous_pem`, `key_file_unparseable`, `key_file_wrong_algorithm`, `key_id_unconfigured`, `key_path_unconfigured` |
| `restore_fence` | The database has a restore-fence row for the timeline it is on **now**, read in one statement with the timeline. A restored, promoted or failed-over database has none until `auth-migrate restore-fence apply` has run for its new timeline, and a database that was never initialised has none until `restore-fence init` has run once. A server still in recovery, and any failed read, is not ready. See [restoring the auth schema](../../operate/maintain/backup-and-restore.md#restoring-the-auth-schema-to-a-point-in-time-the-restore-fence). | `timeline_not_fenced`, `fence_state_unreadable` |
| `outbox_loop` | The audit outbox loop has completed a successful step, and has not failed three steps in a row. See [audit and outbox operations](../../contribute/architecture/auth-control-plane-audit-outbox.md). | (the step error is logged as `outbox step failed`) |
| `valkey` | Registered only while the login, credential or social link routes are mounted. The shared rate-limit store answers a `PING` (see [rate limits](configuration.md#rate-limits)). The store connects on first use, so an unreachable one starts the service not ready instead of stopping it. | `valkey_unavailable` |

A pod is therefore not ready until its first successful outbox step.

## Rate-limit store down

Alerts: `AuthServiceRateLimitStoreErrors` (the counter below moves for five minutes) and `AuthServiceRateLimitStoreNotReady` (the `valkey` check fails for five minutes), both in `alerts/rules.yml`. The counter moves only when a limited request arrives; the check fails whether or not traffic does.

While the shared rate-limit store fails, every request that reaches the limiter on a limited auth route (login, verify, resend-verification, forgot-password, reset-password, change-password, social link and unlink) answers a **closed 500**: a request is never let through uncounted. A malformed request, and on the social routes an unauthenticated one, is refused before the limiter and answers its usual 4xx. This is by design. Session, refresh, logout, the JWKS and the discovery document do not use the store and keep serving; `/readyz` answers 503 naming `valkey`.

1. Confirm: `/readyz` names `valkey`, and `dev_health_runtime_check_failed{check="valkey"}` is 1 on the affected replicas. The service log has `readiness check failed` with the reason `valkey_unavailable`.
2. Find the cause on the store side first: the store itself, its network path (the chart's NetworkPolicy egress to the store, `authService.networkPolicy.egress.valkey`), the `AUTH_VALKEY_URI` Secret, and the ACL login (key prefix `auth_svc_`, and `+client|tracking` among its commands; see the [configuration reference](configuration.md#rate-limits)).
3. Restore the store or its path. Do not raise a limit, remove a limited route or turn the limits off to clear the alert: the 500 is the protection working.
4. Done when the check series is 0 on every replica, `/readyz` is 200, and `dev_health_api_rate_limit_store_errors_total` has stopped rising.

## Metrics

| Series | Type | Use |
| --- | --- | --- |
| `dev_health_runtime_live`, `dev_health_runtime_ready` | gauge | 1 or 0. |
| `dev_health_runtime_check_failed{check}` | gauge | 1 while a required check is failing, so an alert can name the dependency instead of reading one collapsed bit. |
| `dev_health_runtime_required_checks` | gauge | Number of required checks (3, and 4 while a rate-limited route is mounted). |
| `dev_health_runtime_uptime_seconds` | gauge | Process uptime. |
| `dev_health_api_rate_limit_store_errors_total{limit}` | counter | Rate-limit store errors per limiter id (`auth_svc_...`); each answered a closed 500. Present at zero once the limited routes are built; carries limit ids only. |
| `dev_health_api_http_requests_total{listener,method,route,status_class}` | counter | Requests on the API listener. Unmatched paths share the label `unmatched`; a raw path is never a label. |
| `dev_health_api_http_request_duration_seconds` | histogram | Request latency on the API listener. |
| `devhealth_auth_outbox_publisher_enabled` | gauge | 0 in Wave 1 (no sink). |
| `devhealth_auth_outbox_oldest_pending_age_seconds` | gauge | Outbox backlog age. |
| `devhealth_auth_outbox_delivered_total`, `devhealth_auth_outbox_delivery_failures_total{reason}` | counter | Only with a sink. |

## Shutdown order

On `SIGTERM` components stop in reverse start order: readiness gate (so a load
balancer sees not-ready first), API listener, operator listener, outbox loop,
database pool. The whole sequence is bounded by `AUTH_SERVICE_SHUTDOWN_TIMEOUT`
and the process exits 0 on a clean stop. A component that cannot stop in time
makes the process exit 1 and log `service stopped with error`.

## Initial service level objectives

The service is dormant (no route, no consumer), so Wave 1 has no request-latency
or error-rate SLO to defend. The objectives that exist are about the service
being ready to be used, **proposed and not yet measured in production**:

| SLI | Objective | Source |
| --- | --- | --- |
| Readiness: fraction of time `dev_health_runtime_ready == 1` for a deployed replica | 99.9% over 30 days | `/metrics` |
| Time to ready after a start | under 60 s | `dev_health_runtime_uptime_seconds` at first ready |
| Oldest pending outbox event (once a sink exists) | under 5 minutes | `devhealth_auth_outbox_oldest_pending_age_seconds` |

Alert on `dev_health_runtime_check_failed{check}` for any check for more than
five minutes, and on `devhealth_auth_outbox_publisher_enabled == 0` **after** the
first consumer lands (before that it is expected).

**Gap, named:** there is no readiness or SLO dashboard for the auth service yet.
The series above are the inputs for one; building it is deferred until the
service is deployed beyond a dormant environment, because a dashboard over a
service nobody calls would show only its own heartbeat.

## Verified against

- `internal/platform/health/server.go`: `/healthz`, `/readyz` bodies, `allowRead`, the `dev_health_runtime_*` series.
- `internal/auth/authruntime/runtime.go`: check registration (`postgres`, `signing_key`), the component order, `configure`.
- `internal/auth/authstore/postgres.go` and `internal/auth/keystore/keystore.go`: the reason codes.
- `internal/audit/outboxloop.go`: `outbox_loop` and the outbox gauges; `internal/auth/httpapi/accesslog.go`: the request series names.
