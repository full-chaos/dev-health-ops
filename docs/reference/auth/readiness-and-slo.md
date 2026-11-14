---
page_id: ref-auth-readiness-slo
summary: The auth-service operator surface (/healthz, /readyz, /metrics), the readiness checks and reason codes, the metric series, the ordered shutdown, and the service level objectives with their alerts.
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
| `dev_health_api_legacy_path_total{surface,kind}` | counter | Uses of legacy API auth paths, telemetry only. `surface=guard_level` (kind: public, authenticated, admin, admin_org, superuser, optional; one per request entering a guarded route, refused callers included), `role_source` (membership_row, claim_fallback, platform_bypass, platform_gate; where an Admin, AdminOrg or Superuser decision read its role or flag), `direct_read` (users, memberships, org_memberships, impersonation_sessions, auth_sessions; one per policy store read). |
| `dev_health_api_shadow_mismatch_total{namespace,caller,class}` | counter | Shadow PDP decisions that differ from the legacy guard, telemetry only; nothing is enforced and the legacy answer always wins. `namespace` is the first segment of the route's catalog action (admin, billing, integrations, license, members, settings, teams); `caller` is member, admin or superuser; `class` is legacy_bug, new_bug, intentional or unresolved (the default; a reviewed table assigns the others). Off unless the shadow is wired and switched on. |
| `dev_health_api_shadow_results_total{result,reason}` | counter | Shadow PDP calls by result: match, mismatch, error (the PDP gave no verdict, so there is nothing to compare), dropped (the in-flight bound was full) or skipped (a covered route whose caller cannot be asked: a legacy HS256 token, a high-risk row while its switch is off, or no organization where enforcement asks nothing either: a delegated session's bearer, and every such query-api caller; go-api asks a token with no organization the platform action enforcement asks it) or impersonation_refused (a caller with a live go-api impersonation row that enforcement refuses: a write or a platform wrap; never asked, stop route aside) or decided_no_question (a token with no organization on an `AdminOrg` row, which enforcement refuses with the legacy `403` and no question). Its read that the legacy check admits is skipped. `reason` is `none` for every result except a query-api skip of the HS256 edge token (`hs256`); the set is closed, and its `envelope` value is no longer recorded, because query-api refuses the effective-principal envelope before the shadow. Routes with no catalog action are not counted. |
| `dev_health_api_shadow_enabled`, `dev_health_api_shadow_config_invalid` | gauge | The shadow PDP state, set once at start. `enabled` is 1 while the shadow is built and on. `config_invalid` is 1 when `AUTH_SHADOW_PDP` asked for it but a missing input or bad value disabled it (an ERROR log names the variable, never its value); a bad value never fails the start. |
| `dev_health_authz_enforcement_decisions_total{namespace,service,outcome}` | counter | Enforced go-api decisions. `namespace` is a catalog namespace, `service` is `go-api`, `outcome` is allow, deny, error_deny (no verdict: the route was refused with a 503) or impersonation_refused (a write or a platform-wrap request by a caller with a live go-api impersonation row, refused with a 403 before any call). Nothing is counted for a cell that is off. |
| `dev_health_authz_enforcement_unjudged_total{route,reason}` | counter | Requests on an enforced cell whose caller the decision point cannot judge, so the legacy answer stood. `route` is the profile row's `METHOD /route`; `reason` is `org_required` (a token with no organization on an `AdminOrg` row, refused `403` with no question, as the legacy guard refuses it), `delegated_orgless` (a delegated session's bearer with no organization, which keeps the legacy answer) or `impersonation` (a read by a caller with a live go-api impersonation row). This is the closed set (`enforceUnjudgedReasons`, `internal/api/policy/enforcecounters.go`); go-api emits no other reason. One DEBUG line per request names the route and reason. All three are by-design legacy reasons (product rules 1 and F1), so no reason blocks a flip; a series with any other reason (`hs256`, `no_org`, `served_org`) is a go-api build from before their removal. A read may look back over a range, but nobody waits for a window to pass. |
| `dev_health_authz_enforcement_enabled{namespace,service}` | gauge | 1 while the namespace is enforced in the service, else 0; one series per catalog namespace, set from the start-up value. |
| `dev_health_authz_enforcement_config_invalid{service}` | gauge | 1 when a namespace is switched on in go-api but it has no decision point client (a missing `AUTH_PROXY_URL` or workload input; one ERROR log names the variable, never a value), so every request of the switched-on namespaces is refused with a `503` and `reason_code` `authz_enforce_no_decision_point`; else 0. Set once at start. |
| `dev_health_authz_enforcement_ignored_items{service}` | gauge | How many items of the service's enforcement list were not accepted (not a namespace, a repeat, an empty item, `*`). Their namespaces stay off. A non-zero value is the bad-value signal; the start is never refused for it. |
| `dev_health_api_http_request_duration_seconds` | histogram | Request latency on the API listener. |
| `devhealth_auth_outbox_publisher_enabled` | gauge | 0 (no sink). |
| `devhealth_auth_outbox_oldest_pending_age_seconds` | gauge | Outbox backlog age. |
| `devhealth_auth_outbox_delivered_total`, `devhealth_auth_outbox_delivery_failures_total{reason}` | counter | Only with a sink. |

## Shutdown order

On `SIGTERM` components stop in reverse start order: readiness gate (so a load
balancer sees not-ready first), API listener, operator listener, outbox loop,
database pool. The whole sequence is bounded by `AUTH_SERVICE_SHUTDOWN_TIMEOUT`
and the process exits 0 on a clean stop. A component that cannot stop in time
makes the process exit 1 and log `service stopped with error`.

## Service level objectives

Each row names its target, window, source and status. A target comes from a ticket or an existing page; a row with no sourced target says "target not set" and gives the measured baseline where one exists. No number here is a guess. Latency is the `dev_health_api_http_request_duration_seconds` histogram on the auth-service API listener, filtered by `route`; its edges include 25 ms, 75 ms and 200 ms (`internal/auth/httpapi/accesslog.go`), so each latency target is a bucket edge.

| Service and path | SLI | Target | Window | Source of the target | Status |
| --- | --- | --- | --- | --- | --- |
| auth-service, all routes | Availability: share of requests not answered 5xx (`dev_health_api_http_requests_total`, `status_class`) | 99.95% | 30 days | this page | Target only; no production measurement. Alerts: `AuthService5xx` (any 5xx in 5 minutes, live in SigNoz) and `AuthServiceHighServerErrorRate` (5xx ratio above 5% for 10 minutes, in `alerts/rules.yml`, not yet created in SigNoz). No burn-rate rule. |
| PDP `POST /v1/authorize` and `/v1/authorize:batch` | p95, p99 latency; error rate | p95 < 25 ms, p99 < 75 ms, errors 0 | Load leg (`ci/pdp_load_k3s.sh`); alert window 5 minutes | this page | Not yet met at peak. At normal load it holds: low p95 7.1 ms, high 10.2, deny 7.6, batch-10 16.4. At 200 requests/s (client concurrency 64): authorize-high p95 2075 ms with 6.9% dropped, authorize-deny p95 32.6 ms and p99 120 ms, authorize-batch-10 p50 2.6 s with 38% dropped; batch-100 is recorded, not gated (82% dropped). Alerts: `AuthzLatencyP95High`, `AuthzLatencyP99High`. |
| Session issue `/internal/v1/sessions` | p95 latency | < 200 ms | 5 minutes | this page | Measured, one pod of 1 CPU: p95 under 10 ms up to 400 requests/s; 161 ms at 800 requests/s ([capacity](../../operate/plan/auth-service-capacity.md)). Alert: `AuthTokenLatencyP95High`. |
| Refresh `/v1/token/refresh` | p95 latency | < 200 ms | 5 minutes | this page | Measured: p95 about 7 ms at 300 requests/s; 6.8 ms with 6 times the refresh rate (10-minute token lifetime). Alert: `AuthTokenLatencyP95High`. |
| JWKS `GET /.well-known/jwks.json` | p95 latency | target not set | - | no ticket or page states one | Measured baseline: p95 under 2 ms up to 1600 requests/s per pod. No alert. |
| Exchange `POST /internal/v1/token/exchange` | latency, outcome | target not set | - | no ticket or page states one | No measurement: the load harness covers issue, refresh and JWKS. No alert. |
| Audit outbox | Age of the oldest pending event (`devhealth_auth_outbox_oldest_pending_age_seconds`) | under 5 minutes | 5 minutes | this page | Metric exists; not measured under load. Alert: `AuthOutboxOldestPendingHigh` (above 300 seconds). |
| Revocation propagation | Seconds from the revoke request received to the revocation row written: `auth.session.revocation_lag` (Prometheus `auth_session_revocation_lag_seconds`, histogram, label `reason` only; not yet on main) | target not set | - | no ticket or page states one | The SLI is named; the metric is not on main yet. No alert. |
| Signing-key compromise | Seconds from the decision to drop a signing key (before the `compromise` dry run) to the first refusal of a token signed under that key for a live session | go-api and query-api without a restart: 30 s; auth-service: 20 s | per compromise | this page: the verifiers' JWKS cache (5 s), fetch timeout (2 s) and refresh floor (1 s); auth-service's key-state refresh (15 s) | Measured in the compose key-compromise drill, not in production: two runs on the 5 min cache, go-api 277 s and 274 s, query-api 277 s and 274 s, auth-service 18 s and 3 s ([emergency rotation](../../operate/runbooks/auth-signing-key-emergency-rotation-and-rollback.md#the-key-compromise-drill-compose-2026-11-10)). A restart of go-api and query-api closes their window at once. No alert. |
| Readiness | Fraction of time `dev_health_runtime_ready == 1` for a deployed replica | 99.9% | 30 days | this page, kept | Proposed, not measured in production. |
| Readiness | Time to ready after a start | under 60 s | per start | this page, kept | Proposed. Read from `dev_health_runtime_uptime_seconds` at first ready. |

Measured values come from load runs (ops `b4e8ff13`, one pod of 1 CPU, not production) and the PDP load run. The load numbers are test results, not production measurements.

The alert text is in `alerts/rules.yml` (group `auth_slo`) and its SigNoz builder form in [dashboards and alerts](../../operate/observe/dashboards-and-alerts.md#auth-service-slo-alerts). Also alert on `dev_health_runtime_check_failed{check}` for any check for more than five minutes, and on `devhealth_auth_outbox_publisher_enabled == 0` **after** the first consumer lands (before that it is expected).

**Gap, named:** there is no readiness or SLO dashboard for the auth service yet; a separate change builds it from the series above.

## Verified against

- `internal/platform/health/server.go`: `/healthz`, `/readyz` bodies, `allowRead`, the `dev_health_runtime_*` series.
- `internal/auth/authruntime/runtime.go`: check registration (`postgres`, `signing_key`), the component order, `configure`.
- `internal/auth/authstore/postgres.go` and `internal/auth/keystore/keystore.go`: the reason codes.
- `internal/audit/outboxloop.go`: `outbox_loop` and the outbox gauges; `internal/auth/httpapi/accesslog.go`: the request series names.
