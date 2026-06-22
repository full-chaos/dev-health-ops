# GitHub rate-limit validation handoff

Context: PR #1011 (`fix/github-worker-app-credentials`) validates the GitHub provider fix for GitHub App auth and rate-limit observability.

## What was validated

- Database credential `3c840245-0e3f-4beb-961f-2d7403cddd43` decrypts to GitHub App fields:
  - `app_id`
  - `private_key`
  - `installation_id`
  - `auth_mode`
  - `setup_action`
- `github_credentials_from_mapping()` returns app-auth credentials with:
  - `is_app_auth=True`
  - app id/private key/installation id present
  - no PAT token required
- Direct worker probe using the same credential reached GitHub and exchanged an installation token:
  - `POST /app/installations/141773132/access_tokens` returned `201`
  - `GET /repos/full-chaos/dev-health-web` returned `403`
  - response indicated primary rate limit exhaustion for installation `141773132`
- The running worker containers were using the current branch code:
  - source file: `/app/src/dev_health_ops/providers/github/client.py`
  - classifier present: `has_classifier=True`
  - `get_repo()` source contains the classifier path

## Runtime queue work

- Cleared Celery/Valkey queue keys only; databases, Docker volumes, and persisted sync/backfill rows were not cleared.
- Deleted queue keys included:
  - `default`
  - `sync`
  - `sync.github`
  - `sync.github.light`
  - `sync.github.heavy`
  - `sync.gitlab`
  - `sync.gitlab.light`
  - `sync.gitlab.heavy`
  - `sync.linear`
  - `metrics`
  - `backfill`
  - `monitoring`
  - `ingest`
  - `unacked`
  - `unacked_index`
- Restarted `beat`, `worker`, `worker-heavy`, and `worker-ingest`.

## Trigger attempts

- Legacy direct `run_sync_config.delay(config_id)` failed because `org_id` is required.
- Retried with:
  - config id: `b7a78c07-577c-4d43-b4d0-1486371392d5`
  - org id: `70d529e0-3c06-4597-8480-794fd02328b6`
  - task id: `be9b07f5-ecac-4f37-a013-6a653d407dcf`
- Legacy direct task path was not the desired planner-managed route; it surfaced missing owner/repo config.
- Manual planner run inspected:
  - sync run id: `85d7a225-ad95-4337-ad40-28ba97d68ccb`
  - status: `planned`
  - total units: `75`
- Dispatch task:
  - task id: `bcab93e1-2225-4e05-a8eb-8b4d2bedf9b3`
  - full dispatch remained concurrency-capped, and units stayed planned.

## Forced unit validation

- Forced unit:
  - unit id: `6a3ffab3-42d5-4302-a959-16213bd303c2`
  - source: `full-chaos/dev-health-web`
  - dataset: `work-items`
- Manually moved the unit from `planned` to `dispatching` and queued:
  - task id: `d83bcae6-027f-4763-a8bd-b0b442147443`
- Worker log evidence after the PR fix showed the provider now emits an actionable classified rate-limit message, not the old generic PyGithub retry/backoff message:
  - `GitHub: failed to fetch milestones for full-chaos/dev-health-ops: GitHub rate limit on GET /repos/full-chaos/dev-health-ops: 403 {"message": "API rate limit exceeded for installation ID 141773132...`
- Later persisted state after waiting:
  - unit id: `6a3ffab3-42d5-4302-a959-16213bd303c2`
  - status: `running`
  - attempts: `1`
  - error: empty
  - lease expires at: `2026-06-22 02:43:28.133488+00`
  - last heartbeat at: `2026-06-22 01:43:28.133488+00`
- Recent queue check showed named queues empty, with unacked entries still present:
  - `default`, `sync`, `sync.github`, `sync.github.light`, `sync.github.heavy`, `sync.linear`, `backfill`, `monitoring`: empty/missing
  - `unacked`: hash with `162` entries
  - `unacked_index`: zset with `162` entries

## Residual blockers

- GitHub terminal success is blocked by real GitHub App installation primary rate-limit exhaustion:
  - installation id: `141773132`
  - `X-RateLimit-Remaining: 0`
- Full planner dispatch is also affected by concurrency caps/stale active work:
  - observed `dispatch_sync_run.concurrency_capped`
- Linear Sync Now remains independently blocked by:
  - `{"error":"No active subscription"}`
- PR #1011 intentionally fixed only the observed provider `get_repo()` boundary. Lazy PyGithub calls from returned repository objects still need broader wrapper coverage, tracked separately in `CHAOS-2598`.

## Follow-up issue

- `CHAOS-2598 Complete GitHub provider PyGithub error handling across lazy REST calls`
- URL: `https://linear.app/fullchaos/issue/CHAOS-2598/complete-github-provider-pygithub-error-handling-across-lazy-rest`
