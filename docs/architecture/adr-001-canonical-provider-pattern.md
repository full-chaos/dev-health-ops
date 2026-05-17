# ADR-001: Canonical provider pattern

## Status

Accepted.

## Context

TestOps is adding CI/CD and test telemetry providers beyond the current GitHub
Actions and GitLab CI adapters. The next wave includes Jenkins, Buildkite,
CircleCI, and Azure DevOps. These APIs are mostly REST-heavy, have varied auth
schemes, and need polling/backfill support before any webhook path becomes
canonical. The codebase currently has three provider-like patterns.

## Existing pattern survey

### 1. Legacy Git connectors under `src/dev_health_ops/connectors/`

Files surveyed:

| File | Lines | Role |
|---|---:|---|
| `src/dev_health_ops/connectors/base.py` | 451 | `GitConnector`, `BatchResult`, shared sync/async bridge helpers |
| `src/dev_health_ops/connectors/github.py` | 974 | `GitHubConnector` built on PyGithub plus GraphQL utilities |
| `src/dev_health_ops/connectors/gitlab.py` | 1206 | `GitLabConnector` built on python-gitlab plus REST helpers |

- **Sync/async:** Sync-first. `GitConnector` exposes sync methods and provides
  async wrappers / batch helpers around blocking client calls.
- **Transport:** PyGithub, python-gitlab, `requests`, and project utility REST /
  GraphQL helpers.
- **Base class:** `GitConnector` in `connectors/base.py`.
- **Retry/ratelimit story:** `RateLimitGate`, `retry_with_backoff`, provider
  exception handling, and connector-specific rate-limit parsing. Mature but tied
  to Git repository API semantics.
- **Normalization story:** Fetch and normalization are blended into connector
  methods returning connector dataclasses such as `Repository`, `PullRequest`,
  and `RepoStats`.
- **Test coverage approach:** Tests exercise connector behavior through mocked
  client/API responses and downstream processor expectations; transport clients
  are relatively heavy to isolate.

### 2. Work-item providers under `src/dev_health_ops/providers/`

Files surveyed:

| File | Lines | Role |
|---|---:|---|
| `src/dev_health_ops/providers/base.py` | 215 | `Provider`, `ProviderWithClient`, `ProviderBatch`, ingestion context/capabilities |
| `src/dev_health_ops/providers/registry.py` | 125 | Lazy built-in provider registry |
| `src/dev_health_ops/providers/_ratelimit.py` | 75 | Shared `RateLimitGate` wrappers |
| `src/dev_health_ops/providers/github/client.py` | surveyed | GitHub work-item client using PyGithub / GraphQL utilities |
| `src/dev_health_ops/providers/github/provider.py` | surveyed | `ProviderWithClient` implementation for GitHub |
| `src/dev_health_ops/providers/github/normalize.py` | 959 | GitHub API payload to normalized work-item model mapping |

- **Sync/async:** Sync contract today (`Provider.ingest`) with iterable batching.
  The pattern can host async-capable bases alongside the same package boundary.
- **Transport:** Hand-rolled provider clients, existing SDKs where justified,
  GraphQL utilities for providers that need them, and shared rate-limit gates.
- **Base class:** `Provider` / `ProviderWithClient` in `providers/base.py`.
- **Retry/ratelimit story:** Shared helpers in `providers/_ratelimit.py` wrap the
  same `RateLimitGate` machinery used elsewhere while keeping provider clients
  thin.
- **Normalization story:** Explicit normalization modules live beside provider
  clients, e.g. `providers/github/normalize.py`, and return domain models rather
  than raw transport payloads.
- **Test coverage approach:** Provider tests can inject fake clients and assert
  normalized batches without exercising the transport stack; registry tests can
  remain import/lazy-load focused.

### 3. TestOps adapters originally under `src/dev_health_ops/connectors/testops/`

Files surveyed:

| File | Lines | Role |
|---|---:|---|
| `src/dev_health_ops/connectors/testops/base.py` | 198 | `BasePipelineAdapter`, `PipelineSyncBatch`, httpx helpers |
| `src/dev_health_ops/connectors/testops/github_actions.py` | 171 | GitHub Actions pipeline/job adapter |
| `src/dev_health_ops/connectors/testops/gitlab_ci.py` | 174 | GitLab CI pipeline/job adapter |

Current implementation note: the shared TestOps contracts now live in
`src/dev_health_ops/providers/_base.py`, and the GitHub Actions / GitLab CI
adapters now live in provider-owned modules at
`src/dev_health_ops/providers/github/testops_pipeline.py` and
`src/dev_health_ops/providers/gitlab/testops_pipeline.py`. The
`connectors/testops/` paths are compatibility re-export shims only.

- **Sync/async:** Async-first via `httpx.AsyncClient` and async adapter methods.
- **Transport:** `httpx` with injectable `AsyncBaseTransport`, env-token auth,
  and simple page iteration.
- **Base class:** `BasePipelineAdapter` in `providers/_base.py`.
- **Retry/ratelimit story:** Minimal. It raises on 401 and `>=400`; no canonical
  retry, backoff, or 429 handling yet.
- **Normalization story:** Adapters fetch raw REST JSON and immediately map to
  `PipelineRunExtendedRow` / `JobRunRow` rows. This is normalized enough for the
  current TestOps sink, but transport and normalization are in the same class.
- **Test coverage approach:** Strong lightweight tests with `httpx.MockTransport`
  in `tests/testops/test_pipeline_ingestion.py`.

## Comparison matrix

| Axis | Legacy `connectors/` GitConnector | `providers/` work-item providers | `connectors/testops/` adapters |
|---|---|---|---|
| Async support | Sync-first with async wrappers | Sync today; package can host async bases | Async-first |
| GraphQL aware | GitHub yes, GitLab mostly REST | Yes where needed; GitHub provider is GraphQL-aware | No, REST-only today |
| Retry/ratelimit | Mature but Git-specific | Shared `RateLimitGate` helpers; client-owned | Minimal; must be extended |
| Error model | Connector exceptions plus SDK exceptions | Provider/client exceptions surfaced through typed batches | Connector exceptions on auth/API |
| Normalization location | Mixed into connector classes | Dedicated provider-adjacent normalization modules | In adapter classes |
| Testability | Heavier SDK/client mocking | Good fake-client seams and registry isolation | Excellent `httpx.MockTransport` seam |
| Type safety | Dataclasses for repository concepts | Typed context, capabilities, batch models, protocols | Typed row dictionaries and batch dataclass |
| Dependency weight | Heavy (`PyGithub`, `python-gitlab`, `requests`, Redis optional) | Moderate; provider-specific deps lazy-loaded | Light (`httpx`) |

## Decision

The canonical pattern is the **`src/dev_health_ops/providers/` provider package
boundary**: new provider code lives under `src/dev_health_ops/providers/<name>/`,
uses shared provider base contracts from `src/dev_health_ops/providers/base.py`
and TestOps async REST helpers from `src/dev_health_ops/providers/_base.py`, and
keeps normalization beside the provider rather than inside processors or legacy
connectors.

This chooses the provider package pattern, not the legacy connector package. The
initial TestOps base is the lightweight `BasePipelineAdapter` pattern lifted into
`providers/_base.py` because Jenkins, Buildkite, CircleCI, and Azure DevOps need
an async, REST-friendly, mockable foundation with varied auth and low dependency
weight. As each TestOps adapter matures, provider-specific clients should add
shared retry/429 handling and move larger mapping logic into
`providers/<name>/normalize.py` modules.

## Why this fits TestOps

- **REST-heavy APIs:** `httpx` plus injectable transports fit Jenkins,
  Buildkite, CircleCI, and Azure DevOps better than PyGithub/python-gitlab-style
  SDK coupling.
- **Varied auth:** Provider-local clients can encapsulate PATs, bearer tokens,
  basic auth, service principals, or API keys without changing processors.
- **Polling and backfills:** Async pagination and cursor-bearing batches map to
  TestOps requirements for incremental sync and historical backfill.
- **GraphQL optionality:** The provider package already supports GraphQL when a
  provider benefits from it; TestOps providers are not forced into GraphQL.
- **Normalization boundary:** Processors stay orchestration/persistence-only.
  Providers return normalized rows or domain batches; raw fetch remains inside
  provider clients/adapters.
- **Testability:** `httpx.MockTransport` remains the baseline for REST adapters,
  with fake-client injection for higher-level provider tests.

## Legacy status

- `src/dev_health_ops/connectors/github.py` and
  `src/dev_health_ops/connectors/gitlab.py` are **frozen legacy**. Do not delete
  or rewrite them in this ADR. They remain available for existing repository and
  pipeline ingestion paths until CHAOS-1548 handles deduplication/deletion.
- New code must not be added under `src/dev_health_ops/connectors/` except for
  compatibility aliases required during migration.
- Compatibility imports may wrap-and-forward from `connectors/` to `providers/`
  while callers are migrated.

## Migration plan

1. **This PR:** Lift `BasePipelineAdapter` and `PipelineSyncBatch` to
   `src/dev_health_ops/providers/_base.py`; keep
   `src/dev_health_ops/connectors/testops/base.py` as a compatibility import
   alias; update current TestOps adapters to import the lifted base.
2. **Next TestOps adapters:** Add Jenkins, Buildkite, CircleCI, and Azure
   DevOps under `src/dev_health_ops/providers/<provider>/` rather than
   `connectors/testops/`.
3. **Short follow-up:** Re-home the existing GitHub Actions and GitLab CI adapter
   modules from `connectors/testops/` into provider-owned modules once the first
   new adapter confirms the directory shape. This is intentionally separate to
   avoid bloating the ADR PR.
4. **Processor compatibility window:** Existing processors may keep importing the
   compatibility alias until the re-home follow-up updates imports without
   changing processor behavior.

## Consequences

- New provider work has one home: `src/dev_health_ops/providers/`.
- The legacy connector package becomes read-only except compatibility shims.
- TestOps keeps its lightweight async REST test seam while gaining the provider
  boundary and future shared retry/ratelimit improvements.
- Re-homing existing TestOps adapter modules is deferred to a separate Linear
  issue because the base lift is low-risk, but moving modules and updating all
  call sites is broader than this decision PR.
