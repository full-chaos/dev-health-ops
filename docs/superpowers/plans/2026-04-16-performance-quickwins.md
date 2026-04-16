# Performance Quick Wins Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate event-loop blocking and sequential-await bottlenecks in hot async code paths across GraphQL, workers, ingest, and services.

**Architecture:** Small, surgical edits to known hotspots. Each fix is a bite-sized TDD loop: write a behavioural assertion (parallel execution, single gather call, module-level compile, etc.), then apply the minimal code change. No refactors. No new dependencies. Tests use `monkeypatch` timing probes or `mock.patch` call-order assertions instead of wall-clock benchmarks (pytest-benchmark is NOT configured — see `pyproject.toml`).

**Tech Stack:** Python 3.11+, FastAPI async, Strawberry GraphQL, SQLAlchemy async, Celery, ClickHouse, Redis/Valkey, Alembic, pytest + pytest-asyncio.

---

## Audit Verification Notes

Before starting, note the following corrections to the original audit findings (verified against the current source tree):

| # | Audit finding | Status after verification |
|---|---|---|
| 1 | Sequential inner loops in Sankey nodes/edges queries at `analytics.py:281-282, 301-302` | **VALID.** `asyncio.gather(fetch_nodes(), fetch_edges())` already wraps the two outer async defs, but the inner `for sql, params in nodes_queries:` and `for sql, params in edges_queries:` still serialize every query within each list. |
| 2a | Sync `requests` inside `async def` at `settings.py:780-786` | **NOT A BUG — SKIP.** The `requests.get` call lives inside nested `def _discover()` which is already dispatched via `await asyncio.to_thread(_discover)` at line 811. Event loop is not blocked. |
| 2b | Sync `requests` inside `async def` at `connectors/utils/rest.py:89-94` | **NOT A BUG — SKIP.** `RESTClient.get` is a plain `def` (sync) on a sync client. No call site invokes it from an `async def`. |
| 3 | Sequential provider loop in `workers/sync_team.py:36-88` | **VALID.** Three providers are discovered and synced serially inside a single async body. |
| 4 | No concurrency limit on external APIs in `github.py`, `gitlab.py` | **PARTIALLY VALID.** `max_workers` is stored on the connector (`base.py:83`) but never used as an `asyncio.Semaphore`. Since both connectors are primarily synchronous wrappers around PyGithub/python-gitlab, adding a semaphore is only meaningful for the few async call paths. Scope: add an `asyncio.Semaphore` in the base class plumbed through for future async use; no existing async hotspots require it. Keep this as a **small preparatory change** with a single unit test. |
| 5 | Blocking `time.sleep(1)` in ingest consumer at `api/ingest/consumer.py:121` | **MIXED.** The `consume_streams` function is `def` (sync), not `async def`. It is executed in a Celery worker thread. `time.sleep` does NOT block the asyncio loop. However, the fix is still valuable: replace the blanket 1s sleep with bounded exponential backoff + cap, which reduces the tight-loop CPU cost on repeated XREADGROUP failures. |
| 6 | Composite audit-log index `ix_audit_logs_org_action_created` | **ALREADY APPLIED.** Present in `alembic/versions/0001_initial_schema.py:755`. Only action: add a regression test asserting the index name is referenced in the initial migration file. No new migration needed. |
| 7 | N+1 in Jira project member discovery at `settings.py:1200-1210` | **VALID (single-call, but loop-prone).** Only one sync REST call per project inside `asyncio.to_thread(_discover)`. When the caller loops `project_keys`, each call is a fresh HTTPS round-trip. Fix: expose a `discover_members_jira_bulk(project_keys)` that fans out concurrently via `asyncio.gather` with an `asyncio.Semaphore(5)`. |
| 8 | Regex compile inside hot loop at `work_unit_explain.py:180-189` | **VALID.** `re.findall(pattern, text, re.IGNORECASE)` is invoked once per category key per request — pattern is constructed from user-supplied category names, so pre-compiling the literal parts is straightforward. |
| 9 | Sequential ClickHouse investment queries at `investment.py:51-84` (callers) | **VALID.** `build_investment_flow_response` at `services/investment_flow.py:390` awaits `fetch_investment_subcategory_edges`, then `fetch_investment_team_edges` at line 401 — sequential. |
| 10 | Cache-key loop in GraphQL loader at `loaders/base.py:76-97` | **VALID but smaller than claimed.** Cache backend (`core/cache.py`) does not implement `mget`. Minimal-scope fix: add an `mget(keys)` method to `CacheBackend`, memory impl does the loop, Redis impl uses pipelined MGET; `CachedDataLoader._load_with_cache` uses it when available. |

---

## File Structure

### Created
- `src/dev_health_ops/api/graphql/resolvers/_sankey_parallel.py` — internal helper for parallel inner-loop query execution (kept narrow; only used by `analytics.py`).

### Modified
- `src/dev_health_ops/api/graphql/resolvers/analytics.py` — replace inner for-loops with `asyncio.gather`.
- `src/dev_health_ops/workers/sync_team.py` — parallelize provider discovery with `asyncio.gather`.
- `src/dev_health_ops/connectors/base.py` — add `asyncio.Semaphore` to `GitConnector` base.
- `src/dev_health_ops/api/ingest/consumer.py` — swap bare `time.sleep(1)` for bounded exponential backoff with cap.
- `src/dev_health_ops/api/services/settings.py` — add `discover_members_jira_bulk` with gathered concurrency.
- `src/dev_health_ops/api/services/work_unit_explain.py` — hoist regex compilation to module level.
- `src/dev_health_ops/api/services/investment_flow.py` — parallelize sequential `fetch_investment_*_edges` with `asyncio.gather`.
- `src/dev_health_ops/core/cache.py` — add `mget` method to `CacheBackend` + impls.
- `src/dev_health_ops/api/graphql/loaders/base.py` — use `mget` when backend supports it.

### Tests (new)
- `tests/graphql/test_sankey_parallel.py` — asserts parallel execution in Sankey inner loop.
- `tests/test_sync_team_parallel.py` — asserts providers are gathered concurrently.
- `tests/test_base_connector_semaphore.py` — asserts connectors expose an asyncio semaphore.
- `tests/test_ingest_consumer_backoff.py` — asserts exponential backoff on repeated XREADGROUP failures.
- `tests/api/services/test_jira_bulk_members.py` — asserts bulk member discovery calls project lookups concurrently.
- `tests/api/test_work_unit_explain_regex_cached.py` — asserts pattern cache hit on repeat calls.
- `tests/api/test_investment_flow_parallel.py` — asserts investment flow fetches run concurrently.
- `tests/graphql/loaders/test_cached_dataloader_mget.py` — asserts `mget` path is used when available.
- `tests/test_audit_index_migration.py` — asserts composite audit index stays referenced in migration 0001.

---

## Dependency Graph

All findings can be executed INDEPENDENTLY except for:
- **Task 8 (Cache `mget`)** must come before **Task 9 (Loader uses mget)**.
- All other tasks touch disjoint files and can be parallelized across workers.

```
Task 1 (Sankey inner loops)        — independent
Task 2 (sync_team providers)       — independent
Task 3 (Base connector semaphore)  — independent
Task 4 (Ingest backoff)            — independent
Task 5 (Jira bulk members)         — independent
Task 6 (Regex hoist)               — independent
Task 7 (Investment flow parallel)  — independent
Task 8 (Cache mget method)         ─┐
                                    ├─► Task 9 (Loader uses mget)
Task 10 (Audit index regression test) — independent
```

---

## Task 1: Parallelize Sankey inner query loops

**Files:**
- Modify: `src/dev_health_ops/api/graphql/resolvers/analytics.py:278-325`
- Test: `tests/graphql/test_sankey_parallel.py` (new)

Inner `for sql, params in nodes_queries:` and `for sql, params in edges_queries:` currently serialize N queries each. Replace each for-loop with `asyncio.gather(*[query_dicts(...) for sql, params in queries])`.

- [ ] **Step 1: Write the failing test**

Create `tests/graphql/test_sankey_parallel.py`:

```python
"""Assert Sankey resolver executes inner queries concurrently, not serially."""

from __future__ import annotations

import asyncio
from datetime import date
from unittest.mock import patch

import pytest

from dev_health_ops.api.graphql.context import GraphQLContext


@pytest.mark.asyncio
async def test_sankey_nodes_queries_run_concurrently(monkeypatch):
    """When compile_sankey returns N node queries, fetch_nodes must dispatch them
    in parallel via asyncio.gather. We prove this by checking that the maximum
    observed overlap is > 1 (all N queries are in flight at the same time)."""
    from dev_health_ops.api.graphql.resolvers import analytics as mod

    # Build three fake queries
    fake_node_queries = [("SQL1", {"p": 1}), ("SQL2", {"p": 2}), ("SQL3", {"p": 3})]
    fake_edge_queries = [("SQLE", {"p": 9})]

    active = 0
    peak = 0

    async def fake_query_dicts(client, sql, params):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        try:
            await asyncio.sleep(0.05)
            if sql.startswith("SQLE"):
                return [
                    {
                        "source_dimension": "team",
                        "target_dimension": "repo",
                        "source": "t1",
                        "target": "r1",
                        "value": 1,
                    }
                ]
            return [
                {"dimension": "team", "node_id": "t1", "value": 1.0},
            ]
        finally:
            active -= 1

    monkeypatch.setattr(
        "dev_health_ops.api.queries.client.query_dicts",
        fake_query_dicts,
    )

    nodes, edges = await mod._execute_sankey_inner(
        client=object(),
        nodes_queries=fake_node_queries,
        edges_queries=fake_edge_queries,
    )

    assert peak >= 3, f"Expected >=3 concurrent queries, saw peak={peak}"
    assert len(nodes) == 3  # one row per query
    assert len(edges) == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/graphql/test_sankey_parallel.py -v`

Expected: FAIL — either `AttributeError: module ... has no attribute '_execute_sankey_inner'` or peak observed is 1 (serial).

- [ ] **Step 3: Implement the parallel helper**

Edit `src/dev_health_ops/api/graphql/resolvers/analytics.py`. Replace the body of the `if batch.sankey is not None:` block's `fetch_nodes` and `fetch_edges` inner defs, and expose a module-level helper. Locate the block starting at line 277 (`try:` after `compile_sankey` call) and replace the two async inner functions with gather-based implementations, plus add a helper that the test can call directly:

Change (in `analytics.py`, just below the imports block, above `_execute_timeseries_query` — approximately line 47):

```python
async def _execute_sankey_inner(
    client: Any,
    nodes_queries: list[tuple[str, dict[str, Any]]],
    edges_queries: list[tuple[str, dict[str, Any]]],
) -> tuple[list[SankeyNode], list[SankeyEdge]]:
    """Execute all node and edge queries concurrently and aggregate results."""
    from dev_health_ops.api.queries.client import query_dicts

    async def _nodes() -> list[SankeyNode]:
        results = await asyncio.gather(
            *(query_dicts(client, sql, params) for sql, params in nodes_queries)
        )
        out: list[SankeyNode] = []
        for rows in results:
            if not rows:
                continue
            for row in rows:
                dim = str(row.get("dimension", ""))
                node_id = str(row.get("node_id", ""))
                value = float(row.get("value", 0))
                out.append(
                    SankeyNode(
                        id=f"{dim}:{node_id}",
                        label=node_id,
                        dimension=dim,
                        value=value,
                    )
                )
        return out

    async def _edges() -> list[SankeyEdge]:
        results = await asyncio.gather(
            *(query_dicts(client, sql, params) for sql, params in edges_queries)
        )
        out: list[SankeyEdge] = []
        for rows in results:
            if not rows:
                continue
            for row in rows:
                source_dim = str(row.get("source_dimension", ""))
                target_dim = str(row.get("target_dimension", ""))
                source = str(row.get("source", ""))
                target = str(row.get("target", ""))
                value = float(row.get("value", 0))
                out.append(
                    SankeyEdge(
                        source=f"{source_dim}:{source}",
                        target=f"{target_dim}:{target}",
                        value=value,
                    )
                )
        return out

    nodes_task = _nodes()
    edges_task = _edges()
    nodes, edges = await asyncio.gather(nodes_task, edges_task)
    return nodes, edges
```

Then, inside the existing Sankey block (replace lines 277-360 of the existing `try:` block), replace the `fetch_nodes`/`fetch_edges` definitions and the subsequent `asyncio.gather(fetch_nodes(), fetch_edges(), return_exceptions=True)` + exception-handling with a single call:

```python
        try:
            nodes, edges = await _execute_sankey_inner(
                client,
                nodes_queries,
                edges_queries,
            )
        except Exception as exc:
            logger.error("Sankey query failed: %s", exc)
            nodes, edges = [], []
```

Preserve the downstream `sankey_result = SankeyResult(...)` assignment exactly as-is — only the two inner defs and the `asyncio.gather` call they feed are replaced.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/graphql/test_sankey_parallel.py -v`

Expected: PASS with `peak >= 3`.

- [ ] **Step 5: Run existing resolver tests for regression**

Run: `pytest tests/graphql/test_resolvers.py -v`

Expected: PASS (no regressions — same test suite that exercises `resolve_analytics`).

- [ ] **Step 6: Commit**

```bash
git add src/dev_health_ops/api/graphql/resolvers/analytics.py tests/graphql/test_sankey_parallel.py
git commit -m "$(cat <<'EOF'
perf(graphql): parallelize Sankey inner node/edge queries via asyncio.gather

Inner for-loops over nodes_queries and edges_queries previously serialized
every sub-query. Gathering them concurrently saves ~N * per-query latency
for composite Sankey requests.
EOF
)"
```

---

## Task 2: Parallelize worker team-discovery across providers

**Files:**
- Modify: `src/dev_health_ops/workers/sync_team.py:29-91`
- Test: `tests/test_sync_team_parallel.py` (new)

Replace the `for provider in ("github", "gitlab", "jira"):` sequential `await` loop with `asyncio.gather` on a coroutine per provider.

- [ ] **Step 1: Write the failing test**

Create `tests/test_sync_team_parallel.py`:

```python
"""Assert sync_team_drift dispatches provider discovery concurrently."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


@pytest.mark.asyncio
async def test_providers_discovered_concurrently(monkeypatch):
    from dev_health_ops.workers import sync_team as mod

    active = 0
    peak = 0

    async def slow_discover(*_args, **_kwargs):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        try:
            await asyncio.sleep(0.05)
            return []
        finally:
            active -= 1

    fake_creds = MagicMock()
    fake_creds.get = AsyncMock(
        return_value=MagicMock(config={"org": "o", "group": "g", "url": "https://j"})
    )
    fake_creds.get_decrypted_credentials = AsyncMock(
        return_value={"token": "t", "email": "e@x", "api_token": "a"}
    )

    fake_discovery = MagicMock()
    fake_discovery.discover_github = slow_discover
    fake_discovery.discover_gitlab = slow_discover
    fake_discovery.discover_jira = slow_discover

    fake_drift = MagicMock()
    fake_drift.run_drift_sync = AsyncMock(return_value={"provider": "x"})

    class _FakeSession:
        async def __aenter__(self):
            return MagicMock(commit=AsyncMock())

        async def __aexit__(self, *a):
            return False

        async def commit(self):
            return None

    @pytest.fixture(autouse=False)  # not a fixture; imported below
    def _noop(): ...

    with (
        patch.object(mod, "run_async", lambda coro: asyncio.get_event_loop().run_until_complete(coro)),
        patch("dev_health_ops.api.services.settings.IntegrationCredentialsService", return_value=fake_creds),
        patch("dev_health_ops.api.services.settings.TeamDiscoveryService", return_value=fake_discovery),
        patch("dev_health_ops.api.services.settings.TeamDriftSyncService", return_value=fake_drift),
        patch("dev_health_ops.db.get_postgres_session", lambda: _FakeSession()),
    ):
        # Call the async body directly via the helper we'll add
        result = await mod._discover_and_sync_all(org_id="org-1")

    assert peak >= 3, f"Expected 3 concurrent providers, observed peak={peak}"
    assert len(result["results"]) == 3
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_sync_team_parallel.py -v`

Expected: FAIL — `AttributeError: module 'dev_health_ops.workers.sync_team' has no attribute '_discover_and_sync_all'`.

- [ ] **Step 3: Extract and parallelize provider loop**

Edit `src/dev_health_ops/workers/sync_team.py`. Replace the body of `sync_team_drift._run` with a call to a new module-level helper:

```python
async def _discover_and_sync_all(org_id: str | None) -> dict:
    from dev_health_ops.api.services.settings import (
        IntegrationCredentialsService,
        TeamDiscoveryService,
        TeamDriftSyncService,
    )
    from dev_health_ops.db import get_postgres_session

    async with get_postgres_session() as session:
        creds_svc = IntegrationCredentialsService(session, org_id)
        discovery_svc = TeamDiscoveryService(session, org_id)
        drift_svc = TeamDriftSyncService(session, org_id)

        async def _run_one(provider: str) -> dict:
            credential = await creds_svc.get(provider, "default")
            if credential is None:
                return {"provider": provider, "skipped": "no_credential"}
            decrypted = await creds_svc.get_decrypted_credentials(provider, "default")
            if decrypted is None:
                return {"provider": provider, "skipped": "no_decrypted"}
            config = credential.config or {}
            try:
                if provider == "github":
                    token = decrypted.get("token")
                    org_name = config.get("org")
                    if not token or not org_name:
                        return {"provider": provider, "skipped": "missing_config"}
                    teams = await discovery_svc.discover_github(
                        token=token, org_name=org_name
                    )
                elif provider == "gitlab":
                    token = decrypted.get("token")
                    group_path = config.get("group")
                    url = config.get("url", "https://gitlab.com")
                    if not token or not group_path:
                        return {"provider": provider, "skipped": "missing_config"}
                    teams = await discovery_svc.discover_gitlab(
                        token=token, group_path=group_path, url=url
                    )
                else:
                    email = decrypted.get("email")
                    api_token = decrypted.get("api_token") or decrypted.get("token")
                    jira_url = config.get("url") or decrypted.get("url")
                    if not email or not api_token or not jira_url:
                        return {"provider": provider, "skipped": "missing_config"}
                    teams = await discovery_svc.discover_jira(
                        email=email, api_token=api_token, url=jira_url
                    )
                return await drift_svc.run_drift_sync(provider, teams)
            except Exception as exc:
                logger.warning(
                    "Team drift sync failed for provider %s: %s", provider, exc
                )
                return {"provider": provider, "error": str(exc)}

        results = await asyncio.gather(
            _run_one("github"),
            _run_one("gitlab"),
            _run_one("jira"),
        )
        await session.commit()
    return {"status": "success", "results": list(results)}
```

Add `import asyncio` at the top of `sync_team.py` (alongside existing imports). Then update `sync_team_drift`:

```python
@celery_app.task(
    bind=True,
    max_retries=2,
    queue="sync",
    name="dev_health_ops.workers.tasks.sync_team_drift",
)
def sync_team_drift(self, org_id: str | None = None) -> dict:
    try:
        return run_async(_discover_and_sync_all(org_id))
    except Exception as exc:
        logger.exception("sync_team_drift failed: %s", exc)
        raise self.retry(exc=exc, countdown=300)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_sync_team_parallel.py -v`

Expected: PASS with `peak >= 3`.

- [ ] **Step 5: Commit**

```bash
git add src/dev_health_ops/workers/sync_team.py tests/test_sync_team_parallel.py
git commit -m "$(cat <<'EOF'
perf(workers): parallelize team-drift provider discovery via asyncio.gather

Dispatches GitHub, GitLab, and Jira discovery + drift-sync concurrently
instead of serializing them. Roughly cuts wall-time for sync_team_drift
by ~2/3 when all three providers are configured.
EOF
)"
```

---

## Task 3: Add asyncio.Semaphore to base connector

**Files:**
- Modify: `src/dev_health_ops/connectors/base.py` (around line 83)
- Test: `tests/test_base_connector_semaphore.py` (new)

Connectors accept `max_workers` but never use it. Expose an `asyncio.Semaphore(max_workers)` on `GitConnector`, so future async call sites can gate concurrency uniformly.

- [ ] **Step 1: Write the failing test**

Create `tests/test_base_connector_semaphore.py`:

```python
"""Assert GitConnector base class exposes an asyncio.Semaphore with max_workers permits."""

from __future__ import annotations

import asyncio

import pytest

from dev_health_ops.connectors.base import GitConnector


def test_semaphore_created_with_max_workers():
    class _Dummy(GitConnector):
        pass

    c = _Dummy(per_page=25, max_workers=7)
    sem = c.concurrency_semaphore
    assert isinstance(sem, asyncio.Semaphore)
    # BoundedSemaphore/Semaphore expose ._value on CPython
    assert sem._value == 7


@pytest.mark.asyncio
async def test_semaphore_limits_concurrency():
    class _Dummy(GitConnector):
        pass

    c = _Dummy(per_page=10, max_workers=2)
    active = 0
    peak = 0

    async def worker():
        nonlocal active, peak
        async with c.concurrency_semaphore:
            active += 1
            peak = max(peak, active)
            try:
                await asyncio.sleep(0.02)
            finally:
                active -= 1

    await asyncio.gather(*(worker() for _ in range(8)))
    assert peak == 2
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_base_connector_semaphore.py -v`

Expected: FAIL — `AttributeError: '_Dummy' object has no attribute 'concurrency_semaphore'`.

- [ ] **Step 3: Add semaphore to base**

Open `src/dev_health_ops/connectors/base.py` and locate the `GitConnector.__init__`. Currently it sets `self.max_workers = max_workers`. Add:

```python
import asyncio  # add if not present at the top of base.py
```

Then in `__init__` (after the `self.max_workers = max_workers` line):

```python
        # Lazy-created so workers not in an asyncio context don't pay the cost.
        self._concurrency_semaphore: asyncio.Semaphore | None = None

    @property
    def concurrency_semaphore(self) -> asyncio.Semaphore:
        """Shared semaphore gating concurrent async calls to this connector."""
        if self._concurrency_semaphore is None:
            self._concurrency_semaphore = asyncio.Semaphore(self.max_workers)
        return self._concurrency_semaphore
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_base_connector_semaphore.py -v`

Expected: PASS both tests.

- [ ] **Step 5: Commit**

```bash
git add src/dev_health_ops/connectors/base.py tests/test_base_connector_semaphore.py
git commit -m "$(cat <<'EOF'
perf(connectors): add asyncio.Semaphore to GitConnector base

Exposes a lazy-initialised concurrency_semaphore gating future async call
sites without touching the existing sync paths. No behaviour change for
current callers.
EOF
)"
```

---

## Task 4: Bounded exponential backoff in ingest consumer

**Files:**
- Modify: `src/dev_health_ops/api/ingest/consumer.py:106-125`
- Test: `tests/test_ingest_consumer_backoff.py` (new)

Replace the unconditional `time.sleep(1)` on XREADGROUP failure with exponential backoff capped at 30s. Reduces CPU cost when Redis is unreachable for extended periods and gives the operator headroom before killing the worker.

- [ ] **Step 1: Write the failing test**

Create `tests/test_ingest_consumer_backoff.py`:

```python
"""Assert consume_streams uses exponential backoff on repeated XREADGROUP failures."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

fakeredis = pytest.importorskip("fakeredis")


def test_exponential_backoff_on_repeated_failures(monkeypatch):
    from dev_health_ops.api.ingest import consumer as mod

    class BrokenRedis:
        def __init__(self):
            self.calls = 0

        def xreadgroup(self, *a, **kw):
            self.calls += 1
            raise RuntimeError("boom")

        def scan_iter(self, *a, **kw):
            return iter(["ingest:o:commits"])

        def xgroup_create(self, *a, **kw):
            pass

    broken = BrokenRedis()
    sleeps: list[float] = []
    monkeypatch.setattr(mod.time, "sleep", lambda s: sleeps.append(s))
    monkeypatch.setattr(mod, "get_redis_client", lambda: broken)

    mod.consume_streams(stream_patterns=["ingest:*:commits"], max_iterations=5)

    # Five failed iterations → backoff sequence starts at 1s and doubles
    # with a 30s cap. Assert strictly monotonic-non-decreasing and bounded.
    assert len(sleeps) == 5
    assert sleeps[0] == 1
    assert sleeps[-1] <= 30
    for a, b in zip(sleeps, sleeps[1:]):
        assert b >= a, f"backoff should not shrink: {sleeps}"
    assert any(b > a for a, b in zip(sleeps, sleeps[1:])), "expected growth"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_ingest_consumer_backoff.py -v`

Expected: FAIL — current behaviour always sleeps 1 second, so `any(b > a)` is false.

- [ ] **Step 3: Implement bounded backoff**

Edit `src/dev_health_ops/api/ingest/consumer.py`. At the top of `consume_streams`, after `iterations = 0`, add:

```python
    backoff_s = 1.0
    BACKOFF_MAX_S = 30.0
```

Then replace:

```python
        except Exception:
            logger.exception("XREADGROUP failed")
            time.sleep(1)
            continue
```

with:

```python
        except Exception:
            logger.exception("XREADGROUP failed (backoff=%ss)", backoff_s)
            time.sleep(backoff_s)
            backoff_s = min(backoff_s * 2, BACKOFF_MAX_S)
            continue
```

Also reset `backoff_s = 1.0` on successful `xreadgroup` (directly after `results = rc.xreadgroup(...)`):

```python
        backoff_s = 1.0
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_ingest_consumer_backoff.py -v`

Expected: PASS.

- [ ] **Step 5: Run existing consumer tests**

Run: `pytest tests/test_ingest_streams.py -v`

Expected: PASS (no behavioural regression on happy path).

- [ ] **Step 6: Commit**

```bash
git add src/dev_health_ops/api/ingest/consumer.py tests/test_ingest_consumer_backoff.py
git commit -m "$(cat <<'EOF'
perf(ingest): exponential backoff on XREADGROUP failures (cap 30s)

Replaces a flat 1s sleep with doubling backoff capped at 30s so a
prolonged Redis outage does not pin CPU. Backoff resets on first
successful read.
EOF
)"
```

---

## Task 5: Bulk Jira project-member discovery

**Files:**
- Modify: `src/dev_health_ops/api/services/settings.py` (add new method around line 1232)
- Test: `tests/api/services/test_jira_bulk_members.py` (new)

Today each `discover_members_jira(project_key)` call is one HTTPS round-trip wrapped in `asyncio.to_thread`. Callers that loop over many projects serialize those round-trips. Add a `discover_members_jira_bulk(project_keys)` that fans out via `asyncio.gather` with an `asyncio.Semaphore(5)` so we don't hammer Jira.

- [ ] **Step 1: Write the failing test**

Create `tests/api/services/test_jira_bulk_members.py`:

```python
"""Assert discover_members_jira_bulk runs per-project lookups concurrently."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest


@pytest.mark.asyncio
async def test_bulk_members_concurrent(monkeypatch):
    from dev_health_ops.api.services import settings as mod

    active = 0
    peak = 0

    async def slow_single(self, email, api_token, url, project_key):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        try:
            await asyncio.sleep(0.05)
            return [
                # 1 member per project
                MagicMock(provider_identity=f"user-{project_key}")
            ]
        finally:
            active -= 1

    monkeypatch.setattr(mod.TeamDiscoveryService, "discover_members_jira", slow_single)

    svc = mod.TeamDiscoveryService(session=MagicMock(), org_id="org-1")
    out = await svc.discover_members_jira_bulk(
        email="e@x",
        api_token="t",
        url="https://j",
        project_keys=["P1", "P2", "P3", "P4", "P5", "P6"],
        concurrency=5,
    )

    # Members flattened across projects
    assert len(out) == 6
    # >=5 in-flight at peak (limited by semaphore)
    assert peak >= 5
    # Not more than 5 at once
    assert peak <= 5


@pytest.mark.asyncio
async def test_bulk_members_honours_concurrency_cap(monkeypatch):
    from dev_health_ops.api.services import settings as mod

    active = 0
    peak = 0

    async def slow_single(self, email, api_token, url, project_key):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        try:
            await asyncio.sleep(0.02)
            return []
        finally:
            active -= 1

    monkeypatch.setattr(mod.TeamDiscoveryService, "discover_members_jira", slow_single)

    svc = mod.TeamDiscoveryService(session=MagicMock(), org_id="o")
    await svc.discover_members_jira_bulk(
        email="e", api_token="t", url="u", project_keys=[str(i) for i in range(10)],
        concurrency=2,
    )
    assert peak == 2
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/api/services/test_jira_bulk_members.py -v`

Expected: FAIL — `AttributeError: 'TeamDiscoveryService' object has no attribute 'discover_members_jira_bulk'`.

- [ ] **Step 3: Implement bulk method**

Open `src/dev_health_ops/api/services/settings.py`. Immediately after the existing `discover_members_jira` method (around line 1232, just before `async def match_members`), add:

```python
    async def discover_members_jira_bulk(
        self,
        *,
        email: str,
        api_token: str,
        url: str,
        project_keys: list[str],
        concurrency: int = 5,
    ) -> list[Any]:
        """Fan out Jira project member lookups concurrently.

        Uses an asyncio.Semaphore to cap simultaneous HTTPS requests so we
        don't trip Jira's per-IP rate limits.
        """
        sem = asyncio.Semaphore(max(1, concurrency))

        async def _one(project_key: str) -> list[Any]:
            async with sem:
                return await self.discover_members_jira(
                    email=email,
                    api_token=api_token,
                    url=url,
                    project_key=project_key,
                )

        results = await asyncio.gather(*(_one(k) for k in project_keys))
        flat: list[Any] = []
        for group in results:
            flat.extend(group)
        return flat
```

Ensure `from typing import Any` is already imported at the top of the file (it is — see line 14).

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/api/services/test_jira_bulk_members.py -v`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/dev_health_ops/api/services/settings.py tests/api/services/test_jira_bulk_members.py
git commit -m "$(cat <<'EOF'
perf(settings): add discover_members_jira_bulk with gathered concurrency

Fans out per-project Jira member lookups via asyncio.gather +
Semaphore(5), replacing a loop of sequential single-project calls.
Caps simultaneous HTTPS requests so we don't trip Jira rate limits.
EOF
)"
```

---

## Task 6: Hoist regex compilation in work_unit_explain

**Files:**
- Modify: `src/dev_health_ops/api/services/work_unit_explain.py:171-191`
- Test: `tests/api/test_work_unit_explain_regex_cached.py` (new)

`_extract_category_rationale` builds `rf"{category}[^.]*\."` inside the per-category loop. `re.findall(pattern, text, flags)` re-compiles every call. Memoize compilation per category.

- [ ] **Step 1: Write the failing test**

Create `tests/api/test_work_unit_explain_regex_cached.py`:

```python
"""Assert _extract_category_rationale compiles each category pattern at most once."""

from __future__ import annotations

import re
from unittest.mock import patch

import pytest


def test_category_pattern_compiled_once_per_key():
    from dev_health_ops.api.services import work_unit_explain as mod

    compile_calls: list[str] = []
    real_compile = re.compile

    def spy_compile(pattern, flags=0):
        compile_calls.append(pattern)
        return real_compile(pattern, flags)

    with patch.object(mod.re, "compile", side_effect=spy_compile):
        text = "feature_delivery work was extensive. Maintenance: refactoring loops."
        categories = {"feature_delivery": 0.5, "maintenance": 0.5}
        mod._extract_category_rationale(text, categories)
        mod._extract_category_rationale(text, categories)

    # Each category compiles at most once across both invocations.
    compiled_category_patterns = [p for p in compile_calls if "[^.]*" in p]
    # 2 categories, each should appear 0 or 1 time (cache hit on second call)
    counts = {
        k: sum(1 for p in compiled_category_patterns if k in p)
        for k in categories
    }
    for k, n in counts.items():
        assert n <= 1, f"{k} re-compiled {n} times across calls (expected <=1)"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/api/test_work_unit_explain_regex_cached.py -v`

Expected: FAIL — `re.compile` is called every invocation.

- [ ] **Step 3: Add a module-level cache**

Edit `src/dev_health_ops/api/services/work_unit_explain.py`. Near the top of the file (after the `logger = logging.getLogger(__name__)` line on line 26), add:

```python
from functools import lru_cache


@lru_cache(maxsize=256)
def _compiled_category_pattern(category: str) -> "re.Pattern[str]":
    """Cache compiled regex for a category key. Category names are bounded
    (a small, fixed taxonomy) so the unbounded-input regex risk is low."""
    return re.compile(rf"{re.escape(category)}[^.]*\.", re.IGNORECASE)
```

Then replace the body of `_extract_category_rationale` (lines 171-191):

```python
def _extract_category_rationale(
    text: str, categories: dict[str, float]
) -> dict[str, str]:
    """Extract rationale for each category from the response."""
    rationale: dict[str, str] = {}

    # Try to find category analysis section
    analysis_section = _extract_section(text, "Category Analysis")

    for category in categories:
        pattern = _compiled_category_pattern(category)
        matches = pattern.findall(text)
        if matches:
            rationale[category] = matches[0].strip()
        elif analysis_section:
            rationale[category] = "Category appears in overall analysis."
        else:
            rationale[category] = "Category leaning based on structural evidence."

    return rationale
```

Note: `re.escape(category)` is a defensive addition — some categories may contain `.` (e.g. `feature_delivery.customer`). The old inline pattern did not escape them, which was a latent bug. If downstream tests fail because of escaping, drop the `re.escape(...)` and use `category` directly.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/api/test_work_unit_explain_regex_cached.py -v`

Expected: PASS.

- [ ] **Step 5: Run existing work-unit-explain tests**

Run: `pytest tests/api/test_work_unit_explain.py -v`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/dev_health_ops/api/services/work_unit_explain.py tests/api/test_work_unit_explain_regex_cached.py
git commit -m "$(cat <<'EOF'
perf(work_unit_explain): cache compiled category regex via lru_cache

Hoists per-category re.compile out of the hot loop. Patterns are keyed
on the bounded taxonomy of category names, so the cache cannot grow
unbounded. Adds re.escape to close a latent bug with dotted keys.
EOF
)"
```

---

## Task 7: Parallelize investment-flow fetches

**Files:**
- Modify: `src/dev_health_ops/api/services/investment_flow.py:389-410`
- Test: `tests/api/test_investment_flow_parallel.py` (new)

The "fallback mode" path in `build_investment_flow_response` awaits `fetch_investment_subcategory_edges` then `fetch_investment_team_edges` sequentially. Wrap in `asyncio.gather`.

- [ ] **Step 1: Write the failing test**

Create `tests/api/test_investment_flow_parallel.py`:

```python
"""Assert build_investment_flow_response runs repo and team edge fetches in parallel."""

from __future__ import annotations

import asyncio
from datetime import date
from unittest.mock import AsyncMock, MagicMock

import pytest


@pytest.mark.asyncio
async def test_repo_and_team_edges_fetched_in_parallel(monkeypatch):
    import dev_health_ops.api.services.investment_flow as mod

    active = 0
    peak = 0

    async def slow_fetch(*args, **kwargs):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        try:
            await asyncio.sleep(0.05)
            return []
        finally:
            active -= 1

    monkeypatch.setattr(mod, "fetch_investment_subcategory_edges", slow_fetch)
    monkeypatch.setattr(mod, "fetch_investment_team_edges", slow_fetch)

    class _FakeSink:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

    monkeypatch.setattr(mod, "clickhouse_client", lambda _url: _FakeSink())
    monkeypatch.setattr(mod, "require_clickhouse_backend", lambda _s: None)
    monkeypatch.setattr(mod, "_tables_present", AsyncMock(return_value=True))
    monkeypatch.setattr(mod, "_columns_present", AsyncMock(return_value=True))
    monkeypatch.setattr(mod, "resolve_repo_filter_ids", AsyncMock(return_value=[]))
    monkeypatch.setattr(
        mod, "build_scope_filter_multi", lambda *_a, **_kw: ("", {})
    )

    class _Scope:
        level = "org"

    filters = MagicMock()
    filters.scope = _Scope()
    filters.themes = []
    filters.subcategories = []

    monkeypatch.setattr(mod, "time_window", lambda _f: (date(2026, 1, 1), date(2026, 1, 7), None, None))
    monkeypatch.setattr(mod, "_split_category_filters", lambda _f: ([], []))

    await mod.build_investment_flow_response(
        db_url="clickhouse://x", filters=filters, org_id="org"
    )

    assert peak >= 2, f"expected concurrent fetches, saw peak={peak}"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/api/test_investment_flow_parallel.py -v`

Expected: FAIL — sequential execution yields `peak == 1`.

- [ ] **Step 3: Replace sequential fetches with gather**

Edit `src/dev_health_ops/api/services/investment_flow.py`. Ensure `import asyncio` is present near the top of the file (add if missing). Replace lines 389-410 (the `# 1. Fetch both sets of edges` block):

```python
        # 1. Fetch both sets of edges in parallel.
        repo_rows, team_rows = await asyncio.gather(
            fetch_investment_subcategory_edges(
                sink,
                start_ts=start_ts,
                end_ts=end_ts,
                scope_filter=scope_filter,
                scope_params=scope_params,
                org_id=org_id,
                themes=theme_filters or None,
                subcategories=subcategory_filters or None,
            ),
            fetch_investment_team_edges(
                sink,
                start_ts=start_ts,
                end_ts=end_ts,
                scope_filter=scope_filter,
                scope_params=scope_params,
                org_id=org_id,
                themes=theme_filters or None,
                subcategories=subcategory_filters or None,
            ),
        )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/api/test_investment_flow_parallel.py -v`

Expected: PASS.

- [ ] **Step 5: Run existing investment flow tests**

Run: `pytest tests/api/services/test_investment_flow.py -v`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/dev_health_ops/api/services/investment_flow.py tests/api/test_investment_flow_parallel.py
git commit -m "$(cat <<'EOF'
perf(investment_flow): parallelize repo-edge and team-edge ClickHouse fetches

Both queries are now awaited via asyncio.gather in the fallback-mode
branch of build_investment_flow_response. Cuts wall-time roughly in
half on wide date ranges.
EOF
)"
```

---

## Task 8: Add mget to CacheBackend

**Files:**
- Modify: `src/dev_health_ops/core/cache.py:22-108`
- Test: extend existing cache tests or add assertion in Task 9 loader test

Add an `mget(keys)` method to `CacheBackend` with a default loop implementation, plus an optimized Redis MGET for the Redis backend. Task 9 depends on this.

- [ ] **Step 1: Write the failing test**

Add to the existing cache test file, or create `tests/test_cache_mget.py`:

```python
"""Assert CacheBackend exposes mget with correct semantics."""

from __future__ import annotations

import pytest

from dev_health_ops.core.cache import MemoryBackend


def test_memory_mget_returns_aligned_list():
    be = MemoryBackend()
    be.set("a", 1, ttl_seconds=60)
    be.set("c", 3, ttl_seconds=60)

    got = be.mget(["a", "b", "c"])
    assert got == [1, None, 3]


def test_memory_mget_empty_keys():
    be = MemoryBackend()
    assert be.mget([]) == []


def test_memory_mget_all_missing():
    be = MemoryBackend()
    assert be.mget(["x", "y"]) == [None, None]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_cache_mget.py -v`

Expected: FAIL — `AttributeError: 'MemoryBackend' object has no attribute 'mget'`.

- [ ] **Step 3: Add mget to CacheBackend + implementations**

Edit `src/dev_health_ops/core/cache.py`. Add to `CacheBackend` (after the `status` abstract method, approximately line 38):

```python
    def mget(self, keys: list[str]) -> list[Any | None]:
        """Batch get. Default implementation calls get() per key.

        Backends with native multi-get (e.g. Redis MGET) should override this.
        """
        return [self.get(k) for k in keys]
```

Then override on `RedisBackend` (after the existing `get` method around line 90):

```python
    def mget(self, keys: list[str]) -> list[Any | None]:
        if not keys:
            return []
        if not self._available:
            return self._fallback.mget(keys)
        try:
            raw_values = self._client.mget(keys)
        except Exception as e:
            logger.warning("Redis mget failed: %s", e)
            return [None] * len(keys)
        results: list[Any | None] = []
        for raw in raw_values:
            if raw is None:
                results.append(None)
                continue
            try:
                results.append(json.loads(raw))
            except Exception as e:
                logger.warning("Redis mget value decode failed: %s", e)
                results.append(None)
        return results
```

Also propagate to `TTLCache` (around line 122):

```python
    def mget(self, keys: list[str]) -> list[Any | None]:
        return self._backend.mget(keys)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_cache_mget.py -v`

Expected: PASS all three cases.

- [ ] **Step 5: Commit**

```bash
git add src/dev_health_ops/core/cache.py tests/test_cache_mget.py
git commit -m "$(cat <<'EOF'
feat(cache): add mget to CacheBackend with Redis MGET optimisation

Default implementation loops get(); RedisBackend overrides with a single
MGET round-trip. Enables loader batching in a follow-up commit.
EOF
)"
```

---

## Task 9: Use mget in CachedDataLoader

**Depends on:** Task 8.

**Files:**
- Modify: `src/dev_health_ops/api/graphql/loaders/base.py:69-115`
- Test: `tests/graphql/loaders/test_cached_dataloader_mget.py` (new)

Replace the per-key `self._external_cache.get(cache_key)` loop with a single `mget` call when the cache backend exposes it. Fall back to the per-key loop when the cache is a `TTLCache` without `mget` (shouldn't happen after Task 8, but defensive).

- [ ] **Step 1: Create test directory and test**

Create `tests/graphql/loaders/__init__.py` (empty) and `tests/graphql/loaders/test_cached_dataloader_mget.py`:

```python
"""Assert CachedDataLoader batches cache lookups via mget when available."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from dev_health_ops.api.graphql.loaders.base import CachedDataLoader


class _FakeCache:
    def __init__(self, hits: dict[str, Any]):
        self._hits = hits
        self.get_calls = 0
        self.mget_calls = 0
        self.set_calls = 0

    def get(self, key):
        self.get_calls += 1
        return self._hits.get(key)

    def mget(self, keys):
        self.mget_calls += 1
        return [self._hits.get(k) for k in keys]

    def set(self, key, value, *_a, **_kw):
        self.set_calls += 1


class _Loader(CachedDataLoader[str, str]):
    def __init__(self, cache):
        super().__init__(org_id="o", cache=cache, cache_prefix="test")
        self.load_calls: list[list[str]] = []

    async def batch_load(self, keys):
        self.load_calls.append(list(keys))
        return [f"v:{k}" for k in keys]


@pytest.mark.asyncio
async def test_mget_used_when_available():
    # all three hit the cache
    # The loader hashes keys via make_cache_key; precompute matching keys.
    from dev_health_ops.api.graphql.loaders.base import make_cache_key

    expected = {
        make_cache_key("test", "o", "k1"): "cached-k1",
        make_cache_key("test", "o", "k2"): "cached-k2",
    }
    cache = _FakeCache(hits=expected)
    loader = _Loader(cache)

    out = await loader._load_with_cache(["k1", "k2", "k3"])

    assert cache.mget_calls == 1, "expected a single mget batch call"
    assert cache.get_calls == 0, "per-key get should not be used"
    assert out[0] == "cached-k1"
    assert out[1] == "cached-k2"
    assert out[2] == "v:k3"  # miss → batch_load
    assert loader.load_calls == [["k3"]]


@pytest.mark.asyncio
async def test_falls_back_to_get_when_no_mget():
    class _NoMget:
        def __init__(self):
            self.get_calls = 0

        def get(self, key):
            self.get_calls += 1
            return None

        def set(self, *a, **kw):
            pass

    cache = _NoMget()
    loader = _Loader(cache)
    out = await loader._load_with_cache(["a", "b"])
    assert cache.get_calls == 2
    assert out == ["v:a", "v:b"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/graphql/loaders/test_cached_dataloader_mget.py -v`

Expected: FAIL — current impl always calls `.get()` per key (`mget_calls == 0`).

- [ ] **Step 3: Use mget in loader**

Edit `src/dev_health_ops/api/graphql/loaders/base.py`. Replace the body of `_load_with_cache` (lines 69-114):

```python
    async def _load_with_cache(self, keys: list[K]) -> Sequence[V]:
        """Load values with optional cache lookup.

        Uses a single mget() call per batch when the backend supports it,
        falling back to per-key get() otherwise.
        """
        results: dict[int, V] = {}
        missing_keys: list[tuple[int, K]] = []

        if self._external_cache is None:
            missing_keys = [(idx, key) for idx, key in enumerate(keys)]
        else:
            cache_keys = [
                make_cache_key(self._cache_prefix, self._org_id, key) for key in keys
            ]
            mget_fn = getattr(self._external_cache, "mget", None)
            if callable(mget_fn):
                try:
                    cached_values = mget_fn(cache_keys)
                except Exception as e:
                    logger.debug("Cache mget failed: %s", e)
                    cached_values = [None] * len(cache_keys)
            else:
                cached_values = []
                for ck in cache_keys:
                    try:
                        cached_values.append(self._external_cache.get(ck))
                    except Exception as e:
                        logger.debug("Cache get failed for %s: %s", ck, e)
                        cached_values.append(None)

            for idx, key, cached in zip(range(len(keys)), keys, cached_values):
                if cached is not None:
                    results[idx] = cached
                else:
                    missing_keys.append((idx, key))

        # Batch load missing keys
        if missing_keys:
            missing_indices, missing_key_values = (
                zip(*missing_keys) if missing_keys else ([], [])
            )
            loaded_values = await self.batch_load(list(missing_key_values))

            for idx, key, value in zip(
                missing_indices, missing_key_values, loaded_values
            ):
                results[idx] = value
                if self._external_cache and value is not None:
                    cache_key = make_cache_key(self._cache_prefix, self._org_id, key)
                    try:
                        self._external_cache.set(cache_key, value)
                    except Exception as e:
                        logger.debug("Cache set failed for %s: %s", cache_key, e)

        return [results[idx] for idx in range(len(keys))]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/graphql/loaders/test_cached_dataloader_mget.py -v`

Expected: PASS both tests.

- [ ] **Step 5: Commit**

```bash
git add src/dev_health_ops/api/graphql/loaders/base.py tests/graphql/loaders/__init__.py tests/graphql/loaders/test_cached_dataloader_mget.py
git commit -m "$(cat <<'EOF'
perf(graphql-loader): batch cache lookups via mget when available

CachedDataLoader._load_with_cache now issues a single mget() per batch
instead of N get() calls, collapsing round-trips to Redis from O(batch)
to O(1). Falls back to per-key get() when backend lacks mget.
EOF
)"
```

---

## Task 10: Regression test for composite audit index

**Files:**
- Test: `tests/test_audit_index_migration.py` (new)

Finding #6 is already applied in migration 0001, but we want a lightweight guard that someone doesn't drop it in a future edit. No production code change.

- [ ] **Step 1: Write the test**

Create `tests/test_audit_index_migration.py`:

```python
"""Guard: ensure composite audit-log index stays in initial migration."""

from __future__ import annotations

from pathlib import Path


INITIAL_MIGRATION = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "dev_health_ops"
    / "alembic"
    / "versions"
    / "0001_initial_schema.py"
)


def test_initial_migration_has_audit_composite_index():
    src = INITIAL_MIGRATION.read_text(encoding="utf-8")
    # Must reference the composite index name and its covering columns.
    assert "ix_audit_logs_org_action_created" in src, (
        "Composite audit-log index ix_audit_logs_org_action_created is missing "
        "from the initial migration. This index was added to support the "
        "org_id + action + created_at query pattern and must not be removed."
    )


def test_audit_model_declares_composite_index():
    from dev_health_ops.models.audit import AuditLog

    index_names = [ix.name for ix in AuditLog.__table_args__]
    assert "ix_audit_logs_org_action_created" in index_names
```

- [ ] **Step 2: Run test to verify it passes (it should — already applied)**

Run: `pytest tests/test_audit_index_migration.py -v`

Expected: PASS both tests.

- [ ] **Step 3: Commit**

```bash
git add tests/test_audit_index_migration.py
git commit -m "$(cat <<'EOF'
test(audit): guard composite audit-log index in initial migration

Regression test asserts ix_audit_logs_org_action_created is declared on
the AuditLog model AND referenced in alembic/versions/0001_initial_schema.py.
Prevents accidental removal of the (org_id, action, created_at) index.
EOF
)"
```

---

## Self-Review Checklist

Before merging, run the full suite to catch cross-task interactions:

- [ ] `pytest tests/graphql/ -v` — Sankey parallel + loader mget tests all green.
- [ ] `pytest tests/api/ -v` — investment_flow, work_unit_explain, settings tests all green.
- [ ] `pytest tests/test_sync_team_parallel.py tests/test_base_connector_semaphore.py tests/test_ingest_consumer_backoff.py tests/test_cache_mget.py tests/test_audit_index_migration.py -v` — all new integration points green.
- [ ] `ruff check src/ tests/` — no lint regressions.
- [ ] `mypy src/` — no type regressions (optional; matches existing CI policy).

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-04-16-performance-quickwins.md`. Two execution options:

1. **Subagent-Driven (recommended)** — dispatch a fresh subagent per task with review between. Best for the 8 independent tasks (1, 2, 3, 4, 5, 6, 7, 10) which can run in parallel; Tasks 8→9 run sequentially.
2. **Inline Execution** — run tasks in this session using `superpowers:executing-plans` with checkpoints.
