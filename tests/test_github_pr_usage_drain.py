"""CHAOS-2803/CS2: usage-drain plumbing for the PR review-batch enrichment.

``_enrich_prs_with_reviews_batch`` (processors/github.py) constructs a local
``GitHubWorkClient`` to batch-fetch PR reviews via GraphQL. Before CS2 this
client was never drained -- its recorded usage was silently discarded, so the
review-batch's real GraphQL traffic never surfaced as a ``budget_comparison``
actual. This file pins the drain contract through ``_sync_github_prs_to_store``
(the real call chain ``process_github_repo`` uses): an adapter-owned
``usage_sink`` list is threaded down and populated in a ``finally:`` block,
and the "no sink" (legacy CLI batch / webhook) path still drains the client
but only logs the observations -- never persists, never raises.
"""

import asyncio
import uuid
from datetime import datetime, timezone
from typing import Any, cast
from unittest.mock import patch

import pytest

# Initialize the connectors package before processors to avoid the
# pre-existing providers._base <-> connectors circular import when this file
# is collected in isolation (mirrors test_processors_pr_mr_rate_limit.py).
import dev_health_ops.connectors  # noqa: F401
from dev_health_ops.exceptions import RateLimitException
from dev_health_ops.processors.github import _sync_github_prs_to_store


class _NoSleepGate:
    def wait_sync(self) -> None:
        return

    def penalize(self, delay_seconds=None) -> float:
        return float(delay_seconds or 0)

    def reset(self) -> None:
        return


class _FakeStore:
    def __init__(self):
        self.pr_batches: list[list[Any]] = []
        self.review_batches: list[list[Any]] = []

    async def insert_git_pull_requests(self, batch):
        self.pr_batches.append(list(batch))

    async def insert_git_pull_request_reviews(self, batch):
        self.review_batches.append(list(batch))


class _FakeGithub:
    def __init__(self, repo):
        self._repo = repo

    def get_repo(self, _full_name: str):
        return self._repo


class _PRIter:
    def __init__(self, items):
        self._items = list(items)
        self._idx = 0

    def __iter__(self):
        return self

    def __next__(self):
        if self._idx >= len(self._items):
            raise StopIteration
        item = self._items[self._idx]
        self._idx += 1
        return item


class _FakePRUser:
    def __init__(self, login="octo"):
        self.login = login
        self.email = None


class _FakePRRef:
    def __init__(self, ref):
        self.ref = ref


class _FakePR:
    def __init__(self, number: int):
        self.number = number
        self.title = f"PR {number}"
        self.body = None
        self.state = "closed"
        self.user = _FakePRUser()
        self.created_at = datetime(2020, 1, 1, tzinfo=timezone.utc)
        self.merged_at = None
        self.closed_at = None
        self.head = _FakePRRef("feature")
        self.base = _FakePRRef("main")
        self.additions = 0
        self.deletions = 0
        self.changed_files = 0
        self.comments = 0


class _FakeRepo:
    def __init__(self, pull_items):
        self._pull_items = pull_items

    def get_pulls(self, state="all", sort=None, direction=None):
        return _PRIter(self._pull_items)


class _Connector:
    def __init__(self, fake_repo):
        self.github = _FakeGithub(fake_repo)
        self.token = "token"
        self.per_page = 100
        self.graphql = object()

    def _rest_base_url(self):
        return "https://api.github.com"


class _DrainingBatchReviewClient:
    """Fake work client whose ``drain_usage_observations`` returns canned
    records, so tests can assert the DRAIN side of the contract without
    depending on the real GitHubWorkClient/UsageRecorder plumbing (already
    covered separately in tests/providers/test_github_pr_social_batch.py)."""

    instances: list["_DrainingBatchReviewClient"] = []

    def __init__(self, **_kwargs):
        self.graphql = None
        self.drained = False
        type(self).instances.append(self)

    def iter_pr_reviews_batch(self, *, owner, repo, prs, limit, operation_family=None):
        self.operation_family = operation_family
        for pr in prs:
            yield pr.number, ()

    def drain_usage_observations(self) -> list[dict[str, Any]]:
        self.drained = True
        return [
            {
                "transport": "graphql",
                "route_family": "pr_social",
                "dimension": "graphql_cost",
                "request_count": 1,
                "example_operation": "pr_social:POST /graphql PR social data",
            }
        ]


@pytest.fixture(autouse=True)
def _reset_instances():
    _DrainingBatchReviewClient.instances = []
    yield
    _DrainingBatchReviewClient.instances = []


@pytest.mark.asyncio
async def test_sync_prs_drains_review_client_into_adapter_owned_sink():
    """The adapter-owned usage_sink list is populated with the review-batch
    client's drained observations under the pr_social family."""
    loop = asyncio.get_running_loop()
    repo_id = uuid.uuid4()
    store = _FakeStore()
    fake_repo = _FakeRepo([_FakePR(1), _FakePR(2)])
    usage_sink: list[dict[str, Any]] = []

    with patch(
        "dev_health_ops.processors.github.GitHubWorkClient",
        _DrainingBatchReviewClient,
    ):
        total = await loop.run_in_executor(
            None,
            cast(Any, _sync_github_prs_to_store),
            _Connector(fake_repo),
            "o",
            "r",
            repo_id,
            store,
            loop,
            50,  # batch_size
            "all",
            _NoSleepGate(),
            None,  # since
            None,  # until
            usage_sink,
        )

    assert total == 2
    assert len(_DrainingBatchReviewClient.instances) == 1
    assert _DrainingBatchReviewClient.instances[0].drained is True
    assert usage_sink == [
        {
            "transport": "graphql",
            "route_family": "pr_social",
            "dimension": "graphql_cost",
            "request_count": 1,
            "example_operation": "pr_social:POST /graphql PR social data",
        }
    ]


@pytest.mark.asyncio
async def test_sync_prs_without_sink_drains_client_but_logs_only(caplog):
    """CHAOS-2773 plan §2 last bullet: legacy entry points (no adapter-owned
    sink -- the default when usage_sink is omitted, e.g. the CLI batch path
    and webhooks) still drain the review client -- so its recorder does not
    accumulate/leak across calls -- but the drained observations are only
    logged at debug level, never persisted, and no exception is raised."""
    loop = asyncio.get_running_loop()
    repo_id = uuid.uuid4()
    store = _FakeStore()
    fake_repo = _FakeRepo([_FakePR(1)])

    with (
        patch(
            "dev_health_ops.processors.github.GitHubWorkClient",
            _DrainingBatchReviewClient,
        ),
        caplog.at_level("DEBUG", logger="root"),
    ):
        # usage_sink omitted entirely -- exercises the default (None) path.
        total = await loop.run_in_executor(
            None,
            cast(Any, _sync_github_prs_to_store),
            _Connector(fake_repo),
            "o",
            "r",
            repo_id,
            store,
            loop,
            50,
            "all",
            _NoSleepGate(),
        )

    assert total == 1
    assert len(_DrainingBatchReviewClient.instances) == 1
    assert _DrainingBatchReviewClient.instances[0].drained is True
    assert any(
        "drained" in record.message and "no adapter-owned sink" in record.message
        for record in caplog.records
    )


# ---------------------------------------------------------------------------
# Codex HIGH (PR #1151): the review-batch catch-all must NOT swallow a
# RateLimitException -- it must propagate (after the finally-drain) so the
# adapter attaches the partial pr_social usage and sync_units defers the unit
# to RETRYING instead of stamping SUCCESS with reviews silently missing.
# Non-rate-limit enrichment errors keep the pre-existing degrade-and-log
# semantic (reviews are optional garnish on top of the PR rows).
# ---------------------------------------------------------------------------

_PARTIAL_PR_SOCIAL_RECORD = {
    "transport": "graphql",
    "route_family": "pr_social",
    "dimension": "graphql_cost",
    "request_count": 1,
    "example_operation": "pr_social:POST /graphql PR social data",
}


class _RateLimitedAfterOneRequestClient:
    """Fake work client: yields one PR's reviews then raises the canonical
    RateLimitException mid-batch; drain returns the one recorded request."""

    instances: list["_RateLimitedAfterOneRequestClient"] = []

    def __init__(self, **_kwargs):
        self.graphql = None
        self.drained = False
        type(self).instances.append(self)

    def iter_pr_reviews_batch(self, *, owner, repo, prs, limit, operation_family=None):
        yield prs[0].number, ()
        raise RateLimitException("429 Too Many Requests", retry_after_seconds=120)

    def drain_usage_observations(self) -> list[dict[str, Any]]:
        self.drained = True
        return [dict(_PARTIAL_PR_SOCIAL_RECORD)]


class _FailingBatchReviewClient:
    """Fake work client whose batch fetch fails with a NON-rate-limit error."""

    instances: list["_FailingBatchReviewClient"] = []

    def __init__(self, **_kwargs):
        self.graphql = None
        self.drained = False
        type(self).instances.append(self)

    def iter_pr_reviews_batch(self, *, owner, repo, prs, limit, operation_family=None):
        raise RuntimeError("GraphQL schema drift")
        yield  # pragma: no cover - make this a generator

    def drain_usage_observations(self) -> list[dict[str, Any]]:
        self.drained = True
        return [dict(_PARTIAL_PR_SOCIAL_RECORD)]


@pytest.fixture(autouse=True)
def _reset_failure_client_instances():
    _RateLimitedAfterOneRequestClient.instances = []
    _FailingBatchReviewClient.instances = []
    yield
    _RateLimitedAfterOneRequestClient.instances = []
    _FailingBatchReviewClient.instances = []


@pytest.mark.asyncio
async def test_sync_prs_rate_limit_propagates_after_partial_drain():
    """A RateLimitException raised mid-review-batch PROPAGATES out of
    _sync_github_prs_to_store: no success return, no PR rows persisted this
    attempt (the worker defers and retries the whole unit) -- while the
    finally-drain has already moved the partial pr_social usage into the
    adapter-owned sink, so the deferral stamp still records the actuals
    (the sink is exactly what _run_github_dataset attaches to the exception,
    pinned by test_dataset_adapters' failure-path tests)."""
    loop = asyncio.get_running_loop()
    repo_id = uuid.uuid4()
    store = _FakeStore()
    fake_repo = _FakeRepo([_FakePR(1), _FakePR(2)])
    usage_sink: list[dict[str, Any]] = []

    with patch(
        "dev_health_ops.processors.github.GitHubWorkClient",
        _RateLimitedAfterOneRequestClient,
    ):
        with pytest.raises(RateLimitException) as exc_info:
            await loop.run_in_executor(
                None,
                cast(Any, _sync_github_prs_to_store),
                _Connector(fake_repo),
                "o",
                "r",
                repo_id,
                store,
                loop,
                50,
                "all",
                _NoSleepGate(),
                None,  # since
                None,  # until
                usage_sink,
            )

    assert exc_info.value.retry_after_seconds == 120
    # The drain ran BEFORE the exception unwound past the processor.
    assert len(_RateLimitedAfterOneRequestClient.instances) == 1
    assert _RateLimitedAfterOneRequestClient.instances[0].drained is True
    assert usage_sink == [_PARTIAL_PR_SOCIAL_RECORD]
    # No success-path persistence happened: the unit defers, nothing stamped.
    assert store.pr_batches == []
    assert store.review_batches == []


@pytest.mark.asyncio
async def test_sync_prs_non_rate_limit_error_still_degrades_gracefully():
    """The pre-existing degrade-and-log semantic is preserved for
    NON-rate-limit review-batch errors: PRs are still persisted (reviews are
    optional enrichment), no exception propagates, and the client is still
    drained into the sink."""
    loop = asyncio.get_running_loop()
    repo_id = uuid.uuid4()
    store = _FakeStore()
    fake_repo = _FakeRepo([_FakePR(1), _FakePR(2)])
    usage_sink: list[dict[str, Any]] = []

    with patch(
        "dev_health_ops.processors.github.GitHubWorkClient",
        _FailingBatchReviewClient,
    ):
        total = await loop.run_in_executor(
            None,
            cast(Any, _sync_github_prs_to_store),
            _Connector(fake_repo),
            "o",
            "r",
            repo_id,
            store,
            loop,
            50,
            "all",
            _NoSleepGate(),
            None,  # since
            None,  # until
            usage_sink,
        )

    assert total == 2
    assert len(_FailingBatchReviewClient.instances) == 1
    assert _FailingBatchReviewClient.instances[0].drained is True
    assert usage_sink == [_PARTIAL_PR_SOCIAL_RECORD]
    # PRs persisted despite the failed (optional) review enrichment.
    persisted_prs = [pr for batch in store.pr_batches for pr in batch]
    assert len(persisted_prs) == 2
    assert store.review_batches == []
