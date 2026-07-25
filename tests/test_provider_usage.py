"""Tests for the shared provider usage recorder + route-family keying (CHAOS-2754)."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import httpx

from dev_health_ops.providers.base import IngestionContext, IngestionWindow
from dev_health_ops.providers.github.budget import (
    GITHUB_USAGE_RESOLVER,
    GITHUB_USAGE_ROUTE_FAMILY_KEYS,
    GitHubBudgetEstimator,
)
from dev_health_ops.providers.gitlab.budget import (
    GITLAB_USAGE_RESOLVER,
    GITLAB_USAGE_ROUTE_FAMILY_KEYS,
    GitLabBudgetEstimator,
)
from dev_health_ops.providers.jira.budget import (
    JIRA_USAGE_RESOLVER,
    JIRA_USAGE_ROUTE_FAMILY_KEYS,
    JiraBudgetEstimator,
)
from dev_health_ops.providers.launchdarkly.budget import (
    LAUNCHDARKLY_USAGE_RESOLVER,
    LAUNCHDARKLY_USAGE_ROUTE_FAMILY_KEYS,
    LaunchDarklyBudgetEstimator,
)
from dev_health_ops.providers.linear.budget import (
    LINEAR_USAGE_RESOLVER,
    LINEAR_USAGE_ROUTE_FAMILY_KEYS,
    LinearBudgetEstimator,
)
from dev_health_ops.providers.usage import UsageRecorder
from dev_health_ops.sync.budget_types import BudgetDimension, BudgetEstimator
from dev_health_ops.sync.datasets import DatasetKey
from dev_health_ops.workers.sync_bootstrap import SyncTaskContext

WINDOW_START = datetime(2026, 1, 10, tzinfo=timezone.utc)
WINDOW_END = datetime(2026, 1, 12, tzinfo=timezone.utc)


def _context(*, provider: str, dataset_key: str) -> SyncTaskContext:
    return SyncTaskContext(
        unit_id="unit-1",
        sync_run_id="run-1",
        org_id="org-1",
        integration_id="integration-1",
        source_id="source-1",
        source_external_id="acme/repo",
        provider=provider,
        dataset_key=dataset_key,
        cost_class="medium",
        mode="incremental",
        window_start=WINDOW_START,
        window_end=WINDOW_END,
        processor_flags={"sync_prs": True},
        credential_id="cred-1",
        decrypted_credentials={"token": "secret", "base_url": "https://example.test"},
        db_url="clickhouse://localhost/default",
    )


# ---------------------------------------------------------------------------
# Recorder keying / overflow collapse
# ---------------------------------------------------------------------------


def test_usage_keyed_by_route_family_and_dimension() -> None:
    """Per-issue-number operation labels collapse onto ONE route-family key, so
    100+ distinct operation strings never overflow the key cap."""

    recorder = UsageRecorder(resolver=GITHUB_USAGE_RESOLVER)
    for issue_number in range(150):
        recorder.record(
            transport="rest",
            operation=f"GET issue events for #{issue_number}",
            headers={},
            rate_limit={"remaining": str(5000 - issue_number)},
            status=200,
        )

    observations = recorder.drain()

    assert len(observations) == 1, "per-issue labels must collapse to one key"
    (obs,) = observations
    assert obs["route_family"] == "work_items"
    assert obs["dimension"] == "rest_core"
    assert obs["request_count"] == 150
    # No dropped/overflow summary row was produced.
    assert all(o.get("route_family") != "overflow" for o in observations)
    # A sampled example operation label is retained for debugging.
    assert obs["example_operation"].startswith("GET issue events for #")


def test_recorder_separates_transports_into_distinct_route_families() -> None:
    recorder = UsageRecorder(resolver=GITHUB_USAGE_RESOLVER)
    recorder.record(
        transport="rest",
        operation="GET /repos/acme/repo/issues",
        headers={},
        rate_limit={"remaining": "10"},
        status=200,
    )
    recorder.record(
        transport="graphql",
        operation="POST /graphql PR social data",
        headers={},
        rate_limit={},
        status=200,
    )

    by_family = {o["route_family"]: o for o in recorder.drain()}
    assert by_family["work_items"]["dimension"] == "rest_core"
    assert by_family["work_item_prs"]["dimension"] == "graphql_cost"


def test_github_resolver_maps_repo_prefix_to_repo_family() -> None:
    assert GITHUB_USAGE_RESOLVER.resolve(
        transport="rest", operation="repo:GET /repos/acme/widgets"
    ) == ("repo", BudgetDimension.REST_CORE)


def test_recorder_skips_empty_observations() -> None:
    recorder = UsageRecorder(resolver=GITHUB_USAGE_RESOLVER)
    recorder.record(
        transport="rest", operation="GET /x", headers={}, rate_limit={}, status=None
    )
    assert recorder.drain() == []


# ---------------------------------------------------------------------------
# Per-provider resolver vocabulary
# ---------------------------------------------------------------------------


def test_jira_resolver_maps_rest_paths_to_estimator_families() -> None:
    cases = {
        "GET /rest/api/3/search/jql": ("jira_jql", BudgetDimension.SEARCH),
        "GET /rest/api/3/issue/ABC-1/comment": (
            "jira_comments",
            BudgetDimension.REST_CORE,
        ),
        "GET /rest/api/3/issue/ABC-1": (
            "jira_issue_enrichment",
            BudgetDimension.REST_CORE,
        ),
        "GET /rest/api/3/project/search": ("jira_metadata", BudgetDimension.REST_CORE),
        "GET /rest/agile/1.0/board": ("jira_metadata", BudgetDimension.REST_CORE),
        "GET /rest/servicedeskapi/servicedesk": (
            "jira_metadata",
            BudgetDimension.REST_CORE,
        ),
        "jira_jsm_incident_admission:GET https://api.atlassian.com/jsm/incidents/cloud-id/v1/incident/123": (
            "jira_jsm_incident_admission",
            BudgetDimension.REST_CORE,
        ),
    }
    for operation, expected in cases.items():
        assert JIRA_USAGE_RESOLVER.resolve(transport="rest", operation=operation) == (
            expected
        )


def test_launchdarkly_resolver_maps_operations_to_estimator_families() -> None:
    cases = {
        "GET /flags/default": ("flags", BudgetDimension.REST_CORE),
        "GET /auditlog": ("audit_log", BudgetDimension.REST_CORE),
        # Cursor-paginated audit-log pages carry the stripped /api/v2 href
        # (querystring included) as their operation label; the marker must
        # still match.
        "GET /auditlog?limit=20&before=1000": ("audit_log", BudgetDimension.REST_CORE),
        "GET /code-refs/repositories": ("code_refs", BudgetDimension.REST_CORE),
    }
    for operation, expected in cases.items():
        assert (
            LAUNCHDARKLY_USAGE_RESOLVER.resolve(transport="rest", operation=operation)
            == expected
        ), operation


def test_gitlab_resolver_distinguishes_project_metadata_from_iterators() -> None:
    assert GITLAB_USAGE_RESOLVER.resolve(
        transport="rest", operation="GET /projects/:id"
    ) == ("project", BudgetDimension.REST_CORE)
    assert GITLAB_USAGE_RESOLVER.resolve(
        transport="rest", operation="GET iterator page"
    ) == ("issues", BudgetDimension.REST_CORE)


def test_linear_resolver_maps_named_operations() -> None:
    cases = {
        "Issues": "issues",
        "IssueHistory": "history",
        "Teams": "teams",
        "TeamByKey": "teams",
        "TeamMembers": "team_members",
        "Cycles": "cycles",
        "Projects": "projects",
        "Comments": "comments",
        "Attachments": "attachments",
        "WorkflowStates": "issues",  # no estimator family -> default
    }
    for operation, route_family in cases.items():
        resolved, dimension = LINEAR_USAGE_RESOLVER.resolve(
            transport="graphql", operation=operation
        )
        assert resolved == route_family, operation
        assert dimension == BudgetDimension.GRAPHQL_COST


# ---------------------------------------------------------------------------
# Estimator-coverage contract
# ---------------------------------------------------------------------------


def test_operation_route_family_mapping_covers_estimator_families() -> None:
    """Every route_family an estimator emits must appear in that provider's usage
    registry, or recorded actuals would key by a family no estimate exists for
    (a silently-empty calibration join)."""

    providers: list[tuple[str, BudgetEstimator, frozenset[str]]] = [
        ("github", GitHubBudgetEstimator(), GITHUB_USAGE_ROUTE_FAMILY_KEYS),
        ("gitlab", GitLabBudgetEstimator(), GITLAB_USAGE_ROUTE_FAMILY_KEYS),
        ("jira", JiraBudgetEstimator(), JIRA_USAGE_ROUTE_FAMILY_KEYS),
        ("linear", LinearBudgetEstimator(), LINEAR_USAGE_ROUTE_FAMILY_KEYS),
        (
            "launchdarkly",
            LaunchDarklyBudgetEstimator(),
            LAUNCHDARKLY_USAGE_ROUTE_FAMILY_KEYS,
        ),
    ]
    for provider, estimator, registry_keys in providers:
        emitted: set[str] = set()
        for dataset in DatasetKey:
            ctx = _context(provider=provider, dataset_key=dataset.value)
            for estimate in estimator.estimate(ctx):
                emitted.add(estimate.route_family)
        assert emitted, f"{provider} estimator emitted no route families"
        missing = emitted - registry_keys
        assert not missing, f"{provider} emits families absent from registry: {missing}"


# ---------------------------------------------------------------------------
# Provider drains
# ---------------------------------------------------------------------------


def test_gitlab_provider_drains_usage_observations(monkeypatch) -> None:
    from dev_health_ops.providers.gitlab.provider import GitLabProvider

    for flag in (
        "GITLAB_INCLUDE_MRS",
        "GITLAB_FETCH_MILESTONES",
        "GITLAB_FETCH_EPICS",
        "GITLAB_FETCH_NOTES",
        "GITLAB_FETCH_LINKS",
    ):
        monkeypatch.setenv(flag, "false")

    recorder = UsageRecorder(resolver=GITLAB_USAGE_RESOLVER)
    recorder.record(
        transport="rest",
        operation="GET /projects/:id",
        headers={},
        rate_limit={"remaining": "100"},
        status=200,
    )

    client = MagicMock()
    client.iter_project_issues.return_value = []
    client.drain_usage_observations.side_effect = recorder.drain

    provider = GitLabProvider(client=client)
    ctx = IngestionContext(repo="group/project", window=IngestionWindow())
    batch = provider.ingest(ctx)

    usage = batch.observations["provider_usage"]
    assert len(usage) == 1
    assert usage[0]["route_family"] == "project"
    assert usage[0]["dimension"] == "rest_core"
    assert usage[0]["request_count"] == 1


def test_jira_provider_drains_usage_observations() -> None:
    from dev_health_ops.providers.jira.provider import JiraProvider

    recorder = UsageRecorder(resolver=JIRA_USAGE_RESOLVER)
    recorder.record(
        transport="rest",
        operation="GET /rest/api/3/search/jql",
        headers={},
        rate_limit={"remaining": "90"},
        status=200,
    )

    client = MagicMock()
    client.iter_issues.return_value = iter([])
    client.iter_issue_comments.return_value = iter([])
    client.close.return_value = None
    client.drain_usage_observations.side_effect = recorder.drain

    provider = JiraProvider(client=client)
    ctx = IngestionContext(
        window=IngestionWindow(updated_since=datetime(2025, 1, 1, tzinfo=timezone.utc)),
        limit=1,
    )
    batch = provider.ingest(ctx)

    usage = batch.observations["provider_usage"]
    assert len(usage) == 1
    assert usage[0]["route_family"] == "jira_jql"
    assert usage[0]["dimension"] == "search"


# ---------------------------------------------------------------------------
# Linear per-POST counting
# ---------------------------------------------------------------------------


def test_linear_client_counts_graphql_requests(monkeypatch) -> None:
    from dev_health_ops.providers.linear.client import (
        ISSUES_QUERY,
        TEAMS_QUERY,
        LinearAuth,
        LinearClient,
    )

    client = LinearClient(auth=LinearAuth(api_key="secret"))

    def fake_post(url: str, json: dict | None = None) -> httpx.Response:
        return httpx.Response(
            200,
            headers={
                "X-RateLimit-Requests-Limit": "1500",
                "X-RateLimit-Requests-Remaining": "1490",
                "X-RateLimit-Requests-Reset": "0",
            },
            json={"data": {}},
            request=httpx.Request("POST", url),
        )

    monkeypatch.setattr(client._client, "post", fake_post)

    client._execute(ISSUES_QUERY, {"first": 50})
    client._execute(ISSUES_QUERY, {"first": 50})
    client._execute(TEAMS_QUERY, {"first": 50})

    by_family = {o["route_family"]: o for o in client.drain_usage_observations()}

    assert by_family["issues"]["request_count"] == 2
    assert by_family["issues"]["dimension"] == "graphql_cost"
    # X-RateLimit-Requests-* headers are captured onto the observation.
    assert by_family["issues"]["rate_limit"]["remaining"] == "1490"
    assert by_family["issues"]["rate_limit"]["limit"] == "1500"
    assert by_family["teams"]["request_count"] == 1


# ---------------------------------------------------------------------------
# Backward-compatible observation shape
# ---------------------------------------------------------------------------


def test_github_usage_key_backward_compat() -> None:
    """The job builder emits provider_usage alongside github_usage and keeps the
    linear page/batch promotion contract intact."""

    from dev_health_ops.metrics.job_work_items import _build_work_item_observations
    from dev_health_ops.workers.sync_units import _WORK_ITEM_RESULT_OBSERVATION_FIELDS

    github_usage = [{"transport": "rest", "route_family": "work_items"}]
    provider_usage = [{"transport": "rest", "route_family": "work_items"}]
    observations = _build_work_item_observations(
        github_usage=github_usage,
        provider_usage=provider_usage,
        linear_page_count=3,
        linear_batch_count=5,
        include_linear_counts=True,
    )
    assert observations["github_usage"] == github_usage
    assert observations["provider_usage"] == provider_usage
    assert observations["linear_page_count"] == 3
    assert observations["linear_batch_count"] == 5

    # The admin-API promotion contract (fields lifted to the top-level result)
    # must not have silently changed.
    assert _WORK_ITEM_RESULT_OBSERVATION_FIELDS == (
        "linear_page_count",
        "linear_batch_count",
    )


# ---------------------------------------------------------------------------
# Provider-neutral partial-observations alias (CHAOS-2803/CS2)
# ---------------------------------------------------------------------------


def test_attach_and_read_partial_observations_round_trip() -> None:
    from dev_health_ops.providers.usage import (
        attach_partial_observations,
        read_partial_observations,
    )

    exc = RuntimeError("boom")
    observations = {"provider_usage": [{"route_family": "pr_social"}]}

    attach_partial_observations(exc, observations)

    assert read_partial_observations(exc) == observations


def test_attach_partial_observations_is_noop_for_empty_payload() -> None:
    from dev_health_ops.providers.usage import (
        attach_partial_observations,
        read_partial_observations,
    )

    exc = RuntimeError("boom")
    attach_partial_observations(exc, {})

    assert read_partial_observations(exc) is None


def test_read_partial_observations_returns_none_when_never_attached() -> None:
    from dev_health_ops.providers.usage import read_partial_observations

    assert read_partial_observations(RuntimeError("boom")) is None


def test_job_work_items_partial_observations_helpers_delegate_to_providers_usage() -> (
    None
):
    """metrics/job_work_items.py's attach_work_item_partial_observations /
    read_work_item_partial_observations now delegate to the provider-neutral
    alias in providers/usage.py (CHAOS-2803/CS2) -- pinning that BOTH ends
    read/write the SAME exception attribute, so cross-module reads (e.g.
    workers/sync_units.py reading what dataset_adapters.py attached) work."""
    from dev_health_ops.metrics.job_work_items import (
        attach_work_item_partial_observations,
        read_work_item_partial_observations,
    )
    from dev_health_ops.providers.usage import (
        attach_partial_observations,
        read_partial_observations,
    )

    exc = RuntimeError("boom")
    observations = {"provider_usage": [{"route_family": "work_items"}]}

    # Written via the work-items helper, read via the provider-neutral one.
    attach_work_item_partial_observations(exc, observations)
    assert read_partial_observations(exc) == observations

    other_exc = RuntimeError("boom2")
    other_observations = {"provider_usage": [{"route_family": "pr_social"}]}

    # Written via the provider-neutral helper (the code-dataset adapter's call
    # site), read via the work-items helper (workers/sync_units.py's call
    # site) -- proves the SAME underlying attribute either way.
    attach_partial_observations(other_exc, other_observations)
    assert read_work_item_partial_observations(other_exc) == other_observations
