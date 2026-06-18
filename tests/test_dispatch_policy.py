"""Tests for sync dispatch policy and cost-class queue routing (CHAOS-2517)."""

from __future__ import annotations

import pytest

from dev_health_ops.sync.dispatch_policy import DispatchRoute, route
from dev_health_ops.workers.config import task_queues
from dev_health_ops.workers.queues import (
    DEFAULT_SYNC_QUEUE,
    SYNC_COST_CLASS_QUEUE_NAMES,
    SYNC_COST_CLASS_QUEUES,
    SYNC_QUEUE_PROVIDERS,
    _cost_class_queues_enabled,
    _provider_sync_queues_enabled,
    cost_class_queue_for_provider,
    sync_queue_for_provider,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

ORG = "org-abc"


def _route(
    provider: str,
    cost_class: str,
    *,
    flag_on: bool,
    provider_flag_on: bool = True,
    monkeypatch: pytest.MonkeyPatch,
) -> DispatchRoute:
    monkeypatch.setenv("SYNC_COST_CLASS_QUEUES", "true" if flag_on else "false")
    monkeypatch.setenv(
        "PROVIDER_SYNC_QUEUES_ENABLED", "true" if provider_flag_on else "false"
    )
    return route(
        org_id=ORG,
        provider=provider,
        cost_class=cost_class,
        cost_class_queues_enabled=flag_on,
    )


# ---------------------------------------------------------------------------
# DispatchRoute dataclass
# ---------------------------------------------------------------------------


class TestDispatchRouteShape:
    def test_frozen(self) -> None:
        r = DispatchRoute(queue="sync", cost_class="light", concurrency_key="k")
        with pytest.raises((AttributeError, TypeError)):
            r.queue = "other"  # type: ignore[misc]

    def test_fields(self) -> None:
        r = DispatchRoute(
            queue="sync.github.heavy", cost_class="heavy", concurrency_key="x:y:z"
        )
        assert r.queue == "sync.github.heavy"
        assert r.cost_class == "heavy"
        assert r.concurrency_key == "x:y:z"


# ---------------------------------------------------------------------------
# Flag OFF — all routes fall back to sync (or sync.<provider> if provider flag on)
# ---------------------------------------------------------------------------


class TestFlagOff:
    """With cost_class_queues_enabled=False, never route to cost-class queues."""

    def test_github_heavy_falls_back_to_provider_queue(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        r = _route("github", "heavy", flag_on=False, monkeypatch=monkeypatch)
        assert r.queue == "sync.github"

    def test_github_light_falls_back_to_provider_queue(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        r = _route("github", "light", flag_on=False, monkeypatch=monkeypatch)
        assert r.queue == "sync.github"

    def test_gitlab_medium_falls_back_to_provider_queue(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        r = _route("gitlab", "medium", flag_on=False, monkeypatch=monkeypatch)
        assert r.queue == "sync.gitlab"

    def test_jira_medium_falls_back_to_provider_queue(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        r = _route("jira", "medium", flag_on=False, monkeypatch=monkeypatch)
        assert r.queue == "sync.jira"

    def test_linear_medium_falls_back_to_provider_queue(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        r = _route("linear", "medium", flag_on=False, monkeypatch=monkeypatch)
        assert r.queue == "sync.linear"

    def test_both_flags_off_falls_back_to_shared_sync(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        r = _route(
            "github",
            "heavy",
            flag_on=False,
            provider_flag_on=False,
            monkeypatch=monkeypatch,
        )
        assert r.queue == DEFAULT_SYNC_QUEUE

    def test_unknown_provider_falls_back_to_shared_sync(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        r = _route("unknown_provider", "heavy", flag_on=False, monkeypatch=monkeypatch)
        assert r.queue == DEFAULT_SYNC_QUEUE


# ---------------------------------------------------------------------------
# Flag ON — route to cost-class queues when defined
# ---------------------------------------------------------------------------


class TestFlagOn:
    """With cost_class_queues_enabled=True, route to sync.<provider>.<class>."""

    @pytest.mark.parametrize(
        ("provider", "cost_class", "expected_queue"),
        [
            ("github", "light", "sync.github.light"),
            ("github", "medium", "sync.github.medium"),
            ("github", "heavy", "sync.github.heavy"),
            ("gitlab", "light", "sync.gitlab.light"),
            ("gitlab", "medium", "sync.gitlab.medium"),
            ("gitlab", "heavy", "sync.gitlab.heavy"),
            ("jira", "medium", "sync.jira.medium"),
            ("linear", "medium", "sync.linear.medium"),
        ],
    )
    def test_routes_to_cost_class_queue(
        self,
        provider: str,
        cost_class: str,
        expected_queue: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        r = _route(provider, cost_class, flag_on=True, monkeypatch=monkeypatch)
        assert r.queue == expected_queue

    def test_undefined_cost_class_falls_back_to_provider_queue(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # jira has no "light" cost-class queue defined
        r = _route("jira", "light", flag_on=True, monkeypatch=monkeypatch)
        assert r.queue == "sync.jira"

    def test_undefined_provider_falls_back_to_shared_sync(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        r = _route("unknown_provider", "heavy", flag_on=True, monkeypatch=monkeypatch)
        assert r.queue == DEFAULT_SYNC_QUEUE

    def test_provider_flag_off_cost_class_flag_on_falls_back_to_shared_sync(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # cost_class_queues_enabled=True but PROVIDER_SYNC_QUEUES_ENABLED=false
        # The route() call passes cost_class_queues_enabled=True, but the
        # provider-level fallback reads PROVIDER_SYNC_QUEUES_ENABLED.
        # With provider flag off, tier-2 also skips → shared sync.
        monkeypatch.setenv("SYNC_COST_CLASS_QUEUES", "true")
        monkeypatch.setenv("PROVIDER_SYNC_QUEUES_ENABLED", "false")
        r = route(
            org_id=ORG,
            provider="github",
            cost_class="heavy",
            cost_class_queues_enabled=True,
        )
        # Tier 1 now ALSO requires PROVIDER_SYNC_QUEUES_ENABLED: cost-class
        # queues must never be produced while only the shared `sync` queue is
        # consumed (that would strand units). Provider flag off -> shared sync.
        assert r.queue == DEFAULT_SYNC_QUEUE


# ---------------------------------------------------------------------------
# concurrency_key
# ---------------------------------------------------------------------------


class TestConcurrencyKey:
    def test_key_format(self, monkeypatch: pytest.MonkeyPatch) -> None:
        r = _route("github", "heavy", flag_on=True, monkeypatch=monkeypatch)
        assert r.concurrency_key == f"{ORG}:github:heavy"

    def test_key_normalizes_provider_case(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("SYNC_COST_CLASS_QUEUES", "true")
        monkeypatch.setenv("PROVIDER_SYNC_QUEUES_ENABLED", "true")
        r = route(
            org_id=ORG,
            provider="GitHub",
            cost_class="light",
            cost_class_queues_enabled=True,
        )
        assert r.concurrency_key == f"{ORG}:github:light"

    def test_key_present_on_fallback(self, monkeypatch: pytest.MonkeyPatch) -> None:
        r = _route(
            "github",
            "heavy",
            flag_on=False,
            provider_flag_on=False,
            monkeypatch=monkeypatch,
        )
        assert r.concurrency_key == f"{ORG}:github:heavy"


# ---------------------------------------------------------------------------
# Queue coverage: every routable queue must be declared in task_queues
# ---------------------------------------------------------------------------


class TestQueueCoverage:
    """Guard against routing to unconsumed queues."""

    def test_all_cost_class_queues_declared_in_task_queues(self) -> None:
        declared = set(task_queues.keys())
        for queue_name in SYNC_COST_CLASS_QUEUE_NAMES:
            assert queue_name in declared, (
                f"Cost-class queue {queue_name!r} is in SYNC_COST_CLASS_QUEUES "
                f"but not declared in workers.config.task_queues. "
                f"Add it before flipping the flag."
            )

    def test_default_sync_queue_declared(self) -> None:
        assert DEFAULT_SYNC_QUEUE in task_queues

    def test_all_provider_queues_declared(self) -> None:
        declared = set(task_queues.keys())
        for provider in SYNC_QUEUE_PROVIDERS:
            assert f"sync.{provider}" in declared, (
                f"Provider queue sync.{provider!r} not in task_queues"
            )

    def test_route_flag_on_targets_declared_queue(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        declared = set(task_queues.keys())
        for (provider, cost_class), queue_name in SYNC_COST_CLASS_QUEUES.items():
            r = _route(provider, cost_class, flag_on=True, monkeypatch=monkeypatch)
            assert r.queue in declared, (
                f"route({provider!r}, {cost_class!r}) returned {r.queue!r} "
                f"which is not in task_queues"
            )

    def test_route_flag_off_targets_declared_queue(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        declared = set(task_queues.keys())
        for provider, cost_class in SYNC_COST_CLASS_QUEUES:
            r = _route(provider, cost_class, flag_on=False, monkeypatch=monkeypatch)
            assert r.queue in declared, (
                f"route({provider!r}, {cost_class!r}, flag_on=False) returned "
                f"{r.queue!r} which is not in task_queues"
            )


# ---------------------------------------------------------------------------
# Legacy compat: sync_queue_for_provider
# ---------------------------------------------------------------------------


class TestSyncQueueForProviderCompat:
    """sync_queue_for_provider must keep returning legacy values."""

    def test_flag_off_returns_shared_sync(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("PROVIDER_SYNC_QUEUES_ENABLED", "false")
        for provider in SYNC_QUEUE_PROVIDERS:
            assert sync_queue_for_provider(provider) == DEFAULT_SYNC_QUEUE

    def test_flag_on_returns_provider_queue(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("PROVIDER_SYNC_QUEUES_ENABLED", "true")
        assert sync_queue_for_provider("github") == "sync.github"
        assert sync_queue_for_provider("gitlab") == "sync.gitlab"
        assert sync_queue_for_provider("jira") == "sync.jira"
        assert sync_queue_for_provider("linear") == "sync.linear"

    def test_unknown_provider_always_returns_shared_sync(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("PROVIDER_SYNC_QUEUES_ENABLED", "true")
        assert sync_queue_for_provider("unknown") == DEFAULT_SYNC_QUEUE

    def test_empty_provider_returns_shared_sync(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("PROVIDER_SYNC_QUEUES_ENABLED", "true")
        assert sync_queue_for_provider("") == DEFAULT_SYNC_QUEUE


# ---------------------------------------------------------------------------
# cost_class_queue_for_provider helper
# ---------------------------------------------------------------------------


class TestCostClassQueueForProvider:
    def test_known_pair_returns_queue(self) -> None:
        assert cost_class_queue_for_provider("github", "heavy") == "sync.github.heavy"
        assert cost_class_queue_for_provider("jira", "medium") == "sync.jira.medium"

    def test_unknown_pair_returns_none(self) -> None:
        assert cost_class_queue_for_provider("jira", "light") is None
        assert cost_class_queue_for_provider("unknown", "heavy") is None

    def test_case_insensitive_provider(self) -> None:
        assert cost_class_queue_for_provider("GitHub", "light") == "sync.github.light"


# ---------------------------------------------------------------------------
# Feature flag readers
# ---------------------------------------------------------------------------


class TestFeatureFlagReaders:
    @pytest.mark.parametrize("val", ["true", "1", "yes"])
    def test_provider_flag_truthy(
        self, val: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("PROVIDER_SYNC_QUEUES_ENABLED", val)
        assert _provider_sync_queues_enabled() is True

    @pytest.mark.parametrize("val", ["false", "0", "", "no"])
    def test_provider_flag_falsy(
        self, val: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("PROVIDER_SYNC_QUEUES_ENABLED", val)
        assert _provider_sync_queues_enabled() is False

    @pytest.mark.parametrize("val", ["true", "1", "yes"])
    def test_cost_class_flag_truthy(
        self, val: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("PROVIDER_SYNC_QUEUES_ENABLED", "true")
        monkeypatch.setenv("SYNC_COST_CLASS_QUEUES", val)
        assert _cost_class_queues_enabled() is True

    @pytest.mark.parametrize("val", ["false", "0", "", "no"])
    def test_cost_class_flag_falsy(
        self, val: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("SYNC_COST_CLASS_QUEUES", val)
        assert _cost_class_queues_enabled() is False

    @pytest.mark.parametrize("val", ["true", "1", "yes"])
    def test_cost_class_flag_requires_provider_flag(
        self, val: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Cost-class queues are a sub-tier of provider queues: with the
        # provider flag off, the cost-class flag alone must NOT enable them
        # (otherwise units route to queues no worker consumes).
        monkeypatch.setenv("PROVIDER_SYNC_QUEUES_ENABLED", "false")
        monkeypatch.setenv("SYNC_COST_CLASS_QUEUES", val)
        assert _cost_class_queues_enabled() is False
