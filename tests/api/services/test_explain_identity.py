"""Identity resolution contract for explain contributors/drivers (CHAOS-2089).

Govern Incident Correlation + Quality (Reliability Patterns) contributors and
Change-Failure associations are fed by ``/api/v1/explain``. These tests lock in
that the backend returns a resolved ``display_name`` alongside the stable id and
never surfaces a bare UUID as the primary ``label`` (Framework A7/A8).
"""

from __future__ import annotations

from dev_health_ops.api.models.filters import MetricFilter, ScopeFilter
from dev_health_ops.api.services.explain import _build_contributor, _short_token
from dev_health_ops.api.services.identity import (
    looks_like_uuid,
    scope_kind_for_group_by,
)

_REPO_UUID = "698c0211-0000-0000-0000-0000fee29c84"


def _filters() -> MetricFilter:
    return MetricFilter(scope=ScopeFilter(level="repo", ids=["698c0211"]))


def _identity(v: float) -> float:
    return v


def test_looks_like_uuid_matches_bare_uuid() -> None:
    assert looks_like_uuid(_REPO_UUID) is True
    assert looks_like_uuid("acme/backend") is False
    assert looks_like_uuid("") is False
    assert looks_like_uuid(None) is False


def test_scope_kind_maps_group_by() -> None:
    assert scope_kind_for_group_by("team_id") == "team"
    assert scope_kind_for_group_by("repo_id") == "repo"


def test_short_token_never_returns_bare_uuid() -> None:
    token = _short_token(_REPO_UUID)
    assert token == "#698c0211"
    assert not looks_like_uuid(token)
    assert _short_token("") == "Unknown"


def test_build_contributor_uses_resolved_display_name() -> None:
    row = {"id": _REPO_UUID, "value": 5.0}
    contributor = _build_contributor(
        row,
        metric="change_failure_rate",
        filters=_filters(),
        transform=_identity,
        display_names={_REPO_UUID: "meridian/billing-service"},
        delta_value=0.0,
    )

    assert contributor.id == _REPO_UUID  # stable id preserved for drill-down
    assert contributor.label == "meridian/billing-service"
    assert contributor.display_name == "meridian/billing-service"
    assert not looks_like_uuid(contributor.label)


def test_build_contributor_unresolved_falls_back_to_short_token() -> None:
    row = {"id": _REPO_UUID, "value": 3.0}
    contributor = _build_contributor(
        row,
        metric="change_failure_rate",
        filters=_filters(),
        transform=_identity,
        display_names={},  # server could not resolve
        delta_value=0.0,
    )

    # Primary label must never be the raw UUID (A8).
    assert "698c0211-0000" not in contributor.label
    assert contributor.label == "#698c0211"
    # display_name is None so the client renders its controlled Unresolved badge.
    assert contributor.display_name is None


def test_build_contributor_rejects_uuid_display_name() -> None:
    """A resolved value that is itself a UUID must not become the label (A8)."""
    row = {"id": _REPO_UUID, "value": 1.0}
    contributor = _build_contributor(
        row,
        metric="change_failure_rate",
        filters=_filters(),
        transform=_identity,
        display_names={_REPO_UUID: _REPO_UUID},
        delta_value=0.0,
    )

    assert not looks_like_uuid(contributor.label)
    assert contributor.label == "#698c0211"
    assert contributor.display_name is None


def test_build_explain_response_forwards_org_id_to_fetches() -> None:
    """Regression: driver/contributor fetches must scope by org_id (CHAOS-2089).

    Without org_id, the explain queries return zero rows for any real org, which
    is what made the Govern panels render empty/unresolved. Lock in that org_id
    is threaded through so contributor identity can actually be resolved.
    """
    import asyncio
    from contextlib import asynccontextmanager

    from dev_health_ops.api.services import explain as ex
    from dev_health_ops.api.services.cache import TTLCache

    seen: dict[str, str | None] = {}

    async def _fake_contributors(*_a, **kwargs):  # type: ignore[no-untyped-def]
        seen["contributors_org"] = kwargs.get("org_id")
        return [{"id": _REPO_UUID, "value": 1.0}]

    async def _fake_drivers(*_a, **kwargs):  # type: ignore[no-untyped-def]
        seen["drivers_org"] = kwargs.get("org_id")
        return [{"id": _REPO_UUID, "value": 1.0, "delta_pct": 0.0}]

    async def _fake_value(*_a, **_k):  # type: ignore[no-untyped-def]
        return 1.0

    async def _fake_scope_filter(*_a, **_k):  # type: ignore[no-untyped-def]
        return "", {}

    async def _fake_resolve(*_a, **kwargs):  # type: ignore[no-untyped-def]
        seen["resolve_org"] = kwargs.get("org_id")
        return {_REPO_UUID: "meridian/billing-service"}

    @asynccontextmanager
    async def _fake_client(_url):  # type: ignore[no-untyped-def]
        yield object()

    orig = {
        "fetch_metric_contributors": ex.fetch_metric_contributors,
        "fetch_metric_driver_delta": ex.fetch_metric_driver_delta,
        "fetch_metric_value": ex.fetch_metric_value,
        "scope_filter_for_metric": ex.scope_filter_for_metric,
        "resolve_scope_display_names": ex.resolve_scope_display_names,
        "clickhouse_client": ex.clickhouse_client,
    }
    ex.fetch_metric_contributors = _fake_contributors
    ex.fetch_metric_driver_delta = _fake_drivers
    ex.fetch_metric_value = _fake_value
    ex.scope_filter_for_metric = _fake_scope_filter
    ex.resolve_scope_display_names = _fake_resolve
    ex.clickhouse_client = _fake_client
    try:
        response = asyncio.run(
            ex.build_explain_response(
                db_url="clickhouse://x",
                metric="change_failure_rate",
                filters=_filters(),
                cache=TTLCache(ttl_seconds=0),
                org_id="org-meridian",
            )
        )
    finally:
        for name, fn in orig.items():
            setattr(ex, name, fn)

    assert seen["contributors_org"] == "org-meridian"
    assert seen["drivers_org"] == "org-meridian"
    assert seen["resolve_org"] == "org-meridian"
    assert response.contributors
    assert response.contributors[0].display_name == "meridian/billing-service"
    assert response.contributors[0].label == "meridian/billing-service"
