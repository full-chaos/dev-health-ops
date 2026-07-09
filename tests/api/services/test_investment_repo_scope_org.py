from __future__ import annotations

from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import TypedDict

import pytest

from dev_health_ops.api.models.filters import MetricFilter, ScopeFilter, TimeFilter
from dev_health_ops.api.services import investment as investment_service
from dev_health_ops.api.services import investment_flow as investment_flow_service

ORG_ID = "org-uuid"
REPO_ID = "repo-uuid"
EXPECTED_SCOPE_PARAMS = {"scope_ids": [REPO_ID]}


class CapturedScope(TypedDict):
    scope_filter: str
    scope_params: dict[str, list[str]]


@asynccontextmanager
async def _fake_clickhouse_client(_dsn):
    yield SimpleNamespace(backend_type="clickhouse")


async def _tables_present(*_args, **_kwargs):
    return True


async def _columns_present(*_args, **_kwargs):
    return True


async def _empty_mapping(*_args, **_kwargs):
    return {}


async def _zero_count(*_args, **_kwargs):
    return 0


def _repo_filters() -> MetricFilter:
    return MetricFilter(
        time=TimeFilter(range_days=7, compare_days=7),
        scope=ScopeFilter(level="repo", ids=["full-chaos/dev-health-ops"]),
    )


def _capture_scope(captured: CapturedScope, rows):
    async def _query(_sink, **kwargs):
        captured["scope_filter"] = kwargs["scope_filter"]
        captured["scope_params"] = kwargs["scope_params"]
        return rows

    return _query


def _return_rows(rows):
    async def _query(*_args, **_kwargs):
        return rows

    return _query


def _install_common_patches(monkeypatch, service_module, captured_org_ids):
    async def _resolve_repo_filter_ids(_sink, _filters, org_id=""):
        captured_org_ids.append(org_id)
        return [REPO_ID]

    monkeypatch.setattr(service_module, "clickhouse_client", _fake_clickhouse_client)
    monkeypatch.setattr(service_module, "_tables_present", _tables_present)
    monkeypatch.setattr(service_module, "_columns_present", _columns_present)
    monkeypatch.setattr(
        service_module, "resolve_repo_filter_ids", _resolve_repo_filter_ids
    )


def _assert_repo_scope(captured: CapturedScope, captured_org_ids: list[str]):
    assert captured_org_ids == [ORG_ID]
    assert "repo_id IN %(scope_ids)s" in captured["scope_filter"]
    assert captured["scope_params"] == EXPECTED_SCOPE_PARAMS


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("builder", "query_patch", "rows", "extra_patches", "call_kwargs"),
    [
        (
            investment_service.build_investment_response,
            "fetch_investment_breakdown",
            [
                {
                    "theme": "feature_delivery",
                    "subcategory": "feature_delivery.roadmap",
                    "value": 2,
                }
            ],
            {
                "fetch_mock_fixture_investment_row_count": _zero_count,
                "fetch_investment_quality_stats": _empty_mapping,
            },
            {},
        ),
        (
            investment_service.build_investment_sunburst,
            "fetch_investment_sunburst",
            [
                {
                    "theme": "feature_delivery",
                    "subcategory": "feature_delivery.roadmap",
                    "scope": "full-chaos/dev-health-ops",
                    "value": 2,
                }
            ],
            {"fetch_mock_fixture_investment_row_count": _zero_count},
            {},
        ),
    ],
)
async def test_investment_services_resolve_repo_filter_with_org(
    monkeypatch, builder, query_patch, rows, extra_patches, call_kwargs
):
    captured: CapturedScope = {"scope_filter": "", "scope_params": {}}
    captured_org_ids: list[str] = []

    _install_common_patches(monkeypatch, investment_service, captured_org_ids)
    monkeypatch.setattr(investment_service, query_patch, _capture_scope(captured, rows))
    for name, replacement in extra_patches.items():
        monkeypatch.setattr(investment_service, name, replacement)

    await builder(db_url="clickhouse://", filters=_repo_filters(), org_id=ORG_ID)

    _assert_repo_scope(captured, captured_org_ids)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("builder", "query_patch", "rows", "extra_patches", "call_kwargs"),
    [
        (
            investment_flow_service.build_investment_flow_response,
            "fetch_investment_team_category_repo_edges",
            [
                {
                    "team": "Core",
                    "category": "feature_delivery",
                    "repo": "Repo",
                    "value": 2,
                }
            ],
            {"fetch_investment_unassigned_counts": _empty_mapping},
            {"flow_mode": "team_category_repo"},
        ),
        (
            investment_flow_service.build_investment_flow_response,
            "fetch_investment_subcategory_edges",
            [
                {"source": "feature_delivery.roadmap", "target": "Repo A", "value": 2},
                {"source": "feature_delivery.quality", "target": "Repo B", "value": 2},
            ],
            {
                "fetch_investment_team_edges": _return_rows(
                    [
                        {
                            "source": "feature_delivery.roadmap",
                            "target": "unassigned",
                            "value": 4,
                        }
                    ]
                )
            },
            {},
        ),
        (
            investment_flow_service.build_investment_repo_team_flow_response,
            "fetch_investment_repo_team_edges",
            [
                {
                    "subcategory": "feature_delivery.roadmap",
                    "repo": "Repo",
                    "team": "Core",
                    "value": 2,
                }
            ],
            {},
            {},
        ),
    ],
)
async def test_investment_flow_services_resolve_repo_filter_with_org(
    monkeypatch, builder, query_patch, rows, extra_patches, call_kwargs
):
    captured: CapturedScope = {"scope_filter": "", "scope_params": {}}
    captured_org_ids: list[str] = []

    _install_common_patches(monkeypatch, investment_flow_service, captured_org_ids)
    monkeypatch.setattr(
        investment_flow_service, query_patch, _capture_scope(captured, rows)
    )
    for name, replacement in extra_patches.items():
        monkeypatch.setattr(investment_flow_service, name, replacement)

    await builder(
        db_url="clickhouse://",
        filters=_repo_filters(),
        org_id=ORG_ID,
        **call_kwargs,
    )

    _assert_repo_scope(captured, captured_org_ids)
