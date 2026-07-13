from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.models.filters import MetricFilter, ScopeFilter, TimeFilter
from dev_health_ops.api.models.schemas import (
    ConstraintCard,
    Coverage,
    Freshness,
    HomeResponse,
)
from dev_health_ops.api.services import home as home_service
from dev_health_ops.api.services.cache import TTLCache
from dev_health_ops.api.services.filtering import filter_cache_key


@pytest.mark.asyncio
async def test_cached_home_response_refreshes_latest_successful_sync(monkeypatch):
    filters = MetricFilter(
        time=TimeFilter(range_days=14, compare_days=14),
        scope=ScopeFilter(level="org", ids=[]),
    )
    cache = TTLCache(ttl_seconds=60)
    cached_sync_at = datetime(2026, 7, 13, 15, 5, tzinfo=UTC)
    latest_sync_at = datetime(2026, 7, 13, 16, 11, tzinfo=UTC)
    cached_response = HomeResponse(
        freshness=Freshness(
            last_ingested_at=datetime(2026, 7, 12, 0, 7, tzinfo=UTC),
            latest_successful_sync_at=cached_sync_at,
            sources={},
            coverage=Coverage(
                repos_covered_pct=100,
                prs_linked_to_issues_pct=100,
                issues_with_cycle_states_pct=100,
            ),
        ),
        deltas=[],
        summary=[],
        tiles={},
        constraint=ConstraintCard(title="", claim="", evidence=[], experiments=[]),
        events=[],
    )
    cache.set(
        filter_cache_key("home", "org-1", filters),
        cached_response.model_dump(mode="json"),
    )
    semantic_session = MagicMock(spec=AsyncSession)

    async def _fake_latest_successful_sync_at(session, *, org_id):
        assert session is semantic_session
        assert org_id == "org-1"
        return latest_sync_at

    monkeypatch.setattr(
        home_service,
        "fetch_latest_successful_sync_at",
        _fake_latest_successful_sync_at,
    )

    response = await home_service.build_home_response(
        db_url="unused-on-cache-hit",
        filters=filters,
        cache=cache,
        org_id="org-1",
        semantic_session=semantic_session,
    )

    assert response.freshness.latest_successful_sync_at == latest_sync_at
    assert (
        response.freshness.last_ingested_at
        == cached_response.freshness.last_ingested_at
    )


@pytest.mark.asyncio
async def test_cached_home_response_survives_sync_freshness_query_failure(monkeypatch):
    filters = MetricFilter(
        time=TimeFilter(range_days=14, compare_days=14),
        scope=ScopeFilter(level="org", ids=[]),
    )
    cache = TTLCache(ttl_seconds=60)
    cached_sync_at = datetime(2026, 7, 13, 15, 5, tzinfo=UTC)
    cached_response = HomeResponse(
        freshness=Freshness(
            last_ingested_at=datetime(2026, 7, 12, 0, 7, tzinfo=UTC),
            latest_successful_sync_at=cached_sync_at,
            sources={},
            coverage=Coverage(
                repos_covered_pct=100,
                prs_linked_to_issues_pct=100,
                issues_with_cycle_states_pct=100,
            ),
        ),
        deltas=[],
        summary=[],
        tiles={},
        constraint=ConstraintCard(title="", claim="", evidence=[], experiments=[]),
        events=[],
    )
    cache.set(
        filter_cache_key("home", "org-1", filters),
        cached_response.model_dump(mode="json"),
    )

    async def _fail_latest_successful_sync_at(session, *, org_id):
        raise SQLAlchemyError

    monkeypatch.setattr(
        home_service,
        "fetch_latest_successful_sync_at",
        _fail_latest_successful_sync_at,
    )

    response = await home_service.build_home_response(
        db_url="unused-on-cache-hit",
        filters=filters,
        cache=cache,
        org_id="org-1",
        semantic_session=MagicMock(spec=AsyncSession),
    )

    assert response == cached_response
