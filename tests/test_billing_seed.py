"""Tests for billing plan seeding (dev-hops admin billing seed)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI

from dev_health_ops.api.billing.plans import router as billing_router


def _build_app() -> FastAPI:
    app = FastAPI()
    app.include_router(billing_router)
    return app


@pytest.mark.asyncio
async def test_seed_billing_plans_creates_three_plans():
    """The seed function creates Community, Team, and Enterprise plans."""
    from dev_health_ops.api.admin.cli import _seed_billing_plans_async

    mock_session = AsyncMock()
    # Simulate empty database (no existing plans)
    mock_result = MagicMock()
    mock_result.all.return_value = []
    mock_session.execute.return_value = mock_result

    ns = MagicMock()

    with patch("dev_health_ops.api.admin.cli._get_session", return_value=mock_session):
        result = await _seed_billing_plans_async(ns)

    assert result == 0
    # 3 plans + 6 prices (2 per plan) = 9 session.add calls
    assert mock_session.add.call_count == 9
    mock_session.commit.assert_awaited_once()
    mock_session.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_seed_billing_plans_skips_existing():
    """The seed function skips plans that already exist."""
    from dev_health_ops.api.admin.cli import _seed_billing_plans_async

    mock_session = AsyncMock()
    # Simulate all three plans already exist
    mock_result = MagicMock()
    mock_result.all.return_value = [("community",), ("team",), ("enterprise",)]
    mock_session.execute.return_value = mock_result

    ns = MagicMock()

    with patch("dev_health_ops.api.admin.cli._get_session", return_value=mock_session):
        result = await _seed_billing_plans_async(ns)

    assert result == 0
    mock_session.add.assert_not_called()
    mock_session.commit.assert_not_awaited()
    mock_session.close.assert_awaited_once()
