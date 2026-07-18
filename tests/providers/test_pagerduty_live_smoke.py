from __future__ import annotations

import os

import pytest

from dev_health_ops.providers.pagerduty.auth import ApiTokenAuth
from dev_health_ops.providers.pagerduty.client import PagerDutyClient

_LIVE_ENABLED = os.getenv("PAGERDUTY_LIVE_SMOKE") == "1"
_API_TOKEN = os.getenv("PAGERDUTY_API_TOKEN")

pytestmark = pytest.mark.skipif(
    not (_LIVE_ENABLED and _API_TOKEN),
    reason="requires PAGERDUTY_LIVE_SMOKE=1 and PAGERDUTY_API_TOKEN",
)


@pytest.mark.anyio
async def test_pagerduty_live_smoke_lists_services_read_only() -> None:
    assert _API_TOKEN is not None
    client = PagerDutyClient(
        ApiTokenAuth(_API_TOKEN), region=os.getenv("PAGERDUTY_REGION", "us")
    )
    services = await client.list_services()
    assert isinstance(services, list)
