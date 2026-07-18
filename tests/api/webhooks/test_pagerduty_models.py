from __future__ import annotations

import pytest
from pydantic import ValidationError

from dev_health_ops.api.webhooks.pagerduty_models import PagerDutyV3Webhook


@pytest.mark.parametrize(
    "event_type",
    [
        "incident.responder.added",
        "incident.responder.replied",
        "incident.service_updated",
        "incident.status_update_published",
    ],
)
def test_webhook_accepts_pagerduty_v3_incident_event_names(event_type: str) -> None:
    webhook = PagerDutyV3Webhook.model_validate(
        {
            "event": {
                "id": "event-1",
                "event_type": event_type,
                "occurred_at": "2026-07-17T12:00:00Z",
                "data": {"id": "incident-1"},
            }
        }
    )

    assert webhook.event.event_type == event_type


def test_webhook_rejects_naive_occurred_at() -> None:
    with pytest.raises(ValidationError, match="timezone"):
        PagerDutyV3Webhook.model_validate(
            {
                "event": {
                    "id": "event-1",
                    "event_type": "incident.triggered",
                    "occurred_at": "2026-07-17T12:00:00",
                    "data": {"id": "incident-1"},
                }
            }
        )
