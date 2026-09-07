"""Capture the Python webhook path's ClickHouse rows as frozen goldens.

CHAOS-4105 provenance artifact. This script was run ONCE, against the live
Python reconciler (src/dev_health_ops/providers/pagerduty/webhooks.py
reconcile_pagerduty_webhook), BEFORE that reconciler was deleted, and its
output is the *.json in this directory. The Go native reconciler
(internal/providersync/pagerduty_webhook_reconcile.go) is asserted against
those files by pagerduty_webhook_reconcile_golden_test.go.

It is kept -- and kept runnable in shape -- so a reviewer can see exactly
which Python function produced the goldens and with which inputs. It cannot
be re-run after the deletion commits; that is the point. Regenerating it
would require reverting the deletion, which is what the frozen files exist
to make unnecessary.

Run (pre-deletion only):
    python internal/providersync/testdata/pagerduty_webhook_goldens/capture.py
"""

from __future__ import annotations

import asyncio
import json
import pathlib
import sys
from dataclasses import asdict, fields
from datetime import UTC, datetime
from typing import Any

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO_ROOT / "src"))

from dev_health_ops.providers.pagerduty.webhooks import (  # noqa: E402
    reconcile_pagerduty_webhook,
)

from dev_health_ops.api.webhooks.pagerduty_models import (  # noqa: E402
    PagerDutyV3Webhook,
)
from dev_health_ops.models.operational import (  # noqa: E402
    OPERATIONAL_ENTITY_TABLES,
    CanonicalOperationalEntity,
)
from dev_health_ops.models.operational_ordering_types import (  # noqa: E402
    ORDERING_FIELD_NAMES,
)
from dev_health_ops.providers.pagerduty.models import Incident  # noqa: E402

ORG_ID = "org-4105"
PROVIDER_INSTANCE_ID = "acme"
OCCURRED_AT = datetime(2026, 7, 17, 12, 0, 0, tzinfo=UTC)
RECEIVED_AT = datetime(2026, 7, 17, 12, 0, 5, tzinfo=UTC)


class RecordingStore:
    """Records every insert in call order, as the Protocol's 8 methods."""

    def __init__(self) -> None:
        self.written: list[CanonicalOperationalEntity] = []

    async def _record(self, values: list[Any]) -> None:
        self.written.extend(values)

    insert_operational_services = _record
    insert_operational_incidents = _record
    insert_operational_alerts = _record
    insert_operational_incident_timeline_events = _record
    insert_operational_incident_notes = _record
    insert_operational_incident_responders = _record
    insert_operational_users = _record
    insert_operational_teams = _record


class HydratingClient:
    """Stands in for PagerDutyClient.get_incident.

    Only the deliberately-sparse hydration fixture reaches it; every other
    case must never call it, which is asserted rather than assumed.
    """

    def __init__(self) -> None:
        self.calls: list[str] = []

    async def get_incident(self, incident_id: str) -> Incident:
        self.calls.append(incident_id)
        return Incident.model_validate(
            {
                "id": incident_id,
                "title": "Hydrated from the REST API",
                "status": "triggered",
                "created_at": "2026-07-17T11:59:00Z",
                "html_url": "https://acme.pagerduty.com/incidents/" + incident_id,
            }
        )


def _incident_data(**overrides: Any) -> dict[str, Any]:
    data: dict[str, Any] = {
        "id": "PINC1",
        "type": "incident",
        "title": "Payments unavailable",
        "status": "triggered",
        "urgency": "high",
        "created_at": "2026-07-17T11:58:00Z",
        "html_url": "https://acme.pagerduty.com/incidents/PINC1",
        "service": {"id": "PSVC1", "summary": "payments-api"},
        "priority": {"id": "PPRI1", "summary": "P1"},
    }
    data.update(overrides)
    return data


def _service_data(**overrides: Any) -> dict[str, Any]:
    data: dict[str, Any] = {
        "id": "PSVC1",
        "type": "service",
        "name": "payments-api",
        "description": "Payments public API",
        "html_url": "https://acme.pagerduty.com/services/PSVC1",
        "created_at": "2026-01-04T09:00:00Z",
    }
    data.update(overrides)
    return data


def _webhook(
    event_type: str, data: dict[str, Any], event_id: str
) -> PagerDutyV3Webhook:
    return PagerDutyV3Webhook.model_validate(
        {
            "event": {
                "id": event_id,
                "event_type": event_type,
                "occurred_at": OCCURRED_AT.isoformat(),
                "data": data,
            }
        }
    )


# One case per PagerDutyEventType member, plus the two behaviours that are
# not their own event type: the sparse-payload hydration fallback and the
# service tombstone. Names are the golden file names.
CASES: list[tuple[str, str, dict[str, Any]]] = [
    ("incident_triggered", "incident.triggered", _incident_data()),
    (
        "incident_acknowledged",
        "incident.acknowledged",
        _incident_data(status="acknowledged"),
    ),
    (
        "incident_unacknowledged",
        "incident.unacknowledged",
        _incident_data(status="triggered"),
    ),
    ("incident_escalated", "incident.escalated", _incident_data()),
    ("incident_reassigned", "incident.reassigned", _incident_data()),
    ("incident_delegated", "incident.delegated", _incident_data()),
    (
        "incident_priority_updated",
        "incident.priority_updated",
        _incident_data(priority={"id": "PPRI2", "summary": "P2"}),
    ),
    (
        "incident_resolved",
        "incident.resolved",
        _incident_data(
            status="resolved",
            resolved_at="2026-07-17T12:00:00Z",
            last_status_change_at="2026-07-17T12:00:00Z",
        ),
    ),
    ("incident_reopened", "incident.reopened", _incident_data(status="triggered")),
    (
        "incident_annotated",
        "incident.annotated",
        {
            "id": "PNOTE1",
            "type": "incident_note",
            "content": "Rolling back the deploy",
            "created_at": "2026-07-17T12:00:01Z",
            "incident": _incident_data(),
        },
    ),
    (
        "responder_added",
        "incident.responder.added",
        {
            "id": "PRESP1",
            "type": "incident_responder",
            "name": "Ada Lovelace",
            "role": "responder",
            "user": {
                "id": "PUSER1",
                "type": "user",
                "name": "Ada Lovelace",
                "email": "ada@example.com",
                "html_url": "https://acme.pagerduty.com/users/PUSER1",
            },
            "incident": _incident_data(),
        },
    ),
    (
        "responder_replied",
        "incident.responder.replied",
        {
            "id": "PRESP2",
            "type": "incident_responder",
            "name": "Grace Hopper",
            "role": "observer",
            "user": {
                "id": "PUSER2",
                "type": "user",
                "name": "Grace Hopper",
                "email": "grace@example.com",
                "html_url": "https://acme.pagerduty.com/users/PUSER2",
            },
            "incident": _incident_data(),
        },
    ),
    (
        "status_update_published",
        "incident.status_update_published",
        {
            "id": "PLOG1",
            "type": "status_update",
            "message": "We are investigating",
            "created_at": "2026-07-17T12:00:02Z",
            "incident": _incident_data(),
        },
    ),
    (
        "incident_service_updated",
        "incident.service_updated",
        _incident_data(service={"id": "PSVC2", "summary": "payments-api-v2"}),
    ),
    ("service_created", "service.created", _service_data()),
    (
        "service_updated",
        "service.updated",
        _service_data(description="Payments public API (v2)"),
    ),
    ("service_deleted", "service.deleted", _service_data()),
    # Sparse incident payload: title/status/created_at absent, so
    # _needs_incident_hydration fires and the REST client fills them in.
    (
        "incident_triggered_hydrated",
        "incident.triggered",
        {"id": "PINC9", "type": "incident"},
    ),
]


def _contract_one_columns(entity: CanonicalOperationalEntity) -> list[str]:
    """The deployed OPERATIONAL_ORDERING_CONTRACT=1 column list.

    Mirrors ClickHouseStore._operational_columns_for_contract: dataclass
    declaration order minus the four ordering columns, which do not exist in
    the deployed table until CH migration 067.
    """
    return [
        item.name for item in fields(entity) if item.name not in ORDERING_FIELD_NAMES
    ]


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        # ClickHouseStore._normalize_operational_datetime coerces to UTC.
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
    return value


async def _capture_one(
    name: str, event_type: str, data: dict[str, Any]
) -> dict[str, Any]:
    store = RecordingStore()
    client = HydratingClient()
    event_id = "evt-" + name
    await reconcile_pagerduty_webhook(
        webhook=_webhook(event_type, data, event_id),
        org_id=ORG_ID,
        provider_instance_id=PROVIDER_INSTANCE_ID,
        received_at=RECEIVED_AT,
        store=store,
        client=client,
    )
    expect_hydration = name.endswith("_hydrated")
    if bool(client.calls) is not expect_hydration:
        raise RuntimeError(
            f"{name}: hydration calls={client.calls!r}, expected={expect_hydration}"
        )
    rows = []
    for entity in store.written:
        row = asdict(entity)
        rows.append(
            {
                "table": OPERATIONAL_ENTITY_TABLES[type(entity)],
                "columns": {
                    column: _jsonable(row[column])
                    for column in _contract_one_columns(entity)
                },
            }
        )
    return {
        "case": name,
        "event_type": event_type,
        "event_id": event_id,
        "org_id": ORG_ID,
        "provider_instance_id": PROVIDER_INSTANCE_ID,
        "occurred_at": OCCURRED_AT.isoformat().replace("+00:00", "Z"),
        "received_at": RECEIVED_AT.isoformat().replace("+00:00", "Z"),
        "hydrated_via_rest": bool(client.calls),
        "payload": {
            "event": {
                "id": event_id,
                "event_type": event_type,
                "occurred_at": OCCURRED_AT.isoformat().replace("+00:00", "Z"),
                "data": data,
            }
        },
        "rows": rows,
    }


async def main() -> None:
    directory = pathlib.Path(__file__).resolve().parent
    for name, event_type, data in CASES:
        captured = await _capture_one(name, event_type, data)
        target = directory / f"{name}.json"
        target.write_text(
            json.dumps(captured, indent=2, sort_keys=False, ensure_ascii=True) + "\n"
        )
        print(f"{target.name}: {len(captured['rows'])} row(s)")


if __name__ == "__main__":
    asyncio.run(main())
