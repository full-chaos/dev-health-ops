#!/usr/bin/env python3
"""Run the live Linear work-item normalizer for the first parity case.

This is deliberately a producer probe, not a second implementation of the
normalizer.  The fixture is shaped like the object returned by Linear's
GraphQL query, and the result is built by calling the checked-out
``linear_issue_to_work_item`` function.  A future Go pair can consume this
same case without hand-authoring an expected row.
"""

from __future__ import annotations

import json
import os
import sys
from dataclasses import asdict, is_dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

REPO_ROOT = Path(__file__).resolve().parents[3]
os.environ.setdefault("OTEL_ENABLED", "false")
sys.path.insert(0, str(REPO_ROOT / "src"))

from dev_health_ops.models.work_items import WorkItem  # noqa: E402
from dev_health_ops.providers.identity import IdentityResolver  # noqa: E402
from dev_health_ops.providers.linear.normalize import (  # noqa: E402
    linear_issue_to_work_item,
)
from dev_health_ops.providers.status_mapping import StatusMapping  # noqa: E402

ISSUE: dict[str, Any] = {
    "id": "lin-issue-42",
    "identifier": "ENG-42",
    "title": "Preserve the Linear work-item contract",
    "description": "A non-empty issue exercises the canonical normalizer.",
    "priority": 2,
    "estimate": 5,
    "createdAt": "2026-07-25T09:00:00Z",
    "updatedAt": "2026-07-28T16:30:00Z",
    "startedAt": "2026-07-26T10:00:00Z",
    "completedAt": None,
    "canceledAt": None,
    "dueDate": "2026-08-01T00:00:00Z",
    "url": "https://linear.app/fullchaos/issue/ENG-42",
    "state": {"name": "In Progress", "type": "started"},
    "labels": {"nodes": [{"name": "bug"}, {"name": "priority::high"}]},
    "assignee": {"email": "alice@example.com", "name": "Alice"},
    "creator": {"email": "bob@example.com", "name": "Bob"},
    "team": {"id": "team-eng", "key": "ENG", "name": "Engineering"},
    "project": {"id": "project-platform", "name": "Platform"},
    "cycle": {"id": "cycle-7", "name": "Sprint 7", "number": 7},
    "parent": {"identifier": "ENG-1"},
}

HISTORY: list[dict[str, Any]] = [
    {
        "createdAt": "2026-07-26T10:00:00Z",
        "fromState": {"name": "Todo", "type": "unstarted"},
        "toState": {"name": "In Progress", "type": "started"},
        "actor": {"email": "alice@example.com", "name": "Alice"},
    },
    {
        "createdAt": "2026-07-27T11:00:00Z",
        "fromState": {"name": "Done", "type": "completed"},
        "toState": {"name": "In Progress", "type": "started"},
        "actor": {"email": "bob@example.com", "name": "Bob"},
    },
]


def encode(value: Any) -> Any:
    """Encode every returned dataclass field without choosing a subset."""
    if is_dataclass(value) and not isinstance(value, type):
        return encode(asdict(value))
    if isinstance(value, datetime):
        instant = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return instant.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, (date, UUID)):
        return str(value)
    if isinstance(value, dict):
        return {str(key): encode(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [encode(item) for item in value]
    return value


def main() -> int:
    status_mapping = StatusMapping({}, {}, {}, {})
    identity = IdentityResolver({})
    item, transitions = linear_issue_to_work_item(
        issue=ISSUE,
        status_mapping=status_mapping,
        identity=identity,
        history=HISTORY,
    )
    if not isinstance(item, WorkItem) or not item.work_item_id:
        raise AssertionError("live Linear producer returned no work item")
    if not transitions:
        raise AssertionError("live Linear producer returned no history transitions")
    print(
        json.dumps(
            {
                "producer": "src/dev_health_ops/providers/linear/normalize.py::linear_issue_to_work_item",
                "work_item": encode(item),
                "work_item_fields": sorted(encode(item).keys()),
                "transitions": encode(transitions),
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
