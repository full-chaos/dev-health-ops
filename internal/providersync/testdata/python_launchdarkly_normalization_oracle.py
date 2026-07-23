#!/usr/bin/env python3
from __future__ import annotations

import ast
import json
import pathlib
import sys
from dataclasses import asdict, make_dataclass
from datetime import datetime, timezone
from typing import Any

FIXED_NOW = datetime(2026, 7, 23, 12, 34, 56, 789000, tzinfo=timezone.utc)


class FixedDateTime(datetime):
    @classmethod
    def now(cls, tz: Any = None) -> datetime:
        return FIXED_NOW if tz is not None else FIXED_NOW.replace(tzinfo=None)


FeatureFlagRecord = make_dataclass(
    "FeatureFlagRecord",
    [
        "provider",
        "flag_key",
        "project_key",
        "repo_id",
        "environment",
        "flag_type",
        "created_at",
        "archived_at",
        "last_synced",
        "org_id",
    ],
)
FeatureFlagEventRecord = make_dataclass(
    "FeatureFlagEventRecord",
    [
        "event_type",
        "flag_key",
        "environment",
        "repo_id",
        "actor_type",
        "prev_state",
        "next_state",
        "event_ts",
        "ingested_at",
        "source_event_id",
        "dedupe_key",
        "org_id",
    ],
)


def selected_module(source: pathlib.Path) -> dict[str, Any]:
    tree = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
    names = {
        "_EVENT_KIND_MAP",
        "_SOURCE",
        "_parse_iso",
        "normalize_flags",
        "normalize_audit_events",
    }
    selected: list[ast.stmt] = [
        ast.ImportFrom(module="__future__", names=[ast.alias("annotations")], level=0)
    ]
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name in names:
            selected.append(node)
        elif isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            if any(
                isinstance(target, ast.Name) and target.id in names
                for target in targets
            ):
                selected.append(node)
    namespace: dict[str, Any] = {
        "Any": Any,
        "datetime": FixedDateTime,
        "timezone": timezone,
        "FeatureFlagRecord": FeatureFlagRecord,
        "FeatureFlagEventRecord": FeatureFlagEventRecord,
        "logger": type("Logger", (), {"info": lambda *_args: None})(),
    }
    module = ast.fix_missing_locations(ast.Module(body=selected, type_ignores=[]))
    exec(compile(module, str(source), "exec"), namespace)
    return namespace


def encode(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat().replace("+00:00", "Z")
    if isinstance(value, dict):
        return {key: encode(item) for key, item in value.items()}
    if isinstance(value, list):
        return [encode(item) for item in value]
    return value


def main() -> int:
    if len(sys.argv) != 2:
        return 2
    namespace = selected_module(pathlib.Path(sys.argv[1]))
    flags = [
        {
            "key": "checkout",
            "_projectKey": "payments",
            "kind": "multivariate",
            "creationDate": 1_725_000_000_123,
        },
        {"key": "search"},
    ]
    events = [
        {
            "_id": "event-1",
            "kind": "toggleFlag",
            "date": 1_725_000_001_234,
            "member": {"email": "operator@example.test"},
            "target": {
                "resources": [
                    "proj/payments:env/prod:flag/checkout",
                ]
            },
        },
        {
            "_id": "event-2",
            "kind": "customKind",
            "name": "search",
            "date": "2026-07-22T01:02:03Z",
            "member": {"_id": "member-2"},
        },
    ]
    normalized_flags = namespace["normalize_flags"](flags, "org-acme")
    normalized_events = namespace["normalize_audit_events"](events, "org-acme")
    for record in normalized_flags:
        record.environment = "production"
    for record in normalized_events:
        if not record.environment:
            record.environment = "production"
    print(
        json.dumps(
            encode(
                {
                    "flags": [asdict(record) for record in normalized_flags],
                    "events": [asdict(record) for record in normalized_events],
                }
            ),
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
