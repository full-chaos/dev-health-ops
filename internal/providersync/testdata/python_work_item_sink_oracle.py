"""Capture what the REAL Python work-item sink would insert, without a database.

Note on one Python branch this oracle cannot exercise: write_work_item_transitions
falls back to ``datetime.now(timezone.utc)`` when ``occurred_at`` is falsy. That
branch is unreachable from Go, because githubWorkItemTransitionRow.validate
rejects a zero OccurredAt before any effect is built, so no Go-produced case can
reach it. It is therefore left uncompared deliberately rather than overlooked --
covering it would mean asserting behaviour the port can never produce.

Every direct work-item destination is written by
``metrics/sinks/clickhouse/work_graph.py`` (six of seven) or
``metrics/sinks/clickhouse/ai_attribution.py`` (ai_attribution), reached from the
composite unit in ``metrics/job_work_items.py``. Those sinks end in
``self.client.insert(table, matrix, column_names=...)``.

``ClickHouseMetricsSink.__init__`` accepts an injected client, so this oracle
constructs the production sink with a recording stub and calls the production
write method. The captured column list and value matrix are therefore produced
by the real writer -- not by re-reading its source, and not by a hand-authored
fixture that could agree with a stale reading of it.

The sink is reached through python_oracle_loader, not through a plain
``import dev_health_ops.metrics.sinks.clickhouse``. Both are the same
production module; the plain import additionally runs three package __init__
files on the way in, and one of them (``models/__init__`` ->
``licensing/__init__`` -> ``licensing/gating``) imports fastapi, which the
go-quality interpreter deliberately does not have. The loader executes the
same sources with those unrelated initializers skipped -- see
_target_clickhouse_metrics_sink. Nothing about the writer is stubbed.

Values are type-tagged on the way out (the same wire format
python_generic_row_oracle.py uses) so the Go comparison cannot collapse an int
to a float or a UUID to a string.

Usage: python_work_item_sink_oracle.py <cases.json>
"""

from __future__ import annotations

import json
import pathlib
import sys
from datetime import date, datetime, timezone
from typing import Any
from uuid import UUID

REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

from internal.providersync.testdata.python_oracle_loader import (  # noqa: E402
    ROOT as _LOADER_ROOT,
)
from internal.providersync.testdata.python_oracle_loader import (  # noqa: E402
    load_live_module,
)

_SINK_MODULE = load_live_module(
    _LOADER_ROOT
    / "src"
    / "dev_health_ops"
    / "metrics"
    / "sinks"
    / "clickhouse"
    / "__init__.py"
)
ClickHouseMetricsSink = _SINK_MODULE.ClickHouseMetricsSink
AIAttributionRecord = sys.modules[
    "dev_health_ops.models.ai_attribution"
].AIAttributionRecord
_WORK_ITEM_MODELS = sys.modules["dev_health_ops.models.work_items"]
Sprint = _WORK_ITEM_MODELS.Sprint
WorkItem = _WORK_ITEM_MODELS.WorkItem
WorkItemDependency = _WORK_ITEM_MODELS.WorkItemDependency
WorkItemInteractionEvent = _WORK_ITEM_MODELS.WorkItemInteractionEvent
WorkItemReopenEvent = _WORK_ITEM_MODELS.WorkItemReopenEvent
WorkItemStatusTransition = _WORK_ITEM_MODELS.WorkItemStatusTransition


class _RecordingClient:
    """Stub that records insert calls instead of performing them."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def insert(
        self, table: str, matrix: Any, column_names: Any = None, **_: Any
    ) -> None:
        self.calls.append(
            {
                "table": table,
                "column_names": list(column_names or []),
                "matrix": [list(row) for row in matrix],
            }
        )

    def close(self) -> None:  # pragma: no cover - never reached in this oracle
        pass


def _encode(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, bool):
        return {"t": "bool", "v": "true" if value else "false"}
    if isinstance(value, int):
        return {"t": "int", "v": str(value)}
    if isinstance(value, float):
        return {"t": "float", "v": repr(value)}
    if isinstance(value, str):
        return {"t": "str", "v": value}
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return {
            "t": "datetime",
            "v": value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
        }
    if isinstance(value, UUID):
        return {"t": "uuid", "v": str(value).lower()}
    if isinstance(value, date):
        return {"t": "date", "v": value.isoformat()}
    if isinstance(value, (list, tuple)):
        return [_encode(item) for item in value]
    if isinstance(value, dict):
        return {key: _encode(item) for key, item in value.items()}
    raise TypeError(f"unencodable value type {type(value)!r}")


def _parse_datetime(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    text = value.replace("Z", "+00:00")
    return datetime.fromisoformat(text)


_DATETIME_FIELDS = {
    "created_at",
    "updated_at",
    "started_at",
    "completed_at",
    "closed_at",
    "due_at",
    "occurred_at",
    "ended_at",
    "last_synced",
    "observed_at",
    "ingested_at",
}
_UUID_FIELDS = {"repo_id", "org_id", "source_id", "record_id", "superseded_by"}

# The case file crosses the language boundary as JSON, and JSON has one number
# type. Go's json.Marshal writes a float64 of 100000.0 as ``100000``, which
# json.load then reads back as a Python *int* -- so an integral float arrives
# here with its type quietly collapsed, and the sink (which passes story_points
# through untouched) would insert an int where production inserts a float.
#
# Restoring the declared type here keeps the comparison about the production
# code rather than about the transport. It is the same int/float collapse the
# comparison itself is careful to avoid on the way out.
_FLOAT_FIELDS = {"story_points", "confidence"}


def _coerce(model: Any, row: dict[str, Any], uuid_fields: set[str]) -> Any:
    import dataclasses

    names = {field.name for field in dataclasses.fields(model)}
    kwargs: dict[str, Any] = {}
    for key, value in row.items():
        if key not in names:
            # The Go row carries fields the Python dataclass does not have
            # (notably last_synced on WorkItem/WorkItemStatusTransition, whose
            # sinks stamp it from wall-clock). Dropping them here is what makes
            # this a comparison of the PYTHON writer's own output.
            continue
        if value is not None and key in _DATETIME_FIELDS:
            value = _parse_datetime(value)
        if value is not None and key in uuid_fields:
            value = UUID(str(value))
        if value is not None and key in _FLOAT_FIELDS:
            value = float(value)
        kwargs[key] = value
    return model(**kwargs)


_MODELS = {
    "work_items": (WorkItem, "write_work_items", {"repo_id", "source_id"}),
    "work_item_transitions": (
        WorkItemStatusTransition,
        "write_work_item_transitions",
        {"repo_id", "source_id"},
    ),
    "work_item_dependencies": (
        WorkItemDependency,
        "write_work_item_dependencies",
        {"source_id"},
    ),
    "work_item_reopen_events": (
        WorkItemReopenEvent,
        "write_work_item_reopen_events",
        set(),
    ),
    "work_item_interactions": (
        WorkItemInteractionEvent,
        "write_work_item_interactions",
        set(),
    ),
    "sprints": (Sprint, "write_sprints", set()),
}


def main() -> int:
    with open(sys.argv[1], encoding="utf-8") as handle:
        cases = json.load(handle)

    results = []
    for case in cases:
        destination = case["destination"]
        client = _RecordingClient()
        sink = ClickHouseMetricsSink("clickhouse://stub/stub", client=client)
        sink.org_id = case.get("org_id") or ""

        payload = case["rows"] if "rows" in case else [case["row"]]
        if destination == "ai_attribution":
            records = [_coerce(AIAttributionRecord, r, _UUID_FIELDS) for r in payload]
            sink.write_ai_attribution(records)
        else:
            model, method, uuid_fields = _MODELS[destination]
            rows = [_coerce(model, r, uuid_fields) for r in payload]
            getattr(sink, method)(rows)

        if len(client.calls) != 1:
            raise SystemExit(
                f"case {case['id']}: the production sink made {len(client.calls)} "
                "insert calls; this oracle can only speak for exactly one"
            )
        call = client.calls[0]
        results.append(
            {
                "id": case["id"],
                "table": call["table"],
                "column_names": call["column_names"],
                # Every row, not just the first: a multi-row batch is the shape
                # production actually writes, and a projection that is correct
                # for one row can still mis-handle the second.
                "rows": [[_encode(value) for value in row] for row in call["matrix"]],
            }
        )

    json.dump({"cases": results}, sys.stdout)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
