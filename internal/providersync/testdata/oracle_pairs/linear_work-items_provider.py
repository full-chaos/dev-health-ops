"""Execute the real LinearProvider.iter_ingest producer for parity cases."""

from __future__ import annotations

import dataclasses
import pathlib
from datetime import datetime, timezone
from typing import Any

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.field_reflection import dataclass_field_names
from internal.providersync.testdata.python_oracle_loader import load_live_module

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_MODELS_SOURCE = REPO_ROOT / "src/dev_health_ops/models/work_items.py"
_PROVIDER_SOURCE = REPO_ROOT / "src/dev_health_ops/providers/linear/provider.py"


class _InjectedLinearClient:
    def __init__(self, issue: dict[str, Any], team_key: str) -> None:
        self._issue = issue
        self._team_key = team_key

    def get_team_by_key(self, team_key: str) -> dict[str, Any] | None:
        if team_key != self._team_key:
            return None
        return {"id": "team-" + team_key.lower(), "key": team_key, "name": team_key}

    def iter_cycles(self, *, team_id: str | None = None) -> list[dict[str, Any]]:
        return []

    def iter_issues_pages(self, **_kwargs: Any) -> list[list[dict[str, Any]]]:
        return [[self._issue]]

    def iter_issues(self, **_kwargs: Any) -> list[dict[str, Any]]:
        return []


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    provider_module = load_live_module(_PROVIDER_SOURCE)
    from dev_health_ops.providers.base import IngestionContext, IngestionWindow
    from dev_health_ops.providers.identity import IdentityResolver
    from dev_health_ops.providers.status_mapping import StatusMapping

    team_key = str(case.get("team_key", "ENG"))
    issue = dict(case["raw_issue"])
    issue["history"] = {"nodes": list(case.get("history", []))}
    issue.setdefault("attachments", {"nodes": [], "pageInfo": {"hasNextPage": False}})
    issue.setdefault("relations", {"nodes": [], "pageInfo": {"hasNextPage": False}})
    issue.setdefault(
        "inverseRelations", {"nodes": [], "pageInfo": {"hasNextPage": False}}
    )
    issue.setdefault("comments", {"nodes": [], "pageInfo": {"hasNextPage": False}})

    provider = provider_module.LinearProvider(
        status_mapping=StatusMapping({}, {}, {}, {}),
        identity=IdentityResolver({}),
        client=_InjectedLinearClient(issue, team_key),
    )
    context = IngestionContext(
        window=IngestionWindow(
            updated_since=datetime(2026, 7, 1, tzinfo=timezone.utc),
            active_until=datetime(2026, 7, 31, 23, 59, 59, tzinfo=timezone.utc),
        ),
        repo=team_key,
    )
    batches = list(provider.iter_ingest(context))
    work_items = [item for batch in batches for item in batch.work_items]
    work_item = work_items[0] if len(work_items) == 1 else None
    if (
        work_item is None
        or not dataclasses.is_dataclass(work_item)
        or isinstance(work_item, type)
    ):
        raise AssertionError(
            f"real LinearProvider returned {len(work_items)} work items of "
            f"type {type(work_items[0]) if work_items else None!r}"
        )
    return dataclasses.asdict(dataclasses.replace(work_item, org_id=case["org_id"]))


oracle_registry.register(
    oracle_registry.PairSpec(
        id="linear/work-items/provider",
        build_row=_build_row,
        reflected_fields=lambda: dataclass_field_names(
            _MODELS_SOURCE.read_text(), "WorkItem"
        ),
    )
)
