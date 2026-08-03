from __future__ import annotations

import pathlib
import sys
from dataclasses import asdict, dataclass, fields, replace
from datetime import datetime
from types import SimpleNamespace
from typing import Any
from uuid import UUID

from internal.providersync.testdata.python_oracle_loader import load_live_module

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_PROCESSOR_SOURCE = REPO_ROOT / "src/dev_health_ops/processors/gitlab.py"
PROCESSOR = load_live_module(_PROCESSOR_SOURCE)
OPERATIONAL = sys.modules["dev_health_ops.models.operational"]


class _RestClient:
    def __init__(self, pages: list[list[dict[str, Any]]]) -> None:
        self.pages = pages
        self.calls: list[dict[str, Any]] = []

    def get_issues(self, **kwargs: Any) -> list[dict[str, Any]]:
        self.calls.append(dict(kwargs))
        index = int(kwargs["page"]) - 1
        return self.pages[index] if index < len(self.pages) else []


@dataclass(frozen=True)
class TraversalTrace:
    calls: list[dict[str, object]]
    incident_external_ids: list[str]


def reflected(entity_type: type[Any]) -> frozenset[str]:
    return frozenset(field.name for field in fields(entity_type))


def _run(case: dict[str, Any]) -> tuple[_RestClient, Any]:
    rest_client = _RestClient(case["pages"])
    connector = SimpleNamespace(rest_client=rest_client)
    canonical_sources: list[Any] = []
    since = datetime.fromisoformat(case["since"].replace("Z", "+00:00"))
    until = datetime.fromisoformat(case["until"].replace("Z", "+00:00"))
    PROCESSOR._fetch_gitlab_incidents_sync(
        connector,
        case["project_id"],
        UUID(case["repo_id"]),
        case.get("max_issues", 1000),
        since,
        until,
        canonical_sources,
        case["org_id"],
        case["provider_instance_id"],
        case["repo_full_name"],
    )
    batch = PROCESSOR.map_issue_incidents(canonical_sources)
    return rest_client, batch


def build_family(case: dict[str, Any], family: str) -> dict[str, Any]:
    _, batch = _run(case)
    rows = {
        "service": batch.services,
        "mapping": batch.service_repository_mappings,
        "incident": batch.incidents,
    }[family]
    if not rows:
        raise ValueError(f"case produced no {family} row")
    observed_at = datetime.fromisoformat(case["normalized_at"].replace("Z", "+00:00"))
    # The production mapper intentionally lets each dataclass stamp its own
    # clock. Reconstructing the same frozen dataclass with the persisted Go
    # occurrence instant makes retry bytes comparable while still executing
    # the real producer, mapper, identity, conflict, and ordering functions.
    return asdict(replace(rows[0], observed_at=observed_at, last_synced=observed_at))


def build_traversal(case: dict[str, Any]) -> dict[str, Any]:
    rest_client, batch = _run(case)
    calls = [
        {
            "project_id": int(call["project_id"]),
            "issue_type": str(call["issue_type"]),
            "updated_after": str(call["updated_after"]),
            "updated_before": str(call["updated_before"]),
            "state": str(call["state"]),
            "page": int(call["page"]),
            "per_page": int(call["per_page"]),
            "order_by": str(call["order_by"]),
            "sort": str(call["sort"]),
        }
        for call in rest_client.calls
    ]
    return asdict(
        TraversalTrace(
            calls=calls,
            # Effect-row order is canonicalized by Go before hashing and is
            # not persisted semantics. Compare the complete deduplicated ID
            # inventory while keeping request/page order exact above.
            incident_external_ids=sorted(row.external_id for row in batch.incidents),
        )
    )
