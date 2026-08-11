"""Live Python oracle for the legacy Jira work-item semantic row.

The first Jira port slice is deliberately pinned to the producer's normalized
WorkItem boundary.  This file calls the shipped ``jira_issue_to_work_item``
function and reflects the shipped WorkItem dataclass; it does not carry a
hand-authored expected row.  Sink projections and the five alias-family
destinations remain follow-up oracle pairs once a native Go producer exists.
"""

from __future__ import annotations

import dataclasses
import pathlib
import sys
from types import SimpleNamespace
from typing import Any

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.field_reflection import dataclass_field_names
from internal.providersync.testdata.python_oracle_loader import load_live_module

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_SOURCE = REPO_ROOT / "src/dev_health_ops/providers/jira/normalize.py"
_MODELS_SOURCE = REPO_ROOT / "src/dev_health_ops/models/work_items.py"
_STATUS_MAPPING_PATH = REPO_ROOT / "src/dev_health_ops/config/status_mapping.yaml"
_IDENTITY_MAPPING_PATH = REPO_ROOT / "src/dev_health_ops/config/identity_mapping.yaml"

jira_normalize = load_live_module(_SOURCE)
status_mapping_module = sys.modules["dev_health_ops.providers.status_mapping"]
identity_module = sys.modules["dev_health_ops.providers.identity"]


def _to_object(value: Any) -> Any:
    """Mirror the attribute-shaped Atlassian adapter object for a fixture."""
    if isinstance(value, dict):
        return SimpleNamespace(**{key: _to_object(item) for key, item in value.items()})
    if isinstance(value, list):
        return [_to_object(item) for item in value]
    return value


def _object_issue(case: dict[str, Any]) -> Any:
    """Build the object-shaped issue path used by Jira client adapters.

    The legacy JSON path and the adapter-object path are both production
    inputs.  Only user objects need attribute access; Jira fields remain a
    mapping because that is the shape returned by the REST client.
    """
    raw = dict(case["raw_issue"])
    fields = _to_object(dict(raw.get("fields") or {}))
    return SimpleNamespace(
        key=raw.get("key"),
        self=raw.get("self"),
        fields=fields,
        changelog=_to_object(raw.get("changelog")),
    )


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    issue = (
        _object_issue(case)
        if case.get("issue_shape") == "object"
        else case["raw_issue"]
    )
    status_mapping = status_mapping_module.load_status_mapping(
        path=_STATUS_MAPPING_PATH
    )
    identity = identity_module.load_identity_resolver(path=_IDENTITY_MAPPING_PATH)
    work_item, _ = jira_normalize.jira_issue_to_work_item(
        issue=issue,
        status_mapping=status_mapping,
        identity=identity,
        repo_id=None,
        story_points_field=case.get("story_points_field"),
        sprint_field=case.get("sprint_field"),
        epic_link_field=case.get("epic_link_field"),
    )
    if not work_item.work_item_id or not work_item.title:
        raise ValueError(
            "Jira work-item oracle fixture produced an empty semantic row: "
            f"id={work_item.work_item_id!r}, title={work_item.title!r}"
        )
    # The sync job stamps the tenant immediately before sink writes.  Keep the
    # provider's real normalization and that real job boundary explicit.
    return dataclasses.asdict(dataclasses.replace(work_item, org_id=case["org_id"]))


oracle_registry.register(
    oracle_registry.PairSpec(
        id="jira/work-items/issue",
        build_row=_build_row,
        reflected_fields=lambda: dataclass_field_names(
            _MODELS_SOURCE.read_text(), "WorkItem"
        ),
        excluded_fields={},
    )
)
