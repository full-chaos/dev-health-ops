"""Identity resolution contract for Coverage-by-Repository breakdown (CHAOS-2089).

Govern Coverage 'Coverage by Repository' bars are fed by the GraphQL analytics
breakdown. These tests lock in that ``BreakdownItem.label`` carries a resolved
display name for repo/team dimensions and never surfaces a bare UUID as the
primary label (Framework A7/A8).
"""

from __future__ import annotations

from dev_health_ops.api.graphql.resolvers.analytics import _build_breakdown_item

_REPO_UUID = "4e00fff2-df66-5028-8ebd-e4535332300b"


def test_breakdown_item_uses_resolved_label() -> None:
    item = _build_breakdown_item(
        _REPO_UUID, 42.0, {_REPO_UUID: "meridian/billing-service"}
    )
    assert item.key == _REPO_UUID  # stable id preserved
    assert item.label == "meridian/billing-service"


def test_breakdown_item_unresolved_uuid_falls_back_to_short_token() -> None:
    item = _build_breakdown_item(_REPO_UUID, 10.0, {})
    # Primary label must never be the raw UUID (A8).
    assert "4e00fff2-df66" not in (item.label or "")
    assert item.label == "#4e00fff2"


def test_breakdown_item_human_key_passes_through() -> None:
    # Investment-path repo slugs / theme names are already human text.
    item = _build_breakdown_item("acme/web", 7.0, {})
    assert item.label == "acme/web"


def test_breakdown_item_unassigned_passes_through() -> None:
    item = _build_breakdown_item("unassigned", 3.0, {})
    assert item.label == "unassigned"
