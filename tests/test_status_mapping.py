from __future__ import annotations

from dev_health_ops.providers.status_mapping import load_status_mapping


def test_jira_status_mapping_defaults() -> None:
    mapping = load_status_mapping()
    assert (
        mapping.normalize_status(provider="jira", status_raw="In Progress", labels=[])
        == "in_progress"
    )
    assert (
        mapping.normalize_status(provider="jira", status_raw="Done", labels=[])
        == "done"
    )


def test_label_mapping_precedence_blocked_over_in_progress() -> None:
    mapping = load_status_mapping()
    status = mapping.normalize_status(
        provider="github",
        status_raw=None,
        labels=["in progress", "blocked"],
        state="open",
    )
    assert status == "blocked"


def test_state_fallback_when_no_labels() -> None:
    mapping = load_status_mapping()
    assert (
        mapping.normalize_status(
            provider="github", status_raw=None, labels=[], state="open"
        )
        == "todo"
    )
    assert (
        mapping.normalize_status(
            provider="github", status_raw=None, labels=[], state="closed"
        )
        == "done"
    )


def test_type_mapping_from_labels() -> None:
    mapping = load_status_mapping()
    assert (
        mapping.normalize_type(provider="github", type_raw=None, labels=["bug"])
        == "bug"
    )
