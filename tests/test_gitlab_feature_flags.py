from __future__ import annotations

from dev_health_ops.processors.gitlab_feature_flags import (
    normalize_gitlab_feature_flags,
    snapshot_gitlab_feature_flag_events,
)


def test_normalize_gitlab_feature_flags_emits_one_record_per_environment_scope():
    flags = [
        {
            "name": "awesome_feature",
            "active": True,
            "version": "new_version_flag",
            "created_at": "2026-04-10T12:00:00Z",
            "updated_at": "2026-04-15T12:00:00Z",
            "strategies": [
                {"scopes": [{"environment_scope": "production"}]},
                {"scopes": [{"environment_scope": "staging"}]},
            ],
        }
    ]

    records = normalize_gitlab_feature_flags(
        flags,
        project_key="group/project",
        org_id="org-1",
    )

    assert len(records) == 2
    assert {record.environment for record in records} == {"production", "staging"}
    assert {record.flag_key for record in records} == {"awesome_feature"}
    assert all(record.provider == "gitlab" for record in records)
    assert all(record.project_key == "group/project" for record in records)


def test_snapshot_gitlab_feature_flag_events_tracks_current_toggle_state():
    flags = [
        {
            "name": "awesome_feature",
            "active": False,
            "updated_at": "2026-04-15T14:30:00Z",
            "strategies": [
                {"scopes": [{"environment_scope": "production"}]},
                {"scopes": [{"environment_scope": "*"}]},
            ],
        }
    ]

    events = snapshot_gitlab_feature_flag_events(
        flags,
        project_key="group/project",
        org_id="org-1",
    )

    assert len(events) == 2
    assert {event.environment for event in events} == {"production", "*"}
    assert all(event.event_type == "toggle" for event in events)
    assert all(event.next_state == "off" for event in events)
    assert all(
        event.dedupe_key.startswith("gitlab:group/project:awesome_feature")
        for event in events
    )
