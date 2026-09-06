from __future__ import annotations

from dev_health_ops.models.work_items import Sprint
from dev_health_ops.providers.normalize_common import (
    parse_jira_datetime as _parse_datetime,
)


# CHAOS-5329 (parent CHAOS-3092): every other function this module used to
# define (jira_issue_to_work_item, extract_jira_issue_dependencies,
# detect_reopen_events, jira_comment_to_interaction_event, the
# canonical_jira_issue_to_work_item/canonical_worklog_to_model/
# canonical_sprint_to_model/canonical_changelog_to_transitions cluster, and
# their private helpers) is deleted -- they had no live production caller
# (the Python Jira work-items/incidents ingestion path is retired;
# providersync's JiraAtlassianRouteHandler, cmd/dev-health-worker/
# provider_sync.go:394, is the only Jira ingestion writer now).
#
# jira_sprint_payload_to_model survives: workers/team_autoimport_jira.py
# (a SEPARATE, still-live concern -- Jira project/team catalog auto-import,
# not work-items sync) calls it directly to populate sprint rows during
# catalog population. Deleting it would break that live caller.
def jira_sprint_payload_to_model(payload: object) -> Sprint | None:
    if not isinstance(payload, dict):
        return None
    sprint_id = payload.get("id")
    if sprint_id is None:
        return None
    return Sprint(
        provider="jira",
        sprint_id=str(sprint_id),
        name=str(payload.get("name")) if payload.get("name") else None,
        state=str(payload.get("state")) if payload.get("state") else None,
        started_at=_parse_datetime(payload.get("startDate")),
        ended_at=_parse_datetime(payload.get("endDate")),
        completed_at=_parse_datetime(payload.get("completeDate")),
    )
