"""Canonical operational incident evidence edges."""

from __future__ import annotations

import re
import uuid
from datetime import datetime, timedelta

from dev_health_ops.work_graph.ids import generate_edge_id, generate_pr_id
from dev_health_ops.work_graph.models import (
    EdgeType,
    NodeType,
    Provenance,
    WorkGraphEdge,
)

_JIRA_KEY = re.compile(r"\b([A-Z][A-Z0-9]+-\d+)\b")
_GITHUB_PR_URL = re.compile(r"github\.com/([^/]+)/([^/]+)/pull/(\d+)")


def build_operational_incident_edges(
    sink: object,
    org_id: str,
    now: datetime,
    heuristic_days_window: int,
    heuristic_confidence: float,
    from_date: datetime | None = None,
    to_date: datetime | None = None,
    repo_id: uuid.UUID | None = None,
) -> list[WorkGraphEdge]:
    """Build canonical operational edges with source evidence on every link."""
    query_dicts = getattr(sink, "query_dicts")
    params = {
        "org_id": org_id,
        "now": now,
        "from_date": from_date,
        "to_date": to_date,
        "repo_id": repo_id,
    }
    repository_scope = " AND repo_id = {repo_id:UUID}" if repo_id else ""
    incident_window = _timestamp_window("started_at", from_date, to_date)
    deployment_window = _timestamp_window("deployed_at", from_date, to_date)
    mappings = query_dicts(
        "SELECT service_id, repo_id, provider, relationship_provenance, relationship_confidence, "
        "mapping_kind, rule_id, source_url FROM operational_service_repository_mappings FINAL "
        "WHERE org_id = {org_id:String} AND is_active = 1 "
        "AND valid_from <= {now:DateTime} "
        "AND (valid_to IS NULL OR valid_to > {now:DateTime})"
        f"{repository_scope}",
        params,
    )
    incidents = query_dicts(
        "SELECT id, service_id, escalation_policy_id, started_at, source_url FROM operational_incidents FINAL "
        "WHERE org_id = {org_id:String} AND is_deleted = 0"
        f"{incident_window}",
        params,
    )
    services = query_dicts(
        "SELECT id, owning_team_id, escalation_policy_id FROM operational_services FINAL "
        "WHERE org_id = {org_id:String} AND is_deleted = 0",
        params,
    )
    alerts = query_dicts(
        "SELECT id, incident_id, source_url, triggered_at FROM operational_alerts FINAL "
        "WHERE org_id = {org_id:String} AND is_deleted = 0",
        params,
    )
    timeline = query_dicts(
        "SELECT id, incident_id, actor_id, body, source_url, occurred_at FROM operational_incident_timeline_events FINAL "
        "WHERE org_id = {org_id:String}",
        params,
    )
    notes = query_dicts(
        "SELECT id, incident_id, body, author_user_id, source_url, created_at FROM operational_incident_notes FINAL "
        "WHERE org_id = {org_id:String}",
        params,
    )
    responders = query_dicts(
        "SELECT id, incident_id, user_id, source_url, assigned_at FROM operational_incident_responders FINAL "
        "WHERE org_id = {org_id:String}",
        params,
    )
    work_items = query_dicts(
        "SELECT work_item_id FROM work_items FINAL WHERE org_id = {org_id:String}",
        params,
    )
    repos = query_dicts(
        "SELECT id, repo FROM repos FINAL WHERE org_id = {org_id:String}", params
    )
    deployments = query_dicts(
        "SELECT repo_id, deployment_id, environment, deployed_at FROM deployments FINAL "
        "WHERE org_id = {org_id:String}"
        f"{repository_scope}{deployment_window}",
        params,
    )

    edges: list[WorkGraphEdge] = []
    preferred_mappings: dict[tuple[str, uuid.UUID], dict[str, object]] = {}
    for row in mappings:
        mapping_repo_id = row.get("repo_id")
        service_id = str(row.get("service_id") or "")
        if not mapping_repo_id or not service_id:
            continue
        repo_uuid = uuid.UUID(str(mapping_repo_id))
        key = (service_id, repo_uuid)
        current = preferred_mappings.get(key)
        if current is None or _mapping_confidence(row) > _mapping_confidence(current):
            preferred_mappings[key] = row

    service_repos: dict[str, list[uuid.UUID]] = {}
    for (service_id, repo_uuid), row in preferred_mappings.items():
        service_repos.setdefault(service_id, []).append(repo_uuid)
        confidence = _mapping_confidence(row)
        evidence = ":".join(
            str(row.get(key) or "")
            for key in (
                "relationship_provenance",
                "mapping_kind",
                "rule_id",
                "source_url",
            )
        )
        edges.append(
            _edge(
                NodeType.OPERATIONAL_SERVICE,
                service_id,
                EdgeType.MAPS_TO_REPOSITORY,
                NodeType.REPOSITORY,
                str(repo_uuid),
                _mapping_provenance(str(row.get("relationship_provenance") or "")),
                confidence,
                evidence,
                repo_uuid,
                now,
                provider=str(row.get("provider") or "pagerduty"),
            )
        )

    service_teams = {
        str(row.get("id")): str(row.get("owning_team_id"))
        for row in services
        if row.get("owning_team_id")
    }
    service_policies = {
        str(row.get("id")): str(row.get("escalation_policy_id"))
        for row in services
        if row.get("escalation_policy_id")
    }
    incident_by_id = {
        str(row.get("id")): row
        for row in incidents
        if repo_id is None or str(row.get("service_id") or "") in service_repos
    }
    for incident_id, row in incident_by_id.items():
        service_id = str(row.get("service_id") or "")
        event_at = row.get("started_at") or now
        if service_id:
            edges.append(
                _edge(
                    NodeType.OPERATIONAL_SERVICE,
                    service_id,
                    EdgeType.HAS_INCIDENT,
                    NodeType.INCIDENT,
                    incident_id,
                    Provenance.NATIVE,
                    1.0,
                    str(row.get("source_url") or "operational_incident.service_id"),
                    None,
                    event_at,
                )
            )
        policy_id = str(
            row.get("escalation_policy_id") or service_policies.get(service_id) or ""
        )
        if policy_id:
            edges.append(
                _edge(
                    NodeType.INCIDENT,
                    incident_id,
                    EdgeType.ESCALATES_WITH,
                    NodeType.ESCALATION_POLICY,
                    policy_id,
                    Provenance.NATIVE,
                    1.0,
                    "operational_incident.escalation_policy_id",
                    None,
                    event_at,
                )
            )
        team_id = service_teams.get(service_id)
        if team_id:
            edges.append(
                _edge(
                    NodeType.INCIDENT,
                    incident_id,
                    EdgeType.ASSIGNED_TO,
                    NodeType.TEAM,
                    team_id,
                    Provenance.NATIVE,
                    1.0,
                    "operational_service.owning_team_id",
                    None,
                    event_at,
                )
            )
        for mapped_repo_id in service_repos.get(service_id, []):
            for deployment in deployments:
                if str(deployment.get("repo_id")) != str(mapped_repo_id):
                    continue
                deployed_at = deployment.get("deployed_at")
                incident_started_at = row.get("started_at")
                environment = str(deployment.get("environment") or "").casefold()
                if (
                    not isinstance(deployed_at, datetime)
                    or not isinstance(incident_started_at, datetime)
                    or environment in {"", "unknown", "unspecified"}
                ):
                    continue
                if (
                    deployed_at > incident_started_at
                    or incident_started_at - deployed_at
                    > timedelta(days=heuristic_days_window)
                ):
                    continue
                evidence = f"rule:operational_service_mapped_deployment_window.v1;environment:{environment}"
                edges.append(
                    _edge(
                        NodeType.DEPLOYMENT,
                        str(deployment.get("deployment_id")),
                        EdgeType.LINKED_INCIDENT,
                        NodeType.INCIDENT,
                        incident_id,
                        Provenance.HEURISTIC,
                        heuristic_confidence,
                        evidence,
                        mapped_repo_id,
                        deployed_at,
                    )
                )

    for row in alerts:
        if str(row.get("incident_id") or "") not in incident_by_id:
            continue
        _append_direct(
            edges,
            row,
            "incident_id",
            NodeType.OPERATIONAL_ALERT,
            EdgeType.HAS_ALERT,
            now,
        )
    for row in timeline:
        if str(row.get("incident_id") or "") not in incident_by_id:
            continue
        _append_direct(
            edges,
            row,
            "incident_id",
            NodeType.INCIDENT_TIMELINE_EVENT,
            EdgeType.HAS_TIMELINE_EVENT,
            now,
        )
        _append_user(edges, row, "actor_id", now)
    for row in responders:
        if str(row.get("incident_id") or "") not in incident_by_id:
            continue
        _append_direct(
            edges,
            row,
            "incident_id",
            NodeType.INCIDENT_RESPONDER,
            EdgeType.HAS_RESPONDER,
            now,
        )
        _append_user(edges, row, "user_id", now)
    known_work_items = {str(row.get("work_item_id")) for row in work_items}
    repo_ids = {
        str(row.get("repo")): uuid.UUID(str(row.get("id")))
        for row in repos
        if row.get("id")
    }
    for row in [*timeline, *notes]:
        incident_id = str(row.get("incident_id") or "")
        if incident_id not in incident_by_id:
            continue
        body = str(row.get("body") or "")
        event_at = row.get("occurred_at") or row.get("created_at") or now
        for key in _JIRA_KEY.findall(body):
            work_item_id = f"jira:{key}"
            if work_item_id in known_work_items:
                edge_type = (
                    EdgeType.REMEDIATED_BY
                    if "remediat" in body.casefold()
                    else EdgeType.REFERENCES
                )
                edges.append(
                    _edge(
                        NodeType.INCIDENT,
                        incident_id,
                        edge_type,
                        NodeType.ISSUE,
                        work_item_id,
                        Provenance.EXPLICIT_TEXT,
                        0.9,
                        f"incident_evidence:{key}",
                        None,
                        event_at,
                    )
                )
        for owner, repo, number in _GITHUB_PR_URL.findall(body):
            repo_id = repo_ids.get(f"{owner}/{repo}")
            if repo_id is not None:
                edges.append(
                    _edge(
                        NodeType.INCIDENT,
                        incident_id,
                        EdgeType.REFERENCES,
                        NodeType.PR,
                        generate_pr_id(repo_id, int(number)),
                        Provenance.EXPLICIT_TEXT,
                        0.9,
                        f"incident_evidence:https://github.com/{owner}/{repo}/pull/{number}",
                        repo_id,
                        event_at,
                    )
                )
    return edges


def _append_direct(
    edges: list[WorkGraphEdge],
    row: dict[str, object],
    incident_key: str,
    target_type: NodeType,
    edge_type: EdgeType,
    now: datetime,
) -> None:
    incident_id, target_id = str(row.get(incident_key) or ""), str(row.get("id") or "")
    if incident_id and target_id:
        candidate = (
            row.get("occurred_at") or row.get("triggered_at") or row.get("assigned_at")
        )
        event_ts = candidate if isinstance(candidate, datetime) else now
        edges.append(
            _edge(
                NodeType.INCIDENT,
                incident_id,
                edge_type,
                target_type,
                target_id,
                Provenance.NATIVE,
                1.0,
                str(row.get("source_url") or edge_type.value),
                None,
                event_ts,
            )
        )


def _append_user(
    edges: list[WorkGraphEdge], row: dict[str, object], user_key: str, now: datetime
) -> None:
    incident_id, user_id = (
        str(row.get("incident_id") or ""),
        str(row.get(user_key) or ""),
    )
    if incident_id and user_id:
        candidate = row.get("occurred_at") or row.get("assigned_at")
        event_ts = candidate if isinstance(candidate, datetime) else now
        edges.append(
            _edge(
                NodeType.INCIDENT,
                incident_id,
                EdgeType.ASSIGNED_TO,
                NodeType.USER,
                user_id,
                Provenance.NATIVE,
                1.0,
                str(row.get("source_url") or user_key),
                None,
                event_ts,
            )
        )


def _edge(
    source_type: NodeType,
    source_id: str,
    edge_type: EdgeType,
    target_type: NodeType,
    target_id: str,
    provenance: Provenance,
    confidence: float,
    evidence: str,
    repo_id: uuid.UUID | None,
    event_ts: datetime,
    provider: str = "pagerduty",
) -> WorkGraphEdge:
    return WorkGraphEdge(
        generate_edge_id(source_type, source_id, edge_type, target_type, target_id),
        source_type,
        source_id,
        target_type,
        target_id,
        edge_type,
        provenance,
        confidence,
        evidence,
        repo_id=repo_id,
        provider=provider,
        event_ts=event_ts,
    )


def _timestamp_window(
    column: str, from_date: datetime | None, to_date: datetime | None
) -> str:
    clauses: list[str] = []
    if from_date is not None:
        clauses.append(f" AND {column} >= {{from_date:DateTime}}")
    if to_date is not None:
        clauses.append(f" AND {column} <= {{to_date:DateTime}}")
    return "".join(clauses)


def _mapping_confidence(row: dict[str, object]) -> float:
    value = row.get("relationship_confidence")
    return float(value) if isinstance(value, int | float | str) else 0.0


def _mapping_provenance(source: str) -> Provenance:
    return (
        Provenance.HEURISTIC
        if source == "bounded_service_repository_heuristic"
        else Provenance.NATIVE
    )
