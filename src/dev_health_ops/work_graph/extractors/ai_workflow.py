"""Extract AI workflow Work Graph entities from normalized artifacts."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from dev_health_ops.models.ai_workflow import (
    WorkGraphDeploymentIncidentEdge,
    WorkGraphPRDeploymentEdge,
    WorkGraphPRReviewOutcomeEdge,
)


@dataclass(frozen=True)
class AIWorkflowExtractionResult:
    """Work Graph review/deployment/incident edges emitted by the extractor.

    CHAOS-5242: this dataclass used to also carry ``runs``/``issue_edges``/
    ``artifact_edges`` for ``extract_ai_workflow_from_pull_requests``, deleted
    alongside its own native Go port (AIWorkflowExecutor, #2280) -- the native
    executor has no Python fallback (chris's standing CHAOS-5233 rule: once a
    family's Go executor is on main, its Python compute is deleted, never
    skip-gated). The three fields below belong to
    ``extract_review_deployment_incident_edges``, which serves the SEPARATE,
    still-Python work_graph_edges family (CHAOS-4286, write-only skip-gated)
    and is unaffected by this deletion -- verified via rg that neither
    function's helpers overlapped beyond the shared, still-needed
    ``_hash``/``_json``/``_dt``/``_str``/``_int_str`` primitives below.
    """

    review_outcome_edges: list[WorkGraphPRReviewOutcomeEdge] = field(
        default_factory=list
    )
    pr_deployment_edges: list[WorkGraphPRDeploymentEdge] = field(default_factory=list)
    deployment_incident_edges: list[WorkGraphDeploymentIncidentEdge] = field(
        default_factory=list
    )


def _hash(*parts: object) -> str:
    canonical = "|".join("" if part is None else str(part) for part in parts)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _str(row: dict[str, Any], key: str, default: str = "") -> str:
    value = row.get(key)
    return default if value is None else str(value)


def _int_str(row: dict[str, Any], key: str) -> str:
    value = row.get(key)
    return (
        ""
        if value is None
        else str(int(value))
        if isinstance(value, int)
        else str(value)
    )


def _dt(row: dict[str, Any], *keys: str) -> datetime:
    for key in keys:
        value = row.get(key)
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return _now()


def extract_review_deployment_incident_edges(
    *,
    org_id: UUID,
    provider: str,
    reviews: list[dict[str, Any]] | None = None,
    deployments: list[dict[str, Any]] | None = None,
    incidents: list[dict[str, Any]] | None = None,
) -> AIWorkflowExtractionResult:
    """Extract PR→review, PR→deployment, and deployment→incident edges."""

    result = AIWorkflowExtractionResult()
    for row in reviews or []:
        repo_id_raw = row.get("repo_id")
        number = _int_str(row, "number")
        review_id = _str(row, "review_id")
        if repo_id_raw is None or not number or not review_id:
            continue
        repo_id = UUID(str(repo_id_raw))
        pr_id = f"{repo_id}:{number}"
        result.review_outcome_edges.append(
            WorkGraphPRReviewOutcomeEdge(
                edge_id=_hash("pr_review", org_id, pr_id, review_id),
                org_id=org_id,
                pr_id=pr_id,
                review_outcome_id=review_id,
                outcome=_str(row, "state") or None,
                provider=provider,
                repo_id=repo_id,
                confidence=1.0,
                source="native",
                evidence=_json({"review_id": review_id, "state": row.get("state")}),
                observed_at=_dt(row, "submitted_at", "last_synced"),
            )
        )

    deployments_by_repo: dict[str, list[str]] = {}
    for row in deployments or []:
        repo_id_raw = row.get("repo_id")
        deployment_id = _str(row, "deployment_id")
        pr_number_value = row.get("pull_request_number")
        if repo_id_raw is None or not deployment_id:
            continue
        repo_id = UUID(str(repo_id_raw))
        deployments_by_repo.setdefault(str(repo_id), []).append(deployment_id)
        if pr_number_value is None:
            continue
        pr_id = f"{repo_id}:{pr_number_value}"
        result.pr_deployment_edges.append(
            WorkGraphPRDeploymentEdge(
                edge_id=_hash("pr_deployment", org_id, pr_id, deployment_id),
                org_id=org_id,
                pr_id=pr_id,
                deployment_id=deployment_id,
                provider=provider,
                repo_id=repo_id,
                confidence=1.0,
                source="native",
                evidence=_json({"deployment_id": deployment_id}),
                observed_at=_dt(
                    row, "deployed_at", "finished_at", "started_at", "last_synced"
                ),
            )
        )

    for row in incidents or []:
        repo_id_raw = row.get("repo_id")
        incident_id = _str(row, "incident_id")
        deployment_id = _str(row, "deployment_id")
        if repo_id_raw is None or not incident_id:
            continue
        repo_id = UUID(str(repo_id_raw))
        linked_deployments = (
            [deployment_id]
            if deployment_id
            else deployments_by_repo.get(str(repo_id), [])
        )
        for linked_deployment_id in linked_deployments:
            result.deployment_incident_edges.append(
                WorkGraphDeploymentIncidentEdge(
                    edge_id=_hash(
                        "deployment_incident", org_id, linked_deployment_id, incident_id
                    ),
                    org_id=org_id,
                    deployment_id=linked_deployment_id,
                    incident_id=incident_id,
                    provider=provider,
                    repo_id=repo_id,
                    confidence=1.0 if deployment_id else 0.3,
                    source="native" if deployment_id else "heuristic",
                    evidence=_json({"incident_id": incident_id}),
                    observed_at=_dt(row, "started_at", "last_synced"),
                )
            )

    return result
