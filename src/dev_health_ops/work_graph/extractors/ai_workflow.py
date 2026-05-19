"""Extract AI workflow Work Graph entities from normalized artifacts."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from dev_health_ops.models.ai_attribution import AIAttributionSignal
from dev_health_ops.models.ai_workflow import (
    AIWorkflowArtifactEdge,
    AIWorkflowArtifactType,
    AIWorkflowIssueEdge,
    AIWorkflowRun,
    AIWorkflowRunKind,
    AIWorkflowRunStatus,
    WorkGraphDeploymentIncidentEdge,
    WorkGraphPRDeploymentEdge,
    WorkGraphPRReviewOutcomeEdge,
)
from dev_health_ops.providers._ai_detection import (
    AuthorInfo,
    detect_from_author,
    detect_from_branch_name,
    detect_from_pr_body,
    detect_from_pr_labels,
)


@dataclass(frozen=True)
class AIWorkflowExtractionResult:
    """AI workflow entities and edges emitted by the extractor."""

    runs: list[AIWorkflowRun] = field(default_factory=list)
    issue_edges: list[AIWorkflowIssueEdge] = field(default_factory=list)
    artifact_edges: list[AIWorkflowArtifactEdge] = field(default_factory=list)
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


def _signals_from_pr(row: dict[str, Any]) -> list[AIAttributionSignal]:
    signals: list[AIAttributionSignal] = []
    labels_raw = row.get("labels") or []
    labels = (
        [str(label) for label in labels_raw] if isinstance(labels_raw, list) else []
    )
    signals.extend(detect_from_pr_labels(labels))

    author_name = _str(row, "author_name") or _str(row, "author_login")
    if author_name:
        author_signal = detect_from_author(
            AuthorInfo(
                login=author_name,
                user_type=_str(row, "author_user_type"),
                app_slug=_str(row, "author_app_slug"),
            )
        )
        if author_signal:
            signals.append(author_signal)

    branch_signal = detect_from_branch_name(_str(row, "head_branch"))
    if branch_signal:
        signals.append(branch_signal)

    body_signal = detect_from_pr_body(_str(row, "body"))
    if body_signal:
        signals.append(body_signal)

    return signals


def _run_kind(signal: AIAttributionSignal) -> AIWorkflowRunKind:
    if str(signal.kind) == "agent_created":
        return AIWorkflowRunKind.AGENT_AUTONOMOUS
    if str(signal.kind) == "ai_assisted":
        return AIWorkflowRunKind.CHAT_ASSISTED
    return AIWorkflowRunKind.UNKNOWN


def extract_ai_workflow_from_pull_requests(
    pull_requests: list[dict[str, Any]],
    *,
    org_id: UUID,
    provider: str,
    issue_ids_by_pr: dict[str, list[str]] | None = None,
) -> AIWorkflowExtractionResult:
    """Create AI workflow runs and issue/PR edges from normalized PR rows.

    Rows may be partial; missing metadata produces fewer fields, not exceptions.
    ``issue_ids_by_pr`` keys are PR ids in ``repo_id:number`` form.
    """

    issue_ids_by_pr = issue_ids_by_pr or {}
    result = AIWorkflowExtractionResult()

    for row in pull_requests:
        repo_id_raw = row.get("repo_id")
        if repo_id_raw is None:
            continue
        repo_id = UUID(str(repo_id_raw))
        pr_number = _int_str(row, "number")
        if not pr_number:
            continue
        pr_id = f"{repo_id}:{pr_number}"
        signals = _signals_from_pr(row)
        if not signals:
            continue

        observed_at = _dt(row, "merged_at", "closed_at", "created_at", "last_synced")
        strongest = max(signals, key=lambda signal: float(signal.confidence))
        run_id = _hash(org_id, provider, "pull_request", pr_id, strongest.source)
        result.runs.append(
            AIWorkflowRun(
                run_id=run_id,
                org_id=org_id,
                provider=provider,
                run_kind=_run_kind(strongest),
                status=AIWorkflowRunStatus.COMPLETED,
                tool=strongest.actor,
                actor=strongest.actor or _str(row, "author_name") or None,
                repo_id=repo_id,
                prompts_redacted=True,
                started_at=_dt(row, "created_at", "last_synced"),
                completed_at=_dt(row, "merged_at", "closed_at", "last_synced"),
                observed_at=observed_at,
                metadata={
                    "subject_type": "pull_request",
                    "subject_id": pr_id,
                    "signals": [signal.evidence for signal in signals],
                },
            )
        )
        result.artifact_edges.append(
            AIWorkflowArtifactEdge(
                edge_id=_hash("ai_run_pr", org_id, run_id, pr_id),
                org_id=org_id,
                run_id=run_id,
                artifact_type=AIWorkflowArtifactType.PULL_REQUEST,
                artifact_id=pr_id,
                provider=provider,
                repo_id=repo_id,
                confidence=float(strongest.confidence),
                source=str(strongest.source),
                evidence=_json(strongest.evidence),
                observed_at=observed_at,
            )
        )
        for issue_id in issue_ids_by_pr.get(pr_id, []):
            result.issue_edges.append(
                AIWorkflowIssueEdge(
                    edge_id=_hash("issue_ai_run", org_id, issue_id, run_id),
                    org_id=org_id,
                    issue_id=issue_id,
                    run_id=run_id,
                    provider=provider,
                    repo_id=repo_id,
                    confidence=float(strongest.confidence),
                    source=str(strongest.source),
                    evidence=_json({"pr_id": pr_id, "signal": strongest.evidence}),
                    observed_at=observed_at,
                )
            )

    return result


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
