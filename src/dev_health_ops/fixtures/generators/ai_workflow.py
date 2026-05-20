"""AI workflow intelligence fixture generators.

Produces synthetic ``ai_workflow_runs`` rows together with the artifact and
issue edges that connect those runs into the Work Graph. The runs are
metadata-only by contract: no raw prompts, sessions, transcripts, IDE
telemetry, or keystrokes are ever fabricated — only prompt *hashes* and
lengths plus high-level provider/tool/model labels.

Generated data shape:

* One :class:`AIWorkflowRun` per AI-attributed PR (chat-assisted variant) plus
  a handful of standalone agent-autonomous runs that exercise variety
  (``status=completed``/``failed``, no PR linkage).
* For every PR-attached run, one :class:`AIWorkflowArtifactEdge` to the PR
  (``artifact_type=pull_request``) and, when the PR carries a diff hint, one
  edge to a synthetic ``diff`` artifact.
* For every PR-attached run that maps to a referenced issue, one
  :class:`AIWorkflowIssueEdge` to the issue.

The generator is deterministic when the host
:class:`SyntheticDataGenerator` is seeded.
"""

from __future__ import annotations

import random
import uuid
from datetime import datetime, timedelta, timezone

from dev_health_ops.fixtures.generators.base import BaseGeneratorMixin
from dev_health_ops.models.ai_workflow import (
    AIWorkflowArtifactEdge,
    AIWorkflowArtifactType,
    AIWorkflowIssueEdge,
    AIWorkflowRun,
    AIWorkflowRunKind,
    AIWorkflowRunStatus,
)
from dev_health_ops.models.git import GitPullRequest
from dev_health_ops.models.work_items import WorkItem

_TOOL_VARIANTS: tuple[tuple[str, str, AIWorkflowRunKind], ...] = (
    ("claude-code", "claude-sonnet-4", AIWorkflowRunKind.CHAT_ASSISTED),
    ("cursor", "claude-3.5-sonnet", AIWorkflowRunKind.CHAT_ASSISTED),
    ("claude-code-agent", "claude-sonnet-4", AIWorkflowRunKind.AGENT_AUTONOMOUS),
)

_AUTONOMOUS_STATUSES: tuple[AIWorkflowRunStatus, ...] = (
    AIWorkflowRunStatus.COMPLETED,
    AIWorkflowRunStatus.COMPLETED,
    AIWorkflowRunStatus.FAILED,
)


class AiWorkflowGeneratorMixin(BaseGeneratorMixin):
    """Generates synthetic AI workflow runs and their typed evidence edges."""

    def generate_ai_workflow_runs(
        self,
        prs: list[GitPullRequest],
        *,
        org_id: str,
        autonomous_run_count: int = 3,
    ) -> list[AIWorkflowRun]:
        """Generate one workflow run per AI-attributed PR plus a few autonomous runs."""
        if not prs:
            return []

        org_uuid = uuid.UUID(str(org_id))
        runs: list[AIWorkflowRun] = []

        for index, pr in enumerate(prs):
            if index % 3 == 2:
                continue

            tool, model, run_kind = _TOOL_VARIANTS[index % len(_TOOL_VARIANTS)]
            observed_at = pr.merged_at or pr.created_at
            started_at = observed_at - timedelta(minutes=random.randint(5, 90))
            run_id = self._workflow_run_id(org_uuid, pr.number, "pr")
            prompt_seed = f"{org_uuid}:{self.repo_id}:{pr.number}:{tool}:{model}"
            runs.append(
                AIWorkflowRun(
                    run_id=run_id,
                    org_id=org_uuid,
                    provider=self.provider,
                    run_kind=run_kind,
                    status=AIWorkflowRunStatus.COMPLETED,
                    tool=tool,
                    model=model,
                    actor=self._actor_for_run_kind(run_kind),
                    repo_id=self.repo_id,
                    prompts_redacted=True,
                    prompt_hash=AIWorkflowRun.hash_prompt(prompt_seed),
                    prompt_length=random.randint(120, 1800),
                    started_at=started_at,
                    completed_at=observed_at,
                    observed_at=observed_at,
                    metadata={
                        "source": "synthetic_fixture",
                        "subject_type": "pull_request",
                        "subject_id": str(pr.number),
                    },
                )
            )

        if autonomous_run_count > 0:
            base_time = datetime.now(timezone.utc) - timedelta(days=1)
            for i in range(autonomous_run_count):
                started_at = base_time - timedelta(hours=random.randint(1, 48))
                completed_at = started_at + timedelta(minutes=random.randint(5, 25))
                status = _AUTONOMOUS_STATUSES[i % len(_AUTONOMOUS_STATUSES)]
                prompt_seed = f"{org_uuid}:{self.repo_id}:autonomous:{i}"
                runs.append(
                    AIWorkflowRun(
                        run_id=self._workflow_run_id(org_uuid, i, "autonomous"),
                        org_id=org_uuid,
                        provider=self.provider,
                        run_kind=AIWorkflowRunKind.AGENT_AUTONOMOUS,
                        status=status,
                        tool="claude-code-agent",
                        model="claude-sonnet-4",
                        actor="claude-code[bot]",
                        repo_id=self.repo_id,
                        prompts_redacted=True,
                        prompt_hash=AIWorkflowRun.hash_prompt(prompt_seed),
                        prompt_length=random.randint(80, 600),
                        started_at=started_at,
                        completed_at=completed_at
                        if status is AIWorkflowRunStatus.COMPLETED
                        else None,
                        observed_at=started_at,
                        metadata={
                            "source": "synthetic_fixture",
                            "subject_type": "agent_session",
                            "subject_id": f"agent-{i}",
                        },
                    )
                )

        return runs

    def generate_ai_workflow_artifact_edges(
        self,
        runs: list[AIWorkflowRun],
        prs: list[GitPullRequest],
        *,
        org_id: str,
    ) -> list[AIWorkflowArtifactEdge]:
        """Link AI workflow runs to the PR (and a synthetic diff) they produced."""
        if not runs or not prs:
            return []

        org_uuid = uuid.UUID(str(org_id))
        pr_lookup: dict[str, GitPullRequest] = {str(pr.number): pr for pr in prs}
        edges: list[AIWorkflowArtifactEdge] = []

        for run in runs:
            subject_type = str(run.metadata.get("subject_type") or "")
            if subject_type != "pull_request":
                continue
            subject_id = str(run.metadata.get("subject_id") or "")
            pr = pr_lookup.get(subject_id)
            if pr is None:
                continue

            evidence_pr = (
                '{"source":"synthetic_fixture","artifact":"pull_request","number":'
                f"{pr.number}}}"
            )
            edges.append(
                AIWorkflowArtifactEdge(
                    edge_id=self._edge_id(
                        org_uuid, run.run_id, "pull_request", subject_id
                    ),
                    org_id=org_uuid,
                    run_id=run.run_id,
                    artifact_type=AIWorkflowArtifactType.PULL_REQUEST,
                    artifact_id=subject_id,
                    provider=self.provider,
                    confidence=0.9,
                    source="synthetic_fixture",
                    evidence=evidence_pr,
                    observed_at=run.observed_at,
                    repo_id=self.repo_id,
                )
            )

            # Pair every other run with a synthetic diff artifact so the edge
            # table exercises more than one artifact_type.
            if pr.number % 2 == 0:
                diff_id = f"pr-{pr.number}-diff"
                evidence_diff = (
                    '{"source":"synthetic_fixture","artifact":"diff","pr_number":'
                    f"{pr.number}}}"
                )
                edges.append(
                    AIWorkflowArtifactEdge(
                        edge_id=self._edge_id(org_uuid, run.run_id, "diff", diff_id),
                        org_id=org_uuid,
                        run_id=run.run_id,
                        artifact_type=AIWorkflowArtifactType.DIFF,
                        artifact_id=diff_id,
                        provider=self.provider,
                        confidence=0.75,
                        source="synthetic_fixture",
                        evidence=evidence_diff,
                        observed_at=run.observed_at,
                        repo_id=self.repo_id,
                    )
                )

        return edges

    def generate_ai_workflow_issue_edges(
        self,
        runs: list[AIWorkflowRun],
        prs: list[GitPullRequest],
        work_items: list[WorkItem],
        *,
        org_id: str,
    ) -> list[AIWorkflowIssueEdge]:
        """Link runs to the PR's referenced issue, or to a same-repo issue as fallback."""
        if not runs or not work_items:
            return []

        org_uuid = uuid.UUID(str(org_id))
        pr_lookup: dict[str, GitPullRequest] = {str(pr.number): pr for pr in prs}
        edges: list[AIWorkflowIssueEdge] = []

        issue_by_repo: dict[uuid.UUID | None, list[str]] = {}
        for item in work_items:
            wi_id = str(getattr(item, "work_item_id", "") or "")
            if not wi_id:
                continue
            issue_by_repo.setdefault(getattr(item, "repo_id", None), []).append(wi_id)

        repo_issues = issue_by_repo.get(self.repo_id) or [
            issue_id for issues in issue_by_repo.values() for issue_id in issues
        ]
        if not repo_issues:
            return []

        for index, run in enumerate(runs):
            subject_type = str(run.metadata.get("subject_type") or "")
            if subject_type != "pull_request":
                continue
            subject_id = str(run.metadata.get("subject_id") or "")
            pr = pr_lookup.get(subject_id)
            if pr is None:
                continue

            issue_id = self._issue_for_pr(pr, repo_issues, index)
            if not issue_id:
                continue

            evidence = (
                '{"source":"synthetic_fixture","issue_id":"'
                f'{issue_id}","pr_number":{pr.number}}}'
            )
            edges.append(
                AIWorkflowIssueEdge(
                    edge_id=self._edge_id(org_uuid, run.run_id, "issue", issue_id),
                    org_id=org_uuid,
                    issue_id=issue_id,
                    run_id=run.run_id,
                    provider=self.provider,
                    confidence=0.8,
                    source="synthetic_fixture",
                    evidence=evidence,
                    observed_at=run.observed_at,
                    repo_id=self.repo_id,
                )
            )

        return edges

    def _workflow_run_id(self, org_uuid: uuid.UUID, key: int, scope: str) -> str:
        return str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"ai_workflow_run:{org_uuid}:{self.repo_id}:{scope}:{key}",
            )
        )

    def _edge_id(
        self,
        org_uuid: uuid.UUID,
        run_id: str,
        kind: str,
        target_id: str,
    ) -> str:
        return str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"ai_workflow_edge:{org_uuid}:{run_id}:{kind}:{target_id}",
            )
        )

    def _actor_for_run_kind(self, run_kind: AIWorkflowRunKind) -> str:
        if run_kind is AIWorkflowRunKind.AGENT_AUTONOMOUS:
            return "claude-code[bot]"
        if self.repo_authors:
            name, _email = self.repo_authors[0]
            return name
        return "synthetic-author"

    def _issue_for_pr(
        self,
        pr: GitPullRequest,
        repo_issues: list[str],
        index: int,
    ) -> str | None:
        ref = getattr(pr, "issue_id", None) or getattr(pr, "referenced_issue", None)
        if ref:
            return str(ref)
        if not repo_issues:
            return None
        return repo_issues[index % len(repo_issues)]
