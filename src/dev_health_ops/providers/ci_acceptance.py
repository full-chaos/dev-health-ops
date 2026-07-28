"""Canonical CI acceptance projection shared by provider adapters.

The projection never infers a requirement from a workflow/job name.  Provider
policy must positively identify required work; unavailable policy remains
``unknown`` so a green pipeline cannot become a false completion signal.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime
from typing import Any
from uuid import UUID

from dev_health_ops.metrics.testops_schemas import CIAcceptanceCheckRow

CI_ACCEPTANCE_RULE_VERSION = "ci-acceptance.v1"


def canonical_result(value: object) -> str:
    normalized = str(value or "").casefold()
    if normalized in {"success", "passed", "pass", "green", "succeeded"}:
        return "passed"
    if normalized in {
        "failure",
        "failed",
        "error",
        "timed_out",
        "timeout",
        "action_required",
        "startup_failure",
    }:
        return "failed"
    if normalized in {"skipped", "manual"}:
        return "skipped"
    if normalized in {
        "queued",
        "pending",
        "requested",
        "waiting",
        "created",
        "preparing",
        "running",
        "in_progress",
    }:
        return "pending"
    return "unknown"


def check_key(provider: str, name: str) -> str:
    digest = hashlib.sha256(f"{provider}\0{name}".encode()).hexdigest()[:24]
    return f"{provider}:{digest}"


def project_checks(
    *,
    repo_id: UUID,
    org_id: str | None,
    run_id: str,
    provider: str,
    observed_at: datetime,
    jobs: Sequence[Mapping[str, Any]],
    required_names: Iterable[str] | None,
    provenance: str,
    target_branch: str | None = None,
    pr_number: int | None = None,
    source_url: str | None = None,
) -> list[CIAcceptanceCheckRow]:
    """Project provider jobs with fail-closed requirement semantics.

    ``required_names=None`` means the policy source was unavailable.  An empty
    iterable means the policy was authoritatively read and declares no jobs.
    Missing required jobs receive an explicit ``unknown`` result row.
    """

    required = None if required_names is None else {str(v) for v in required_names}
    by_name = {str(job.get("name") or "job"): job for job in jobs}
    names = set(by_name)
    if required is not None:
        names.update(required)
    rows: list[CIAcceptanceCheckRow] = []
    for name in sorted(names, key=str.casefold):
        job = by_name.get(name)
        requirement = (
            "unknown"
            if required is None
            else ("required" if name in required else "optional")
        )
        row: CIAcceptanceCheckRow = {
            "repo_id": repo_id,
            "run_id": run_id,
            "check_key": check_key(provider, name),
            "check_name": name,
            "provider": provider,
            "requirement": requirement,
            "result": canonical_result(job.get("status") if job else None),
            "rule_version": CI_ACCEPTANCE_RULE_VERSION,
            "provenance": provenance,
            "observed_at": observed_at,
            "target_branch": target_branch,
            "pr_number": pr_number,
            "source_url": source_url,
        }
        if org_id:
            row["org_id"] = org_id
        rows.append(row)
    return rows
