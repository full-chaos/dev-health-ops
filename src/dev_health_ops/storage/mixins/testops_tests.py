from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Protocol

from .base import SQLAlchemyStoreMixinProtocol


class _TestOpsStoreProtocol(SQLAlchemyStoreMixinProtocol, Protocol):
    _test_suite_results_table: Any
    _test_case_results_table: Any
    _coverage_snapshots_table: Any


class TestOpsTestsMixin(_TestOpsStoreProtocol):
    __test__ = False

    async def insert_test_suite_results(self, suites: list[Any]) -> None:
        if not suites:
            return
        synced_at_default = datetime.now(timezone.utc)
        rows: list[dict[str, Any]] = []
        for item in suites:
            is_dict = isinstance(item, dict)
            get = (
                item.get
                if is_dict
                else lambda key, default=None: getattr(item, key, default)
            )
            repo_id = get("repo_id")
            rows.append(
                {
                    "repo_id": str(repo_id) if repo_id is not None else None,
                    "run_id": str(get("run_id") or ""),
                    "suite_id": str(get("suite_id") or ""),
                    "suite_name": str(get("suite_name") or ""),
                    "framework": get("framework"),
                    "environment": get("environment"),
                    "total_count": int(get("total_count") or 0),
                    "passed_count": int(get("passed_count") or 0),
                    "failed_count": int(get("failed_count") or 0),
                    "skipped_count": int(get("skipped_count") or 0),
                    "error_count": int(get("error_count") or 0),
                    "quarantined_count": int(get("quarantined_count") or 0),
                    "retried_count": int(get("retried_count") or 0),
                    "duration_seconds": get("duration_seconds"),
                    "started_at": get("started_at"),
                    "finished_at": get("finished_at"),
                    "team_id": get("team_id"),
                    "service_id": get("service_id"),
                    "org_id": str(get("org_id") or ""),
                    "last_synced": get("last_synced") or synced_at_default,
                }
            )

        await self._upsert_many(
            self._test_suite_results_table,
            rows,
            conflict_columns=["repo_id", "run_id", "suite_id"],
            update_columns=[
                "suite_name",
                "framework",
                "environment",
                "total_count",
                "passed_count",
                "failed_count",
                "skipped_count",
                "error_count",
                "quarantined_count",
                "retried_count",
                "duration_seconds",
                "started_at",
                "finished_at",
                "team_id",
                "service_id",
                "org_id",
                "last_synced",
            ],
        )

    async def insert_test_case_results(self, cases: list[Any]) -> None:
        if not cases:
            return
        synced_at_default = datetime.now(timezone.utc)
        rows: list[dict[str, Any]] = []
        for item in cases:
            is_dict = isinstance(item, dict)
            get = (
                item.get
                if is_dict
                else lambda key, default=None: getattr(item, key, default)
            )
            repo_id = get("repo_id")
            rows.append(
                {
                    "repo_id": str(repo_id) if repo_id is not None else None,
                    "run_id": str(get("run_id") or ""),
                    "suite_id": str(get("suite_id") or ""),
                    "case_id": str(get("case_id") or ""),
                    "case_name": str(get("case_name") or ""),
                    "class_name": get("class_name"),
                    "status": str(get("status") or "passed"),
                    "duration_seconds": get("duration_seconds"),
                    "retry_attempt": int(get("retry_attempt") or 0),
                    "failure_message": get("failure_message"),
                    "failure_type": get("failure_type"),
                    "stack_trace": get("stack_trace"),
                    "is_quarantined": bool(get("is_quarantined") or False),
                    "org_id": str(get("org_id") or ""),
                    "last_synced": get("last_synced") or synced_at_default,
                }
            )

        await self._upsert_many(
            self._test_case_results_table,
            rows,
            conflict_columns=["repo_id", "run_id", "suite_id", "case_id"],
            update_columns=[
                "case_name",
                "class_name",
                "status",
                "duration_seconds",
                "retry_attempt",
                "failure_message",
                "failure_type",
                "stack_trace",
                "is_quarantined",
                "org_id",
                "last_synced",
            ],
        )

    async def insert_coverage_snapshots(self, snapshots: list[Any]) -> None:
        if not snapshots:
            return
        synced_at_default = datetime.now(timezone.utc)
        rows: list[dict[str, Any]] = []
        for item in snapshots:
            is_dict = isinstance(item, dict)
            get = (
                item.get
                if is_dict
                else lambda key, default=None: getattr(item, key, default)
            )
            repo_id = get("repo_id")
            rows.append(
                {
                    "repo_id": str(repo_id) if repo_id is not None else None,
                    "run_id": str(get("run_id") or ""),
                    "snapshot_id": str(get("snapshot_id") or ""),
                    "report_format": get("report_format"),
                    "lines_total": get("lines_total"),
                    "lines_covered": get("lines_covered"),
                    "line_coverage_pct": get("line_coverage_pct"),
                    "branches_total": get("branches_total"),
                    "branches_covered": get("branches_covered"),
                    "branch_coverage_pct": get("branch_coverage_pct"),
                    "functions_total": get("functions_total"),
                    "functions_covered": get("functions_covered"),
                    "commit_hash": get("commit_hash"),
                    "branch": get("branch"),
                    "pr_number": get("pr_number"),
                    "team_id": get("team_id"),
                    "service_id": get("service_id"),
                    "org_id": str(get("org_id") or ""),
                    "last_synced": get("last_synced") or synced_at_default,
                }
            )

        await self._upsert_many(
            self._coverage_snapshots_table,
            rows,
            conflict_columns=["repo_id", "run_id", "snapshot_id"],
            update_columns=[
                "report_format",
                "lines_total",
                "lines_covered",
                "line_coverage_pct",
                "branches_total",
                "branches_covered",
                "branch_coverage_pct",
                "functions_total",
                "functions_covered",
                "commit_hash",
                "branch",
                "pr_number",
                "team_id",
                "service_id",
                "org_id",
                "last_synced",
            ],
        )
