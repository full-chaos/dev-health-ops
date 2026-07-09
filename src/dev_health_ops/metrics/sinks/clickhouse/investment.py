"""
InvestmentMixin — investment classification, metrics, and work unit write methods.

Tables: investment_classifications_daily, investment_metrics_daily,
        issue_type_metrics_daily, work_unit_investments,
        work_unit_investment_quotes, investment_explanations.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from dev_health_ops.metrics.schemas import (
    InvestmentClassificationRecord,
    InvestmentExplanationRecord,
    InvestmentMetricsRecord,
    IssueTypeMetricsRecord,
    WorkUnitInvestmentEvidenceQuoteRecord,
    WorkUnitInvestmentRecord,
    WorkUnitMembershipRecord,
    WorkUnitMembershipRunRecord,
    WorkUnitRepoEffortRecord,
    WorkUnitScopedMembershipRunRecord,
)

if TYPE_CHECKING:
    from dev_health_ops.metrics.sinks.clickhouse._insert import _ClickHouseSinkBase
else:

    class _ClickHouseSinkBase:
        pass


logger = logging.getLogger(__name__)

# Reserved run_id of the seeded legacy completion marker (migration 048). Its
# membership rows carry run_id='' (migration 047 default), so retention maps the
# '__legacy__' marker back to '' when deleting its rows. MUST match
# api/graphql/resolvers/work_graph._LEGACY_RUN_ID.
_LEGACY_RUN_ID = "__legacy__"


class InvestmentMixin(_ClickHouseSinkBase):
    """Mixin for investment and work-unit classification write methods."""

    def write_investment_classifications(
        self, rows: Sequence[InvestmentClassificationRecord]
    ) -> None:
        if not rows:
            return
        self._insert_rows(
            "investment_classifications_daily",
            [
                "repo_id",
                "day",
                "artifact_type",
                "artifact_id",
                "provider",
                "investment_area",
                "project_stream",
                "confidence",
                "rule_id",
                "computed_at",
                "org_id",
            ],
            rows,
        )

    def write_investment_metrics(self, rows: Sequence[InvestmentMetricsRecord]) -> None:
        if not rows:
            return
        self._insert_rows(
            "investment_metrics_daily",
            [
                "repo_id",
                "day",
                "team_id",
                "investment_area",
                "project_stream",
                "delivery_units",
                "work_items_completed",
                "prs_merged",
                "churn_loc",
                "cycle_p50_hours",
                "computed_at",
                "org_id",
            ],
            rows,
        )

    def write_issue_type_metrics(self, rows: Sequence[IssueTypeMetricsRecord]) -> None:
        if not rows:
            return
        self._insert_rows(
            "issue_type_metrics_daily",
            [
                "repo_id",
                "day",
                "provider",
                "team_id",
                "issue_type_norm",
                "created_count",
                "completed_count",
                "active_count",
                "cycle_p50_hours",
                "cycle_p90_hours",
                "lead_p50_hours",
                "computed_at",
                "org_id",
            ],
            rows,
        )

    def write_work_unit_investments(
        self, rows: Sequence[WorkUnitInvestmentRecord]
    ) -> None:
        if not rows:
            return
        self._insert_rows(
            "work_unit_investments",
            [
                "work_unit_id",
                "work_unit_type",
                "work_unit_name",
                "from_ts",
                "to_ts",
                "repo_id",
                "provider",
                "effort_metric",
                "effort_value",
                "theme_distribution_json",
                "subcategory_distribution_json",
                "structural_evidence_json",
                "evidence_quality",
                "evidence_quality_band",
                "categorization_status",
                "categorization_errors_json",
                "categorization_model_version",
                "categorization_input_hash",
                "categorization_run_id",
                "computed_at",
                "org_id",
            ],
            rows,
        )

    def write_work_unit_repo_effort(
        self, rows: Sequence[WorkUnitRepoEffortRecord]
    ) -> None:
        if not rows:
            return
        self._insert_rows(
            "work_unit_repo_effort",
            [
                "work_unit_id",
                "repo_id",
                "effort_metric",
                "effort_value",
                "allocation_weight",
                "allocation_source",
                "categorization_run_id",
                "computed_at",
                "org_id",
            ],
            rows,
        )

    def write_work_unit_investment_quotes(
        self, rows: Sequence[WorkUnitInvestmentEvidenceQuoteRecord]
    ) -> None:
        if not rows:
            return
        self._insert_rows(
            "work_unit_investment_quotes",
            [
                "work_unit_id",
                "quote",
                "source_type",
                "source_id",
                "computed_at",
                "categorization_run_id",
                "org_id",
            ],
            rows,
        )

    def write_work_unit_memberships(
        self, rows: Sequence[WorkUnitMembershipRecord]
    ) -> None:
        if not rows:
            return
        self._insert_rows(
            "work_unit_membership",
            [
                "org_id",
                "node_type",
                "node_id",
                "work_unit_id",
                "category_kind",
                "category",
                "weight",
                "is_dominant",
                "categorization_status",
                "computed_at",
                "run_id",
            ],
            rows,
        )

    def write_membership_run(self, record: WorkUnitMembershipRunRecord) -> None:
        """Write the completion-marker for a membership run (CHAOS-2433).

        Must be called as the LAST step after all membership rows for the run
        have been written.  Readers use the latest completed_at row here to
        identify the current valid run_id.
        """
        self._insert_rows(
            "work_unit_membership_runs",
            ["org_id", "run_id", "completed_at"],
            [record],
        )

    def write_scoped_membership_runs(
        self, records: Sequence[WorkUnitScopedMembershipRunRecord]
    ) -> None:
        if not records:
            return
        self._insert_rows(
            "work_unit_membership_scoped_runs",
            ["org_id", "scope_kind", "scope_id", "run_id", "completed_at"],
            records,
        )

    def prune_membership_runs(self, org_id: str, *, keep: int = 2) -> int:
        """Retain only the latest ``keep`` COMPLETE membership runs for an org.

        CHAOS-2433 round-5 (unbounded growth): migration 049 put run_id in the
        ReplacingMergeTree key so generations are intentionally NOT collapsed.
        Without retention, every org-wide projection (daily beat + post-sync)
        adds a full copy of the org's memberships forever — storage blowup and
        the resolver's latest-run filter scanning ever-more historical rows.
        This prunes old generations right after a new marker is published.

        Mechanism: read the org's completion markers ordered by completed_at DESC;
        KEEP the latest ``keep`` run_ids; DELETE — via lightweight
        ``ALTER TABLE ... DELETE WHERE`` — the work_unit_membership rows AND the
        work_unit_membership_runs markers for every OLDER run. Both deletes are
        scoped to ``org_id`` and to the explicit set of dropped run_ids, so the
        pass is idempotent and safe to run concurrently across orgs (each org
        only touches its own run_ids).

        KEEP-LATEST-2 RATIONALE: keep the current latest run PLUS one prior, so a
        reader/overlap mid-flight against the immediately-previous complete run
        (e.g. an argMax that resolved to it microseconds before a newer marker
        landed) is not pulled out from under it. Keeping 1 would risk deleting the
        run a concurrent reader is actively scanning.

        IN-FLIGHT SAFETY: retention operates ONLY on run_ids that HAVE a
        completion marker (the candidate set comes exclusively from
        work_unit_membership_runs). A markerless in-flight run — rows written, no
        marker yet, about to become the next generation — is never in the
        delete set, so a concurrent retention pass can never delete its rows.

        LEGACY: the seeded ``__legacy__`` marker is just another complete run; its
        membership rows carry run_id='' (migration 047 default), so when the
        legacy marker ages out we translate it to '' for the row delete. It ages
        out naturally once ``keep`` real runs exist (rollout continuity done).

        Returns the number of old run generations pruned (markers deleted).
        """
        # Candidate set is EXCLUSIVELY markered runs (in-flight runs have no
        # marker and are therefore never eligible for deletion).
        result = self.client.query(
            """
            SELECT run_id
            FROM work_unit_membership_runs
            WHERE org_id = {org_id:String}
            ORDER BY completed_at DESC, run_id DESC
            """,
            parameters={"org_id": org_id},
        )
        run_ids = [str(row[0]) for row in (result.result_rows or [])]
        # Dedup preserving order (ReplacingMergeTree may surface unmerged dupes;
        # we read without FINAL so collapse the latest-per-run_id ourselves).
        seen: set[str] = set()
        ordered_unique: list[str] = []
        for rid in run_ids:
            if rid not in seen:
                seen.add(rid)
                ordered_unique.append(rid)

        if len(ordered_unique) <= keep:
            return 0

        drop_marker_run_ids = ordered_unique[keep:]
        # Membership rows for the legacy marker carry run_id='' (not '__legacy__').
        drop_row_run_ids = [
            "" if rid == _LEGACY_RUN_ID else rid for rid in drop_marker_run_ids
        ]

        # Lightweight DELETE scoped to org + the explicit dropped run_ids. Both
        # statements are idempotent (a re-run finds those run_ids already gone)
        # and org-scoped so concurrent per-org passes never collide.
        self.client.command(
            "ALTER TABLE work_unit_membership "
            "DELETE WHERE org_id = {org_id:String} AND run_id IN {run_ids:Array(String)}",
            parameters={"org_id": org_id, "run_ids": drop_row_run_ids},
        )
        self.client.command(
            "ALTER TABLE work_unit_membership_runs "
            "DELETE WHERE org_id = {org_id:String} AND run_id IN {run_ids:Array(String)}",
            parameters={"org_id": org_id, "run_ids": drop_marker_run_ids},
        )
        logger.info(
            "Pruned %d old membership run generation(s) for org=%s (kept latest %d)",
            len(drop_marker_run_ids),
            org_id,
            keep,
        )
        return len(drop_marker_run_ids)

    def write_investment_explanation(self, record: InvestmentExplanationRecord) -> None:
        """Write or replace an investment explanation to the cache."""
        self._insert_rows(
            "investment_explanations",
            [
                "cache_key",
                "explanation_json",
                "llm_provider",
                "llm_model",
                "computed_at",
                "org_id",
            ],
            [record],
        )

    def read_investment_explanation(
        self, cache_key: str, org_id: str = ""
    ) -> InvestmentExplanationRecord | None:
        """
        Read a cached investment explanation by cache_key, scoped to ``org_id``.

        Uses FINAL to ensure we get the latest version from ReplacingMergeTree.
        Returns None if no cached explanation exists.

        The ``org_id`` predicate is required for tenant isolation: the cache_key
        is a 32-char SHA256 prefix and two tenants with identical
        filters/theme/subcategory would otherwise collide and read each other's
        cached LLM explanation (CHAOS-2393). The cache_key itself also includes
        org_id (see ``_compute_cache_key``); filtering here is defence in depth.
        """
        result = self.client.query(
            """
            SELECT
                cache_key,
                explanation_json,
                llm_provider,
                llm_model,
                computed_at,
                org_id
            FROM investment_explanations FINAL
            WHERE cache_key = {cache_key:String} AND org_id = {org_id:String}
            LIMIT 1
            """,
            parameters={"cache_key": cache_key, "org_id": org_id},
        )
        rows = result.result_rows or []
        if not rows:
            return None
        row = rows[0]
        return InvestmentExplanationRecord(
            cache_key=str(row[0]),
            explanation_json=str(row[1]),
            llm_provider=str(row[2]),
            llm_model=str(row[3]) if row[3] else None,
            computed_at=row[4]
            if isinstance(row[4], datetime)
            else datetime.now(timezone.utc),
            org_id=str(row[5]) if row[5] else "",
        )
