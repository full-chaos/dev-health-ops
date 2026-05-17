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
from typing import TYPE_CHECKING, Any

from dev_health_ops.metrics.schemas import (
    InvestmentClassificationRecord,
    InvestmentExplanationRecord,
    InvestmentMetricsRecord,
    IssueTypeMetricsRecord,
    WorkUnitInvestmentEvidenceQuoteRecord,
    WorkUnitInvestmentRecord,
)
from dev_health_ops.metrics.sinks.clickhouse._insert import DEFAULT_BATCH_SIZE

logger = logging.getLogger(__name__)


class InvestmentMixin:
    """Mixin for investment and work-unit classification write methods."""

    if TYPE_CHECKING:
        client: Any
        org_id: str

        def _insert_rows(
            self,
            table: str,
            columns: list[str],
            rows: Any,
            batch_size: int = DEFAULT_BATCH_SIZE,
        ) -> None: ...

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
        self, cache_key: str
    ) -> InvestmentExplanationRecord | None:
        """
        Read a cached investment explanation by cache_key.

        Uses FINAL to ensure we get the latest version from ReplacingMergeTree.
        Returns None if no cached explanation exists.
        """
        result = self.client.query(
            """
            SELECT
                cache_key,
                explanation_json,
                llm_provider,
                llm_model,
                computed_at
            FROM investment_explanations FINAL
            WHERE cache_key = {cache_key:String}
            LIMIT 1
            """,
            parameters={"cache_key": cache_key},
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
        )
