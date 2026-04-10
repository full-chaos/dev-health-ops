from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ReportAuditRecord:
    audit_id: str
    plan_id: str
    org_id: str
    report_type: str
    metrics_requested: list[str]
    metrics_available: list[str]
    metrics_unavailable: list[str]
    insights_generated: int
    insights_filtered: int
    provenance_violations: int
    confidence_threshold: str
    generated_at: datetime
    duration_seconds: float


def log_report_audit(record: ReportAuditRecord) -> None:
    logger.info(
        "Report audit: plan_id=%s type=%s metrics_req=%d avail=%d unavail=%d "
        "insights=%d filtered=%d violations=%d threshold=%s duration=%.2fs",
        record.plan_id,
        record.report_type,
        len(record.metrics_requested),
        len(record.metrics_available),
        len(record.metrics_unavailable),
        record.insights_generated,
        record.insights_filtered,
        record.provenance_violations,
        record.confidence_threshold,
        record.duration_seconds,
    )
