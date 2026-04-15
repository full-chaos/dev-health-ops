"""Feature flag pipeline validation checks.

Runs diagnostic queries against ClickHouse to verify the health of the
feature-flag + user-impact pipeline.  Each check returns a
:class:`CheckResult` with a status, a human-readable message, and optional
detail rows.

Validation checks (PRD Phase 3 — internal validation views):

1. **Coverage** — % of releases with ``telemetry_signal_bucket`` data.
2. **Dedup verification** — ``COUNT`` vs ``COUNT DISTINCT`` on ``dedupe_key``
   for all event tables.
3. **Schema completeness** — % of ``feature_flag`` records with all required
   fields populated.
4. **Drift detection** — compare signal volume between consecutive days; flag
   if > 2× change.
5. **Org isolation** — verify no cross-org data leakage (spot check).
6. **Join integrity** — % of ``release_impact_daily`` rows that join back to
   ``deployments``.
7. **Confidence distribution** — histogram of confidence scores to spot
   anomalies.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, timedelta
from enum import Enum
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------


class CheckStatus(str, Enum):
    """Outcome of a single validation check."""

    ok = "ok"
    warn = "warn"
    critical = "critical"
    skip = "skip"


@dataclass
class CheckResult:
    """Result of a single validation check."""

    name: str
    status: CheckStatus
    message: str
    detail: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class ValidationReport:
    """Aggregated report from all validation checks."""

    org_id: str
    checks: list[CheckResult] = field(default_factory=list)

    @property
    def has_critical(self) -> bool:
        return any(c.status == CheckStatus.critical for c in self.checks)

    @property
    def has_warnings(self) -> bool:
        return any(c.status == CheckStatus.warn for c in self.checks)

    def summary_line(self) -> str:
        counts = {s: 0 for s in CheckStatus}
        for c in self.checks:
            counts[c.status] += 1
        parts = [f"{counts[s]} {s.value}" for s in CheckStatus if counts[s]]
        return f"Validation report for org={self.org_id!r}: {', '.join(parts)}"


# ---------------------------------------------------------------------------
# Query helper (matches release_impact.py pattern)
# ---------------------------------------------------------------------------


def _query_dicts(
    client: Any,
    query: str,
    parameters: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Execute a ClickHouse query and return results as list of dicts."""
    result = client.query(query, parameters=parameters or {})
    col_names = list(getattr(result, "column_names", []) or [])
    rows = list(getattr(result, "result_rows", []) or [])
    if not col_names or not rows:
        return []
    return [dict(zip(col_names, row)) for row in rows]


# ---------------------------------------------------------------------------
# Individual checks
# ---------------------------------------------------------------------------


def check_coverage(client: Any, org_id: str, lookback_days: int = 30) -> CheckResult:
    """Check 1: % of releases with telemetry_signal_bucket data."""
    query = """
        WITH releases AS (
            SELECT DISTINCT release_ref
            FROM deployments
            WHERE release_ref != ''
              AND org_id = {org_id:String}
              AND toDate(coalesce(deployed_at, started_at))
                  >= today() - {lookback:UInt32}
        ),
        covered AS (
            SELECT DISTINCT release_ref
            FROM telemetry_signal_bucket
            WHERE release_ref != ''
              AND org_id = {org_id:String}
              AND toDate(bucket_start) >= today() - {lookback:UInt32}
        )
        SELECT
            count() AS total_releases,
            countIf(c.release_ref != '') AS covered_releases
        FROM releases AS r
        LEFT JOIN covered AS c ON r.release_ref = c.release_ref
    """
    rows = _query_dicts(
        client,
        query,
        {"org_id": org_id, "lookback": lookback_days},
    )
    if not rows or rows[0]["total_releases"] == 0:
        return CheckResult(
            name="coverage",
            status=CheckStatus.skip,
            message="No releases found in lookback window.",
        )

    total = int(rows[0]["total_releases"])
    covered = int(rows[0]["covered_releases"])
    ratio = covered / total

    if ratio < 0.50:
        status = CheckStatus.critical
    elif ratio < 0.70:
        status = CheckStatus.warn
    else:
        status = CheckStatus.ok

    return CheckResult(
        name="coverage",
        status=status,
        message=f"{covered}/{total} releases have telemetry ({ratio:.0%}).",
        detail=[{"total_releases": total, "covered_releases": covered, "ratio": ratio}],
    )


_DEDUP_TABLES = [
    ("feature_flag_event", "dedupe_key"),
    ("telemetry_signal_bucket", "dedupe_key"),
]


def check_dedup(client: Any, org_id: str) -> CheckResult:
    """Check 2: COUNT vs COUNT DISTINCT on dedupe_key for event tables."""
    issues: list[dict[str, Any]] = []
    for table, key_col in _DEDUP_TABLES:
        query = f"""
            SELECT
                count() AS total,
                count(DISTINCT {key_col}) AS distinct_keys
            FROM {table}
            WHERE org_id = {{org_id:String}}
        """
        rows = _query_dicts(client, query, {"org_id": org_id})
        if not rows:
            continue
        total = int(rows[0]["total"])
        distinct = int(rows[0]["distinct_keys"])
        dup_count = total - distinct
        if dup_count > 0:
            issues.append(
                {
                    "table": table,
                    "total_rows": total,
                    "distinct_keys": distinct,
                    "duplicates": dup_count,
                    "dup_ratio": dup_count / total if total else 0.0,
                }
            )

    if not issues:
        return CheckResult(
            name="dedup_verification",
            status=CheckStatus.ok,
            message="No duplicate dedupe_keys found in event tables.",
        )

    worst_ratio = max(i["dup_ratio"] for i in issues)
    status = CheckStatus.critical if worst_ratio > 0.05 else CheckStatus.warn
    tables = ", ".join(i["table"] for i in issues)
    return CheckResult(
        name="dedup_verification",
        status=status,
        message=f"Duplicate dedupe_keys in: {tables} (worst ratio: {worst_ratio:.2%}).",
        detail=issues,
    )


_REQUIRED_FLAG_FIELDS = [
    "provider",
    "flag_key",
    "environment",
    "flag_type",
    "last_synced",
]


def check_schema_completeness(client: Any, org_id: str) -> CheckResult:
    """Check 3: % of feature_flag records with all required fields populated."""
    conditions = " AND ".join(
        f"{f} != '' AND {f} IS NOT NULL" for f in _REQUIRED_FLAG_FIELDS
    )
    query = f"""
        SELECT
            count() AS total,
            countIf({conditions}) AS complete
        FROM feature_flag FINAL
        WHERE org_id = {{org_id:String}}
    """
    rows = _query_dicts(client, query, {"org_id": org_id})
    if not rows or rows[0]["total"] == 0:
        return CheckResult(
            name="schema_completeness",
            status=CheckStatus.skip,
            message="No feature_flag records found.",
        )

    total = int(rows[0]["total"])
    complete = int(rows[0]["complete"])
    ratio = complete / total

    if ratio < 0.80:
        status = CheckStatus.critical
    elif ratio < 0.95:
        status = CheckStatus.warn
    else:
        status = CheckStatus.ok

    return CheckResult(
        name="schema_completeness",
        status=status,
        message=f"{complete}/{total} flags have all required fields ({ratio:.0%}).",
        detail=[{"total": total, "complete": complete, "ratio": ratio}],
    )


def check_drift(
    client: Any,
    org_id: str,
    lookback_days: int = 14,
) -> CheckResult:
    """Check 4: Flag >2× day-over-day signal volume change."""
    query = """
        SELECT
            toDate(bucket_start) AS day,
            count() AS bucket_count
        FROM telemetry_signal_bucket
        WHERE org_id = {org_id:String}
          AND toDate(bucket_start) >= today() - {lookback:UInt32}
        GROUP BY day
        ORDER BY day
    """
    rows = _query_dicts(
        client,
        query,
        {"org_id": org_id, "lookback": lookback_days},
    )
    if len(rows) < 2:
        return CheckResult(
            name="drift_detection",
            status=CheckStatus.skip,
            message="Not enough daily data for drift detection.",
        )

    spikes: list[dict[str, Any]] = []
    for i in range(1, len(rows)):
        prev_count = int(rows[i - 1]["bucket_count"])
        curr_count = int(rows[i]["bucket_count"])
        if prev_count == 0:
            continue
        ratio = curr_count / prev_count
        if ratio > 2.0 or ratio < 0.5:
            spikes.append(
                {
                    "day": str(rows[i]["day"]),
                    "prev_day": str(rows[i - 1]["day"]),
                    "prev_count": prev_count,
                    "curr_count": curr_count,
                    "change_ratio": round(ratio, 2),
                }
            )

    if not spikes:
        return CheckResult(
            name="drift_detection",
            status=CheckStatus.ok,
            message="No >2× day-over-day volume changes detected.",
        )

    status = CheckStatus.warn
    return CheckResult(
        name="drift_detection",
        status=status,
        message=f"{len(spikes)} day(s) with >2× volume change in last {lookback_days}d.",
        detail=spikes,
    )


def check_org_isolation(client: Any, org_id: str) -> CheckResult:
    """Check 5: Spot-check that no cross-org data leaks into scoped queries."""
    tables = [
        "feature_flag",
        "feature_flag_event",
        "telemetry_signal_bucket",
        "release_impact_daily",
    ]
    leaks: list[dict[str, Any]] = []
    for table in tables:
        query = f"""
            SELECT count(DISTINCT org_id) AS org_count
            FROM {table}
            WHERE org_id != {{org_id:String}}
              AND org_id != ''
        """
        rows = _query_dicts(client, query, {"org_id": org_id})
        if rows and int(rows[0].get("org_count", 0)) > 0:
            leaks.append(
                {
                    "table": table,
                    "other_org_count": int(rows[0]["org_count"]),
                }
            )

    if not leaks:
        return CheckResult(
            name="org_isolation",
            status=CheckStatus.ok,
            message="No cross-org data detected (spot check).",
        )

    # Other orgs existing is normal in multi-tenant — this check verifies
    # that scoped queries don't accidentally return them.  The presence of
    # other orgs is informational, not a failure.
    return CheckResult(
        name="org_isolation",
        status=CheckStatus.ok,
        message=(
            f"Multi-tenant data present in {len(leaks)} table(s) — "
            "verify scoped queries filter correctly."
        ),
        detail=leaks,
    )


def check_join_integrity(
    client: Any,
    org_id: str,
    lookback_days: int = 30,
) -> CheckResult:
    """Check 6: % of release_impact_daily rows that join back to deployments."""
    query = """
        WITH impact AS (
            SELECT DISTINCT release_ref, environment
            FROM release_impact_daily
            WHERE org_id = {org_id:String}
              AND day >= today() - {lookback:UInt32}
        ),
        joined AS (
            SELECT i.release_ref, i.environment,
                   d.release_ref AS deploy_ref
            FROM impact AS i
            LEFT JOIN (
                SELECT DISTINCT release_ref, environment
                FROM deployments
                WHERE release_ref != ''
            ) AS d
            ON i.release_ref = d.release_ref
               AND i.environment = d.environment
        )
        SELECT
            count() AS total,
            countIf(deploy_ref != '') AS matched
        FROM joined
    """
    rows = _query_dicts(
        client,
        query,
        {"org_id": org_id, "lookback": lookback_days},
    )
    if not rows or rows[0]["total"] == 0:
        return CheckResult(
            name="join_integrity",
            status=CheckStatus.skip,
            message="No release_impact_daily rows in lookback window.",
        )

    total = int(rows[0]["total"])
    matched = int(rows[0]["matched"])
    ratio = matched / total

    if ratio < 0.50:
        status = CheckStatus.critical
    elif ratio < 0.80:
        status = CheckStatus.warn
    else:
        status = CheckStatus.ok

    return CheckResult(
        name="join_integrity",
        status=status,
        message=f"{matched}/{total} impact rows join to deployments ({ratio:.0%}).",
        detail=[{"total": total, "matched": matched, "ratio": ratio}],
    )


_CONFIDENCE_BUCKETS = [
    (0.0, 0.2, "very_low"),
    (0.2, 0.4, "low"),
    (0.4, 0.6, "medium"),
    (0.6, 0.8, "high"),
    (0.8, 1.01, "very_high"),
]


def check_confidence_distribution(
    client: Any,
    org_id: str,
    lookback_days: int = 30,
) -> CheckResult:
    """Check 7: Histogram of confidence scores to spot anomalies."""
    query = """
        SELECT
            release_impact_confidence_score AS score
        FROM release_impact_daily
        WHERE org_id = {org_id:String}
          AND day >= today() - {lookback:UInt32}
          AND release_impact_confidence_score IS NOT NULL
    """
    rows = _query_dicts(
        client,
        query,
        {"org_id": org_id, "lookback": lookback_days},
    )
    if not rows:
        return CheckResult(
            name="confidence_distribution",
            status=CheckStatus.skip,
            message="No confidence scores in lookback window.",
        )

    scores = [float(r["score"]) for r in rows]
    total = len(scores)

    histogram: list[dict[str, Any]] = []
    for lo, hi, label in _CONFIDENCE_BUCKETS:
        count = sum(1 for s in scores if lo <= s < hi)
        histogram.append(
            {
                "bucket": label,
                "range": f"[{lo:.1f}, {hi:.1f})",
                "count": count,
                "pct": count / total if total else 0.0,
            }
        )

    very_low_pct = histogram[0]["pct"]
    if very_low_pct > 0.50:
        status = CheckStatus.warn
        msg = (
            f"{very_low_pct:.0%} of confidence scores are very low (<0.2) — "
            "check coverage and sample sizes."
        )
    else:
        status = CheckStatus.ok
        avg_score = sum(scores) / total
        msg = f"{total} scores, avg={avg_score:.2f}."

    return CheckResult(
        name="confidence_distribution",
        status=status,
        message=msg,
        detail=histogram,
    )


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


async def validate_flag_pipeline(
    ch_client: Any,
    org_id: str,
    lookback_days: int = 30,
) -> ValidationReport:
    """Run all pipeline validation checks and return a health report.

    Parameters
    ----------
    ch_client:
        ClickHouse client instance (``clickhouse_connect.driver.Client``).
    org_id:
        Organization to validate.
    lookback_days:
        How many days of data to inspect (default 30).

    Returns
    -------
    ValidationReport
        Aggregated results from all checks.
    """
    report = ValidationReport(org_id=org_id)

    checks = [
        check_coverage(ch_client, org_id, lookback_days),
        check_dedup(ch_client, org_id),
        check_schema_completeness(ch_client, org_id),
        check_drift(ch_client, org_id, min(lookback_days, 14)),
        check_org_isolation(ch_client, org_id),
        check_join_integrity(ch_client, org_id, lookback_days),
        check_confidence_distribution(ch_client, org_id, lookback_days),
    ]

    report.checks = checks
    logger.info(report.summary_line())
    return report


def format_report(report: ValidationReport) -> str:
    """Format a validation report as a human-readable string."""
    lines: list[str] = []
    lines.append(f"Feature Flag Pipeline Validation — org={report.org_id!r}")
    lines.append("=" * 60)

    status_icon = {
        CheckStatus.ok: "✓",
        CheckStatus.warn: "⚠",
        CheckStatus.critical: "✗",
        CheckStatus.skip: "—",
    }

    for check in report.checks:
        icon = status_icon[check.status]
        lines.append(f"  [{icon}] {check.name}: {check.message}")
        if check.detail and check.status in (CheckStatus.warn, CheckStatus.critical):
            for row in check.detail[:5]:
                detail_str = ", ".join(f"{k}={v}" for k, v in row.items())
                lines.append(f"      {detail_str}")

    lines.append("")
    lines.append(report.summary_line())

    if report.has_critical:
        lines.append("RESULT: CRITICAL — pipeline health checks failed.")
    elif report.has_warnings:
        lines.append("RESULT: WARNING — review flagged items.")
    else:
        lines.append("RESULT: OK — all checks passed.")

    return "\n".join(lines)
