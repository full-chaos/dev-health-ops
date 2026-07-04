"""MetricsLoader protocol + ClickHouseMetricsLoader implementation.

The ``MetricsLoader`` is the hexagonal-architecture boundary between the
rule engine and the analytics backend.  Rule evaluators receive a
``MetricsSnapshot`` — a pure, frozen dataclass — so they are completely
decoupled from I/O.

ClickHouseMetricsLoader
-----------------------
Reads from the **canonical metric tables** following the
``argMax(…, computed_at)`` append-only read pattern used throughout the
codebase (see ``metrics/operating_review.py``).

Tables used
~~~~~~~~~~~
* ``work_item_metrics_daily``   — wip, throughput, cycle time
* ``team_metrics_daily``        — after-hours commit ratio
* ``user_metrics_daily``        — review load per reviewer (Gini input)
* ``repo_metrics_daily``        — rework ratio, p75 PR cycle time
* ``repo_complexity_daily``     — cyclomatic complexity
* ``file_hotspot_daily``        — high-risk hotspot files
"""

from __future__ import annotations

import json
import math
from datetime import date, timedelta
from typing import Any, Protocol, runtime_checkable

from dev_health_ops.recommendations.snapshot import (
    MetricsSnapshot,
    RecommendationRecord,
)

# ---------------------------------------------------------------------------
# MetricsLoader protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class MetricsLoader(Protocol):
    """Port (hexagonal architecture) for loading team metrics snapshots.

    Implementations
    ---------------
    * ``ClickHouseMetricsLoader`` — production backend.
    * Tests: pass any object implementing ``load_team_metrics_window``.
    """

    def load_team_metrics_window(
        self,
        team_id: str,
        org_id: str,
        window_start: date,
        window_end: date,
    ) -> MetricsSnapshot:
        """Return a frozen ``MetricsSnapshot`` for the given team and window.

        Missing data fields are ``None`` / empty list — never raised.
        """
        raise NotImplementedError


# ---------------------------------------------------------------------------
# ClickHouseMetricsLoader
# ---------------------------------------------------------------------------


def _gini(values: list[float]) -> float | None:
    """Gini coefficient of *values*; ``None`` when < 2 positive entries."""
    positives = [v for v in values if v > 0]
    if len(positives) < 2:
        return None
    total = sum(positives)
    if total == 0.0:
        return 0.0
    n = len(positives)
    sorted_vals = sorted(positives)
    cumulative = sum((i + 1) * v for i, v in enumerate(sorted_vals))
    return (2.0 * cumulative) / (n * total) - (n + 1.0) / n


def _safe_float(value: Any) -> float | None:
    """Coerce *value* to float; return ``None`` for ``None`` or NaN."""
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(f) else f


class ClickHouseMetricsLoader:
    """Production ``MetricsLoader`` backed by ClickHouse.

    Args:
        client:  Synchronous ``clickhouse_connect`` client.
        org_id:  Organisation ID for multi-tenant scoping.
    """

    def __init__(self, client: Any, org_id: str = "") -> None:
        self._client = client
        self._org_id = org_id

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _qd(self, query: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        result = self._client.query(query, parameters=params)
        col_names = list(getattr(result, "column_names", []) or [])
        rows = list(getattr(result, "result_rows", []) or [])
        if not col_names or not rows:
            return []
        return [dict(zip(col_names, row)) for row in rows]

    def _oc(self) -> str:
        return "AND org_id = %(org_id)s" if self._org_id else ""

    def _p(self, team_id: str, ws: date, we: date) -> dict[str, Any]:
        p: dict[str, Any] = {"team_id": team_id, "start": ws, "end": we}
        if self._org_id:
            p["org_id"] = self._org_id
        return p

    # ------------------------------------------------------------------
    # Signal loaders
    # ------------------------------------------------------------------

    def _load_wip_throughput(
        self, team_id: str, ws: date, we: date
    ) -> tuple[list[float], list[float]]:
        oc = self._oc()
        q = f"""
            SELECT day, sum(wip) AS wip_total, sum(completed) AS tp_total
            FROM (
                SELECT day,
                       argMax(wip_count_end_of_day, computed_at) AS wip,
                       argMax(items_completed, computed_at) AS completed
                FROM work_item_metrics_daily
                WHERE team_id = %(team_id)s
                  AND day >= %(start)s AND day < %(end)s {oc}
                GROUP BY day, provider, work_scope_id
            )
            GROUP BY day ORDER BY day
        """
        rows = self._qd(q, self._p(team_id, ws, we))
        wip = [float(r.get("wip_total") or 0.0) for r in rows]
        tp = [float(r.get("tp_total") or 0.0) for r in rows]
        return wip, tp

    def _load_review_signals(
        self, team_id: str, ws: date, we: date
    ) -> tuple[float | None, float | None]:
        oc = self._oc()
        params = self._p(team_id, ws, we)

        # p75 PR cycle time across repos
        q_lat = f"""
            SELECT avg(p75) AS avg_p75
            FROM (
                SELECT repo_id, argMax(pr_cycle_p75_hours, computed_at) AS p75
                FROM repo_metrics_daily
                WHERE day >= %(start)s AND day < %(end)s {oc}
                GROUP BY repo_id
            )
        """
        lat_rows = self._qd(q_lat, params)
        review_latency = _safe_float(lat_rows[0].get("avg_p75") if lat_rows else None)

        # Reviewer Gini from user_metrics_daily
        q_gini = f"""
            SELECT author_email, sum(rev) AS total_reviews
            FROM (
                SELECT repo_id, author_email, day,
                       argMax(reviews_given, computed_at) AS rev
                FROM user_metrics_daily
                WHERE team_id = %(team_id)s
                  AND day >= %(start)s AND day < %(end)s {oc}
                GROUP BY repo_id, author_email, day
            )
            GROUP BY author_email
        """
        gini_rows = self._qd(q_gini, params)
        loads = [float(r.get("total_reviews") or 0.0) for r in gini_rows]
        return review_latency, _gini(loads)

    def _load_rework_ratio(self, team_id: str, ws: date, we: date) -> float | None:
        oc = self._oc()
        q = f"""
            SELECT avg(rework) AS avg_rework
            FROM (
                SELECT repo_id, argMax(pr_rework_ratio, computed_at) AS rework
                FROM repo_metrics_daily
                WHERE day >= %(start)s AND day < %(end)s {oc}
                GROUP BY repo_id
            )
        """
        rows = self._qd(q, self._p(team_id, ws, we))
        return _safe_float(rows[0].get("avg_rework") if rows else None)

    def _load_sustainability_signals(
        self, team_id: str, ws: date, we: date
    ) -> tuple[float | None, list[float]]:
        oc = self._oc()
        params = self._p(team_id, ws, we)

        # Average after_hours_commit_ratio over window
        q_ah = f"""
            SELECT avg(ratio) AS avg_ratio
            FROM (
                SELECT day, argMax(after_hours_commit_ratio, computed_at) AS ratio
                FROM team_metrics_daily
                WHERE team_id = %(team_id)s
                  AND day >= %(start)s AND day < %(end)s {oc}
                GROUP BY day
            )
        """
        ah_rows = self._qd(q_ah, params)
        after_hours = _safe_float(ah_rows[0].get("avg_ratio") if ah_rows else None)

        # Cycle time per day (avg across scopes)
        q_ct = f"""
            SELECT day, avg(ct) AS avg_ct
            FROM (
                SELECT day, provider, work_scope_id,
                       argMax(cycle_time_p50_hours, computed_at) AS ct
                FROM work_item_metrics_daily
                WHERE team_id = %(team_id)s
                  AND day >= %(start)s AND day < %(end)s {oc}
                GROUP BY day, provider, work_scope_id
            )
            GROUP BY day ORDER BY day
        """
        ct_rows = self._qd(q_ct, params)
        cycle_times = [
            float(r["avg_ct"]) for r in ct_rows if r.get("avg_ct") is not None
        ]
        return after_hours, cycle_times

    def _load_compounding_signals(
        self, team_id: str, ws: date, we: date
    ) -> tuple[float | None, float | None]:
        oc = self._oc()
        mid = ws + timedelta(days=max(1, (we - ws).days // 2))
        params = {**self._p(team_id, ws, we), "mid": mid}

        # Complexity delta: second half avg vs first half avg
        q_cpx = f"""
            SELECT
                avg(if(day < %(mid)s, cpk, NULL)) AS first_half,
                avg(if(day >= %(mid)s, cpk, NULL)) AS second_half
            FROM (
                SELECT day, repo_id,
                       argMax(cyclomatic_per_kloc, computed_at) AS cpk
                FROM repo_complexity_daily
                WHERE day >= %(start)s AND day < %(end)s {oc}
                GROUP BY day, repo_id
            )
        """
        cpx_rows = self._qd(q_cpx, params)
        complexity_delta: float | None = None
        if cpx_rows:
            first = _safe_float(cpx_rows[0].get("first_half"))
            second = _safe_float(cpx_rows[0].get("second_half"))
            if first is not None and second is not None:
                complexity_delta = (second - first) / max(first, 1.0)

        # Hotspot count in second half (files with risk_score > 0)
        q_hs = f"""
            SELECT count(DISTINCT file_path) AS total
            FROM (
                SELECT file_path, argMax(risk_score, computed_at) AS risk_score
                FROM file_hotspot_daily
                WHERE day >= %(mid)s AND day < %(end)s {oc}
                GROUP BY file_path
            ) WHERE risk_score > 0
        """
        hs_rows = self._qd(q_hs, params)
        total_hotspots = int(hs_rows[0]["total"]) if hs_rows else 0

        churn_overlap: float | None = None
        if total_hotspots > 0:
            # Proxy: if overall complexity is rising, treat overlap as the
            # normalised delta (capped at 1.0). Full file-level join is a
            # follow-up optimisation.
            churn_overlap = (
                min(1.0, max(0.0, complexity_delta))
                if complexity_delta is not None and complexity_delta > 0
                else 0.0
            )

        return complexity_delta, churn_overlap

    def _load_compounding_risk_persisted(
        self, team_id: str, ws: date, we: date
    ) -> tuple[float | None, str | None]:
        """Read the persisted Compounding Risk score for the team from
        ``compounding_risk_daily`` (CHAOS-1641).

        Returns ``(score, severity)`` from the latest team-scope row in the
        window. Returns ``(None, None)`` when no row is available (e.g. the
        backfill has not yet run), in which case the rule falls back to the
        legacy hotspot proxy.
        """
        oc = self._oc()
        params = self._p(team_id, ws, we)
        query = f"""
            SELECT
                tupleElement(latest_row, 1) AS score,
                tupleElement(latest_row, 2) AS severity
            FROM (
                SELECT argMax(tuple(compounding_risk, severity), computed_at) AS latest_row
                FROM compounding_risk_daily
                WHERE scope = 'team'
                  AND scope_id = %(team_id)s
                  AND day >= %(start)s AND day < %(end)s {oc}
            )
        """
        rows = self._qd(query, params)
        if not rows:
            return None, None
        score = _safe_float(rows[0].get("score"))
        severity = rows[0].get("severity")
        return score, (str(severity) if severity else None)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load_team_metrics_window(
        self,
        team_id: str,
        org_id: str,
        window_start: date,
        window_end: date,
    ) -> MetricsSnapshot:
        """Load all signals for *team_id* in ``[window_start, window_end)``."""
        prev_org = self._org_id
        if org_id:
            self._org_id = org_id
        try:
            wip, throughput = self._load_wip_throughput(
                team_id, window_start, window_end
            )
            rev_lat, rev_gini = self._load_review_signals(
                team_id, window_start, window_end
            )
            rework = self._load_rework_ratio(team_id, window_start, window_end)
            after_hours, cycle_times = self._load_sustainability_signals(
                team_id, window_start, window_end
            )
            complexity_delta, churn_overlap = self._load_compounding_signals(
                team_id, window_start, window_end
            )
            compounding_score, compounding_severity = (
                self._load_compounding_risk_persisted(team_id, window_start, window_end)
            )
        finally:
            self._org_id = prev_org

        return MetricsSnapshot(
            team_id=team_id,
            org_id=org_id,
            window_start=window_start,
            window_end=window_end,
            wip_by_day=wip,
            throughput_by_cycle=throughput,
            review_latency_p75_hours=rev_lat,
            reviewer_gini=rev_gini,
            rework_churn_ratio=rework,
            after_hours_ratio=after_hours,
            cycle_time_by_day=cycle_times,
            hotspot_complexity_delta=complexity_delta,
            hotspot_churn_overlap=churn_overlap,
            compounding_risk_score=compounding_score,
            compounding_risk_severity=compounding_severity,
        )


# ---------------------------------------------------------------------------
# Helper — Recommendation → RecommendationRecord
# ---------------------------------------------------------------------------


def recommendation_to_record(
    rec: Any, rule_version: str = "1.0.0"
) -> RecommendationRecord:
    """Convert a ``Recommendation`` to a ``RecommendationRecord`` for the sink."""
    evidence_list = [
        {
            "team_id": e.team_id,
            "metric_table": e.metric_table,
            "window_start": e.window_start.isoformat(),
            "window_end": e.window_end.isoformat(),
            "field": e.field,
            "value": e.value,
        }
        for e in rec.evidence
    ]
    return RecommendationRecord(
        team_id=rec.team_id,
        org_id=rec.org_id,
        rule_id=rec.rule_id,
        rule_version=rule_version,
        window_start=rec.window_start,
        window_end=rec.window_end,
        fired=True,
        severity=rec.severity,
        title=rec.title,
        rationale=rec.rationale,
        success_criterion=rec.success_criterion,
        evidence_json=json.dumps(evidence_list),
        computed_at=rec.computed_at,
    )
