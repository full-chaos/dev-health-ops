from __future__ import annotations

import json
import logging
from datetime import datetime, time, timezone
from typing import Dict, Iterable, List, Optional, Tuple

from ..models.filters import MetricFilter
from ..models.schemas import (
    EvidenceQuality,
    InvestmentBreakdown,
    WorkUnitEvidence,
    WorkUnitEffort,
    WorkUnitInvestment,
    WorkUnitTimeRange,
)
from ..queries.client import clickhouse_client
from ..queries.work_unit_investments import (
    fetch_work_unit_investment_quotes,
    fetch_work_unit_investments,
    fetch_repo_scopes,
    fetch_work_item_team_assignments,
)
from .filtering import resolve_repo_filter_ids, time_window

logger = logging.getLogger(__name__)


def _ensure_utc(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _clean_optional_text(value: object) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _split_category_filters(filters: MetricFilter) -> Tuple[List[str], List[str]]:
    themes: List[str] = []
    subcategories: List[str] = []
    for category in filters.why.work_category or []:
        if not category:
            continue
        category_str = str(category).strip()
        if not category_str:
            continue
        if "." in category_str:
            subcategories.append(category_str)
            themes.append(category_str.split(".", 1)[0])
        else:
            themes.append(category_str)
    return list(dict.fromkeys(themes)), list(dict.fromkeys(subcategories))


def _parse_distribution(value: object) -> Dict[str, float]:
    if isinstance(value, dict):
        return {str(k): float(v or 0.0) for k, v in value.items()}
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        if isinstance(parsed, dict):
            return {str(k): float(v or 0.0) for k, v in parsed.items()}
    return {}


def _extract_issue_ids(structural_payload: object) -> List[str]:
    if not structural_payload:
        return []
    try:
        parsed = (
            json.loads(structural_payload)
            if isinstance(structural_payload, str)
            else structural_payload
        )
    except Exception:
        return []
    if not isinstance(parsed, dict):
        return []
    issues = parsed.get("issues")
    if not isinstance(issues, list):
        return []
    return [str(item) for item in issues if item]


def _majority_team_for_issues(
    issue_ids: Iterable[str],
    team_map: Dict[str, Dict[str, str]],
) -> Tuple[str, str]:
    counts: Dict[str, int] = {}
    names: Dict[str, str] = {}
    for issue_id in issue_ids:
        assignment = team_map.get(str(issue_id)) or {}
        team_id = str(assignment.get("team_id") or "").strip()
        team_name = str(assignment.get("team_name") or "").strip()
        if not team_id:
            continue
        counts[team_id] = counts.get(team_id, 0) + 1
        if team_name:
            names.setdefault(team_id, team_name)
    if not counts:
        return "unassigned", "Unassigned"
    team_id = max(counts.items(), key=lambda item: (item[1], item[0]))[0]
    return team_id, names.get(team_id) or team_id


def _matches_category_filter(
    theme_distribution: Dict[str, float],
    subcategory_distribution: Dict[str, float],
    themes: Iterable[str],
    subcategories: Iterable[str],
) -> bool:
    theme_set = set(themes)
    subcategory_set = set(subcategories)
    if not theme_set and not subcategory_set:
        return True
    if subcategory_set:
        for key, value in subcategory_distribution.items():
            if key in subcategory_set and value > 0:
                return True
    if theme_set:
        for key, value in theme_distribution.items():
            if key in theme_set and value > 0:
                return True
    return False


async def build_work_unit_investments(
    *,
    db_url: str,
    filters: MetricFilter,
    limit: int = 200,
    include_text: bool = True,
    work_unit_id: Optional[str] = None,
) -> List[WorkUnitInvestment]:
    start_day, end_day, _, _ = time_window(filters)
    start_ts = datetime.combine(start_day, time.min, tzinfo=timezone.utc)
    end_ts = datetime.combine(end_day, time.min, tzinfo=timezone.utc)
    theme_filters, subcategory_filters = _split_category_filters(filters)

    repo_scopes: Dict[str, str] = {}
    team_assignments: Dict[str, Dict[str, str]] = {}

    async with clickhouse_client(db_url) as sink:
        repo_ids = await resolve_repo_filter_ids(sink, filters)
        rows = await fetch_work_unit_investments(
            sink,
            start_ts=start_ts,
            end_ts=end_ts,
            repo_ids=repo_ids or None,
            limit=max(1, int(limit)),
            work_unit_id=work_unit_id,
        )

        if not rows:
            return []

        if theme_filters or subcategory_filters:
            filtered_rows = []
            for row in rows:
                theme_distribution = _parse_distribution(
                    row.get("theme_distribution_json")
                )
                subcategory_distribution = _parse_distribution(
                    row.get("subcategory_distribution_json")
                )
                if _matches_category_filter(
                    theme_distribution,
                    subcategory_distribution,
                    theme_filters,
                    subcategory_filters,
                ):
                    filtered_rows.append(row)
            rows = filtered_rows

        quote_rows: List[Dict[str, object]] = []
        if include_text:
            unit_runs = [
                (str(row.get("work_unit_id")), str(row.get("categorization_run_id")))
                for row in rows
                if row.get("work_unit_id") and row.get("categorization_run_id")
            ]
            quote_rows = await fetch_work_unit_investment_quotes(
                sink, unit_runs=unit_runs
            )

        repo_id_values = [
            str(row.get("repo_id") or "") for row in rows if row.get("repo_id")
        ]
        repo_scopes = await fetch_repo_scopes(sink, repo_ids=repo_id_values)

        issue_ids: List[str] = []
        for row in rows:
            issue_ids.extend(_extract_issue_ids(row.get("structural_evidence_json")))
        team_assignments = await fetch_work_item_team_assignments(
            sink, work_item_ids=issue_ids
        )

    quotes_by_unit: Dict[str, List[Dict[str, object]]] = {}
    for quote in quote_rows:
        work_unit = str(quote.get("work_unit_id") or "")
        if not work_unit:
            continue
        quotes_by_unit.setdefault(work_unit, []).append(quote)

    results: List[WorkUnitInvestment] = []
    for row in rows:
        unit_id = str(row.get("work_unit_id") or "")
        if not unit_id:
            continue
        from_ts = _ensure_utc(row.get("from_ts")) or start_ts
        to_ts = _ensure_utc(row.get("to_ts")) or end_ts
        theme_distribution = _parse_distribution(row.get("theme_distribution_json"))
        subcategory_distribution = _parse_distribution(
            row.get("subcategory_distribution_json")
        )
        effort_metric = str(row.get("effort_metric") or "churn_loc")
        effort_value = float(row.get("effort_value") or 0.0)

        structural_evidence: List[Dict[str, object]] = []
        structural_payload = row.get("structural_evidence_json")
        if structural_payload:
            try:
                parsed = json.loads(structural_payload)
                if isinstance(parsed, dict):
                    structural_evidence.append({"type": "work_unit_nodes", **parsed})
            except json.JSONDecodeError:
                logger.warning(
                    "Failed to decode structural_evidence_json for work_unit_id %s",
                    unit_id,
                )

        textual_evidence: List[Dict[str, object]] = []
        for quote in quotes_by_unit.get(unit_id, []):
            textual_evidence.append(
                {
                    "type": "evidence_quote",
                    "quote": quote.get("quote"),
                    "source": quote.get("source_type"),
                    "id": quote.get("source_id"),
                }
            )

        span_days = max(0.0, (to_ts - from_ts).total_seconds() / 86400.0)
        contextual_evidence = [
            {
                "type": "time_range",
                "start": from_ts.isoformat(),
                "end": to_ts.isoformat(),
                "span_days": span_days,
            }
        ]

        repo_scope = "unassigned"
        repo_id = row.get("repo_id")
        if repo_id:
            repo_id_str = str(repo_id)
            repo_scope = repo_scopes.get(repo_id_str) or repo_id_str or "unassigned"
        contextual_evidence.append({"type": "repo_scope", "repo_ids": [repo_scope]})

        unit_issue_ids = _extract_issue_ids(structural_payload)
        team_id, team_name = _majority_team_for_issues(unit_issue_ids, team_assignments)
        contextual_evidence.append(
            {
                "type": "team_scope",
                "team_ids": [team_id],
                "team_names": [team_name],
            }
        )

        raw_quality = row.get("evidence_quality")
        evidence_quality_value = float(raw_quality) if raw_quality is not None else None
        raw_band = row.get("evidence_quality_band")
        evidence_band = (
            str(raw_band)
            if raw_band
            else ("unknown" if raw_quality is None else "very_low")
        )

        results.append(
            WorkUnitInvestment(
                work_unit_id=unit_id,
                work_unit_type=_clean_optional_text(row.get("work_unit_type")),
                work_unit_name=_clean_optional_text(row.get("work_unit_name")),
                time_range=WorkUnitTimeRange(start=from_ts, end=to_ts),
                effort=WorkUnitEffort(metric=effort_metric, value=effort_value),
                investment=InvestmentBreakdown(
                    themes=theme_distribution,
                    subcategories=subcategory_distribution,
                ),
                evidence_quality=EvidenceQuality(
                    value=evidence_quality_value,
                    band=evidence_band,
                ),
                evidence=WorkUnitEvidence(
                    textual=textual_evidence,
                    structural=structural_evidence,
                    contextual=contextual_evidence,
                ),
            )
        )

    results.sort(key=lambda item: item.effort.value, reverse=True)
    return results[: max(1, int(limit))]
