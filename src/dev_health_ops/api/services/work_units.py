from __future__ import annotations

import json
import logging
import re
from collections.abc import Iterable
from datetime import datetime, time, timezone
from typing import Any, Literal

from dev_health_ops.external_ingest.ids import derive_work_item_id

from ..models.filters import MetricFilter
from ..models.schemas import (
    EvidenceQuality,
    InvestmentBreakdown,
    WorkUnitEffort,
    WorkUnitEvidence,
    WorkUnitInvestment,
    WorkUnitTimeRange,
)
from ..queries.client import clickhouse_client, require_clickhouse_backend
from ..queries.work_unit_investments import (
    fetch_repo_identities,
    fetch_repo_scopes,
    fetch_work_item_team_assignments,
    fetch_work_unit_investment_quotes,
    fetch_work_unit_investments,
)
from .filtering import resolve_repo_filter_ids, time_window
from .provenance import warn_once_for_mock_fixture_rows

logger = logging.getLogger(__name__)

EffortMetric = Literal["churn_loc", "active_hours"]
EvidenceQualityBand = Literal["high", "moderate", "low", "very_low", "unknown"]


def _ensure_utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _clean_optional_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _effort_metric(value: object) -> EffortMetric:
    return "active_hours" if value == "active_hours" else "churn_loc"


def _evidence_quality_band(value: object) -> EvidenceQualityBand:
    text = str(value or "")
    if text == "high":
        return "high"
    if text == "moderate":
        return "moderate"
    if text == "low":
        return "low"
    if text == "very_low":
        return "very_low"
    return "unknown"


def _split_category_filters(filters: MetricFilter) -> tuple[list[str], list[str]]:
    themes: list[str] = []
    subcategories: list[str] = []
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


def _parse_distribution(value: object) -> dict[str, float]:
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


def _parse_structural_payload(structural_payload: object) -> dict[str, Any] | None:
    if not structural_payload:
        return None
    try:
        parsed = (
            json.loads(structural_payload)
            if isinstance(structural_payload, str)
            else structural_payload
        )
    except Exception:
        return None
    return parsed if isinstance(parsed, dict) else None


def _extract_issue_ids(structural_payload: object) -> list[str]:
    parsed = _parse_structural_payload(structural_payload)
    if parsed is None:
        return []
    issues = parsed.get("issues")
    if not isinstance(issues, list):
        return []
    return [str(item) for item in issues if item]


# `{repo_uuid}#pr{number}` -- work_graph/ids.py:generate_pr_id. Anchored so an
# `issues` entry can never be mistaken for a PR ref.
_PR_EVIDENCE_REF_RE = re.compile(r"^([0-9a-fA-F-]{36})#pr(\d+)$")


def _extract_pr_refs(structural_payload: object) -> list[str]:
    """CHAOS-2416: the `prs` array is the second bridge from a work unit to a
    team, alongside `issues`. This mirrors the `unit_team` SQL CTE in
    `api/queries/investment.py`; the Sankey and this drilldown must agree on
    which units have a team, so the two must be changed together."""
    parsed = _parse_structural_payload(structural_payload)
    if parsed is None:
        return []
    prs = parsed.get("prs")
    if not isinstance(prs, list):
        return []
    return [str(item) for item in prs if item]


def _pr_ref_work_item_id(
    pr_ref: str, repo_identities: dict[str, tuple[str, str]]
) -> str | None:
    """Resolve a `prs` evidence ref into the work_items id space.

    Returns None when the ref is not PR-shaped or its repo is unknown -- the
    drilldown must not guess a work-item id the resolver never minted. Mirrors
    the `multiIf` arms of `RESOLVED_EVIDENCE_WORK_ITEM_ID`.
    """
    match = _PR_EVIDENCE_REF_RE.match(pr_ref)
    if match is None:
        return None
    identity = repo_identities.get(match.group(1))
    if identity is None:
        return None
    slug, provider = identity
    if not slug:
        return None
    return derive_work_item_id(
        system="gitlab" if provider == "gitlab" else "github",
        instance=slug,
        external_key=match.group(2),
        work_item_type="merge_request" if provider == "gitlab" else "pr",
    )


def _majority_team_for_issues(
    issue_ids: Iterable[str],
    team_map: dict[str, dict[str, str]],
) -> tuple[str, str]:
    """Pick a unit's team from its evidence: most-cited team wins.

    CHAOS-2416: this MUST agree with `build_unit_team_subquery` for every
    unit, or the Sankey and this drilldown name different teams for the same
    work unit -- the divergence the PR bridge exists to close. Three rules are
    shared verbatim with the SQL:

    * votes are counted per TEAM ID, never per rendered label. Two attribution
      rows can spell one team_id with different team_names, and counting by
      label would split that single team into two candidates on one side only.
    * the label is ``max(label)`` over the winning id's refs, so it is a
      deterministic function of the id rather than of row order (the SQL takes
      ``max()`` in the same place).
    * ties break on (count, TEAM ID), matching
      ``argMax(resolved_team, (cnt, resolved_team_id))``. Ties are ordinary
      now that a unit can reach one team through an issue and another through
      a PR.

    Refs are de-duplicated first, mirroring the SQL's ``arrayDistinct`` over
    the combined issues+prs array: a duplicated ref in persisted evidence must
    not vote twice here and once there.
    """
    counts: dict[str, int] = {}
    labels: dict[str, str] = {}
    for issue_id in dict.fromkeys(str(i) for i in issue_ids):
        assignment = team_map.get(issue_id) or {}
        team_id = str(assignment.get("team_id") or "").strip()
        team_name = str(assignment.get("team_name") or "").strip()
        if not team_id:
            continue
        counts[team_id] = counts.get(team_id, 0) + 1
        label = team_name or team_id
        if label > labels.get(team_id, ""):
            labels[team_id] = label
    if not counts:
        return "unassigned", "Unassigned"
    team_id = max(counts.items(), key=lambda item: (item[1], item[0]))[0]
    return team_id, labels.get(team_id) or team_id


def _matches_category_filter(
    theme_distribution: dict[str, float],
    subcategory_distribution: dict[str, float],
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
    org_id: str = "",
    limit: int = 200,
    include_text: bool = True,
    work_unit_id: str | None = None,
) -> list[WorkUnitInvestment]:
    start_day, end_day, _, _ = time_window(filters)
    start_ts = datetime.combine(start_day, time.min, tzinfo=timezone.utc)
    end_ts = datetime.combine(end_day, time.min, tzinfo=timezone.utc)
    theme_filters, subcategory_filters = _split_category_filters(filters)

    repo_scopes: dict[str, str] = {}
    repo_identities: dict[str, tuple[str, str]] = {}
    team_assignments: dict[str, dict[str, str]] = {}

    async with clickhouse_client(db_url) as sink:
        require_clickhouse_backend(sink)
        repo_ids = await resolve_repo_filter_ids(sink, filters, org_id=org_id)
        rows = await fetch_work_unit_investments(
            sink,
            start_ts=start_ts,
            end_ts=end_ts,
            repo_ids=repo_ids or None,
            limit=max(1, int(limit)),
            work_unit_id=work_unit_id,
            org_id=org_id,
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

        warn_once_for_mock_fixture_rows(org_id=org_id, surface="work_units", rows=rows)

        quote_rows: list[dict[str, object]] = []
        if include_text:
            unit_runs = [
                (str(row.get("work_unit_id")), str(row.get("categorization_run_id")))
                for row in rows
                if row.get("work_unit_id") and row.get("categorization_run_id")
            ]
            quote_rows = await fetch_work_unit_investment_quotes(
                sink,
                unit_runs=unit_runs,
                org_id=org_id,
            )

        repo_id_values = [
            str(row.get("repo_id") or "") for row in rows if row.get("repo_id")
        ]
        repo_scopes = await fetch_repo_scopes(
            sink,
            repo_ids=repo_id_values,
            org_id=org_id,
        )

        issue_ids: list[str] = []
        pr_refs: list[str] = []
        for row in rows:
            payload = row.get("structural_evidence_json")
            issue_ids.extend(_extract_issue_ids(payload))
            pr_refs.extend(_extract_pr_refs(payload))
        # CHAOS-2416: a unit's PRs bridge to a team as well as its issues. The
        # `prs` refs are work-graph node ids, so they are resolved through the
        # repos table into the work-items id space before the attribution
        # lookup -- the same translation the `unit_team` SQL CTE performs.
        pr_repo_uuids = [
            match.group(1)
            for match in (_PR_EVIDENCE_REF_RE.match(ref) for ref in pr_refs)
            if match is not None
        ]
        repo_identities = await fetch_repo_identities(
            sink,
            repo_ids=pr_repo_uuids,
            org_id=org_id,
        )
        pr_work_item_ids = [
            work_item_id
            for work_item_id in (
                _pr_ref_work_item_id(ref, repo_identities) for ref in pr_refs
            )
            if work_item_id
        ]
        team_assignments = await fetch_work_item_team_assignments(
            sink,
            work_item_ids=[*issue_ids, *pr_work_item_ids],
            org_id=org_id,
        )

    quotes_by_unit: dict[str, list[dict[str, object]]] = {}
    for quote in quote_rows:
        work_unit = str(quote.get("work_unit_id") or "")
        if not work_unit:
            continue
        quotes_by_unit.setdefault(work_unit, []).append(quote)

    results: list[WorkUnitInvestment] = []
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
        effort_metric = _effort_metric(row.get("effort_metric"))
        effort_value = float(row.get("effort_value") or 0.0)

        structural_evidence: list[dict[str, object]] = []
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

        textual_evidence: list[dict[str, object]] = []
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
        unit_pr_work_item_ids = [
            work_item_id
            for work_item_id in (
                _pr_ref_work_item_id(ref, repo_identities)
                for ref in _extract_pr_refs(structural_payload)
            )
            if work_item_id
        ]
        # arrayDistinct parity: the SQL de-duplicates the combined
        # issues+prs ref array before voting, so a ref reachable both
        # directly and via a PR translation counts once on both paths.
        team_id, team_name = _majority_team_for_issues(
            dict.fromkeys([*unit_issue_ids, *unit_pr_work_item_ids]),
            team_assignments,
        )
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
        evidence_band = _evidence_quality_band(
            raw_band if raw_band else ("unknown" if raw_quality is None else "very_low")
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

    results.sort(key=lambda item: (-item.effort.value, item.work_unit_id))
    return results[: max(1, int(limit))]
