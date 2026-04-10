"""Regex and keyword based prompt parser for report planning."""

from __future__ import annotations

import calendar
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta

MONTH_LOOKUP = {
    month.lower(): index for index, month in enumerate(calendar.month_name) if month
}

REPORT_TYPE_PATTERNS = (
    (
        re.compile(r"\b(ci stability|pipeline stability|pipeline report|ci report)\b"),
        "ci_stability",
    ),
    (re.compile(r"\b(quality trend|quality review|quality report)\b"), "quality_trend"),
    (re.compile(r"\b(monthly review|monthly report)\b"), "monthly_review"),
    (re.compile(r"\b(weekly health|weekly report|weekly summary)\b"), "weekly_health"),
)

GROUP_BY_PATTERNS = (
    (re.compile(r"\b(by|per|grouped by) team\b"), "team"),
    (re.compile(r"\b(by|per|grouped by) repo\b"), "repo"),
    (re.compile(r"\b(by|per|grouped by) service\b"), "service"),
    (re.compile(r"\b(by|per|grouped by) week\b|\bweekly trend\b"), "week"),
    (re.compile(r"\b(by|per|grouped by) month\b|\bmonthly trend\b"), "month"),
    (re.compile(r"\b(by|per|grouped by) day\b|\bdaily trend\b"), "day"),
)

COMPARISON_PATTERNS = (
    (
        re.compile(r"\b(compared to last week|vs last week|versus last week)\b"),
        "prior_week",
    ),
    (
        re.compile(r"\b(compared to last month|vs last month|versus last month)\b"),
        "prior_month",
    ),
    (
        re.compile(
            r"\b(vs prior period|versus prior period|compare to prior period|compared to prior period)\b"
        ),
        "prior_period",
    ),
)

METRIC_PHRASES = (
    "cycle time",
    "lead time",
    "throughput",
    "wip",
    "work in progress",
    "flaky tests",
    "flake rate",
    "coverage",
    "test coverage",
    "pipeline success rate",
    "success rate",
    "queue time",
    "reruns",
    "retry dependency",
    "pass rate",
    "failure rate",
    "after hours",
    "weekend work",
)


@dataclass(frozen=True)
class ParsedScope:
    teams: list[str] = field(default_factory=list)
    repos: list[str] = field(default_factory=list)
    services: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class ParsedPrompt:
    raw_prompt: str
    report_type: str | None
    scope: ParsedScope
    metric_terms: list[str]
    time_range_start: date | None
    time_range_end: date | None
    group_by: str | None
    comparison_period: str | None
    audience: str | None
    invalid_reasons: list[str] = field(default_factory=list)


def _clean_phrase(value: str) -> str:
    cleaned = re.split(
        r"\b(compare|compared|versus|vs|for|with|show|including)\b", value, maxsplit=1
    )[0]
    cleaned = cleaned.strip(" .,:;")
    return cleaned


def _split_values(value: str) -> list[str]:
    parts = re.split(r",|\band\b|\b&\b", value)
    cleaned = []
    for part in parts:
        item = _clean_phrase(part)
        if item:
            cleaned.append(item)
    return cleaned


def _extract_scopes(prompt: str) -> ParsedScope:
    lowered = prompt.lower()
    teams: list[str] = []
    repos: list[str] = []
    services: list[str] = []
    patterns = (
        (
            re.finditer(
                r"\bteams?\s+(.+?)(?=\b(?:repos?|repositories|services?|on|about|covering|showing|compared|compare|vs|versus|by)\b|[.;]|$)",
                lowered,
            ),
            teams,
        ),
        (
            re.finditer(
                r"\b(?:repos?|repositories)\s+(.+?)(?=\b(?:teams?|services?|on|about|covering|showing|compared|compare|vs|versus|by)\b|[.;]|$)",
                lowered,
            ),
            repos,
        ),
        (
            re.finditer(
                r"\bservices?\s+(.+?)(?=\b(?:teams?|repos?|repositories|on|about|covering|showing|compared|compare|vs|versus|by)\b|[.;]|$)",
                lowered,
            ),
            services,
        ),
    )
    for matches, bucket in patterns:
        for match in matches:
            bucket.extend(_split_values(match.group(1)))
    return ParsedScope(teams=teams, repos=repos, services=services)


def _extract_metric_terms(prompt: str) -> list[str]:
    lowered = prompt.lower()
    found: list[str] = []
    for phrase in METRIC_PHRASES:
        if re.search(rf"\b{re.escape(phrase)}\b", lowered):
            found.append(phrase)
    for match in re.finditer(r"\b(?:on|about|covering|showing)\s+([^.;]+)", lowered):
        for candidate in _split_values(match.group(1)):
            if candidate and candidate not in found:
                found.append(candidate)
    return found


def _infer_report_type(prompt: str) -> str | None:
    lowered = prompt.lower()
    for pattern, report_type in REPORT_TYPE_PATTERNS:
        if pattern.search(lowered):
            return report_type
    if "quality" in lowered or "coverage" in lowered or "flaky" in lowered:
        return "quality_trend"
    if "pipeline" in lowered or "ci" in lowered:
        return "ci_stability"
    return None


def _infer_group_by(prompt: str) -> str | None:
    lowered = prompt.lower()
    for pattern, grouping in GROUP_BY_PATTERNS:
        if pattern.search(lowered):
            return grouping
    return None


def _infer_comparison(prompt: str) -> str | None:
    lowered = prompt.lower()
    for pattern, period in COMPARISON_PATTERNS:
        if pattern.search(lowered):
            return period
    return None


def _infer_audience(prompt: str) -> str | None:
    lowered = prompt.lower()
    if re.search(r"\b(exec|executive|leadership|vp|cto)\b", lowered):
        return "executive"
    if re.search(r"\b(manager|lead|team lead|director)\b", lowered):
        return "team_lead"
    if re.search(r"\b(developer|engineer|ic)\b", lowered):
        return "developer"
    return None


def _month_window(year: int, month: int) -> tuple[date, date]:
    last_day = calendar.monthrange(year, month)[1]
    return date(year, month, 1), date(year, month, last_day)


def _parse_explicit_range(prompt: str) -> tuple[date | None, date | None, list[str]]:
    match = re.search(
        r"\b(?:from|between)\s+(\d{4}-\d{2}-\d{2})\s+(?:to|and)\s+(\d{4}-\d{2}-\d{2})\b",
        prompt.lower(),
    )
    if not match:
        return None, None, []
    try:
        start = datetime.strptime(match.group(1), "%Y-%m-%d").date()
        end = datetime.strptime(match.group(2), "%Y-%m-%d").date()
    except ValueError:
        return None, None, ["invalid_time_range"]
    if start > end:
        return None, None, ["invalid_time_range"]
    return start, end, []


def _parse_quarter(prompt: str) -> tuple[date | None, date | None]:
    match = re.search(r"\bq([1-4])\s+(\d{4})\b", prompt.lower())
    if not match:
        return None, None
    quarter = int(match.group(1))
    year = int(match.group(2))
    start_month = ((quarter - 1) * 3) + 1
    start = date(year, start_month, 1)
    end_month = start_month + 2
    end = date(year, end_month, calendar.monthrange(year, end_month)[1])
    return start, end


def _parse_month(prompt: str, today: date) -> tuple[date | None, date | None]:
    pattern = re.compile(
        r"\b("
        + "|".join(re.escape(name) for name in MONTH_LOOKUP)
        + r")(?:\s+(\d{4}))?\b"
    )
    match = pattern.search(prompt.lower())
    if not match:
        return None, None
    month = MONTH_LOOKUP[match.group(1)]
    year = int(match.group(2)) if match.group(2) else today.year
    return _month_window(year, month)


def _parse_relative_range(prompt: str, today: date) -> tuple[date | None, date | None]:
    lowered = prompt.lower()
    if "last week" in lowered:
        start_of_week = today - timedelta(days=today.weekday())
        end = start_of_week - timedelta(days=1)
        start = end - timedelta(days=6)
        return start, end
    if "last month" in lowered:
        first_of_month = today.replace(day=1)
        end = first_of_month - timedelta(days=1)
        return date(end.year, end.month, 1), end
    match = re.search(r"\b(?:past|last)\s+(\d+)\s+days\b", lowered)
    if match:
        days = int(match.group(1))
        return today - timedelta(days=days - 1), today
    match = re.search(r"\b(?:past|last)\s+(\d+)\s+weeks\b", lowered)
    if match:
        weeks = int(match.group(1))
        total_days = weeks * 7
        return today - timedelta(days=total_days - 1), today
    return None, None


def parse_prompt(prompt: str, *, today: date | None = None) -> ParsedPrompt:
    today = today or date.today()
    invalid_reasons: list[str] = []

    start, end, errors = _parse_explicit_range(prompt)
    invalid_reasons.extend(errors)
    if start is None and end is None:
        start, end = _parse_quarter(prompt)
    if start is None and end is None:
        start, end = _parse_month(prompt, today)
    if start is None and end is None:
        start, end = _parse_relative_range(prompt, today)

    return ParsedPrompt(
        raw_prompt=prompt,
        report_type=_infer_report_type(prompt),
        scope=_extract_scopes(prompt),
        metric_terms=_extract_metric_terms(prompt),
        time_range_start=start,
        time_range_end=end,
        group_by=_infer_group_by(prompt),
        comparison_period=_infer_comparison(prompt),
        audience=_infer_audience(prompt),
        invalid_reasons=invalid_reasons,
    )
