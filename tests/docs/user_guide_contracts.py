from collections.abc import Mapping
from typing import Final

DIAGNOSTIC_PAGES: Final = {
    "quadrants.md": (
        "Purpose",
        "When to use",
        "How to read",
        "Worked example",
        "Evidence path",
        "Empty and error states",
        "Caveats",
        "Next step",
    ),
    "flame-diagrams.md": (
        "Purpose",
        "When to use",
        "How to read",
        "Worked example",
        "Evidence path",
        "Empty and error states",
        "Caveats",
        "Next step",
    ),
    "code-hotspots.md": (
        "Purpose",
        "When to use",
        "How to read",
        "Worked example",
        "Evidence path",
        "Empty and error states",
        "Caveats",
        "Next step",
    ),
}
FORBIDDEN_DIAGNOSTIC_COMPARISONS: Final = ("percentile", "leaderboard", "top performer")
FLOW_PAGES: Final = {
    "pr-flow.md": (
        "Purpose",
        "When to use",
        "How to read",
        "Worked example",
        "Evidence path",
        "Empty and error states",
        "Caveats",
        "Next step",
    ),
    "capacity-planning.md": (
        "Purpose",
        "When to use",
        "How to read",
        "Worked example",
        "Evidence path",
        "Empty and error states",
        "Caveats",
        "Next step",
    ),
    "work-graph.md": (
        "Purpose",
        "When to use",
        "How to read",
        "Worked example",
        "Evidence path",
        "Empty and error states",
        "Caveats",
        "Next step",
    ),
}
FORBIDDEN_FLOW_COMPARISONS: Final = ("leaderboard", "top performer", "rank people")
AI_VIEW_PAGES: Final = {
    "ai-impact.md": (
        "AI-assisted work share",
        "Agent-created work share",
        "Unknown attribution",
        "Net delivery lift",
        "Last computed",
    ),
    "ai-review-load.md": (
        "Pickup latency",
        "Review comments per LOC",
        "Change request rate",
        "Approval friction",
        "Review amplification",
    ),
    "ai-risk.md": (
        "Rework rate",
        "Revert rate",
        "Test gap rate",
        "Incident rate",
        "Linked incidents",
    ),
    "ai-attribution.md": (
        "Attribution mix",
        "Attribution evidence",
        "Subject",
        "Provider",
        "Observed",
        "highest-precedence",
        "Unknown attribution",
        "human bucket",
    ),
}
AI_GUIDE_SECTIONS: Final = (
    "Purpose",
    "What it measures",
    "How to read",
    "Confidence and provenance",
    "Empty and error states",
    "Caveats and limits",
    "Next step",
)
CALIBRATED_AI_LANGUAGE: Final = ("appears", "leans", "suggests")
AI_ESTIMATE_LABELS: Final = ("**estimate**", "**estimates**")
FORBIDDEN_AI_LANGUAGE: Final = (
    "determined",
    "detected",
    "rank individual",
    "individual ranking",
    "verdict",
    "browser recompute",
    "browser recalculates",
    "browser recomputes",
)
INVENTED_AI_VIEW_FIELDS: Final = {
    "ai-impact.md": ("Leverage components",),
    "ai-risk.md": ("incident counts",),
}
REPORTS_AND_METRICS_GUIDES: Final = {
    "reports.md": (
        "Purpose",
        "Create a report",
        "Clone a report",
        "Schedule a report",
        "Run Now",
        "Rendered Markdown",
        "Provenance",
        "cron expression",
        "timezone",
        "operator details",
    ),
    "metrics-interpretation.md": (
        "Cycle time",
        "Lead time",
        "Throughput",
        "WIP",
        "After-hours ratio",
        "Weekend ratio",
        "Bus factor",
        "trends",
        "not a ranking",
        "does not mean zero",
    ),
}
FORBIDDEN_REPORTS_IMPLEMENTATION_JARGON: Final = (
    "GraphQL",
    "ClickHouse",
    "Celery",
    "ReportRun",
    "SavedReport",
    "triggerReport",
)
NULL_AS_ZERO_CLAIMS: Final = ("null means zero", "null is zero", "null = zero")


def diagnostic_contract_errors(pages: Mapping[str, str]) -> tuple[str, ...]:
    errors: list[str] = []
    for page_name, required_sections in DIAGNOSTIC_PAGES.items():
        content = pages[page_name]
        for section in required_sections:
            if section.casefold() not in content.casefold():
                errors.append(f"{page_name}: missing section {section!r}")
        if "../glossary.md" not in content.casefold():
            errors.append(f"{page_name}: missing glossary link")
        if "evidence" not in content.casefold():
            errors.append(f"{page_name}: missing evidence path")
        for phrase in FORBIDDEN_DIAGNOSTIC_COMPARISONS:
            if phrase in content.casefold():
                errors.append(f"{page_name}: contains comparison framing {phrase!r}")

    quadrants = pages["quadrants.md"]
    for pair in (
        "Churn × Throughput",
        "Cycle Time × Throughput",
        "WIP × Throughput",
        "Review Load × Review Latency",
    ):
        if pair.casefold() not in quadrants.casefold():
            errors.append(f"quadrants.md: missing required pair {pair!r}")
    if "raw values only" not in quadrants.casefold():
        errors.append("quadrants.md: missing raw-value guard")

    flame = pages["flame-diagrams.md"]
    if "single item" not in flame.casefold():
        errors.append("flame-diagrams.md: missing single-item diagnosis contract")

    hotspots = pages["code-hotspots.md"]
    for term in ("churn", "complexity", "not blame"):
        if term not in hotspots.casefold():
            errors.append(f"code-hotspots.md: missing interpretation guard {term!r}")
    return tuple(errors)


def flow_contract_errors(pages: Mapping[str, str]) -> tuple[str, ...]:
    errors: list[str] = []
    for page_name, required_sections in FLOW_PAGES.items():
        content = pages[page_name]
        for section in required_sections:
            if section.casefold() not in content.casefold():
                errors.append(f"{page_name}: missing section {section!r}")
        if "../glossary.md" not in content.casefold():
            errors.append(f"{page_name}: missing glossary link")
        for phrase in FORBIDDEN_FLOW_COMPARISONS:
            if phrase in content.casefold():
                errors.append(f"{page_name}: contains comparison framing {phrase!r}")

    pr_flow = pages["pr-flow.md"]
    for term in (
        "PR stages",
        "State Flow",
        "review latency",
        "Current behavior",
        "Planned behavior",
    ):
        if term.casefold() not in pr_flow.casefold():
            errors.append(f"pr-flow.md: missing flow contract {term!r}")

    capacity = pages["capacity-planning.md"]
    for term in (
        "backlog",
        "historical throughput",
        "Monte Carlo",
        "P50",
        "P85",
        "P95",
        "Refresh Forecast",
        "not a promise",
        "Current behavior",
    ):
        if term.casefold() not in capacity.casefold():
            errors.append(f"capacity-planning.md: missing capacity contract {term!r}")

    graph = pages["work-graph.md"]
    for term in (
        "issue",
        "pull request",
        "commit",
        "file",
        "Theme → Subcategory → Evidence",
        "Work Graph Explorer",
        "connection type",
        "Current behavior",
        "Planned behavior",
    ):
        if term.casefold() not in graph.casefold():
            errors.append(f"work-graph.md: missing work-graph contract {term!r}")
    return tuple(errors)


def ai_view_contract_errors(pages: Mapping[str, str]) -> tuple[str, ...]:
    errors: list[str] = []
    for page_name, required_fields in AI_VIEW_PAGES.items():
        content = pages[page_name]
        for section in AI_GUIDE_SECTIONS:
            if section.casefold() not in content.casefold():
                errors.append(f"{page_name}: missing section {section!r}")
        for field in required_fields:
            if field.casefold() not in content.casefold():
                errors.append(f"{page_name}: missing current field {field!r}")
        for phrase in CALIBRATED_AI_LANGUAGE:
            if phrase not in content.casefold():
                errors.append(f"{page_name}: missing calibrated language {phrase!r}")
        if "[glossary](../glossary.md)" not in content.casefold():
            errors.append(f"{page_name}: missing glossary link")
        if (
            "[how to read dev health](../how-to-read-dev-health.md)"
            not in content.casefold()
        ):
            errors.append(f"{page_name}: missing interpretation overview link")
        for phrase in FORBIDDEN_AI_LANGUAGE:
            if phrase in content.casefold():
                errors.append(
                    f"{page_name}: contains definitive or ranking language {phrase!r}"
                )
        for field in INVENTED_AI_VIEW_FIELDS.get(page_name, ()):
            if field.casefold() in content.casefold():
                errors.append(f"{page_name}: contains invented current field {field!r}")
        if "estimate" in content.casefold() and not any(
            label in content.casefold() for label in AI_ESTIMATE_LABELS
        ):
            errors.append(f"{page_name}: estimate is not explicitly labeled")
    return tuple(errors)


def reports_and_metrics_contract_errors(pages: Mapping[str, str]) -> tuple[str, ...]:
    errors: list[str] = []
    for page_name, required_terms in REPORTS_AND_METRICS_GUIDES.items():
        content = pages[page_name]
        for term in required_terms:
            if term.casefold() not in content.casefold():
                errors.append(f"{page_name}: missing user-guide contract {term!r}")
        for term in NULL_AS_ZERO_CLAIMS:
            if term in content.casefold():
                errors.append(f"{page_name}: claims missing data is zero {term!r}")

    reports = pages["reports.md"]
    if "**ai-generated**" not in reports.casefold():
        errors.append("reports.md: AI content is not explicitly labeled")
    for term in FORBIDDEN_REPORTS_IMPLEMENTATION_JARGON:
        if term.casefold() in reports.casefold():
            errors.append(f"reports.md: contains implementation jargon {term!r}")
    return tuple(errors)
