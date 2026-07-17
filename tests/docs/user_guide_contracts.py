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
