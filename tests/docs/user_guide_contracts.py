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
