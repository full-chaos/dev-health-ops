from collections.abc import Mapping
from typing import Final

PR_FLOW_REQUIRED_TERMS: Final = ("State Flow", "work-item state-transition Sankey")
PR_FLOW_UNSUPPORTED_CLAIMS: Final = (
    "review latency",
    "first review",
    "merge timing",
    "PR stages",
)
REPORT_SCHEDULE_OPTIONS: Final = ("None", "Weekly", "Monthly", "fixed schedule")
REPORT_UNAVAILABLE_CONTROLS: Final = ("cron", "timezone")
BUS_FACTOR_UNSUPPORTED_THRESHOLDS: Final = ("at least half", "50%")
FIXTURE_RENDERED_STATES: Final = {
    "/dashboard": "populated cockpit with source-connection prompt",
    "/investment": "team-plan availability gate",
}
FIXTURE_GUIDE_STATE_PHRASES: Final = {
    "first-10-minutes.md": "populated Cockpit signals alongside a source-connection prompt",
    "journeys/investment-view.md": "Team-plan availability gate",
}


def _normalized(content: str) -> str:
    return " ".join(content.casefold().split())


def guide_source_accuracy_errors(pages: Mapping[str, str]) -> tuple[str, ...]:
    errors: list[str] = []

    pr_flow = _normalized(pages["pr-flow.md"])
    for term in PR_FLOW_REQUIRED_TERMS:
        if term.casefold() not in pr_flow:
            errors.append(f"pr-flow.md: missing current behavior {term!r}")
    for claim in PR_FLOW_UNSUPPORTED_CLAIMS:
        if claim.casefold() in pr_flow:
            errors.append(f"pr-flow.md: contains unsupported claim {claim!r}")

    reports = _normalized(pages["reports.md"])
    for option in REPORT_SCHEDULE_OPTIONS:
        if option.casefold() not in reports:
            errors.append(f"reports.md: missing fixed schedule option {option!r}")
    for control in REPORT_UNAVAILABLE_CONTROLS:
        if control.casefold() in reports:
            errors.append(
                f"reports.md: contains unavailable Report Center control {control!r}"
            )
    if "does not show a separate provenance panel" not in reports:
        errors.append("reports.md: overstates completed-report provenance surface")

    metrics = _normalized(pages["metrics-interpretation.md"])
    if "at least 80%" not in metrics:
        errors.append("metrics-interpretation.md: missing 80% bus-factor threshold")
    for threshold in BUS_FACTOR_UNSUPPORTED_THRESHOLDS:
        if threshold.casefold() in metrics:
            errors.append(
                "metrics-interpretation.md: contains unsupported bus-factor threshold "
                f"{threshold!r}"
            )

    return tuple(errors)


def fixture_truth_errors(
    fixture_states: Mapping[str, str], pages: Mapping[str, str]
) -> tuple[str, ...]:
    errors: list[str] = []
    for route, expected_state in FIXTURE_RENDERED_STATES.items():
        if fixture_states.get(route) != expected_state:
            errors.append(f"fixture metadata: {route} must record {expected_state!r}")
    for page_name, phrase in FIXTURE_GUIDE_STATE_PHRASES.items():
        if _normalized(phrase) not in _normalized(pages[page_name]):
            errors.append(f"{page_name}: missing fixture state disclosure {phrase!r}")
    return tuple(errors)


def work_graph_search_errors(entry: Mapping[str, str]) -> tuple[str, ...]:
    errors: list[str] = []
    if entry.get("query") != "Work Graph":
        errors.append("search acceptance: Work Graph query is not exact")
    if entry.get("canonical_url") != "/user-guide/views/work-graph/":
        errors.append(
            "search acceptance: Work Graph does not target the canonical journey"
        )
    return tuple(errors)
