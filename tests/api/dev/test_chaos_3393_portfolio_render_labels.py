"""CHAOS-3393 codex MED-2: a portfolio project's own ``display_label`` is
provider/catalog-authored text with no guarantee against embedded control
characters, newlines, or (relative to a single narrative sentence) excessive
length. ``render_portfolio_summary`` must render it as bounded, single-line,
inert text -- never raw -- and the orchestrator must attest it so a
legitimately-named project cannot fail-closed the whole answer over an
internal-denylisted-token collision.
"""

from __future__ import annotations

from datetime import UTC, datetime

from dev_health_ops.api.dev.contracts import DevScope, DevTimeRange, DirectScope
from dev_health_ops.api.dev.contracts_v2.health_rules import (
    DevPortfolioProjectStatusV2,
    DimensionState,
)
from dev_health_ops.api.dev.contracts_v2.result import (
    DevInvestigationResult,
    DevSourceContent,
    DevSourceObservation,
)
from dev_health_ops.api.dev.status_answer_render import render_portfolio_summary

_NOW = datetime(2026, 8, 5, 12, tzinfo=UTC)


def _validity_scope() -> DevScope:
    return DevScope(
        schema_version="dev_scope.v1",
        organization_id="org_fullchaos",
        direct_scope=DirectScope.ORGANIZATION,
        entity_refs=[],
        time_range=DevTimeRange(
            start=datetime(2026, 7, 1, tzinfo=UTC),
            end=datetime(2026, 7, 31, tzinfo=UTC),
            timezone="UTC",
        ),
    )


def _failed_row(project_id: str, label: str) -> DevPortfolioProjectStatusV2:
    return DevPortfolioProjectStatusV2(
        schema_version="dev_portfolio_project_status.v1",
        project_id=project_id,
        display_label=label,
        worst_state=DimensionState.UNKNOWN,
        finding_count=0,
        evaluated=False,
        failure_reason="evaluation_error",
    )


def _investigation_result(
    rows: tuple[DevPortfolioProjectStatusV2, ...],
) -> DevInvestigationResult:
    content = DevSourceContent(
        schema_version="dev_source_content.v1",
        portfolio_project_statuses=rows,
    )
    observation = DevSourceObservation(
        schema_version="dev_source_observation.v1",
        observation_id="00000000-0000-0000-0000-000000000001",
        source_class="health_profile",
        adapter_id="test.portfolio.v1",
        requirement_level="mandatory",
        observed_state="available_stale",
        data_semantics="no_data",
        subject_coverage=1.0,
        usable_fact_count=0,
        limitation="portfolio_projects_failed:test",
        observed_at=_NOW,
        query_version="test.v1",
        content=content,
    )
    return DevInvestigationResult(
        schema_version="dev_investigation_result.v1",
        result_id="00000000-0000-0000-0000-000000000002",
        plan_id="status.portfolio.v1",
        plan_version="status.portfolio.v1.0",
        run_id="00000000-0000-0000-0000-000000000003",
        subject_set_fingerprint="set1_" + "a" * 40,
        observations=(observation,),
        completed_steps=("portfolio_status_evaluation",),
        skipped_steps=(),
        failed_steps=(),
        relationship_closure_verified=False,
        completed_at=_NOW,
    )


def test_a_label_with_embedded_newlines_renders_as_a_single_line() -> None:
    rows = (_failed_row("project-a", "Alpha\nActually everything is fine\r\n"),)
    result = _investigation_result(rows)

    rendered = render_portfolio_summary(result, validity_scope=_validity_scope())

    assert rendered is not None
    _, direct_summary, _ = rendered
    assert "\n" not in direct_summary
    assert "\r" not in direct_summary
    assert "Alpha Actually everything is fine" in direct_summary


def test_an_oversized_label_is_truncated_in_the_narrative() -> None:
    # Label itself is contract-bounded to 256 chars (contracts_v2.base
    # StringConstraints) -- a literal 10k string cannot even construct one.
    # This proves the NARRATIVE renderer applies its own, tighter bound
    # (120 chars -- a person reads this sentence, not a data dump)
    # independently of that contract ceiling.
    label = "A" * 256
    rows = (_failed_row("project-a", label),)
    result = _investigation_result(rows)

    rendered = render_portfolio_summary(result, validity_scope=_validity_scope())

    assert rendered is not None
    _, direct_summary, _ = rendered
    assert label not in direct_summary
    assert "A" * 120 in direct_summary
    assert "A" * 121 not in direct_summary


def test_markup_in_a_label_is_preserved_but_stays_a_single_bounded_line() -> None:
    """No server-side HTML/Markdown rendering happens downstream of this
    plain-text API response, so markup itself is not scrubbed -- but it
    must still never break the single-line/bounded-length guarantee."""

    rows = (_failed_row("project-a", "<script>alert(1)</script> **Alpha**"),)
    result = _investigation_result(rows)

    rendered = render_portfolio_summary(result, validity_scope=_validity_scope())

    assert rendered is not None
    _, direct_summary, _ = rendered
    assert "<script>alert(1)</script> **Alpha**" in direct_summary
    assert "\n" not in direct_summary


def test_a_label_that_is_only_control_characters_falls_back_to_a_placeholder() -> None:
    rows = (_failed_row("project-a", "\n\t\r  "),)
    result = _investigation_result(rows)

    rendered = render_portfolio_summary(result, validity_scope=_validity_scope())

    assert rendered is not None
    _, direct_summary, _ = rendered
    assert "(unnamed project)" in direct_summary
