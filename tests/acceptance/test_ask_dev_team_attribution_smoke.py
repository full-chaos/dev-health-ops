from __future__ import annotations

from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
_SMOKE = _ROOT / "scripts" / "acceptance" / "smoke_ask_dev_team_attribution.py"


def test_smoke_uses_public_conversation_and_bounded_sse_contract() -> None:
    smoke = _SMOKE.read_text(encoding="utf-8")
    assert "/api/v1/dev/conversations" in smoke
    assert "text/event-stream" in smoke
    assert "DevStreamEvent.model_validate" in smoke
    assert "StreamEventType.SCOPE_RESOLVED" in smoke
    assert "StreamEventType.ANSWER_COMPLETED" in smoke


def test_smoke_asserts_the_named_team_subject_was_actually_committed() -> None:
    smoke = _SMOKE.read_text(encoding="utf-8")
    # Not just "no error" -- the named team subject must appear in the
    # committed, authorized scope via authorized_entity_ids (TEAM subjects
    # use authorized_entity_ids, not authorized_repository_ids).
    assert "authorized_entity_ids" in smoke
    assert "the named team subject was never committed" in smoke


def test_smoke_asserts_the_chaos_3332_crash_is_gone() -> None:
    smoke = _SMOKE.read_text(encoding="utf-8")
    assert "no_internal_error_event" in smoke
    assert 'error_event.error.code != "internal_error"' in smoke
    assert "CHAOS-3332 regression" in smoke


def test_smoke_asserts_a_degraded_but_honest_non_error_answer() -> None:
    smoke = _SMOKE.read_text(encoding="utf-8")
    assert 'answer.status.value != "error"' in smoke
    assert 'terminal_kind == "answer"' in smoke


def test_smoke_asserts_the_exact_chaos_3333_characterization() -> None:
    """Not just 'not error' -- 'degraded' specifically, metrics genuinely
    empty, and the limitation named, matching the manifest row's claim."""

    smoke = _SMOKE.read_text(encoding="utf-8")
    assert 'answer.status.value == "degraded"' in smoke
    assert "answer_status_is_degraded" in smoke
    assert "len(answer.metrics) == 0" in smoke
    assert "metrics_empty_for_team_scope" in smoke
    assert "unavailable_required_sources" in smoke
    assert "limitation_names_unavailable_metric_source" in smoke


def test_smoke_targets_a_real_fixture_generated_team() -> None:
    smoke = _SMOKE.read_text(encoding="utf-8")
    assert 'NAMED_TEAM = "Core"' in smoke


def test_smoke_requires_live_acceptance_env_before_touching_the_network() -> None:
    smoke = _SMOKE.read_text(encoding="utf-8")
    assert 'os.getenv("ASK_DEV_LIVE_ACCEPTANCE") != "1"' in smoke
    assert "return 64" in smoke


def test_launcher_runs_team_attribution_smoke_after_metric_comparison_before_web() -> (
    None
):
    launcher = (_ROOT / "scripts" / "acceptance" / "run_ask_dev_compose.sh").read_text(
        encoding="utf-8"
    )
    assert "smoke_ask_dev_team_attribution.py" in launcher
    metric_comparison_index = launcher.index("smoke_ask_dev_metric_comparison.py")
    team_attribution_index = launcher.index("smoke_ask_dev_team_attribution.py")
    web_index = launcher.index("up -d --build --wait web")
    assert metric_comparison_index < team_attribution_index < web_index
