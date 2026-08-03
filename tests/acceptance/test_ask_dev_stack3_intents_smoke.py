from __future__ import annotations

from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
_SMOKE = _ROOT / "scripts" / "acceptance" / "smoke_ask_dev_stack3_intents.py"


def test_smoke_uses_public_conversation_and_bounded_sse_contract() -> None:
    smoke = _SMOKE.read_text(encoding="utf-8")
    assert "/api/v1/dev/conversations" in smoke
    assert "text/event-stream" in smoke
    assert "DevStreamEvent.model_validate" in smoke
    assert "StreamEventType.ANSWER_COMPLETED" in smoke


def test_smoke_covers_all_four_newly_wired_intents_plus_the_portfolio_gap() -> None:
    smoke = _SMOKE.read_text(encoding="utf-8")
    assert 'scenario_id="team_health"' in smoke
    assert 'scenario_id="team_workload_balance"' in smoke
    assert 'scenario_id="operational_deficiency_team"' in smoke
    assert 'scenario_id="portfolio_status_gap"' in smoke


def test_smoke_asserts_named_team_committed_for_team_scoped_scenarios() -> None:
    smoke = _SMOKE.read_text(encoding="utf-8")
    assert "authorized_entity_ids" in smoke
    assert "the named team subject was never committed" in smoke


def test_smoke_asserts_a_real_answer_not_a_hang_or_crash() -> None:
    smoke = _SMOKE.read_text(encoding="utf-8")
    assert 'answer.status.value != "error"' in smoke
    assert 'terminal_kind == "answer"' in smoke


def test_smoke_writes_one_artifact_per_scenario_independently() -> None:
    """Each scenario gets its own recorder and artifact -- one broken
    scenario (e.g. CHAOS-3337) must not prevent the others (portfolio_
    status_gap) from being measured and written."""

    smoke = _SMOKE.read_text(encoding="utf-8")
    assert "for scenario in SCENARIOS:" in smoke
    assert "recorder.write(" in smoke
    assert "if failures:" in smoke


def test_smoke_requires_live_acceptance_env_before_touching_the_network() -> None:
    smoke = _SMOKE.read_text(encoding="utf-8")
    assert 'os.getenv("ASK_DEV_LIVE_ACCEPTANCE") != "1"' in smoke
    assert "return 64" in smoke


def test_launcher_runs_stack3_intents_smoke_after_team_attribution_before_web() -> None:
    launcher = (_ROOT / "scripts" / "acceptance" / "run_ask_dev_compose.sh").read_text(
        encoding="utf-8"
    )
    assert "smoke_ask_dev_stack3_intents.py" in launcher
    team_attribution_index = launcher.index("smoke_ask_dev_team_attribution.py")
    stack3_index = launcher.index("smoke_ask_dev_stack3_intents.py")
    web_index = launcher.index("up -d --build --wait web")
    assert team_attribution_index < stack3_index < web_index
    # Allowed to fail (CHAOS-3337) without aborting the rest of the launcher.
    assert 'smoke_ask_dev_stack3_intents.py" || true' in launcher
