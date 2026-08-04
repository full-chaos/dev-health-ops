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


def test_smoke_asserts_scope_resolved_unconditionally() -> None:
    """Codex finding (HIGH, 2026-08-03, round 3): scope resolution was only
    checked for team-scoped scenarios -- portfolio's own row claims a
    committed scope too, and that must be asserted for every scenario, not
    only the ones that happen to name a team. The check site must precede
    the team_scoped-gated named_team_committed check, not live inside it."""

    smoke = _SMOKE.read_text(encoding="utf-8")
    assert "scope_resolved_event_present" in smoke
    scope_resolved_index = smoke.index('"scope_resolved_event_present"')
    team_scoped_gate_index = smoke.index("scenario.team_scoped\n")
    named_team_index = smoke.index('"named_team_committed"')
    assert scope_resolved_index < team_scoped_gate_index < named_team_index


def test_smoke_asserts_the_exact_partial_status_for_portfolio() -> None:
    """'not error' alone is satisfied by complete/partial/degraded alike --
    the portfolio row's claim is specifically the legacy-fallback PARTIAL
    outcome."""

    smoke = _SMOKE.read_text(encoding="utf-8")
    assert 'answer.status.value == "partial"' in smoke
    assert "answer_status_is_exactly_partial" in smoke


def test_smoke_discloses_the_warning_signal_is_not_client_observable() -> None:
    """The plan_registry_gap WARNING log line + counter have no public API
    surface -- this must be disclosed, not silently implied by a generic
    'a warning event fired' check standing in for the real signal."""

    smoke = _SMOKE.read_text(encoding="utf-8")
    assert "warnings_present_but_not_a_plan_registry_gap_signal" in smoke
    assert "no client-observable signal" in smoke


def test_smoke_proves_the_claimed_plan_executed_via_dev_runs() -> None:
    """Codex finding (HIGH, 2026-08-03, round 4): scope+commit+non-error
    does not distinguish the claimed plan running from a legacy-loop
    fallback silently absorbing a missing plan-registry entry -- the
    trustworthy signal is a direct dev_runs read (the same technique used
    diagnosing CHAOS-3332), never the scripted provider's narrative."""

    smoke = _SMOKE.read_text(encoding="utf-8")
    assert "preflight_proceeded_committed_subject" in smoke
    assert '"proceeded_committed_subject"' in smoke
    assert "claimed_plan_step_completed" in smoke
    assert "plan_step_partition" in smoke
    assert '"docker",' in smoke
    assert '"exec",' in smoke
    assert "_POSTGRES_CONTAINER" in smoke


def test_smoke_pins_each_scenarios_own_distinct_plan_step_name() -> None:
    """The three plans' mandatory steps differ (wave_3_1_plans.py) -- this
    is what lets the assertion distinguish the three plans from EACH
    OTHER, not just from the legacy loop. A shared/generic step name would
    not catch team_health's plan silently swapped for team_workload's."""

    smoke = _SMOKE.read_text(encoding="utf-8")
    assert 'expected_plan_step="health_evaluation"' in smoke
    assert 'expected_plan_step="workload_evaluation"' in smoke
    assert 'expected_plan_step="deficiency_evaluation"' in smoke


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
    # CHAOS-3337 shipped: no longer allowed to fail silently -- a hard
    # failure here now aborts the launcher like every other scenario.
    assert 'smoke_ask_dev_stack3_intents.py" || true' not in launcher
