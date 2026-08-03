from __future__ import annotations

from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
_SMOKE = _ROOT / "scripts" / "acceptance" / "smoke_ask_dev_core_intents.py"


def test_smoke_uses_public_conversation_and_bounded_sse_contract() -> None:
    smoke = _SMOKE.read_text(encoding="utf-8")
    assert "/api/v1/dev/conversations" in smoke
    assert "text/event-stream" in smoke
    assert "DevStreamEvent.model_validate" in smoke
    assert "StreamEventType.ANSWER_COMPLETED" in smoke


def test_smoke_covers_data_trust_and_remaining_work_question_classes() -> None:
    smoke = _SMOKE.read_text(encoding="utf-8")
    assert 'question_class="data_trust"' in smoke
    assert 'question_class="remaining_work"' in smoke


def test_smoke_asserts_a_safe_grounded_non_error_answer() -> None:
    smoke = _SMOKE.read_text(encoding="utf-8")
    assert 'answer.status.value != "error"' in smoke
    assert "answer summary was empty" in smoke
    assert 'terminal_kind == "answer"' in smoke


def test_smoke_requires_live_acceptance_env_before_touching_the_network() -> None:
    smoke = _SMOKE.read_text(encoding="utf-8")
    assert 'os.getenv("ASK_DEV_LIVE_ACCEPTANCE") != "1"' in smoke
    assert "return 64" in smoke


def test_launcher_runs_core_intents_smoke_before_bringing_up_web() -> None:
    launcher = (_ROOT / "scripts" / "acceptance" / "run_ask_dev_compose.sh").read_text(
        encoding="utf-8"
    )
    assert "smoke_ask_dev_core_intents.py" in launcher
    not_found_index = launcher.index("smoke_ask_dev_not_found.py")
    core_intents_index = launcher.index("smoke_ask_dev_core_intents.py")
    web_index = launcher.index("up -d --build --wait web")
    assert not_found_index < core_intents_index < web_index
