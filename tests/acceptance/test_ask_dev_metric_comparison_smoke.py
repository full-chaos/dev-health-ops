from __future__ import annotations

from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
_SMOKE = _ROOT / "scripts" / "acceptance" / "smoke_ask_dev_metric_comparison.py"


def test_smoke_uses_public_conversation_and_bounded_sse_contract() -> None:
    smoke = _SMOKE.read_text(encoding="utf-8")
    assert "/api/v1/dev/conversations" in smoke
    assert "text/event-stream" in smoke
    assert "DevStreamEvent.model_validate" in smoke
    assert "StreamEventType.ANSWER_COMPLETED" in smoke


def test_smoke_requests_two_canonical_metric_ids() -> None:
    smoke = _SMOKE.read_text(encoding="utf-8")
    assert '"items_completed"' in smoke
    assert '"cyclomatic_per_kloc"' in smoke
    assert "REQUESTED_METRIC_IDS" in smoke


def test_smoke_asserts_a_non_error_answer() -> None:
    smoke = _SMOKE.read_text(encoding="utf-8")
    assert 'answer.status.value != "error"' in smoke
    assert 'terminal_kind == "answer"' in smoke


def test_smoke_requires_live_acceptance_env_before_touching_the_network() -> None:
    smoke = _SMOKE.read_text(encoding="utf-8")
    assert 'os.getenv("ASK_DEV_LIVE_ACCEPTANCE") != "1"' in smoke
    assert "return 64" in smoke


def test_launcher_runs_metric_comparison_smoke_before_web_comes_up() -> None:
    launcher = (_ROOT / "scripts" / "acceptance" / "run_ask_dev_compose.sh").read_text(
        encoding="utf-8"
    )
    assert "smoke_ask_dev_metric_comparison.py" in launcher
    smoke_index = launcher.index("smoke_ask_dev_metric_comparison.py")
    web_index = launcher.index("up -d --build --wait web")
    assert smoke_index < web_index
