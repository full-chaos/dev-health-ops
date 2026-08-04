from __future__ import annotations

from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
_SMOKE = _ROOT / "scripts" / "acceptance" / "smoke_ask_dev_not_found.py"


def test_smoke_uses_public_conversation_and_bounded_sse_contract() -> None:
    smoke = _SMOKE.read_text(encoding="utf-8")
    assert "/api/v1/dev/conversations" in smoke
    assert "text/event-stream" in smoke
    assert "DevStreamEvent.model_validate" in smoke
    assert "StreamEventType.ERROR" in smoke


def test_smoke_asserts_the_exact_defect_reproduction_outcome() -> None:
    smoke = _SMOKE.read_text(encoding="utf-8")
    # The CHAOS-3300 original defect reproduction: not_found, no answer, no
    # leaked subject name -- not a guess at "some safe-looking error".
    assert 'error_event.error.code == "scope_not_found"' in smoke
    assert "an unauthorized project name must never produce an answer" in smoke
    assert '"Ask Dev" not in safe_message' in smoke


def test_smoke_enables_wave_3_1_without_touching_shared_prepare_script() -> None:
    smoke = _SMOKE.read_text(encoding="utf-8")
    prepare = (
        _ROOT / "scripts" / "acceptance" / "prepare_ask_dev_acceptance.py"
    ).read_text(encoding="utf-8")
    assert "ask_dev_wave_3_1" in smoke
    # The shared readiness script intentionally leaves Wave 3.1 preflight
    # off (the positive-control oracle exercises the legacy loop on
    # purpose) -- this scenario must not have flipped that shared behavior.
    assert "ask_dev_wave_3_1" not in prepare


def test_smoke_requires_live_acceptance_env_before_touching_the_network() -> None:
    smoke = _SMOKE.read_text(encoding="utf-8")
    assert 'os.getenv("ASK_DEV_LIVE_ACCEPTANCE") != "1"' in smoke
    assert "return 64" in smoke
