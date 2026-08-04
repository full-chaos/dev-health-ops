from __future__ import annotations

from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
_SMOKE = _ROOT / "scripts" / "acceptance" / "smoke_ask_dev_exact_commit.py"


def test_smoke_uses_public_conversation_and_bounded_sse_contract() -> None:
    smoke = _SMOKE.read_text(encoding="utf-8")
    assert "/api/v1/dev/conversations" in smoke
    assert "text/event-stream" in smoke
    assert "DevStreamEvent.model_validate" in smoke
    assert "StreamEventType.SCOPE_RESOLVED" in smoke
    assert "StreamEventType.ANSWER_COMPLETED" in smoke


def test_smoke_asserts_the_subject_was_actually_committed() -> None:
    smoke = _SMOKE.read_text(encoding="utf-8")
    # Not just "no error" -- the named subject must appear in the committed,
    # authorized scope, or this would be indistinguishable from an
    # organization-wide fallback answer.
    assert "authorized_repository_ids" in smoke
    assert "the named repository subject was never committed" in smoke


def test_smoke_asserts_a_substantive_non_error_answer() -> None:
    smoke = _SMOKE.read_text(encoding="utf-8")
    assert 'answer.status.value != "error"' in smoke
    assert "answer summary was empty" in smoke
    assert 'terminal_kind == "answer"' in smoke


def test_smoke_targets_the_fixture_generated_repository() -> None:
    smoke = _SMOKE.read_text(encoding="utf-8")
    assert "meridian/web-app" in smoke


def test_smoke_requires_live_acceptance_env_before_touching_the_network() -> None:
    smoke = _SMOKE.read_text(encoding="utf-8")
    assert 'os.getenv("ASK_DEV_LIVE_ACCEPTANCE") != "1"' in smoke
    assert "return 64" in smoke


def test_launcher_runs_exact_commit_smoke_between_not_found_and_core_intents() -> None:
    launcher = (_ROOT / "scripts" / "acceptance" / "run_ask_dev_compose.sh").read_text(
        encoding="utf-8"
    )
    assert "smoke_ask_dev_exact_commit.py" in launcher
    not_found_index = launcher.index("smoke_ask_dev_not_found.py")
    exact_commit_index = launcher.index("smoke_ask_dev_exact_commit.py")
    core_intents_index = launcher.index("smoke_ask_dev_core_intents.py")
    web_index = launcher.index("up -d --build --wait web")
    assert not_found_index < exact_commit_index < core_intents_index < web_index
