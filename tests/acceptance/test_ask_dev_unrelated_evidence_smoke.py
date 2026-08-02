from __future__ import annotations

from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
_SMOKE = _ROOT / "scripts" / "acceptance" / "smoke_ask_dev_unrelated_evidence.py"


def test_smoke_uses_public_conversation_and_bounded_sse_contract() -> None:
    smoke = _SMOKE.read_text(encoding="utf-8")
    assert "/api/v1/dev/conversations" in smoke
    assert "text/event-stream" in smoke
    assert "DevStreamEvent.model_validate" in smoke
    assert "StreamEventType.SCOPE_RESOLVED" in smoke


def test_smoke_targets_two_distinct_real_repositories() -> None:
    smoke = _SMOKE.read_text(encoding="utf-8")
    assert 'NAMED_REPO = "meridian/web-app"' in smoke
    assert 'UNRELATED_REPO = "meridian/core-api"' in smoke


def test_smoke_asserts_the_negative_control_exclusion() -> None:
    smoke = _SMOKE.read_text(encoding="utf-8")
    assert "leaked into the named-subject" in smoke
    assert "UNRELATED_REPO in str(item.entity_id)" in smoke


def test_smoke_discloses_the_positive_control_limitation_rather_than_hiding_it() -> (
    None
):
    """The scripted provider's search_evidence.v1 call hardcodes its query
    to "meridian/web-app" regardless of question -- confirmed live -- so the
    org-wide positive control cannot independently prove multi-repo evidence
    appears. This must stay documented, not silently dropped or overclaimed."""

    smoke = _SMOKE.read_text(encoding="utf-8")
    assert "WEAKER than originally intended" in smoke
    assert "hardcodes its query argument" in smoke


def test_smoke_requires_its_own_repo_count_2_bringup_not_the_shared_launcher() -> None:
    smoke = _SMOKE.read_text(encoding="utf-8")
    assert "NOT wired into that launcher" in smoke
    launcher = (_ROOT / "scripts" / "acceptance" / "run_ask_dev_compose.sh").read_text(
        encoding="utf-8"
    )
    assert "smoke_ask_dev_unrelated_evidence.py" not in launcher


def test_smoke_requires_live_acceptance_env_before_touching_the_network() -> None:
    smoke = _SMOKE.read_text(encoding="utf-8")
    assert 'os.getenv("ASK_DEV_LIVE_ACCEPTANCE") != "1"' in smoke
    assert "return 64" in smoke
