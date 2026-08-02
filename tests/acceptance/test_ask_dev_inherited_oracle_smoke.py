from __future__ import annotations

from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
_SMOKE = _ROOT / "scripts" / "acceptance" / "smoke_ask_dev_inherited_oracle.py"


def test_smoke_reads_the_same_oracle_file_the_playwright_leg_uses() -> None:
    smoke = _SMOKE.read_text(encoding="utf-8")
    assert "ask-dev-oracle.v1.json" in smoke
    assert 'oracle["question"]' in smoke
    assert 'oracle["expected_metric_id"]' in smoke
    assert 'oracle["expected_evidence_entity_fragment"]' in smoke
    assert 'oracle["expected_claim_kind"]' in smoke


def test_smoke_asserts_all_three_oracle_claims_not_just_no_error() -> None:
    smoke = _SMOKE.read_text(encoding="utf-8")
    assert "expected metric" in smoke
    assert "expected evidence entity fragment" in smoke
    assert "expected claim kind" in smoke


def test_smoke_uses_a_comparison_window_inside_the_fixture_backfill() -> None:
    smoke = _SMOKE.read_text(encoding="utf-8")
    # 28+28 falls outside `dev-hops fixtures generate --days 28`'s history
    # and comparison_value comes back null -- caught live 2026-08-02.
    assert "days=14" in smoke
    assert "days=28" not in smoke


def test_smoke_requires_live_acceptance_env_before_touching_the_network() -> None:
    smoke = _SMOKE.read_text(encoding="utf-8")
    assert 'os.getenv("ASK_DEV_LIVE_ACCEPTANCE") != "1"' in smoke
    assert "return 64" in smoke


def test_launcher_runs_inherited_oracle_smoke_before_web_comes_up() -> None:
    launcher = (_ROOT / "scripts" / "acceptance" / "run_ask_dev_compose.sh").read_text(
        encoding="utf-8"
    )
    assert "smoke_ask_dev_inherited_oracle.py" in launcher
    smoke_index = launcher.index("smoke_ask_dev_inherited_oracle.py")
    web_index = launcher.index("up -d --build --wait web")
    assert smoke_index < web_index
