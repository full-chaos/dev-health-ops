from __future__ import annotations

import json
import subprocess
from pathlib import Path

from scripts.acceptance.acceptance_artifact import (
    AcceptanceFailure,
    ScenarioRecorder,
    redact_secrets,
)

_FAKE_JWT = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"
    ".eyJzdWIiOiJ0ZXN0In0"
    ".c2lnbmF0dXJlLXBsYWNlaG9sZGVyLWJ5dGVz"
)


def _init_throwaway_git_repo(root: Path) -> None:
    subprocess.run(["git", "init", "-q"], cwd=root, check=True)
    subprocess.run(
        ["git", "config", "user.email", "test@example.invalid"], cwd=root, check=True
    )
    subprocess.run(["git", "config", "user.name", "Test"], cwd=root, check=True)
    (root / "placeholder.txt").write_text("x")
    subprocess.run(["git", "add", "placeholder.txt"], cwd=root, check=True)
    subprocess.run(["git", "commit", "-q", "-m", "initial"], cwd=root, check=True)


# --- redact_secrets: codex finding (HIGH, 2026-08-02) -- the primary fix ---


def test_redact_secrets_replaces_a_jwt_shaped_token() -> None:
    detail = f"{{'access_token': '{_FAKE_JWT}', 'token_type': 'bearer'}}"
    redacted = redact_secrets(detail)
    assert _FAKE_JWT not in redacted
    assert "[REDACTED_JWT]" in redacted
    assert "token_type" in redacted, "non-secret structure should survive redaction"


def test_redact_secrets_leaves_ordinary_text_untouched() -> None:
    detail = "login returned no access token"
    assert redact_secrets(detail) == detail


def test_redact_secrets_handles_multiple_tokens_in_one_string() -> None:
    other_jwt = _FAKE_JWT.replace("test", "other")
    detail = f"access={_FAKE_JWT} refresh={other_jwt}"
    redacted = redact_secrets(detail)
    assert _FAKE_JWT not in redacted
    assert other_jwt not in redacted
    assert redacted.count("[REDACTED_JWT]") == 2


# --- ScenarioRecorder.check: redaction must happen at the recorder, not
# rely on every caller remembering to sanitize its own detail string ---


def test_scenario_recorder_check_redacts_before_storing(tmp_path: Path) -> None:
    script_path = tmp_path / "smoke_throwaway.py"
    script_path.write_text("# throwaway\n")
    recorder = ScenarioRecorder(scenario_id="throwaway", script_path=script_path)
    recorder.check(
        "login_response_is_object", True, f"{{'access_token': '{_FAKE_JWT}'}}"
    )
    assert len(recorder.assertions) == 1
    assert _FAKE_JWT not in recorder.assertions[0].detail
    assert "[REDACTED_JWT]" in recorder.assertions[0].detail


def test_scenario_recorder_check_redacts_the_raised_failure_message(
    tmp_path: Path,
) -> None:
    script_path = tmp_path / "smoke_throwaway.py"
    script_path.write_text("# throwaway\n")
    recorder = ScenarioRecorder(scenario_id="throwaway", script_path=script_path)
    try:
        recorder.check("bad_login", False, f"leaked: {_FAKE_JWT}")
    except AcceptanceFailure as exc:
        assert _FAKE_JWT not in str(exc)
        assert "[REDACTED_JWT]" in str(exc)
    else:
        raise AssertionError("expected AcceptanceFailure")


def test_scenario_recorder_write_redacts_a_leaked_error_string(tmp_path: Path) -> None:
    _init_throwaway_git_repo(tmp_path)
    script_path = tmp_path / "smoke_throwaway.py"
    script_path.write_text("# throwaway\n")
    recorder = ScenarioRecorder(scenario_id="throwaway", script_path=script_path)
    artifact = recorder.write(
        tmp_path / "artifacts" / "throwaway.json",
        error=f"POST failed, body leaked: {_FAKE_JWT}",
    )
    assert _FAKE_JWT not in json.dumps(artifact)
    assert "[REDACTED_JWT]" in str(artifact["error"])


# --- tree_clean / tree_digest: codex finding (HIGH, 2026-08-02, round 2) ---


def test_write_records_tree_clean_true_over_a_clean_repo(tmp_path: Path) -> None:
    _init_throwaway_git_repo(tmp_path)
    script_path = tmp_path / "smoke_throwaway.py"
    script_path.write_text("# throwaway\n")
    # A committed script (part of the "clean" tree) so status --porcelain
    # has nothing left to report once the placeholder commit lands.
    subprocess.run(["git", "add", "smoke_throwaway.py"], cwd=tmp_path, check=True)
    subprocess.run(
        ["git", "commit", "-q", "-m", "add script"], cwd=tmp_path, check=True
    )
    recorder = ScenarioRecorder(scenario_id="throwaway", script_path=script_path)
    artifact = recorder.write(tmp_path / "artifacts" / "throwaway.json")
    assert artifact["tree_clean"] is True


def test_write_records_tree_clean_false_over_a_dirty_repo(tmp_path: Path) -> None:
    _init_throwaway_git_repo(tmp_path)
    script_path = tmp_path / "smoke_throwaway.py"
    script_path.write_text("# throwaway\n")
    # An uncommitted, tracked-elsewhere production file with local edits --
    # exactly the scenario tree_clean exists to catch: commit_sha would not
    # actually describe what this run executed against.
    (tmp_path / "placeholder.txt").write_text("locally edited, never committed")
    recorder = ScenarioRecorder(scenario_id="throwaway", script_path=script_path)
    artifact = recorder.write(tmp_path / "artifacts" / "throwaway.json")
    assert artifact["tree_clean"] is False


def test_write_ignores_the_artifacts_directory_itself_when_judging_clean(
    tmp_path: Path,
) -> None:
    """A multi-scenario minting batch writes several artifact JSON files
    before any of them are committed -- those files being "dirty" must not
    make every OTHER artifact in the same batch report tree_clean=False."""

    _init_throwaway_git_repo(tmp_path)
    script_path = tmp_path / "smoke_throwaway.py"
    script_path.write_text("# throwaway\n")
    subprocess.run(["git", "add", "smoke_throwaway.py"], cwd=tmp_path, check=True)
    subprocess.run(
        ["git", "commit", "-q", "-m", "add script"], cwd=tmp_path, check=True
    )
    # Simulate an artifact from an earlier scenario in the same batch,
    # already on disk and untracked.
    other_artifact = tmp_path / "tests" / "acceptance" / "artifacts" / "earlier.json"
    other_artifact.parent.mkdir(parents=True, exist_ok=True)
    other_artifact.write_text("{}")
    recorder = ScenarioRecorder(scenario_id="throwaway", script_path=script_path)
    artifact = recorder.write(tmp_path / "artifacts" / "throwaway.json")
    assert artifact["tree_clean"] is True
