from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path

import pytest

from scripts.acceptance.acceptance_artifact import (
    RUNTIME_DEPENDENCY_PATHS,
    AcceptanceFailure,
    ScenarioRecorder,
    redact_secrets,
    runtime_dependency_digest,
)

_REPO_ROOT = Path(__file__).resolve().parents[2]

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
    # Pin the untracked-file reporting mode to git's own default. A developer
    # with `status.showUntrackedFiles=all` set globally would otherwise never
    # reproduce the directory-collapsing behaviour CI gets, and the
    # artifacts-exclusion test below would pass locally for the wrong reason.
    subprocess.run(
        ["git", "config", "status.showUntrackedFiles", "normal"], cwd=root, check=True
    )
    (root / "placeholder.txt").write_text("x")
    # ScenarioRecorder.write digests the shared fixture surface and refuses
    # to run without it, so a throwaway repo needs those paths present --
    # committed, so the tree still starts clean.
    for relative in RUNTIME_DEPENDENCY_PATHS:
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("throwaway runtime dependency\n")
    subprocess.run(["git", "add", "-A"], cwd=root, check=True)
    subprocess.run(["git", "commit", "-q", "-m", "initial"], cwd=root, check=True)


# --- runtime_dependency_digest: codex finding (HIGH, 2026-08-03) --
# script_sha256 bound the smoke script's bytes and nothing else, so a
# changed fixture provider left every artifact validating clean ---


#: An INDEPENDENT literal restatement of the covered set. Codex finding
#: (MED, 2026-08-03): the checks below used to iterate
#: RUNTIME_DEPENDENCY_PATHS itself, so deleting an entry removed it from
#: both the setup and the assertions and every test stayed green -- a test
#: self-oracling against the very list it exists to protect. Written out
#: here by hand, so dropping a path from the production tuple fails.
_EXPECTED_RUNTIME_DEPENDENCIES = (
    "compose.yml",
    "docker/Dockerfile",
    "pyproject.toml",
    "requirements.txt",
    "scripts/acceptance/acceptance_artifact.py",
    "scripts/acceptance/prepare_ask_dev_acceptance.py",
    "scripts/acceptance/run_ask_dev_compose.sh",
    "src/dev_health_ops/llm/agent/scripted_openai_service.py",
    "tests/acceptance/compose.ask-dev-acr.yml",
    "tests/acceptance/compose.ask-dev-provider-profile.yml",
    "tests/acceptance/compose.ask-dev.yml",
)


def test_the_covered_dependency_set_matches_its_independent_inventory() -> None:
    """Deleting a path from RUNTIME_DEPENDENCY_PATHS must fail here. This
    is deliberately a second, hand-maintained copy: a check derived from
    the thing it checks cannot detect that thing shrinking."""

    assert tuple(RUNTIME_DEPENDENCY_PATHS) == _EXPECTED_RUNTIME_DEPENDENCIES


def test_every_runtime_dependency_path_exists_in_this_repo() -> None:
    """The digest is only worth what it covers. A path that has been moved
    or renamed must fail here, loudly, rather than quietly dropping out of
    the covered set the next time someone reorganizes the harness."""

    assert _EXPECTED_RUNTIME_DEPENDENCIES
    missing = [
        relative
        for relative in _EXPECTED_RUNTIME_DEPENDENCIES
        if not (_REPO_ROOT / relative).is_file()
    ]
    assert not missing, f"runtime dependency paths no longer exist: {missing}"


def test_dropping_a_dependency_from_the_set_changes_the_digest(
    tmp_path: Path,
) -> None:
    """The mutation the inventory test exists to catch, proven directly:
    a digest computed over a SHORTER set must differ from the real one, so
    silently narrowing coverage cannot produce a matching digest."""

    for relative in _EXPECTED_RUNTIME_DEPENDENCIES:
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("original\n")
    full = runtime_dependency_digest(tmp_path)
    narrowed = hashlib.sha256()
    for relative in _EXPECTED_RUNTIME_DEPENDENCIES[:-1]:
        narrowed.update(relative.encode("utf-8"))
        narrowed.update(b"\0")
        narrowed.update(
            hashlib.sha256((tmp_path / relative).read_bytes()).hexdigest().encode()
        )
        narrowed.update(b"\0")
    assert full != narrowed.hexdigest()


def test_runtime_digest_changes_when_a_covered_dependency_changes(
    tmp_path: Path,
) -> None:
    """The property the digest exists for, asserted directly: editing a
    covered file must produce a different digest. Asserted per path, so a
    dependency silently dropped from the hashed set fails here."""

    for relative in _EXPECTED_RUNTIME_DEPENDENCIES:
        root = tmp_path / relative.replace("/", "_")
        for candidate in _EXPECTED_RUNTIME_DEPENDENCIES:
            path = root / candidate
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("original\n")
        before = runtime_dependency_digest(root)
        (root / relative).write_text("edited\n")
        assert runtime_dependency_digest(root) != before, (
            f"{relative} is listed in RUNTIME_DEPENDENCY_PATHS but changing "
            "it does not change the digest"
        )


def test_runtime_digest_is_stable_for_unchanged_inputs(tmp_path: Path) -> None:
    for relative in RUNTIME_DEPENDENCY_PATHS:
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("original\n")
    assert runtime_dependency_digest(tmp_path) == runtime_dependency_digest(tmp_path)


def test_runtime_digest_refuses_to_cover_less_than_it_claims(tmp_path: Path) -> None:
    """A missing dependency must raise, not digest over what happens to be
    present -- a measurement that did not happen must fail, not return a
    stable-looking hash covering fewer files."""

    for relative in RUNTIME_DEPENDENCY_PATHS[1:]:
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("original\n")
    with pytest.raises(AcceptanceFailure) as excinfo:
        runtime_dependency_digest(tmp_path)
    assert RUNTIME_DEPENDENCY_PATHS[0] in str(excinfo.value)


def test_write_refuses_when_the_fixture_surface_moved_mid_run(
    tmp_path: Path,
) -> None:
    """codex finding (HIGH, 2026-08-03): sampling the digest only at write
    time meant a covered file edited WHILE the scenario ran produced an
    artifact whose digest matched the tree, though the containers had run
    the older code. The recorder must refuse rather than record that."""

    _init_throwaway_git_repo(tmp_path)
    script_path = tmp_path / "smoke_throwaway.py"
    script_path.write_text("# throwaway\n")
    recorder = ScenarioRecorder(scenario_id="throwaway", script_path=script_path)
    recorder.capture_runtime_digest(tmp_path)
    # The provider changes underneath a scenario that is already running.
    (tmp_path / "src/dev_health_ops/llm/agent/scripted_openai_service.py").write_text(
        "# edited while the scenario was still running\n"
    )
    with pytest.raises(AcceptanceFailure) as excinfo:
        recorder.write(tmp_path / "artifacts" / "throwaway.json")
    assert "changed while this scenario was running" in str(excinfo.value)


# --- acr_armed: codex finding (MEDIUM, 2026-08-05) -- an ACR-backed case's
# evidence must be provably from a run where ACR was actually armed ---


def test_write_records_acr_armed_true_when_the_env_var_is_set(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("ASK_DEV_ACCEPTANCE_ACR", "1")
    _init_throwaway_git_repo(tmp_path)
    script_path = tmp_path / "smoke_throwaway.py"
    script_path.write_text("# throwaway\n")
    recorder = ScenarioRecorder(scenario_id="throwaway", script_path=script_path)
    artifact = recorder.write(tmp_path / "artifacts" / "throwaway.json")
    assert artifact["acr_armed"] is True


def test_write_records_acr_armed_false_when_the_env_var_is_unset(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("ASK_DEV_ACCEPTANCE_ACR", raising=False)
    _init_throwaway_git_repo(tmp_path)
    script_path = tmp_path / "smoke_throwaway.py"
    script_path.write_text("# throwaway\n")
    recorder = ScenarioRecorder(scenario_id="throwaway", script_path=script_path)
    artifact = recorder.write(tmp_path / "artifacts" / "throwaway.json")
    assert artifact["acr_armed"] is False


def test_write_records_acr_armed_false_for_any_non_1_value(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Only the exact string "1" arms it -- same convention as
    ASK_DEV_ACCEPTANCE_ACR everywhere else in the launcher, so "true"/"yes"/
    a stray leftover value from a previous run cannot silently arm it."""

    monkeypatch.setenv("ASK_DEV_ACCEPTANCE_ACR", "0")
    _init_throwaway_git_repo(tmp_path)
    script_path = tmp_path / "smoke_throwaway.py"
    script_path.write_text("# throwaway\n")
    recorder = ScenarioRecorder(scenario_id="throwaway", script_path=script_path)
    artifact = recorder.write(tmp_path / "artifacts" / "throwaway.json")
    assert artifact["acr_armed"] is False


def test_write_records_the_start_digest_not_a_later_one(tmp_path: Path) -> None:
    """The control: when nothing moves, the recorded digest is the one
    captured at start and equals the tree's own."""

    _init_throwaway_git_repo(tmp_path)
    script_path = tmp_path / "smoke_throwaway.py"
    script_path.write_text("# throwaway\n")
    subprocess.run(["git", "add", "smoke_throwaway.py"], cwd=tmp_path, check=True)
    subprocess.run(
        ["git", "commit", "-q", "-m", "add script"], cwd=tmp_path, check=True
    )
    recorder = ScenarioRecorder(scenario_id="throwaway", script_path=script_path)
    started = recorder.capture_runtime_digest(tmp_path)
    artifact = recorder.write(tmp_path / "artifacts" / "throwaway.json")
    assert artifact["runtime_dependencies"] == started
    assert artifact["runtime_digest"] == runtime_dependency_digest(tmp_path)
    # Every covered path is recorded individually, not just rolled up --
    # that is what lets a staleness declaration name exactly what drifted.
    assert set(artifact["runtime_dependencies"]) == set(RUNTIME_DEPENDENCY_PATHS)


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


def test_write_still_reports_dirty_for_an_untracked_file_outside_the_artifacts_dir(
    tmp_path: Path,
) -> None:
    """The negative control for the exclusion above: ignoring the artifacts
    directory must not become ignoring untracked files in general. An
    untracked source file nested inside an otherwise-untracked directory --
    the case git's default status output collapses -- is exactly what
    tree_clean exists to catch."""

    _init_throwaway_git_repo(tmp_path)
    script_path = tmp_path / "smoke_throwaway.py"
    script_path.write_text("# throwaway\n")
    subprocess.run(["git", "add", "smoke_throwaway.py"], cwd=tmp_path, check=True)
    subprocess.run(
        ["git", "commit", "-q", "-m", "add script"], cwd=tmp_path, check=True
    )
    stray = tmp_path / "src" / "dev_health_ops" / "never_committed.py"
    stray.parent.mkdir(parents=True, exist_ok=True)
    stray.write_text("# uncommitted production code that a run would execute\n")
    recorder = ScenarioRecorder(scenario_id="throwaway", script_path=script_path)
    artifact = recorder.write(tmp_path / "artifacts" / "throwaway.json")
    assert artifact["tree_clean"] is False


def test_write_reports_dirty_for_a_production_path_that_merely_contains_the_prefix(
    tmp_path: Path,
) -> None:
    """codex finding (HIGH, 2026-08-03): the exclusion was a substring test
    against the whole status line, so a genuinely dirty PRODUCTION file
    whose path happened to contain "tests/acceptance/artifacts/" anywhere
    was discarded and the tree reported clean. This is the exact repro."""

    _init_throwaway_git_repo(tmp_path)
    script_path = tmp_path / "smoke_throwaway.py"
    script_path.write_text("# throwaway\n")
    subprocess.run(["git", "add", "smoke_throwaway.py"], cwd=tmp_path, check=True)
    subprocess.run(
        ["git", "commit", "-q", "-m", "add script"], cwd=tmp_path, check=True
    )
    impostor = tmp_path / "src" / "tests" / "acceptance" / "artifacts"
    impostor.mkdir(parents=True, exist_ok=True)
    (impostor / "runtime_override.py").write_text("# dirty production code\n")
    recorder = ScenarioRecorder(scenario_id="throwaway", script_path=script_path)
    artifact = recorder.write(tmp_path / "artifacts" / "throwaway.json")
    assert artifact["tree_clean"] is False


def test_write_reports_dirty_for_a_rename_out_of_the_artifacts_directory(
    tmp_path: Path,
) -> None:
    """codex finding (HIGH, 2026-08-03), second half: a rename record names
    TWO paths. Excluding the whole record because its source is inside the
    artifacts directory hid a destination that landed in production code."""

    _init_throwaway_git_repo(tmp_path)
    script_path = tmp_path / "smoke_throwaway.py"
    script_path.write_text("# throwaway\n")
    tracked_artifact = tmp_path / "tests" / "acceptance" / "artifacts" / "baseline.json"
    tracked_artifact.parent.mkdir(parents=True, exist_ok=True)
    tracked_artifact.write_text("{}")
    subprocess.run(
        [
            "git",
            "add",
            "smoke_throwaway.py",
            "tests/acceptance/artifacts/baseline.json",
        ],
        cwd=tmp_path,
        check=True,
    )
    subprocess.run(
        ["git", "commit", "-q", "-m", "add script and artifact"],
        cwd=tmp_path,
        check=True,
    )
    destination = tmp_path / "src" / "dev_health_ops" / "runtime_override.py"
    destination.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [
            "git",
            "mv",
            "tests/acceptance/artifacts/baseline.json",
            "src/dev_health_ops/runtime_override.py",
        ],
        cwd=tmp_path,
        check=True,
    )
    recorder = ScenarioRecorder(scenario_id="throwaway", script_path=script_path)
    artifact = recorder.write(tmp_path / "artifacts" / "throwaway.json")
    assert artifact["tree_clean"] is False
