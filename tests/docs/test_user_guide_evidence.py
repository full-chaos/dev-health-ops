import hashlib
import os
import subprocess
import sys
from datetime import timedelta
from pathlib import Path

import pytest

from scripts import validate_user_guide_evidence
from scripts.user_guide_evidence_contract import canonical_tasks
from scripts.user_guide_evidence_validation import validate_evidence_root
from tests.docs.user_guide_evidence_fixtures import (
    CAPTURE_STARTED_AT,
    FILE_MTIME,
    SOURCE_REVISION,
    VIEWPORTS,
    JsonObject,
    _manifest_path,
    _rewrite_manifest,
    _write_png,
)


def test_accepts_exact_canonical_manifest_inventory(valid_evidence_root: Path) -> None:
    tasks = canonical_tasks()

    assert len(tasks) == 5
    assert sum(len(task.routes) for task in tasks) == 16
    assert sum(len(task.routes) for task in tasks) * len(VIEWPORTS) == 48
    assert [len(task.routes) * len(VIEWPORTS) for task in tasks] == [
        12,
        9,
        9,
        12,
        6,
    ]
    assert validate_evidence_root(valid_evidence_root, SOURCE_REVISION) == ()


def test_ignores_unrelated_historical_evidence_outside_the_five_tasks(
    valid_evidence_root: Path,
) -> None:
    manifest_path = (
        valid_evidence_root
        / "task-12-unified-cloudflare-documentation"
        / "manifest.json"
    )
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text("{}", encoding="utf-8")
    _write_png(valid_evidence_root / "historical" / "nested" / "stale.png", 375)

    assert validate_evidence_root(valid_evidence_root, SOURCE_REVISION) == ()


def test_rejects_known_task_three_final_orphan(valid_evidence_root: Path) -> None:
    (valid_evidence_root / "task-3-final").mkdir()

    errors = validate_evidence_root(valid_evidence_root, SOURCE_REVISION)

    assert "known noncanonical evidence orphan for this wave: task-3-final" in errors


def test_rejects_recursive_undeclared_png(valid_evidence_root: Path) -> None:
    stale = (
        valid_evidence_root
        / "task-9-unified-cloudflare-documentation"
        / "captures"
        / "nested"
        / "stale.png"
    )
    _write_png(stale, 375)

    errors = validate_evidence_root(valid_evidence_root, SOURCE_REVISION)

    assert (
        f"undeclared PNG artifact: {stale.relative_to(valid_evidence_root)}" in errors
    )


def test_rejects_source_head_digest_and_freshness_drift(
    valid_evidence_root: Path,
) -> None:
    manifest_path = _manifest_path(valid_evidence_root, 7)

    def mutate(manifest: JsonObject) -> None:
        source = manifest["source"]
        assert isinstance(source, dict)
        source["head_sha"] = "0" * 40
        artifacts = manifest["artifacts"]
        assert isinstance(artifacts, list)
        first_artifact = artifacts[0]
        assert isinstance(first_artifact, dict)
        first_artifact["sha256"] = "0" * 64
        first_artifact["captured_at"] = CAPTURE_STARTED_AT.isoformat().replace(
            "+00:00", "Z"
        )

    _rewrite_manifest(manifest_path, mutate)

    errors = validate_evidence_root(valid_evidence_root, SOURCE_REVISION)

    assert "task 7: source.head_sha does not match source HEAD" in errors
    assert "task 7: captures/first-10-minutes-375.png SHA-256 mismatch" in errors
    assert "task 7: captures/first-10-minutes-375.png predates capture start" in errors


def test_rejects_file_modification_time_before_capture_start(
    valid_evidence_root: Path,
) -> None:
    artifact_path = (
        valid_evidence_root
        / "task-7-unified-cloudflare-documentation"
        / "captures"
        / "first-10-minutes-375.png"
    )
    stale_mtime = CAPTURE_STARTED_AT - timedelta(seconds=1)
    os.utime(artifact_path, (stale_mtime.timestamp(), stale_mtime.timestamp()))

    errors = validate_evidence_root(valid_evidence_root, SOURCE_REVISION)

    assert (
        "task 7: captures/first-10-minutes-375.png predates capture start on disk"
        in errors
    )


def test_rejects_missing_canonical_artifact(valid_evidence_root: Path) -> None:
    artifact_path = (
        valid_evidence_root
        / "task-7-unified-cloudflare-documentation"
        / "captures"
        / "first-10-minutes-375.png"
    )
    artifact_path.unlink()

    errors = validate_evidence_root(valid_evidence_root, SOURCE_REVISION)

    assert "task 7: captures/first-10-minutes-375.png is missing" in errors


def test_rejects_capture_start_before_final_source_commit(
    valid_evidence_root: Path,
) -> None:
    manifest_path = _manifest_path(valid_evidence_root, 7)

    def mutate(manifest: JsonObject) -> None:
        source = manifest["source"]
        assert isinstance(source, dict)
        source["capture_started_at"] = SOURCE_REVISION.committed_at.isoformat().replace(
            "+00:00", "Z"
        )

    _rewrite_manifest(manifest_path, mutate)

    errors = validate_evidence_root(valid_evidence_root, SOURCE_REVISION)

    assert "task 7: capture start predates source commit" in errors


def test_rejects_incorrect_png_dimensions(valid_evidence_root: Path) -> None:
    artifact_path = (
        valid_evidence_root
        / "task-8-unified-cloudflare-documentation"
        / "captures"
        / "quadrants-375.png"
    )
    _write_png(artifact_path, 320)
    os.utime(artifact_path, (FILE_MTIME.timestamp(), FILE_MTIME.timestamp()))

    errors = validate_evidence_root(valid_evidence_root, SOURCE_REVISION)

    assert "task 8: captures/quadrants-375.png has incorrect PNG dimensions" in errors


def test_rejects_malformed_png_even_when_its_digest_matches(
    valid_evidence_root: Path,
) -> None:
    artifact_path = (
        valid_evidence_root
        / "task-8-unified-cloudflare-documentation"
        / "captures"
        / "quadrants-375.png"
    )
    payload = b"not a PNG"
    artifact_path.write_bytes(payload)
    os.utime(artifact_path, (FILE_MTIME.timestamp(), FILE_MTIME.timestamp()))
    manifest_path = _manifest_path(valid_evidence_root, 8)

    def mutate(manifest: JsonObject) -> None:
        artifacts = manifest["artifacts"]
        assert isinstance(artifacts, list)
        first_artifact = artifacts[0]
        assert isinstance(first_artifact, dict)
        first_artifact["sha256"] = hashlib.sha256(payload).hexdigest()

    _rewrite_manifest(manifest_path, mutate)

    errors = validate_evidence_root(valid_evidence_root, SOURCE_REVISION)

    assert "task 8: captures/quadrants-375.png is not a valid PNG" in errors


def test_rejects_future_capture_timestamp(valid_evidence_root: Path) -> None:
    manifest_path = _manifest_path(valid_evidence_root, 7)

    def mutate(manifest: JsonObject) -> None:
        artifacts = manifest["artifacts"]
        assert isinstance(artifacts, list)
        first_artifact = artifacts[0]
        assert isinstance(first_artifact, dict)
        first_artifact["captured_at"] = (
            (SOURCE_REVISION.validated_at + timedelta(seconds=1))
            .isoformat()
            .replace("+00:00", "Z")
        )

    _rewrite_manifest(manifest_path, mutate)

    errors = validate_evidence_root(valid_evidence_root, SOURCE_REVISION)

    assert "task 7: captures/first-10-minutes-375.png postdates validation" in errors


def test_rejects_timezone_naive_capture_start(valid_evidence_root: Path) -> None:
    manifest_path = _manifest_path(valid_evidence_root, 7)

    def mutate(manifest: JsonObject) -> None:
        source = manifest["source"]
        assert isinstance(source, dict)
        source["capture_started_at"] = "2026-07-17T12:01:00"

    _rewrite_manifest(manifest_path, mutate)

    errors = validate_evidence_root(valid_evidence_root, SOURCE_REVISION)

    assert "task 7: capture start must include a timezone" in errors


def test_rejects_nonzero_browser_and_sanitization_receipts(
    valid_evidence_root: Path,
) -> None:
    manifest_path = _manifest_path(valid_evidence_root, 8)

    def mutate(manifest: JsonObject) -> None:
        artifacts = manifest["artifacts"]
        assert isinstance(artifacts, list)
        first_artifact = artifacts[0]
        assert isinstance(first_artifact, dict)
        console = first_artifact["console"]
        network = first_artifact["network"]
        a11y = first_artifact["a11y"]
        sanitization = first_artifact["sanitization"]
        assert isinstance(console, dict)
        assert isinstance(network, dict)
        assert isinstance(a11y, dict)
        assert isinstance(sanitization, dict)
        console["errors"] = 1
        network["failed_requests"] = 1
        a11y["serious"] = 1
        sanitization["status"] = "raw"

    _rewrite_manifest(manifest_path, mutate)

    errors = validate_evidence_root(valid_evidence_root, SOURCE_REVISION)

    assert "task 8: captures/quadrants-375.png has console failures" in errors
    assert "task 8: captures/quadrants-375.png has network failures" in errors
    assert (
        "task 8: captures/quadrants-375.png has blocking accessibility violations"
        in errors
    )
    assert "task 8: captures/quadrants-375.png is not marked sanitized" in errors


def test_main_requires_an_explicit_evidence_root(
    monkeypatch: pytest.MonkeyPatch, valid_evidence_root: Path
) -> None:
    monkeypatch.setattr(
        validate_user_guide_evidence, "_source_revision", lambda: SOURCE_REVISION
    )

    assert (
        validate_user_guide_evidence.main(["--evidence-root", str(valid_evidence_root)])
        == 0
    )


def test_validator_cli_runs_from_the_documentation_qa_directory() -> None:
    repository_root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [
            sys.executable,
            "../scripts/validate_user_guide_evidence.py",
            "--help",
        ],
        capture_output=True,
        check=False,
        cwd=repository_root / "docs-qa",
        text=True,
    )

    assert result.returncode == 0
