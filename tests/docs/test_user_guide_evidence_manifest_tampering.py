import os
from pathlib import Path

import pytest

from scripts.user_guide_evidence_validation import validate_evidence_root
from tests.docs.user_guide_evidence_fixtures import (
    FILE_MTIME,
    SOURCE_REVISION,
    JsonObject,
    _manifest_path,
    _rewrite_manifest,
)


@pytest.mark.parametrize(
    "field",
    (
        "file",
        "route",
        "viewport",
        "state",
        "browser",
        "captured_at",
        "sha256",
        "console",
        "network",
        "a11y",
        "sanitization",
    ),
)
def test_requires_each_artifact_receipt_field(
    valid_evidence_root: Path, field: str
) -> None:
    manifest_path = _manifest_path(valid_evidence_root, 7)

    def mutate(manifest: JsonObject) -> None:
        artifacts = manifest["artifacts"]
        assert isinstance(artifacts, list)
        first_artifact = artifacts[0]
        assert isinstance(first_artifact, dict)
        del first_artifact[field]

    _rewrite_manifest(manifest_path, mutate)

    errors = validate_evidence_root(valid_evidence_root, SOURCE_REVISION)

    assert any(
        error.startswith("invalid manifest") and field in error for error in errors
    )


def test_rejects_manifest_task_identity_mismatch(valid_evidence_root: Path) -> None:
    manifest_path = _manifest_path(valid_evidence_root, 7)

    def mutate(manifest: JsonObject) -> None:
        manifest["task"] = 8

    _rewrite_manifest(manifest_path, mutate)

    errors = validate_evidence_root(valid_evidence_root, SOURCE_REVISION)

    assert "task 7: manifest task does not match directory" in errors


def test_rejects_duplicate_artifact_file_reuse(valid_evidence_root: Path) -> None:
    manifest_path = _manifest_path(valid_evidence_root, 9)

    def mutate(manifest: JsonObject) -> None:
        artifacts = manifest["artifacts"]
        assert isinstance(artifacts, list)
        first_artifact = artifacts[0]
        second_artifact = artifacts[1]
        assert isinstance(first_artifact, dict)
        assert isinstance(second_artifact, dict)
        second_artifact["file"] = first_artifact["file"]
        second_artifact["sha256"] = first_artifact["sha256"]

    _rewrite_manifest(manifest_path, mutate)

    errors = validate_evidence_root(valid_evidence_root, SOURCE_REVISION)

    assert (
        "task 9: duplicate artifact file reused across route/viewport pairs" in errors
    )


def test_rejects_duplicate_route_and_viewport_artifact(
    valid_evidence_root: Path,
) -> None:
    manifest_path = _manifest_path(valid_evidence_root, 9)

    def mutate(manifest: JsonObject) -> None:
        artifacts = manifest["artifacts"]
        assert isinstance(artifacts, list)
        first_artifact = artifacts[0]
        second_artifact = artifacts[1]
        assert isinstance(first_artifact, dict)
        assert isinstance(second_artifact, dict)
        second_artifact["route"] = first_artifact["route"]
        second_artifact["viewport"] = first_artifact["viewport"]

    _rewrite_manifest(manifest_path, mutate)

    errors = validate_evidence_root(valid_evidence_root, SOURCE_REVISION)

    assert "task 9: duplicate route and viewport artifact" in errors


def test_rejects_digest_identical_state_image_under_different_name(
    valid_evidence_root: Path,
) -> None:
    manifest_path = _manifest_path(valid_evidence_root, 10)
    source_path = (
        valid_evidence_root
        / "task-10-unified-cloudflare-documentation"
        / "captures"
        / "ai-impact-375.png"
    )
    target_path = (
        valid_evidence_root
        / "task-10-unified-cloudflare-documentation"
        / "captures"
        / "ai-review-load-375.png"
    )
    target_path.write_bytes(source_path.read_bytes())
    os.utime(target_path, (FILE_MTIME.timestamp(), FILE_MTIME.timestamp()))

    def mutate(manifest: JsonObject) -> None:
        artifacts = manifest["artifacts"]
        assert isinstance(artifacts, list)
        source_artifact = artifacts[0]
        target_artifact = artifacts[3]
        assert isinstance(source_artifact, dict)
        assert isinstance(target_artifact, dict)
        target_artifact["sha256"] = source_artifact["sha256"]
        target_artifact["state"] = "alternate evidence-trail state"

    _rewrite_manifest(manifest_path, mutate)

    errors = validate_evidence_root(valid_evidence_root, SOURCE_REVISION)

    assert (
        "task 10: duplicate image digest reused across differently named or stateful artifacts"
        in errors
    )


@pytest.mark.parametrize(
    "scheme",
    [
        "http://evil.example/x",
        "https://evil.example/x",
        "file:///etc/passwd",
        "data:text/plain;base64,eA==",
    ],
)
def test_rejects_raw_url_scheme_in_free_text_fields(
    valid_evidence_root: Path, scheme: str
) -> None:
    manifest_path = _manifest_path(valid_evidence_root, 10)

    def mutate(manifest: JsonObject) -> None:
        artifacts = manifest["artifacts"]
        assert isinstance(artifacts, list)
        first_artifact = artifacts[0]
        assert isinstance(first_artifact, dict)
        sanitization = first_artifact["sanitization"]
        assert isinstance(sanitization, dict)
        sanitization["notes"] = f"No secrets or user data. See {scheme}"

    _rewrite_manifest(manifest_path, mutate)

    errors = validate_evidence_root(valid_evidence_root, SOURCE_REVISION)

    assert any(
        "contains a forbidden URL scheme in sanitization.notes" in error
        for error in errors
    )
