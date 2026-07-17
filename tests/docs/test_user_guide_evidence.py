import hashlib
import json
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Final, TypeAlias

import pytest

from scripts import validate_user_guide_evidence
from scripts.user_guide_evidence_contract import CANONICAL_TASKS, SourceRevision
from scripts.user_guide_evidence_validation import validate_evidence_root

SOURCE_REVISION: Final = SourceRevision(
    head_sha="f" * 40,
    committed_at=datetime(2026, 7, 17, 12, 0, tzinfo=UTC),
)
VIEWPORTS: Final = (375, 768, 1280)
HEIGHT: Final = 900
JsonValue: TypeAlias = str | int | dict[str, "JsonValue"] | list["JsonValue"]
JsonObject: TypeAlias = dict[str, JsonValue]


def _write_png(path: Path, width: int) -> str:
    payload = (
        b"\x89PNG\r\n\x1a\n"
        + b"\x00\x00\x00\rIHDR"
        + width.to_bytes(4, "big")
        + HEIGHT.to_bytes(4, "big")
        + b"\x08\x02\x00\x00\x00"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return hashlib.sha256(payload).hexdigest()


def _artifact(
    route: str, width: int, captured_at: datetime, directory: Path
) -> JsonObject:
    slug = route.strip("/").split("/")[-1]
    relative_file = f"captures/{slug}-{width}.png"
    return {
        "file": relative_file,
        "route": route,
        "viewport": {"width": width, "height": HEIGHT},
        "state": "documentation article with evidence trail",
        "browser": {
            "engine": "Chrome Stable via Playwright channel chrome",
            "version": "150.0",
        },
        "captured_at": captured_at.isoformat().replace("+00:00", "Z"),
        "sha256": _write_png(directory / relative_file, width),
        "console": {"errors": 0, "page_errors": 0},
        "network": {"failed_requests": 0, "http_error_responses": 0},
        "a11y": {"serious": 0, "critical": 0},
        "sanitization": {"status": "sanitized", "notes": "No secrets or user data."},
    }


@pytest.fixture
def valid_evidence_root(tmp_path: Path) -> Path:
    captured_at = SOURCE_REVISION.committed_at + timedelta(minutes=1)
    for task in CANONICAL_TASKS:
        directory = tmp_path / f"task-{task.number}-unified-cloudflare-documentation"
        artifacts: list[JsonValue] = [
            _artifact(route, width, captured_at, directory)
            for route in task.routes
            for width in VIEWPORTS
        ]
        manifest: JsonObject = {
            "task": task.number,
            "source": {"head_sha": SOURCE_REVISION.head_sha},
            "artifacts": artifacts,
        }
        (directory / "manifest.json").write_text(
            json.dumps(manifest, indent=2), encoding="utf-8"
        )
    return tmp_path


def _manifest_path(evidence_root: Path, task_number: int) -> Path:
    return (
        evidence_root
        / f"task-{task_number}-unified-cloudflare-documentation"
        / "manifest.json"
    )


def _rewrite_manifest(
    manifest_path: Path, mutate: Callable[[JsonObject], None]
) -> None:
    manifest: JsonObject = json.loads(manifest_path.read_text(encoding="utf-8"))
    mutate(manifest)
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def test_accepts_exact_canonical_manifest_inventory(valid_evidence_root: Path) -> None:
    assert sum(len(task.routes) for task in CANONICAL_TASKS) * len(VIEWPORTS) == 48
    assert validate_evidence_root(valid_evidence_root, SOURCE_REVISION) == ()


def test_rejects_stale_or_undeclared_png(valid_evidence_root: Path) -> None:
    stale = (
        valid_evidence_root / "task-9-unified-cloudflare-documentation" / "stale.png"
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
        first_artifact["captured_at"] = "2026-07-17T11:59:59Z"

    _rewrite_manifest(manifest_path, mutate)

    errors = validate_evidence_root(valid_evidence_root, SOURCE_REVISION)

    assert "task 7: source.head_sha does not match source HEAD" in errors
    assert "task 7: captures/first-10-minutes-375.png SHA-256 mismatch" in errors
    assert "task 7: captures/first-10-minutes-375.png predates source commit" in errors


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
