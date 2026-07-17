import hashlib
import json
import os
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Final, TypeAlias

import pytest

from scripts.user_guide_evidence_contract import CANONICAL_TASKS, SourceRevision

SOURCE_COMMITTED_AT: Final = datetime(2026, 7, 17, 12, 0, tzinfo=UTC)
CAPTURE_STARTED_AT: Final = SOURCE_COMMITTED_AT + timedelta(minutes=1)
CAPTURED_AT: Final = CAPTURE_STARTED_AT + timedelta(minutes=1)
FILE_MTIME: Final = CAPTURED_AT + timedelta(seconds=30)
VALIDATED_AT: Final = CAPTURED_AT + timedelta(minutes=2)
SOURCE_REVISION: Final = SourceRevision(
    head_sha="f" * 40,
    committed_at=SOURCE_COMMITTED_AT,
    validated_at=VALIDATED_AT,
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
        + path.as_posix().encode()
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
    for task in CANONICAL_TASKS:
        directory = tmp_path / f"task-{task.number}-unified-cloudflare-documentation"
        artifacts: list[JsonValue] = [
            _artifact(route, width, CAPTURED_AT, directory)
            for route in task.routes
            for width in VIEWPORTS
        ]
        manifest: JsonObject = {
            "task": task.number,
            "source": {
                "head_sha": SOURCE_REVISION.head_sha,
                "capture_started_at": CAPTURE_STARTED_AT.isoformat().replace(
                    "+00:00", "Z"
                ),
            },
            "artifacts": artifacts,
        }
        (directory / "manifest.json").write_text(
            json.dumps(manifest, indent=2), encoding="utf-8"
        )
        for artifact_path in directory.rglob("*.png"):
            os.utime(artifact_path, (FILE_MTIME.timestamp(), FILE_MTIME.timestamp()))
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
