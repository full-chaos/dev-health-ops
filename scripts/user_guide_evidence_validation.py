from __future__ import annotations

import hashlib
from pathlib import Path

from pydantic import ValidationError

from scripts.user_guide_evidence_contract import (
    CANONICAL_TASKS,
    VIEWPORT_HEIGHT,
    VIEWPORT_WIDTHS,
    Artifact,
    CanonicalTask,
    Manifest,
    SourceRevision,
)

PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"
PNG_IHDR_MARKER = b"\x00\x00\x00\rIHDR"


def _task_directory(evidence_root: Path, task: CanonicalTask) -> Path:
    return evidence_root / f"task-{task.number}-unified-cloudflare-documentation"


def _expected_file(route: str, width: int) -> str:
    return f"captures/{route.strip('/').split('/')[-1]}-{width}.png"


def _expected_keys(task: CanonicalTask) -> set[tuple[str, int, int]]:
    return {
        (route, width, VIEWPORT_HEIGHT)
        for route in task.routes
        for width in VIEWPORT_WIDTHS
    }


def _png_dimensions(path: Path) -> tuple[int, int] | None:
    header = path.read_bytes()[:24]
    if not header.startswith(PNG_SIGNATURE) or header[8:16] != PNG_IHDR_MARKER:
        return None
    return (int.from_bytes(header[16:20], "big"), int.from_bytes(header[20:24], "big"))


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _load_manifest(path: Path) -> tuple[Manifest | None, tuple[str, ...]]:
    if not path.is_file():
        return None, (f"missing canonical manifest: {path}",)
    try:
        return Manifest.model_validate_json(path.read_text(encoding="utf-8")), ()
    except ValidationError as error:
        return None, (f"invalid manifest {path}: {error}",)


def _artifact_errors(
    artifact: Artifact,
    task: CanonicalTask,
    task_directory: Path,
    source: SourceRevision,
) -> tuple[str, ...]:
    prefix = f"task {task.number}: {artifact.file}"
    errors: list[str] = []
    key = (artifact.route, artifact.viewport.width, artifact.viewport.height)
    if key not in _expected_keys(task):
        errors.append(f"{prefix} declares an unexpected route or viewport")
    elif artifact.file != _expected_file(artifact.route, artifact.viewport.width):
        errors.append(f"{prefix} does not use the canonical artifact filename")
    if (
        artifact.captured_at.tzinfo is None
        or artifact.captured_at <= source.committed_at
    ):
        errors.append(f"{prefix} predates source commit")
    if artifact.console.errors != 0 or artifact.console.page_errors != 0:
        errors.append(f"{prefix} has console failures")
    if (
        artifact.network.failed_requests != 0
        or artifact.network.http_error_responses != 0
    ):
        errors.append(f"{prefix} has network failures")
    if artifact.a11y.serious != 0 or artifact.a11y.critical != 0:
        errors.append(f"{prefix} has blocking accessibility violations")
    if artifact.sanitization.status != "sanitized":
        errors.append(f"{prefix} is not marked sanitized")

    candidate = task_directory / artifact.file
    if candidate.is_symlink() or not candidate.resolve().is_relative_to(
        task_directory.resolve()
    ):
        errors.append(f"{prefix} escapes its task directory")
    elif not candidate.is_file():
        errors.append(f"{prefix} is missing")
    else:
        if _png_dimensions(candidate) != (
            artifact.viewport.width,
            artifact.viewport.height,
        ):
            errors.append(f"{prefix} has incorrect PNG dimensions")
        if _sha256(candidate) != artifact.sha256:
            errors.append(f"{prefix} SHA-256 mismatch")
    return tuple(errors)


def _manifest_errors(
    task: CanonicalTask,
    evidence_root: Path,
    source: SourceRevision,
) -> tuple[str, ...]:
    task_directory = _task_directory(evidence_root, task)
    manifest, errors = _load_manifest(task_directory / "manifest.json")
    if manifest is None:
        return errors

    findings = list(errors)
    if manifest.task != task.number:
        findings.append(f"task {task.number}: manifest task does not match directory")
    if manifest.source.head_sha != source.head_sha:
        findings.append(
            f"task {task.number}: source.head_sha does not match source HEAD"
        )

    expected_keys = _expected_keys(task)
    artifact_keys = {
        (artifact.route, artifact.viewport.width, artifact.viewport.height)
        for artifact in manifest.artifacts
    }
    if len(manifest.artifacts) != len(artifact_keys):
        findings.append(f"task {task.number}: duplicate route and viewport artifact")
    if artifact_keys != expected_keys:
        findings.append(
            f"task {task.number}: route and viewport inventory is not exact"
        )
    for artifact in manifest.artifacts:
        findings.extend(_artifact_errors(artifact, task, task_directory, source))

    declared_pngs = {task_directory / artifact.file for artifact in manifest.artifacts}
    for png_path in sorted(task_directory.rglob("*.png")):
        if png_path not in declared_pngs:
            findings.append(
                f"undeclared PNG artifact: {png_path.relative_to(evidence_root)}"
            )
    return tuple(findings)


def validate_evidence_root(
    evidence_root: Path, source: SourceRevision
) -> tuple[str, ...]:
    errors = [
        error
        for task in CANONICAL_TASKS
        for error in _manifest_errors(task, evidence_root, source)
    ]
    return tuple(sorted(errors))
