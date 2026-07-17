from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path
from typing import Final

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
FORBIDDEN_SCHEMES: Final = ("http://", "https://", "file:", "data:")
KNOWN_NONCANONICAL_ORPHAN: Final = "task-3-final"


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


def _forbidden_scheme_errors(
    prefix: str, fields: tuple[tuple[str, str], ...]
) -> tuple[str, ...]:
    return tuple(
        f"{prefix} contains a forbidden URL scheme in {name}"
        for name, value in fields
        if any(scheme in value.lower() for scheme in FORBIDDEN_SCHEMES)
    )


def _artifact_errors(
    artifact: Artifact,
    task: CanonicalTask,
    task_directory: Path,
    source: SourceRevision,
    capture_started_at: datetime,
) -> tuple[str, ...]:
    prefix = f"task {task.number}: {artifact.file}"
    errors: list[str] = []
    key = (artifact.route, artifact.viewport.width, artifact.viewport.height)
    if key not in _expected_keys(task):
        errors.append(f"{prefix} declares an unexpected route or viewport")
    elif artifact.file != _expected_file(artifact.route, artifact.viewport.width):
        errors.append(f"{prefix} does not use the canonical artifact filename")
    if artifact.captured_at.tzinfo is None:
        errors.append(f"{prefix} predates capture start")
    elif capture_started_at.tzinfo is None:
        pass
    elif artifact.captured_at <= capture_started_at:
        errors.append(f"{prefix} predates capture start")
    elif artifact.captured_at > source.validated_at:
        errors.append(f"{prefix} postdates validation")
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
    errors.extend(
        _forbidden_scheme_errors(
            prefix,
            (
                ("state", artifact.state),
                ("browser.engine", artifact.browser.engine),
                ("browser.version", artifact.browser.version),
                ("sanitization.status", artifact.sanitization.status),
                ("sanitization.notes", artifact.sanitization.notes),
            ),
        )
    )

    candidate = task_directory / artifact.file
    if candidate.is_symlink() or not candidate.resolve().is_relative_to(
        task_directory.resolve()
    ):
        errors.append(f"{prefix} escapes its task directory")
    elif not candidate.is_file():
        errors.append(f"{prefix} is missing")
    else:
        modified_at = datetime.fromtimestamp(candidate.stat().st_mtime, tz=UTC)
        if capture_started_at.tzinfo is not None and modified_at <= capture_started_at:
            errors.append(f"{prefix} predates capture start on disk")
        dimensions = _png_dimensions(candidate)
        if dimensions is None:
            errors.append(f"{prefix} is not a valid PNG")
        elif dimensions != (artifact.viewport.width, artifact.viewport.height):
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
    capture_started_at = manifest.source.capture_started_at
    if capture_started_at.tzinfo is None:
        findings.append(f"task {task.number}: capture start must include a timezone")
    elif capture_started_at <= source.committed_at:
        findings.append(f"task {task.number}: capture start predates source commit")

    expected_keys = _expected_keys(task)
    artifact_keys = {
        (artifact.route, artifact.viewport.width, artifact.viewport.height)
        for artifact in manifest.artifacts
    }
    if len(manifest.artifacts) != len(artifact_keys):
        findings.append(f"task {task.number}: duplicate route and viewport artifact")
    if len({artifact.file for artifact in manifest.artifacts}) != len(
        manifest.artifacts
    ):
        findings.append(
            f"task {task.number}: duplicate artifact file reused across route/viewport pairs"
        )
    if artifact_keys != expected_keys:
        findings.append(
            f"task {task.number}: route and viewport inventory is not exact"
        )
    for artifact in manifest.artifacts:
        findings.extend(
            _artifact_errors(artifact, task, task_directory, source, capture_started_at)
        )
    return tuple(findings)


def _recursive_png_paths(directory: Path) -> set[Path]:
    return {
        path
        for path in directory.rglob("*")
        if path.is_file() and path.suffix.lower() == ".png"
    }


def _canonical_inventory_errors(evidence_root: Path) -> tuple[str, ...]:
    errors: list[str] = []
    for task in CANONICAL_TASKS:
        task_directory = _task_directory(evidence_root, task)
        manifest, _ = _load_manifest(task_directory / "manifest.json")
        if manifest is not None:
            declared_pngs = {
                task_directory / artifact.file for artifact in manifest.artifacts
            }
            errors.extend(
                f"undeclared PNG artifact: {path.relative_to(evidence_root)}"
                for path in sorted(_recursive_png_paths(task_directory) - declared_pngs)
            )
    return tuple(errors)


def _known_orphan_errors(evidence_root: Path) -> tuple[str, ...]:
    orphan = evidence_root / KNOWN_NONCANONICAL_ORPHAN
    if orphan.exists():
        return (
            f"known noncanonical evidence orphan for this wave: {KNOWN_NONCANONICAL_ORPHAN}",
        )
    return ()


def _duplicate_image_errors(evidence_root: Path) -> tuple[str, ...]:
    by_digest: dict[str, list[tuple[int, Artifact]]] = {}
    for task in CANONICAL_TASKS:
        task_directory = _task_directory(evidence_root, task)
        manifest, _ = _load_manifest(task_directory / "manifest.json")
        if manifest is not None:
            for artifact in manifest.artifacts:
                candidate = task_directory / artifact.file
                if (
                    candidate.is_file()
                    and not candidate.is_symlink()
                    and candidate.resolve().is_relative_to(task_directory.resolve())
                ):
                    by_digest.setdefault(_sha256(candidate), []).append(
                        (task.number, artifact)
                    )
    errors: list[str] = []
    for artifacts in by_digest.values():
        names_and_states = {
            (artifact.file, artifact.state) for _, artifact in artifacts
        }
        if len(names_and_states) > 1:
            task_numbers = {task_number for task_number, _ in artifacts}
            prefix = (
                f"task {next(iter(task_numbers))}"
                if len(task_numbers) == 1
                else "evidence root"
            )
            errors.append(
                f"{prefix}: duplicate image digest reused across differently named or stateful artifacts"
            )
    return tuple(errors)


def validate_evidence_root(
    evidence_root: Path, source: SourceRevision
) -> tuple[str, ...]:
    if not evidence_root.is_dir():
        return (f"missing evidence root: {evidence_root}",)
    errors = list(_known_orphan_errors(evidence_root))
    errors.extend(_canonical_inventory_errors(evidence_root))
    errors.extend(
        error
        for task in CANONICAL_TASKS
        for error in _manifest_errors(task, evidence_root, source)
    )
    errors.extend(_duplicate_image_errors(evidence_root))
    return tuple(sorted(errors))
