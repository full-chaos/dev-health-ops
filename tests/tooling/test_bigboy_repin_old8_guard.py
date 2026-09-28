"""bigboy-repin.sh: OLD8 is checked BEFORE any file is touched, both for shape and against the
ACTUALLY-running build (CHAOS-7014).

D2801: a mistyped OLD8 (9 chars, "bd25cd0a9" instead of "bd25cd0a") made the repin's own `cp` of
_records/bigboy-$OLD8/*.sh fail under `set -euo pipefail` BEFORE the sed rewrite of
compose.bigboy.images.yml ever ran. compose.bigboy.images.yml was backed up but never actually
repointed, and every downstream STEP in bigboy-cut.sh (migrate/up/up-workers/rest/...) still
reported rc=0 against the stale, pre-cut images -- a void proof run that looked green.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
REPIN = ROOT / "ci" / "bigboy" / "bigboy-repin.sh"


def _lines() -> list[str]:
    return REPIN.read_text().splitlines()


def _first(lines: list[str], needle: str, *, start: int = 0) -> int:
    for i in range(start, len(lines)):
        stripped = lines[i].lstrip()
        if needle in lines[i] and not stripped.startswith("#"):
            return i
    raise AssertionError(
        f"{needle!r} not found in bigboy-repin.sh after line {start + 1}"
    )


def test_old8_shape_is_checked_before_any_file_is_touched() -> None:
    lines = _lines()
    shape_at = _first(lines, "is not exactly 8 lowercase hex characters")
    backup_at = _first(lines, 'cp -n "$OV" "$OV".bak-')
    mkdir_at = _first(lines, "mkdir -p")
    assert shape_at < backup_at, (
        f"the OLD8 shape guard (line {shape_at + 1}) must run before the first file write "
        f"(cp backup, line {backup_at + 1})"
    )
    assert shape_at < mkdir_at


def _build_root(tmp_path: Path) -> Path:
    """A minimal real-shaped bigboy tree (BIGBOY_ROOT, CHAOS-7022 D2895): just enough to pass
    bigboy-repin.sh's own layout check (compose/compose.bigboy.images.yml + _records must
    exist) without needing anything the shape guard runs before ever reaches."""
    root = tmp_path / "root"
    (root / "compose").mkdir(parents=True)
    (root / "compose" / "compose.bigboy.images.yml").write_text("placeholder\n")
    (root / "_records").mkdir()
    return root


def test_old8_shape_guard_actually_rejects_a_bad_old8_before_any_write(
    tmp_path: Path,
) -> None:
    """D2922: the source-order pin above proves TEXT position, which a mutation that keeps
    the same ordering but neuters the check (e.g. drops the `exit 4`) would not catch. This
    drives the REAL bigboy-repin.sh with the exact D2801 typo shape (9 characters,
    'bd25cd0a9') and asserts the OUTCOME: nonzero exit, the D2801 error text, and -- the part
    a text pin cannot see at all -- that neither compose.bigboy.images.yml nor any
    `_records/bigboy-<N8>` dir was actually touched."""
    root = _build_root(tmp_path)
    ov_path = root / "compose" / "compose.bigboy.images.yml"
    ov_before = ov_path.read_text()
    proc = subprocess.run(
        ["bash", str(REPIN), "bd25cd0a9", "b" * 40],
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
        env={"PATH": "/usr/bin:/bin", "BIGBOY_ROOT": str(root)},
    )
    assert proc.returncode != 0, f"stdout={proc.stdout!r} stderr={proc.stderr!r}"
    assert "not exactly 8 lowercase hex characters" in proc.stdout + proc.stderr, (
        f"stdout={proc.stdout!r} stderr={proc.stderr!r}"
    )
    assert ov_path.read_text() == ov_before, (
        "compose.bigboy.images.yml must not be touched"
    )
    assert not any((root / "_records").iterdir()), (
        "no _records/bigboy-<N8> dir must be created for a rejected OLD8"
    )


def test_old8_shape_guard_rejects_nine_characters() -> None:
    """The exact D2801 shape: bd25cd0a9 (9 chars) must not match the 8-hex-char case pattern."""
    lines = _lines()
    shape_at = _first(lines, 'case "$OLD8" in')
    pattern_line = lines[shape_at + 1]
    hex_class_count = pattern_line.count("[0-9a-f]")
    assert hex_class_count == 8, (
        f"expected exactly 8 hex-character classes in the OLD8 case pattern, got "
        f"{hex_class_count}: {pattern_line!r}"
    )


def test_old8_cross_checked_against_the_actually_pinned_digest_before_repin() -> None:
    lines = _lines()
    resolved_at = _first(lines, "OLD_DIGEST_RESOLVED=")
    pinned_at = _first(lines, "OLD_DIGEST_PINNED=", start=resolved_at)
    mismatch_at = _first(
        lines,
        'if [[ "$OLD_DIGEST_RESOLVED" != "$OLD_DIGEST_PINNED" ]]',
        start=pinned_at,
    )
    dig_fn_at = _first(lines, "dig() {", start=mismatch_at)
    assert resolved_at < pinned_at < mismatch_at < dig_fn_at, (
        "OLD8 must be resolved against the registry and cross-checked against the digest "
        "compose.bigboy.images.yml currently pins BEFORE any digest resolution for the NEW "
        "build begins"
    )


def test_digest_mismatch_fails_loudly_not_a_silent_noop() -> None:
    lines = _lines()
    mismatch_at = _first(
        lines, 'if [[ "$OLD_DIGEST_RESOLVED" != "$OLD_DIGEST_PINNED" ]]'
    )
    block = "\n".join(lines[mismatch_at : mismatch_at + 5])
    assert "exit 4" in block, (
        "an OLD8/live-digest mismatch must abort (non-zero exit), never continue"
    )
    assert "does not name the ACTUALLY-running build" in block
