"""bigboy-repin.sh: OLD8 is checked BEFORE any file is touched, both for shape and against the
ACTUALLY-running build (CHAOS-7014).

D2801: a mistyped OLD8 (9 chars, "bd25cd0a9" instead of "bd25cd0a") made the repin's own `cp` of
_records/bigboy-$OLD8/*.sh fail under `set -euo pipefail` BEFORE the sed rewrite of
compose.bigboy.images.yml ever ran. compose.bigboy.images.yml was backed up but never actually
repointed, and every downstream STEP in bigboy-cut.sh (migrate/up/up-workers/rest/...) still
reported rc=0 against the stale, pre-cut images -- a void proof run that looked green.
"""

from __future__ import annotations

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
