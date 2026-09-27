"""bigboy-cut.sh: the operator image is exported before any STEP reads the compose chain.

compose.bigboy.workers.yml requires BIGBOY_OPERATOR_IMAGE. The SSO group cut ran the
secret-refs STEP before the export, so compose config failed on the missing variable and five
set secrets were reported BLANK (a false DRIFT). A config read that fails must also be named
as a config failure, never read as a blank-secret verdict.
"""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CUT = ROOT / "ci" / "bigboy" / "bigboy-cut.sh"


def _lines() -> list[str]:
    return CUT.read_text().splitlines()


def _first(lines: list[str], needle: str, *, start: int = 0) -> int:
    for i in range(start, len(lines)):
        stripped = lines[i].lstrip()
        if needle in lines[i] and not stripped.startswith("#"):
            return i
    raise AssertionError(
        f"{needle!r} not found in bigboy-cut.sh after line {start + 1}"
    )


def test_operator_image_exported_before_first_compose_config_read() -> None:
    lines = _lines()
    export_at = _first(lines, "export BIGBOY_OPERATOR_IMAGE=")
    read_at = _first(lines, "compose-config-redacted.sh")
    assert export_at < read_at, (
        f"BIGBOY_OPERATOR_IMAGE is exported at line {export_at + 1}, after the first compose "
        f"config read at line {read_at + 1}"
    )


def test_operator_image_exported_once() -> None:
    exports = [
        line
        for line in _lines()
        if line.lstrip().startswith("export BIGBOY_OPERATOR_IMAGE=")
    ]
    assert len(exports) == 1, exports


def test_empty_operator_digest_fails_loud() -> None:
    lines = _lines()
    export_at = _first(lines, "export BIGBOY_OPERATOR_IMAGE=")
    guard = "\n".join(lines[max(0, export_at - 3) : export_at])
    assert "sha256:*" in guard and "exit 1" in guard, guard


def test_config_read_failure_is_not_a_blank_verdict() -> None:
    lines = _lines()
    read_at = _first(lines, "compose-config-redacted.sh")
    assert "RED_RC=$?" in lines[read_at], lines[read_at]
    rc_branch = _first(lines, '[ "$RED_RC" -ne 0 ]', start=read_at)
    blank_branch = _first(lines, '[ -n "$BLANK" ]', start=read_at)
    assert rc_branch < blank_branch, (
        "the config-failure branch must be decided before BLANK"
    )
