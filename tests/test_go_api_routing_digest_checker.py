"""``ci/check_go_api_routing_digest.py`` -- red/green against real trees.

Follows the precedent of ``tests/test_endpoint_profiles_contract.py``: the
checker is a CI gate, and loading + running it from pytest is what puts it
under a REQUIRED context rather than an optional workflow step someone can
skip.

The gate has two halves, and both are proven to actually fail here. A gate
that cannot be shown to fail is not a gate:

* Move the SDL without updating the pin -> fail.
* Update the pin without recording the digest in the history table -> also
  fail. This second half is the one that matters. Without it, the obvious
  way to make a red build green is to bump the pin -- which silences the
  alarm and leaves every routing row dead, which is exactly the outcome
  the check exists to prevent.
"""

from __future__ import annotations

import importlib.util
import json
import shutil
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
CHECKER_PATH = REPO_ROOT / "ci" / "check_go_api_routing_digest.py"


def _load_checker():
    spec = importlib.util.spec_from_file_location(
        "_check_go_api_routing_digest_under_test", CHECKER_PATH
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


checker = _load_checker()


@pytest.fixture
def tree(tmp_path: Path) -> Path:
    """A minimal repo-shaped tree carrying real copies of the three files."""
    (tmp_path / "contracts" / "graphql" / "v1").mkdir(parents=True)
    (tmp_path / "docs" / "contribute" / "architecture").mkdir(parents=True)
    for relative in (
        checker.SDL_RELATIVE,
        checker.PIN_RELATIVE,
        checker.HISTORY_DOC_RELATIVE,
    ):
        shutil.copy(REPO_ROOT / relative, tmp_path / relative)
    return tmp_path


def test_real_tree_passes_the_gate(capsys: pytest.CaptureFixture[str]) -> None:
    """The checked-in tree is consistent right now."""
    assert checker.main(["--root", str(REPO_ROOT)]) == 0
    assert "OK: Go-API schema digest" in capsys.readouterr().out


def test_moving_the_sdl_without_updating_the_pin_fails(
    tree: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """One byte of SDL change is enough to kill every routing row."""
    sdl = tree / checker.SDL_RELATIVE
    sdl.write_bytes(sdl.read_bytes() + b"\n# a one-line SDL change\n")

    assert checker.main(["--root", str(tree)]) == 1
    err = capsys.readouterr().err
    assert "schema digest MOVED" in err
    # The message must say what it BREAKS and what to do, not just that two
    # strings differ -- the 2026-09-01 failure was a person not knowing this
    # table existed.
    assert "DEAD" in err
    assert "dev-hops go-api routing enable" in err


def test_bumping_the_pin_without_documenting_it_still_fails(
    tree: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """The half that stops the alarm being silenced.

    Simulates the tempting fix for the previous test: recompute the digest,
    paste it into the pin, move on. The routing rows are still dead and
    nobody has been told, so the gate must stay red.
    """
    sdl = tree / checker.SDL_RELATIVE
    sdl.write_bytes(sdl.read_bytes() + b"\n# a one-line SDL change\n")
    moved = checker.compute_schema_digest(sdl)

    pin_path = tree / checker.PIN_RELATIVE
    pin = json.loads(pin_path.read_text())
    pin["schema_digest"] = moved
    pin_path.write_text(json.dumps(pin, indent=2))

    assert checker.main(["--root", str(tree)]) == 1
    err = capsys.readouterr().err
    assert "is NOT recorded" in err
    assert moved in err
    assert "Schema-digest history" in err


def test_documenting_the_moved_digest_makes_it_pass(
    tree: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """The full, correct operator flow goes green -- so the gate is passable.

    A gate with no demonstrated green path is one people learn to bypass.
    """
    sdl = tree / checker.SDL_RELATIVE
    sdl.write_bytes(sdl.read_bytes() + b"\n# a one-line SDL change\n")
    moved = checker.compute_schema_digest(sdl)

    pin_path = tree / checker.PIN_RELATIVE
    pin = json.loads(pin_path.read_text())
    pin["schema_digest"] = moved
    pin_path.write_text(json.dumps(pin, indent=2))

    doc_path = tree / checker.HISTORY_DOC_RELATIVE
    doc_path.write_text(
        doc_path.read_text()
        + f"\n| `{moved}` | 2026-09-07 | a test commit | Added by the operator |\n"
    )

    assert checker.main(["--root", str(tree)]) == 0
    assert "OK: Go-API schema digest" in capsys.readouterr().out


def test_a_digest_recorded_only_outside_the_history_section_does_not_count(
    tree: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """The digest must appear in the HISTORY TABLE, not merely somewhere.

    Guards against the check degrading into a substring search over the
    whole document -- the recovery procedure above the table quotes digests
    as examples, and a passing grade earned by one of those would be
    meaningless.
    """
    sdl = tree / checker.SDL_RELATIVE
    sdl.write_bytes(sdl.read_bytes() + b"\n# a one-line SDL change\n")
    moved = checker.compute_schema_digest(sdl)

    pin_path = tree / checker.PIN_RELATIVE
    pin = json.loads(pin_path.read_text())
    pin["schema_digest"] = moved
    pin_path.write_text(json.dumps(pin, indent=2))

    # Mentioned BEFORE the history heading only.
    doc_path = tree / checker.HISTORY_DOC_RELATIVE
    text = doc_path.read_text()
    heading_at = text.index(checker.HISTORY_HEADING)
    doc_path.write_text(
        text[:heading_at] + f"\nAn aside mentioning {moved}.\n" + text[heading_at:]
    )

    assert checker.main(["--root", str(tree)]) == 1
    assert "is NOT recorded" in capsys.readouterr().err


def test_missing_files_fail_rather_than_pass_vacuously(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """A tree with nothing to check fails; it never reports OK.

    "A measurement that did not happen must FAIL, loudly" (root
    ``AGENTS.md``) -- a checker that quietly succeeds when its inputs are
    absent is worse than no checker, because CI then shows green.
    """
    assert checker.main(["--root", str(tmp_path)]) == 1
    assert "not found" in capsys.readouterr().err
