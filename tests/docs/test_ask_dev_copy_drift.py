"""Ask Dev published-copy drift guard.

``docs/use/ai-workflows/ask-dev-answers.md`` prints server-owned user-facing
strings -- outcome display labels, the canonical no-answer sentence and
remediation, and the withheld-content sentence -- as the product's own words.
``scripts/check_ask_dev_copy_drift.py`` fails when the page and the runtime
constants disagree in either direction.

These tests exist because a guard nobody has watched fail is not evidence that
it can fail. ``test_drifted_fixture_is_reported`` runs the guard against a
committed page that is deliberately wrong in exactly two places, so the
failure path is re-proven on every CI run rather than proven once by hand.
"""

from __future__ import annotations

import importlib.util
import subprocess
import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DRIFT_SCRIPT = ROOT / "scripts" / "check_ask_dev_copy_drift.py"
ANSWERS_DOC = ROOT / "docs" / "use" / "ai-workflows" / "ask-dev-answers.md"
DRIFTED_FIXTURE = (
    ROOT / "tests" / "docs" / "fixtures" / "ask_dev_copy" / ("drifted-answers.md")
)

REGIONS = (
    "ASK-DEV OUTCOME LABELS",
    "ASK-DEV REFUSAL COPY",
    "ASK-DEV NO-ANSWER COPY",
    "ASK-DEV WITHHELD COPY",
)


def _load_drift_module() -> types.ModuleType:
    spec = importlib.util.spec_from_file_location(
        "check_ask_dev_copy_drift", DRIFT_SCRIPT
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_ask_dev_copy_drift_check_exits_clean() -> None:
    """The published page must agree with the runtime constants."""
    assert DRIFT_SCRIPT.is_file(), f"missing drift script: {DRIFT_SCRIPT}"
    result = subprocess.run(
        [sys.executable, str(DRIFT_SCRIPT)],
        check=False,
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"Ask Dev copy drift check failed:\n{result.stdout}\n{result.stderr}"
    )
    assert "ERROR:" not in result.stdout, f"drift check reported:\n{result.stdout}"


def test_published_page_carries_every_checked_region() -> None:
    """Deleting a marker pair must not silently shrink what is checked.

    The script already errors on a missing pair, but that only fires if the
    script is run. This asserts the regions directly, so a page edit that drops
    one is caught even by a reader diffing the tests.
    """
    assert ANSWERS_DOC.is_file(), f"missing published page: {ANSWERS_DOC}"
    document = ANSWERS_DOC.read_text(encoding="utf-8")
    for region in REGIONS:
        assert f"<!-- BEGIN {region} -->" in document, f"missing BEGIN {region}"
        assert f"<!-- END {region} -->" in document, f"missing END {region}"


def test_drifted_fixture_is_reported() -> None:
    """A page drifted in exactly two places yields exactly those two errors.

    The count matters as much as the content: it proves the guard fires on the
    two wrong strings *and* stays silent on the surrounding correct ones, which
    a test asserting only "some error was raised" would not.
    """
    drift = _load_drift_module()
    document = DRIFTED_FIXTURE.read_text(encoding="utf-8")

    errors: list[str] = []
    drift.check_outcome_labels(document, errors)
    drift.check_no_answer_copy(document, errors)
    drift.check_withheld_copy(document, errors)

    joined = "\n".join(errors)
    assert len(errors) == 2, f"expected exactly 2 drift errors, got:\n{joined}"
    assert any(
        "display label for 'refused' drifted" in error
        and "Not something Ask Dev cannot do" in error
        and "Not something Ask Dev can do" in error
        for error in errors
    ), f"refused label drift not reported with both strings:\n{joined}"
    assert any(
        "canonical copy for 'denied' drifted" in error
        and "You do not have permission to ask about this." in error
        and "You do not have access to ask about this." in error
        for error in errors
    ), f"denied copy drift not reported with both strings:\n{joined}"


def test_missing_marker_is_an_error_not_a_pass() -> None:
    """A region the guard cannot find must fail, never quietly measure nothing."""
    drift = _load_drift_module()
    document = DRIFTED_FIXTURE.read_text(encoding="utf-8").replace(
        "<!-- END ASK-DEV NO-ANSWER COPY -->", ""
    )
    errors: list[str] = []
    try:
        drift.check_no_answer_copy(document, errors)
    except drift.SourceLookupError as exc:
        assert "ASK-DEV NO-ANSWER COPY" in str(exc)
    else:  # pragma: no cover - the guard would be vacuous
        raise AssertionError(
            "a missing marker pair must raise, not return a passing result"
        )


def test_unitalicised_cell_is_rejected_as_a_silent_paraphrase() -> None:
    """A checked column must quote, not paraphrase.

    Italics are how the page marks "these are the product's words". A cell that
    drops them is a paraphrase sitting in a table that claims to quote, so the
    guard refuses to compare it rather than passing it through.
    """
    drift = _load_drift_module()
    document = DRIFTED_FIXTURE.read_text(encoding="utf-8").replace(
        "*This question is not supported yet.*",
        "roughly, that it is not supported yet",
    )
    errors: list[str] = []
    try:
        drift.check_no_answer_copy(document, errors)
    except drift.SourceLookupError as exc:
        assert "italicised product string" in str(exc)
    else:  # pragma: no cover - the guard would admit paraphrase
        raise AssertionError("an unitalicised checked cell must raise")
