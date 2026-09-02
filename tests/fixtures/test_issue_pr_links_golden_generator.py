"""CHAOS-4803: the golden generator's recording sink must SNAPSHOT what it captures.

`generate_issue_pr_links_python_golden.py` freezes both halves of a parity
contract: the rows the deployed producer read, and the rows it wrote. The Go
port is then proven by replaying those frozen inputs.

That only holds if the captured "input" is what the producer was HANDED. A sink
that records the row objects by reference shares them with the producer, so an
in-place mutation corrupts the captured input and the derived output
*consistently* -- and a golden regenerated afterwards encodes the mutated state
as if it had been the input all along. The oracle then accepts the very
regression it exists to catch, and nothing about it looks wrong.

The tests below plant that mutation rather than asserting a copy exists.
Asserting the copy is a check that cannot fail in the interesting direction: it
passes for a shallow copy that still shares the row dicts, which is the same
hole one level down.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

import pytest

GENERATOR_PATH = Path(__file__).with_name("generate_issue_pr_links_python_golden.py")


def _load_generator() -> Any:
    """Import the generator by path.

    It is a script under tests/fixtures, not an installed module, and it imports
    `dev_health_ops.work_graph.builder` at module scope. If that import is
    unavailable the whole module is skipped rather than failed -- an absent
    Python environment is not this test's subject.
    """
    spec = importlib.util.spec_from_file_location(
        "issue_pr_links_golden_generator", GENERATOR_PATH
    )
    if spec is None or spec.loader is None:  # pragma: no cover - defensive
        pytest.skip(f"cannot load {GENERATOR_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    try:
        spec.loader.exec_module(module)
    except ImportError as exc:  # pragma: no cover - environment-dependent
        pytest.skip(f"generator dependencies unavailable: {exc}")
    return module


generator = _load_generator()


class _MutatingInnerSink:
    """A sink whose caller mutates the rows it returns, in place."""

    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def query_dicts(self, query: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        return self._rows


def test_recording_sink_snapshots_rows_against_in_place_mutation() -> None:
    """A producer that mutates a returned row must not alter the capture."""
    original = {"org_id": "org-1", "source_work_item_id": "ghpr:owner/repo#12"}
    inner = _MutatingInnerSink([original])
    sink = generator.RecordingSink(inner)

    rows = sink.query_dicts("SELECT 1", {})

    # The producer mutates what it was handed. builder.py does not do this
    # today, which is exactly why the instrument's weakness is invisible: the
    # capture is faithful by luck, not by construction.
    rows[0]["source_work_item_id"] = "ghpr:owner/repo#999"
    rows.append({"org_id": "org-1", "source_work_item_id": "ghpr:owner/repo#13"})

    captured = sink.reads[0]
    assert len(captured) == 1, (
        "the recorded read grew when the producer appended to the list it was "
        "handed: the capture is sharing the producer's list object"
    )
    assert captured[0]["source_work_item_id"] == "ghpr:owner/repo#12", (
        "the recorded read changed when the producer mutated a row in place: "
        "the capture is sharing the producer's row dicts, so a regenerated "
        "golden would freeze the mutated value as if it were the input"
    )


def test_recording_sink_snapshot_is_deep() -> None:
    """A shallow copy leaves nested values shared; the snapshot must be deep.

    Rows are `dict[str, Any]` and their values can themselves be mutable, so
    `list(rows)` or `dict(row)` would satisfy the test above while still
    sharing everything one level down -- the same defect, harder to see.
    """
    original = {"org_id": "org-1", "labels": ["a"], "nested": {"k": "v"}}
    sink = generator.RecordingSink(_MutatingInnerSink([original]))

    rows = sink.query_dicts("SELECT 1", {})
    rows[0]["labels"].append("b")
    rows[0]["nested"]["k"] = "mutated"

    captured = sink.reads[0][0]
    assert captured["labels"] == ["a"], "nested list is shared with the producer"
    assert captured["nested"] == {"k": "v"}, "nested dict is shared with the producer"


def test_recording_sink_still_returns_the_live_rows_to_the_producer() -> None:
    """Snapshotting must not change what the producer sees.

    The capture is a side effect; handing the producer a copy instead of the
    real rows would make the generator exercise a different object graph than
    production does, which is a different kind of infidelity.
    """
    original = {"org_id": "org-1"}
    sink = generator.RecordingSink(_MutatingInnerSink([original]))

    rows = sink.query_dicts("SELECT 1", {})

    assert rows[0] is original, "the producer must receive the inner sink's own rows"


def test_recording_sink_refuses_unknown_attributes_with_attributeerror() -> None:
    """Guards the CodeQL fix that landed with the producer (PR #2086).

    `getattr(obj, name, default)` falls back to its default only on
    AttributeError, and the deployed builder probes the sink that way at
    builder.py:805 and :933.
    """
    sink = generator.RecordingSink(_MutatingInnerSink([]))

    with pytest.raises(AttributeError):
        _ = sink.write_work_graph_edges

    assert getattr(sink, "client", None) is None, (
        "a probe written to tolerate a missing attribute must get its default"
    )
