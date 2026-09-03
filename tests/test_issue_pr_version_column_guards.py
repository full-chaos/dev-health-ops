"""The catalogue guards in migration 084 must fail closed, not guess.

Migration 084 rebuilds `work_graph_issue_pr` so that provenance outranks
recency. Before it can plan a copy it has to answer two questions about the live
catalogue: does the table exist, and what shape is it. Both answers come from a
client, and a client that answers something other than the question asked is the
case these tests pin.

The history is why they are worth having. An earlier revision treated an
unreadable answer as "absent" or "nothing to migrate" and SKIPPED. A skip is not
a no-op here: `upgrade()` returns normally, the runner records 084 as APPLIED,
and the schema silently diverges from a ledger that says the change landed --
which lane-ci-flakes traced and which is harder to diagnose than the data-loss
case the guard was written for. Failing closed is the whole point, so the
failure paths get their own tests rather than being covered incidentally.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any

import pytest

_MIGRATION = (
    Path(__file__).parents[1]
    / "src/dev_health_ops/migrations/clickhouse"
    / "084_issue_pr_provenance_version_precedence.py"
)


def _load_migration() -> Any:
    """Load 084 as a module.

    importlib, not runpy.run_path: run_path returns a COPY of the namespace, so
    anything rebound on the result is invisible to the module's own functions.
    That cost a review round elsewhere in this file's history.
    """
    spec = importlib.util.spec_from_file_location("migration_084", _MIGRATION)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _Answering:
    """A client returning one canned `result_rows` for any query."""

    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self._rows = rows

    def query(self, *_args: object, **_kwargs: object) -> Any:
        rows = self._rows

        class _Result:
            result_rows = rows

        return _Result()


@pytest.mark.parametrize(
    ("rows", "expected"),
    [
        ([(1,)], True),
        ([(0,)], False),
    ],
)
def test_table_exists_accepts_only_the_two_real_answers(
    rows: list[tuple[object, ...]], expected: bool
) -> None:
    """A real server answers EXISTS TABLE with exactly 0 or 1."""
    module = _load_migration()
    assert module._table_exists(_Answering(rows), "work_graph_issue_pr") is expected


@pytest.mark.parametrize(
    "rows",
    [
        pytest.param([], id="no_rows"),
        pytest.param([()], id="empty_row"),
        pytest.param([("067_operational_ordering_contract.py",)], id="a_filename"),
        pytest.param([(None,)], id="null"),
        pytest.param([(7,)], id="out_of_range_integer"),
        pytest.param([(-1,)], id="negative_integer"),
    ],
)
def test_table_exists_refuses_to_guess_from_any_other_answer(
    rows: list[tuple[object, ...]],
) -> None:
    """Anything that is not 0 or 1 raises rather than being coerced.

    `out_of_range_integer` is the case this test was added for. An earlier
    version parsed the cell and returned `int(cell) > 0`, so 7 came back as True
    without raising -- while the error text already said "which is not 0 or 1".
    The message promised a stricter check than the code performed, which is
    worse than either being wrong alone: a reader debugging an odd client would
    have trusted it and ruled out the one thing still possible.

    A real server cannot return 7, so nothing was broken in production. The test
    exists because the DOCUMENTED contract and the enforced one had drifted
    apart, and the next reader has only the documented one.
    """
    module = _load_migration()
    with pytest.raises(RuntimeError) as caught:
        module._table_exists(_Answering(rows), "work_graph_issue_pr")
    assert "work_graph_issue_pr" in str(caught.value)


def test_the_refusal_message_names_the_value_it_refused() -> None:
    """A refusal has to say what it saw, or it cannot be diagnosed.

    The failing client is by definition one nobody expected, so the offending
    value is the only lead its operator will have.
    """
    module = _load_migration()
    with pytest.raises(RuntimeError) as caught:
        module._table_exists(_Answering([(7,)]), "work_graph_issue_pr")
    message = str(caught.value)
    assert "7" in message
    assert "not 0 or 1" in message
