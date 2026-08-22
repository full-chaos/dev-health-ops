"""Guards on the compute-parity fixture provisioner (CHAOS-3092 P0).

`provision` runs `DROP DATABASE`. Both of the ways that can go wrong are
covered here: a database name that is not a name at all, and a name that is a
real database somebody wanted to keep.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[2]


def _load(name: str, relative: str) -> Any:
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


fixtures = _load("compute_parity_fixtures", "scripts/worker/compute_parity_fixtures.py")


@pytest.mark.parametrize(
    "database",
    [
        "default",
        "",
        "${CLICKHOUSE_DB:-default}",
        "scratch; DROP DATABASE devhealth",
        "scratch`; SELECT 1; --",
        "has space",
        "1leading_digit",
        "x" * 64,
    ],
)
def test_unusable_database_names_are_refused_before_any_ddl(database: str):
    dsn = f"clickhouse://ch:ch@localhost:8123/{database}"
    with pytest.raises(fixtures.FixtureError):
        fixtures.guard(dsn)


@pytest.mark.parametrize("database", ["parity_left", "ci_local_validate_ab12_pl", "_x"])
def test_plain_identifiers_are_accepted(database: str):
    dsn = f"clickhouse://ch:ch@localhost:8123/{database}"
    assert fixtures.guard(dsn) == database
    assert fixtures.quoted(database) == f"`{database}`"


def test_quoting_refuses_an_identifier_that_never_passed_the_guard():
    """The second layer holds on its own, for a future caller that skips guard."""
    with pytest.raises(fixtures.FixtureError):
        fixtures.quoted("evil`; DROP DATABASE devhealth; --")


def test_provision_requires_reset_before_it_will_drop_anything(monkeypatch):
    """A DSN typo landing on a real database must not cost that database."""
    commands: list[str] = []

    class FakeResult:
        result_rows = [[1]]

    class FakeClient:
        def query(self, *_: Any, **__: Any) -> FakeResult:
            return FakeResult()

        def command(self, statement: str) -> None:
            commands.append(statement)

        def close(self) -> None:
            return None

    monkeypatch.setattr(fixtures, "server_client", lambda _dsn: FakeClient())
    with pytest.raises(
        fixtures.FixtureError, match="database_exists_pass_reset_to_drop"
    ):
        fixtures.provision("clickhouse://ch:ch@localhost:8123/parity_left")
    assert commands == [], "no DDL may run before the caller has opted into the drop"
