"""``result_proves_execution`` vs the SQL it mirrors (CHAOS-4114).

The executed-proof verdict is defined once, in SQL
(``executedProofProvenPredicateSQL``, internal/providersync/executed_proof.go).
The Go writers and the alembic 0109 backfill evaluate that literal expression.
The Python writer cannot: it runs on SQLite in the unit suite, where ``#>>``
and ``~`` do not exist, so it has a hand-written mirror.

A mirror nobody checks is just a second definition with better manners. This
runs both against a shared corpus on a REAL PostgreSQL -- the same engine
production evaluates the SQL half on -- and fails on any disagreement. The
corpus is the union of every shape the gate's own history has cared about:
the CHAOS-4049 "succeeded and persisted nothing" counterexample, the empty
effect batch that inflates ``effects_written`` while ``records`` stays zero,
the malformed blob, the bigint overflow on both key shapes, and the 18-digit
boundary the regex bound was written for.

Skipped without ``DEV_HEALTH_POSTGRES_TEST_URI``, and a hard failure in CI:
the whole point is that the SQL half is evaluated by PostgreSQL and not by an
approximation of it.
"""

from __future__ import annotations

import json
import os
import re
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest
import sqlalchemy as sa
from sqlalchemy.engine import Engine, make_url

from dev_health_ops.sync.executed_proof_ledger import result_proves_execution

_POSTGRES_URI_ENV = "DEV_HEALTH_POSTGRES_TEST_URI"
_GO_SOURCE = (
    Path(__file__).resolve().parents[1]
    / "internal"
    / "providersync"
    / "executed_proof.go"
)

#: ``(label, status, result payload)``. The verdict is deliberately NOT
#: written down here: asserting a hand-written expectation would only pin
#: whichever of the two implementations the author happened to read. The
#: assertion is that the two AGREE, plus a separate check that the corpus is
#: not degenerate (both verdicts actually occur).
_CORPUS: tuple[tuple[str, str, object], ...] = (
    ("go route wrote rows", "success", {"go_provider_route": {"records": 3}}),
    ("go route empty window", "success", {"go_provider_route": {"records": 0}}),
    (
        "empty effect batch inflates effects_written only",
        "success",
        {"go_provider_route": {"records": 0, "effects_written": 1}},
    ),
    ("legacy python persisted nothing", "success", {"persisted": 0}),
    ("legacy python persisted rows", "success", {"persisted": 5}),
    ("records but never terminal", "failed", {"go_provider_route": {"records": 9}}),
    ("non-numeric records", "success", {"go_provider_route": {"records": "NaN"}}),
    ("numeric-as-string records", "success", {"go_provider_route": {"records": "4"}}),
    ("negative records", "success", {"go_provider_route": {"records": -3}}),
    ("fractional records", "success", {"go_provider_route": {"records": 1.5}}),
    ("boolean records", "success", {"go_provider_route": {"records": True}}),
    ("null records", "success", {"go_provider_route": {"records": None}}),
    ("records nested under a non-object", "success", {"go_provider_route": 7}),
    ("no keys at all", "success", {}),
    ("json null result", "success", None),
    ("result is not an object", "success", [1, 2, 3]),
    (
        "bigint overflow on the go key",
        "success",
        {"go_provider_route": {"records": 99999999999999999999}},
    ),
    (
        "bigint overflow on the legacy key",
        "success",
        {"persisted": 99999999999999999999},
    ),
    (
        "eighteen digits is in range and must prove",
        "success",
        {"go_provider_route": {"records": 999999999999999999}},
    ),
    ("nineteen digits overflows", "success", {"persisted": 9999999999999999999}),
    ("legacy key wins when the go key is absent", "success", {"persisted": 1}),
    (
        "either key alone is enough",
        "success",
        {"go_provider_route": {"records": 0}, "persisted": 2},
    ),
    ("whitespace-padded digits", "success", {"persisted": " 4 "}),
    ("leading zeroes", "success", {"persisted": "007"}),
)


def _proven_predicate_sql() -> str:
    source = _GO_SOURCE.read_text(encoding="utf-8")
    match = re.search(
        r"const executedProofProvenPredicateSQL = `(.*?)`", source, re.DOTALL
    )
    assert match is not None, (
        "executedProofProvenPredicateSQL is gone from executed_proof.go -- this "
        "oracle has nothing left to compare the Python mirror against"
    )
    return match.group(1)


@pytest.fixture
def oracle_engine() -> Iterator[Engine]:
    configured_uri = os.environ.get(_POSTGRES_URI_ENV)
    if configured_uri is None:
        if os.getenv("CI") or os.getenv("GITHUB_ACTIONS"):
            pytest.fail(
                f"{_POSTGRES_URI_ENV} must be configured: this oracle is worthless "
                "without a real PostgreSQL to evaluate the SQL half"
            )
        pytest.skip(f"requires {_POSTGRES_URI_ENV}")
    configured_url = make_url(configured_uri)
    if configured_url.get_backend_name() != "postgresql":
        pytest.fail(f"{_POSTGRES_URI_ENV} must use PostgreSQL")
    database_name = f"test_chaos_4114_oracle_{uuid.uuid4().hex}"
    admin_engine = sa.create_engine(
        configured_url.set(drivername="postgresql+psycopg2", database="postgres"),
        isolation_level="AUTOCOMMIT",
    )
    engine: Engine | None = None
    created = False
    try:
        with admin_engine.connect() as connection:
            connection.exec_driver_sql(f'CREATE DATABASE "{database_name}"')
            created = True
        engine = sa.create_engine(
            configured_url.set(drivername="postgresql+psycopg2", database=database_name)
        )
        yield engine
    finally:
        if engine is not None:
            engine.dispose()
        if created:
            with admin_engine.connect() as connection:
                connection.exec_driver_sql(f'DROP DATABASE "{database_name}"')
        admin_engine.dispose()


def test_the_python_mirror_agrees_with_postgresql_on_every_known_shape(
    oracle_engine: Engine,
) -> None:
    predicate = _proven_predicate_sql()
    disagreements: list[str] = []
    truths: set[bool] = set()
    with oracle_engine.connect() as connection:
        connection.exec_driver_sql(
            "CREATE TEMP TABLE unit (label text, status text, result jsonb)"
        )
        for label, status, payload in _CORPUS:
            connection.execute(
                sa.text(
                    "INSERT INTO unit (label, status, result) "
                    "VALUES (:label, :status, CAST(:result AS jsonb))"
                ),
                {"label": label, "status": status, "result": json.dumps(payload)},
            )
        rows = connection.execute(
            sa.text(f"SELECT unit.label, COALESCE({predicate}, false) FROM unit")
        ).all()

    sql_verdicts = {str(row[0]): bool(row[1]) for row in rows}
    assert len(sql_verdicts) == len(_CORPUS), "corpus labels must be unique"
    for label, status, payload in _CORPUS:
        mirrored = status == "success" and result_proves_execution(payload)
        truths.add(sql_verdicts[label])
        if mirrored != sql_verdicts[label]:
            disagreements.append(
                f"{label}: postgresql={sql_verdicts[label]} python={mirrored} "
                f"(status={status} result={payload!r})"
            )
    assert not disagreements, (
        "result_proves_execution disagrees with the SQL predicate it mirrors:\n  "
        + "\n  ".join(disagreements)
    )
    # Non-vacuity: a corpus that only ever produced one verdict would let a
    # mirror that always answers that verdict pass.
    assert truths == {True, False}, (
        "the corpus no longer exercises both verdicts, so agreement proves nothing"
    )
