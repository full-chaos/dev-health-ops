"""CHAOS-3337 codex full-branch review (2026-08-03), finding 4 (pre-authorized
by team-lead regardless of verdict): three SEPARATE, hand-maintained
definitions of the same closed ``source_class`` vocabulary can drift from
each other independently of ``contracts_v2.base.SourceClass`` itself:

1. ``persistence.service._SOURCE_CLASSES`` -- the Python-level allowlist
   ``append_source_observation`` checks.
2. ``models.dev_persistence.DevRunSourceObservation``'s own
   ``ck_dev_run_source_observations_source_class`` CHECK constraint string
   (``__table_args__``) -- what SQLite (every unit test's ``create_all``)
   and a freshly-provisioned Postgres both enforce at the DB layer.
3. Migration 0081's ``ADD CONSTRAINT ... CHECK (...) NOT VALID`` DDL text
   -- what an EXISTING Postgres database's constraint actually becomes
   after upgrading (0082 only validates 0081's constraint; it never
   redefines the value list).

CHAOS-3337 itself was exactly definitions 1 and 2 drifting from
``contracts_v2.base.SourceClass`` independently, in two different ways, at
the same time. This suite locks all three of the above together with each
other (not merely against the enum, which ``test_chaos_3337_source_class_
persistence_allowlist.py``'s import-time check already covers for the
Python-importable side) -- so the next SourceClass this repo widens the
allowlist for, but only in one or two of these three places, fails a test
instead of live.

Parses the model's and the migration's raw SQL/DDL text (never
hand-transcribes the expected value list a second time) so this test
cannot itself become a fourth, independently-driftable copy.
"""

from __future__ import annotations

import importlib
import re
from typing import cast

from sqlalchemy import CheckConstraint, Table

from dev_health_ops.api.dev.persistence.service import (
    _SOURCE_CLASSES as _PERSISTENCE_SOURCE_CLASSES,
)
from dev_health_ops.models.dev_persistence import DevRunSourceObservation

_CONSTRAINT_NAME = "ck_dev_run_source_observations_source_class"

#: Matches every single-quoted SQL string literal in a
#: ``source_class IN ('a', 'b', ...)`` CHECK constraint fragment. Deliberately
#: general (any quoted literal, not anchored to "source_class IN" specifically)
#: so it works unchanged against both the model's plain CHECK body and a
#: migration's ``ALTER TABLE ... ADD CONSTRAINT ... CHECK (...) NOT VALID``
#: DDL text, which wraps the same fragment in more SQL around it.
_QUOTED_LITERAL_RE = re.compile(r"'([^']*)'")


def _parse_quoted_values(sql: str) -> frozenset[str]:
    """Every single-quoted string literal in ``sql`` -- the closed value
    set a ``... IN (...)`` CHECK constraint fragment enumerates."""

    return frozenset(_QUOTED_LITERAL_RE.findall(sql))


def _model_check_constraint_sql() -> str:
    table = cast(Table, DevRunSourceObservation.__table__)
    for constraint in table.constraints:
        if (
            isinstance(constraint, CheckConstraint)
            and constraint.name == _CONSTRAINT_NAME
        ):
            return str(constraint.sqltext)
    raise AssertionError(
        f"{_CONSTRAINT_NAME!r} not found on DevRunSourceObservation.__table_args__ "
        "-- has it been renamed?"
    )


def _migration_0081_check_constraint_sql() -> str:
    """0081's widened CHECK body, built the exact same way the migration's
    own ``upgrade()`` builds it for the ``NOT VALID`` DDL -- never a second,
    hand-copied literal list."""

    migration = importlib.import_module(
        "dev_health_ops.alembic.versions.0081_widen_source_observation_source_class"
    )
    return migration._source_class_check(
        (*migration._PRIOR_SOURCE_CLASSES, *migration._NEW_SOURCE_CLASSES)
    )


def test_model_check_constraint_matches_persistence_allowlist() -> None:
    model_values = _parse_quoted_values(_model_check_constraint_sql())
    assert model_values == _PERSISTENCE_SOURCE_CLASSES


def test_migration_0081_constraint_matches_persistence_allowlist() -> None:
    migration_values = _parse_quoted_values(_migration_0081_check_constraint_sql())
    assert migration_values == _PERSISTENCE_SOURCE_CLASSES


def test_model_and_migration_constraints_agree_with_each_other() -> None:
    """Direct pairwise check (not merely implied by both matching
    ``_SOURCE_CLASSES`` above): if a future change edits the model's CHECK
    string and the persistence frozenset together but forgets the
    migration DDL (or vice versa), THIS is the assertion that catches it,
    independent of whether ``_SOURCE_CLASSES`` itself was touched.
    """

    model_values = _parse_quoted_values(_model_check_constraint_sql())
    migration_values = _parse_quoted_values(_migration_0081_check_constraint_sql())
    assert model_values == migration_values


def test_parser_detects_a_planted_gap_on_the_model_side() -> None:
    """Plant defect: a model CHECK constraint string missing one value the
    persistence allowlist and the migration both still carry. Proves the
    comparison is sensitive on the MODEL side specifically, not merely
    reporting a passing/trivial result regardless of content.
    """

    shrunk_model_sql = _model_check_constraint_sql().replace("'health_profile', ", "")
    assert "health_profile" not in _parse_quoted_values(shrunk_model_sql)
    assert _parse_quoted_values(shrunk_model_sql) != _PERSISTENCE_SOURCE_CLASSES
    assert _parse_quoted_values(shrunk_model_sql) != _parse_quoted_values(
        _migration_0081_check_constraint_sql()
    )


def test_parser_detects_a_planted_gap_on_the_migration_side() -> None:
    """Plant defect: a migration CHECK constraint string missing one value
    the persistence allowlist and the model both still carry. Proves the
    comparison is sensitive on the MIGRATION side specifically.
    """

    shrunk_migration_sql = _migration_0081_check_constraint_sql().replace(
        "'deficiency_inventory'", ""
    )
    assert "deficiency_inventory" not in _parse_quoted_values(shrunk_migration_sql)
    assert _parse_quoted_values(shrunk_migration_sql) != _PERSISTENCE_SOURCE_CLASSES
    assert _parse_quoted_values(shrunk_migration_sql) != _parse_quoted_values(
        _model_check_constraint_sql()
    )


def test_parser_detects_a_planted_gap_on_the_persistence_side() -> None:
    """Plant defect: a persistence allowlist missing one value the model
    and migration both still carry -- proves the comparison is sensitive
    on the THIRD side too, not only the two SQL-text sides.
    """

    shrunk_allowlist = _PERSISTENCE_SOURCE_CLASSES - {"health_profile"}
    assert _parse_quoted_values(_model_check_constraint_sql()) != shrunk_allowlist
    assert (
        _parse_quoted_values(_migration_0081_check_constraint_sql()) != shrunk_allowlist
    )
