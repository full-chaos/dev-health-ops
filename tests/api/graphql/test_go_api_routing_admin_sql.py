"""The proof lookup's SQL, without a database.

The wrong-document regression test lives in a file gated on
``DEV_HEALTH_POSTGRES_TEST_URI`` and therefore SKIPS in CI -- so the
four-column proof key, a real correctness fix, had no CI-visible coverage.
This is the same defect class as the tests the earlier fix had to be moved
out of, and I missed it the first time while sweeping for exactly this.

The behaviour is provable without a server: compile the statement against
the PostgreSQL dialect and read what it actually constrains. That runs
everywhere, on every PR.
"""

from __future__ import annotations

import pytest
from sqlalchemy.dialects import postgresql

from dev_health_ops.api.graphql.go_api_routing_admin import (
    ENABLEMENT_TARGET_MODE_EDGE_ONLY,
    ENABLEMENT_TARGET_MODES,
    MEASUREMENT_ROUTE_EDGE,
    build_enablement_proof_select,
)


def _compiled_proof_query(
    operations: dict[str, str], target_mode: str = "canary"
) -> str:
    """Compile THE PRODUCTION statement.

    This used to rebuild the same clauses by hand and compile
    the copy, which proves only that the test agrees with itself -- a
    regression in the real function would not have failed it. It now imports
    `build_enablement_proof_select`, the seam the async function actually
    executes, so the SQL under assertion is the SQL that runs.
    """
    from dev_health_ops.api.graphql.go_api_routing_admin import (
        build_enablement_proof_select,
    )

    stmt = build_enablement_proof_select(
        schema_digest="sha256:live",
        candidate_build="build-1",
        operations=operations,
        target_mode=target_mode,
    )
    return str(
        stmt.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )


def test_the_production_query_constrains_document_digest() -> None:
    """The fourth key column must be in the SQL that actually runs.

    Asserted against the COMPILED production statement, not against the
    source text and not against a rebuilt copy: if a regression drops
    `document_digest` from the predicate, the emitted SQL loses it and this
    fails, with no database required.
    """
    sql = " ".join(_compiled_proof_query({"featureFlags": "doc-ff"}).split())
    assert "document_digest = 'doc-ff'" in sql, (
        "the proof lookup no longer constrains document_digest -- a proof "
        "recorded against a DIFFERENT registered document would authorize "
        "an enablement (plan section 8.3: a proof is evidence for exactly "
        "one 4-column tuple)"
    )
    assert "selected_operation = 'featureFlags'" in sql


def test_the_predicate_pairs_operation_with_its_own_document() -> None:
    """Tuple-OR, not a cross product.

    `selected_operation IN (...) AND document_digest IN (...)` would accept
    operation A paired with operation B's document -- exactly the mismatch
    the fix exists to reject. Compiling proves the pairing is per-operation.
    """
    sql = _compiled_proof_query({"featureFlags": "doc-ff", "reviewEdges": "doc-re"})
    normalised = " ".join(sql.split())

    assert "doc-ff" in normalised and "doc-re" in normalised
    # Each operation is bound next to ITS OWN document digest.
    assert "'featureFlags' AND" in normalised
    assert "'reviewEdges' AND" in normalised
    # And never as two independent membership tests.
    assert "IN (" not in normalised.upper().replace("DISTINCT", "")


def test_the_predicate_carries_all_four_key_columns() -> None:
    sql = " ".join(_compiled_proof_query({"featureFlags": "doc-ff"}).split())
    for column in (
        "schema_digest",
        "document_digest",
        "selected_operation",
        "candidate_build",
    ):
        assert column in sql, f"{column} missing from the proof predicate"
    assert "deployed_executed" in sql
    assert "'match'" in sql


def test_zero_operations_never_reaches_the_database() -> None:
    """An empty mapping short-circuits before any query is built."""
    import asyncio

    from dev_health_ops.api.graphql.go_api_routing_admin import (
        operations_with_enablement_proof,
    )

    class _ExplodingSession:
        async def execute(self, *_a, **_k):  # pragma: no cover - must not run
            raise AssertionError("queried the database for an empty mapping")

    assert (
        asyncio.run(
            operations_with_enablement_proof(
                _ExplodingSession(),  # type: ignore[arg-type]
                schema_digest="sha256:live",
                candidate_build="b",
                operations={},
                target_mode="canary",
            )
        )
        == frozenset()
    )


def test_one_operation_still_pairs_both_columns() -> None:
    sql = " ".join(_compiled_proof_query({"featureFlags": "doc-ff"}).split())
    assert "selected_operation = 'featureFlags'" in sql
    assert "document_digest = 'doc-ff'" in sql


def test_sql_metacharacters_in_an_operation_name_stay_data() -> None:
    """A quote in an operation name must not change the parse."""
    hostile = "feature'; DROP TABLE go_api_proof_run; --"
    sql = " ".join(_compiled_proof_query({hostile: "doc"}).split())
    assert "DROP TABLE go_api_proof_run" not in sql.replace("''", "'").split("WHERE")[0]
    # The quote is escaped by doubling, i.e. it is a literal, not syntax.
    assert "feature''; DROP TABLE" in sql


def test_an_unknown_target_mode_is_refused_rather_than_defaulted() -> None:
    """Disabling this guard once passed all six tests in this file.

    Fail-closed on an unrecognised mode is not decoration. The two known
    modes have DIFFERENT route rules -- ``primary`` demands edge evidence,
    ``canary`` accepts any recorded route -- so a mode that falls through
    to the laxer branch is a promotion to served traffic accepted on
    measurement-only evidence. That is the exact claim the mode split
    exists to stop, and it would be silent: the predicate would compile
    and the query would return rows.
    """
    for unknown in ("", "shadow", "Primary", "canary ", "unknown"):
        with pytest.raises(ValueError) as caught:
            build_enablement_proof_select(
                schema_digest="sha256:live",
                candidate_build="b",
                operations={"featureFlags": "doc-ff"},
                target_mode=unknown,
            )
        message = str(caught.value)
        assert "unknown target mode" in message, message
        # The refusal must NAME the offending value and the legal ones, or
        # an operator sees a rejection with nothing to act on.
        assert repr(unknown) in message, message
        for legal in ENABLEMENT_TARGET_MODES:
            assert legal in message, message


def test_the_two_known_modes_compile_to_different_route_rules() -> None:
    """The control: the guard above must not be refusing everything.

    And the two modes must actually DIFFER -- if both compiled to the same
    predicate the split would be decorative, and every test asserting the
    split would be asserting nothing.
    """
    compiled = {}
    for mode in ENABLEMENT_TARGET_MODES:
        statement = build_enablement_proof_select(
            schema_digest="sha256:live",
            candidate_build="b",
            operations={"featureFlags": "doc-ff"},
            target_mode=mode,
        )
        compiled[mode] = " ".join(
            str(
                statement.compile(
                    dialect=postgresql.dialect(),
                    compile_kwargs={"literal_binds": True},
                )
            ).split()
        )

    assert len(set(compiled.values())) == len(compiled), (
        "the target modes compile to identical predicates, so the mode "
        f"split authorizes nothing: {compiled}"
    )
    # primary is the STRICTER one: it must name the edge route explicitly.
    assert MEASUREMENT_ROUTE_EDGE in compiled[ENABLEMENT_TARGET_MODE_EDGE_ONLY]


@pytest.mark.parametrize("target_mode", ["canary", "primary"])
def test_the_python_predicate_binds_the_cutset_rather_than_writing_a_literal(
    target_mode: str,
) -> None:
    """The escape-literal fix, swept to the Python sibling and EXECUTED.

    The Go side's hand-typed ``E'...\\v...'`` meant the letter
    v on PostgreSQL 16. The Python predicate is only version-independent if
    it never writes the cutset into the SQL text at all -- so this compiles
    the production statement WITHOUT literal binds and requires that the SQL
    text carry no escape string and no backslash, and that the cutset reach
    the server only as a bound parameter, byte for byte.
    """
    from dev_health_ops.api.graphql.go_api_routing_admin import (
        _BLANK_CUTSET,
        build_enablement_proof_select,
    )

    compiled = build_enablement_proof_select(
        schema_digest="sha256:" + "a" * 64,
        candidate_build="b" * 40,
        operations={"featureFlags": "c" * 64},
        target_mode=target_mode,
    ).compile(dialect=postgresql.dialect())
    sql = str(compiled)
    assert "E'" not in sql and "\\" not in sql, (
        f"the Python predicate writes an escape string or a backslash into SQL:\n{sql}"
    )
    bound = [v for v in compiled.params.values() if v == _BLANK_CUTSET]
    assert len(bound) == 2, (
        f"the cutset must be bound twice (citation and build), found {len(bound)} "
        f"in params {sorted(compiled.params)}"
    )
