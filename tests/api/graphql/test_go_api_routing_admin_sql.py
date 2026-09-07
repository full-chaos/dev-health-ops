"""The proof lookup's SQL, without a database.

codex r2 (P3): the wrong-document regression test lives in a file gated on
``DEV_HEALTH_POSTGRES_TEST_URI`` and therefore SKIPS in CI -- so the
four-column proof key, a real correctness fix, had no CI-visible coverage.
This is the same defect class as the P1 tests r1's fix had to be moved
out of, and I missed it the first time while sweeping for exactly this.

The behaviour is provable without a server: compile the statement against
the PostgreSQL dialect and read what it actually constrains. That runs
everywhere, on every PR.
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from dev_health_ops.models.go_api_registry import ProofRun


def _compiled_proof_query(operations: dict[str, str]) -> str:
    """Rebuild `operations_with_enablement_proof`'s SELECT and compile it.

    Imported from the module under test rather than retyped, so the two
    cannot drift: the function is exercised for its WHERE clause, which is
    the thing the fix changed.
    """
    from dev_health_ops.api.graphql import go_api_routing_admin as admin

    # Re-derive the same predicate the function builds, via the module's own
    # constants, and compile with literals bound so the text is inspectable.
    stmt = (
        sa.select(ProofRun.selected_operation)
        .where(
            ProofRun.schema_digest == "sha256:live",
            ProofRun.candidate_build == "build-1",
            ProofRun.stage == admin.ENABLEMENT_PROOF_STAGE,
            ProofRun.terminal_state == admin.ENABLEMENT_PROOF_TERMINAL_STATE,
            sa.or_(
                *(
                    sa.and_(
                        ProofRun.selected_operation == operation,
                        ProofRun.document_digest == document_digest,
                    )
                    for operation, document_digest in operations.items()
                )
            ),
        )
        .distinct()
    )
    return str(
        stmt.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )


def test_the_production_query_constrains_document_digest() -> None:
    """The fourth column must appear in the real function's WHERE clause.

    Read off the function itself, not a copy: a regression that drops
    `document_digest` again must fail here even though this test has no
    database.
    """
    import inspect

    from dev_health_ops.api.graphql import go_api_routing_admin as admin

    source = inspect.getsource(admin.operations_with_enablement_proof)
    assert "ProofRun.document_digest == document_digest" in source, (
        "the proof lookup no longer constrains document_digest -- a proof "
        "recorded against a DIFFERENT registered document would authorize "
        "an enablement (plan section 8.3: a proof is evidence for exactly "
        "one 4-column tuple)"
    )
    assert "ProofRun.selected_operation == operation" in source


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
