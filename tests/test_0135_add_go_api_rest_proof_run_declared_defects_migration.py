"""0135 adds go_api_rest_proof_run.baseline_defect_declared.

Head-pin/chain check only, following the same most-recent-supersedes-
the-previous convention 0134's own migration test established when it
superseded 0133's head-pin (see tests/_alembic_heads.py's docstring and
test_0134_add_go_api_rest_proof_run_migration.py's own
test_0134_chains_after_0133).

The column's own shape and round trip -- a nil DeclaredDefects writes
SQL NULL, a non-nil (even empty) slice writes a real array, and both
read back exactly -- is proven against a real, migrated PostgreSQL by
internal/goapiproof's TestWriteREST_DeclaredDefectsRoundTripsThroughThe
MigratedSchema and the ReadRESTFiringHistory integration tests in
restfiring_integration_test.go, both driving the same DDL mirror
(internal/testsupport/registryschema) this migration is mirrored into;
this file does not repeat that proof. The idempotency this migration's
own upgrade()/downgrade() claim (re-running either against a database
that already reflects the target state is a no-op) is exercised live
through the `dev-hops migrate postgres upgrade` hook, not re-asserted
here as a second, narrower unit test of the same introspection helper
0010's own precedent already established the shape of.
"""

from __future__ import annotations

import importlib
from types import ModuleType

_MODULE = (
    "dev_health_ops.alembic.versions.0135_add_go_api_rest_proof_run_declared_defects"
)


def _migration() -> ModuleType:
    return importlib.import_module(_MODULE)


def test_0135_chains_after_0134() -> None:
    """0136 superseded the head check (see tests/_alembic_heads.py); this
    migration keeps only its place in the chain."""
    migration = _migration()
    assert migration.revision == "0135"
    assert migration.down_revision == "0134"
