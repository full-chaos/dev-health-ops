"""0134 adds the REST live-prove receipt ledger, go_api_rest_proof_run.

Head-pin/chain check only, matching the sibling pattern
test_0133_seed_go_api_prove_service_principal_postgres.py's own
test_0133_chains_after_0132 leaves behind for the next migration author
(see that test's docstring and tests/_alembic_heads.py). The migration's
own schema -- the table shape, its CHECK constraints, and a receipt
actually written and read back through it -- is proven against a real
PostgreSQL by internal/goapiproof's TestWriteRESTThenReadBackThroughThe
MigratedSchema and internal/migrationmatrix's
TestReadRESTProofAppliesEnablementProofClause, both driving the same DDL
mirror (internal/testsupport/registryschema) this migration is mirrored
into; this file does not repeat that proof.
"""

from __future__ import annotations

import importlib
from types import ModuleType

from tests._alembic_heads import application_schema_head

_MODULE = "dev_health_ops.alembic.versions.0134_add_go_api_rest_proof_run"


def _migration() -> ModuleType:
    return importlib.import_module(_MODULE)


def test_0134_chains_after_0133() -> None:
    """0134 is no longer the application_schema head (0135 superseded it)
    -- see tests/_alembic_heads.py and
    test_0135_add_go_api_rest_proof_run_declared_defects_migration.py's
    sibling check for the current head.
    """
    migration = _migration()
    assert migration.revision != application_schema_head()
    assert migration.down_revision == "0133"
