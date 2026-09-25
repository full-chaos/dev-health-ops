"""Add the single-plane write-proof receipt kind to ``go_api_proof_run``.

Revision ID: 0143
Revises: 0142

CHAOS-6810. A GraphQL MUTATION cannot be proven by running it on both planes
(it would write twice), so its receipt is a stage of its own, ``write_executed``:
one execution of the deployed build inside the fixture org, the digest of the
rows and River outbox payloads it persisted (``side_effect_digest``), compared
with the digest the CI oracle committed for the same case. Enablement admits a
mutation only on such a receipt and a query only on a ``deployed_executed`` one.

This widens ``ck_go_api_proof_run_stage`` with ``write_executed`` and adds
``ck_go_api_proof_run_write_executed_shape``: a write proof carries its digest
and records a route (``edge``, or ``proof`` = a direct POST to query-api's /query:
the /query/proof route refuses a mutation and an operation not yet routed to Go
cannot reach the Go build through the edge, so an edge-only rule would make the
first enablement impossible; the admission predicate still requires ``edge`` for
primary). No data
changes; no existing row can violate the new check (no row has that stage).

``go_api_rest_proof_run`` (0134) keeps its four stages: a REST route has no write
proof form. Downgrade restores the four-stage check and drops the shape check;
it fails while ``write_executed`` rows exist.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0143"
down_revision: str | None = "0142"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = "go_api_proof_run"
_STAGE_CHECK = "ck_go_api_proof_run_stage"
_SHAPE_CHECK = "ck_go_api_proof_run_write_executed_shape"

_PREVIOUS_STAGES = ("dual_run", "deployed_executed", "shadow", "canary")
_STAGES = (*_PREVIOUS_STAGES, "write_executed")


def _replace_stage_check(stages: tuple[str, ...]) -> None:
    with op.batch_alter_table(_TABLE) as batch_op:
        batch_op.drop_constraint(_STAGE_CHECK, type_="check")
        batch_op.create_check_constraint(_STAGE_CHECK, f"stage IN {stages!r}")


def upgrade() -> None:
    _replace_stage_check(_STAGES)
    with op.batch_alter_table(_TABLE) as batch_op:
        batch_op.create_check_constraint(
            _SHAPE_CHECK,
            "stage <> 'write_executed' OR "
            "(side_effect_digest IS NOT NULL AND measurement_route IS NOT NULL)",
        )


def downgrade() -> None:
    with op.batch_alter_table(_TABLE) as batch_op:
        batch_op.drop_constraint(_SHAPE_CHECK, type_="check")
    _replace_stage_check(_PREVIOUS_STAGES)
