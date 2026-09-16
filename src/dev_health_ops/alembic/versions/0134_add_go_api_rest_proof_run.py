"""Add the REST live-prove receipt ledger, ``go_api_rest_proof_run``.

Revision ID: 0134
Revises: 0133

``go-api-rest-prove`` proves a ported ``/api/v1/*`` REST route by calling
the Python api service and query-api directly, in cluster, and recording
an immutable receipt -- the REST sibling of the ``prove`` verb's
``go_api_proof_run`` rows (0114/0127/0128/0129).

A REST route carries no GraphQL schema digest, document digest or
selected-operation name to key a receipt by, and it is not routed through
``go_api_routing_state``: a mounted REST path is either compiled into the
binary or it is not, there is no live rollout row to read a candidate
build from. So this table's identity is what the ported route actually
is: ``(method, path, candidate_build)``, not the four-column composite key
``go_api_proof_run`` uses. There is deliberately no companion
``go_api_rest_candidate_build`` registry table and no foreign key to one:
that second table exists on the GraphQL side because
``go_api_routing_state`` needs one immutable thing to reference a
candidate build by; nothing on the REST side plays that role, so a second
table here would carry the GraphQL shape without the GraphQL reason for
it.

Every other column -- ``stage``, ``terminal_state``, ``measurement_route``,
``baseline_defect``, ``differences_outside_baseline_defect``,
``build_binding`` -- is named and constrained IDENTICALLY to
``go_api_proof_run``'s own columns of the same name, on purpose: this is
what lets ``internal/goapiproof.EnablementProofClause`` -- one predicate,
parameterised only by a SQL alias -- judge a row from EITHER table without
a second copy of the admission rule. Duplicating the vocabulary CHECKs
with different names would have made that reuse impossible for no
benefit.

No watermark column: ``go_api_proof_run.data_watermark`` backs the
``shadow``-stage requirement (a shadow GraphQL operation is compared
against a same-watermark baseline), and no REST route in this system is
ever proven in shadow -- ``go-api-rest-prove`` calls both planes directly,
never through a routing mode. Omitted rather than carried as a column
nothing will ever populate.

Deliberately no data migration: this is new infrastructure: no REST proof
receipt existed anywhere before ``go-api-rest-prove``.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

from dev_health_ops.models.go_api_registry import STAGES, TERMINAL_STATES

revision: str = "0134"
down_revision: str | None = "0133"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_TABLE = "go_api_rest_proof_run"


def upgrade() -> None:
    op.create_table(
        _TABLE,
        sa.Column("id", UUID(as_uuid=True), nullable=False),
        sa.Column("method", sa.Text(), nullable=False),
        sa.Column("path", sa.Text(), nullable=False),
        sa.Column("candidate_build", sa.Text(), nullable=False),
        sa.Column("request_identity", sa.Text(), nullable=False),
        sa.Column("stage", sa.Text(), nullable=False),
        sa.Column("terminal_state", sa.Text(), nullable=False),
        sa.Column("baseline_response_ref", sa.Text(), nullable=True),
        sa.Column("candidate_response_ref", sa.Text(), nullable=True),
        sa.Column("org_id", sa.Text(), nullable=True),
        sa.Column("review_evidence", sa.Text(), nullable=True),
        sa.Column("recorded_by", sa.Text(), nullable=True),
        sa.Column("measurement_route", sa.Text(), nullable=True),
        sa.Column("baseline_defect", sa.ARRAY(sa.Text()), nullable=True),
        sa.Column(
            "differences_outside_baseline_defect",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
        sa.Column("build_binding", sa.Text(), nullable=True),
        sa.Column(
            "observed_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("id", name="pk_go_api_rest_proof_run"),
        sa.CheckConstraint(
            f"stage IN {STAGES!r}",
            name="ck_go_api_rest_proof_run_stage",
        ),
        sa.CheckConstraint(
            f"terminal_state IN {TERMINAL_STATES!r}",
            name="ck_go_api_rest_proof_run_terminal_state",
        ),
        sa.CheckConstraint(
            "measurement_route IS NULL OR measurement_route IN ('edge', 'proof')",
            name="ck_go_api_rest_proof_run_measurement_route",
        ),
        sa.CheckConstraint(
            "build_binding IS NULL OR build_binding IN ('per_request', 'absent')",
            name="ck_go_api_rest_proof_run_build_binding",
        ),
    )
    op.create_index(
        "ix_go_api_rest_proof_run_route",
        _TABLE,
        ["method", "path", "observed_at"],
    )
    op.create_index(
        "ix_go_api_rest_proof_run_candidate_build",
        _TABLE,
        ["method", "path", "candidate_build"],
    )


def downgrade() -> None:
    op.drop_index("ix_go_api_rest_proof_run_candidate_build", table_name=_TABLE)
    op.drop_index("ix_go_api_rest_proof_run_route", table_name=_TABLE)
    op.drop_table(_TABLE)
