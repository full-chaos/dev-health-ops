"""Add work_item_attribution to the remaining_metric_runs family CHECK constraint.

Revision ID: 0126
Revises: 0125

CHAOS-3092 PR-B follow-up: internal/jobs/metrics/remaining/families.json has
listed ``work_item_attribution`` as a valid remaining-metrics family since it
shipped Go-native (0123 seeded its ``worker_job_routes`` row for the same
reason), but the Postgres ``ck_remaining_metric_run_family`` CHECK constraint
on ``remaining_metric_runs`` -- created in 0058, narrowed in 0110 -- was never
updated to include it. No migration in this history ever added it (checked:
`rg -ln ck_remaining_metric_run_family alembic/versions` returns only 0058
and 0110).

The result is a deterministic, silent failure: every StartRunTx INSERT for
family="work_item_attribution" violates this CHECK constraint
(SQLSTATE 23514), on every environment running this schema, on every fixed-
schedule tick, forever -- not a transient pool/contention issue. Reproduced
live (00:36-00:41Z boot storm) and confirmed via a rolled-back transaction
against bigboy's compose coordinator database:

    remaining metrics durable state is unavailable: insert run: ERROR: new
    row for relation "remaining_metric_runs" violates check constraint
    "ck_remaining_metric_run_family" (SQLSTATE 23514)

Prod (currently being rebuilt) and every restored dump carry this same
drift -- this migration is the fix for all of them, not just this host.

See internal/jobs/metrics/remaining/postgres.go's ErrUnavailable wrapping
(this same investigation) for why the underlying SQLSTATE was previously
invisible in the scheduler's log line.
"""

from __future__ import annotations

from alembic import op

revision: str = "0126"
down_revision: str | None = "0125"
branch_labels = None
depends_on = None

# 0110's list plus work_item_attribution -- the full, current families.json
# family set (internal/jobs/metrics/remaining/families.json). Kept as a
# module-level constant, same convention as 0110's _NARROW_FAMILY_CHECK /
# _WIDE_FAMILY_CHECK, so a drift-guard test can resolve it by name.
_FAMILY_CHECK_WITH_WORK_ITEM_ATTRIBUTION = (
    "family IN ('capacity', 'complexity', 'dora', "
    "'membership_backfill', 'recommendations', 'release_impact', "
    "'work_item_attribution')"
)

# 0110's narrowed list, restored verbatim on downgrade.
_FAMILY_CHECK_WITHOUT_WORK_ITEM_ATTRIBUTION = (
    "family IN ('capacity', 'complexity', 'dora', "
    "'membership_backfill', 'recommendations', 'release_impact')"
)


def upgrade() -> None:
    op.drop_constraint(
        "ck_remaining_metric_run_family", "remaining_metric_runs", type_="check"
    )
    op.create_check_constraint(
        "ck_remaining_metric_run_family",
        "remaining_metric_runs",
        _FAMILY_CHECK_WITH_WORK_ITEM_ATTRIBUTION,
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_remaining_metric_run_family", "remaining_metric_runs", type_="check"
    )
    op.create_check_constraint(
        "ck_remaining_metric_run_family",
        "remaining_metric_runs",
        _FAMILY_CHECK_WITHOUT_WORK_ITEM_ATTRIBUTION,
    )
