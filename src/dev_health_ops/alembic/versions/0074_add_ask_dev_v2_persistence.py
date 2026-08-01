"""Add Ask Dev Wave 3.1 v2 persistence (intent, resolution, plans, frames, replay).

Revision ID: 0074
Revises: 0073
Create Date: 2026-07-31 00:00:00

Additive only. Adds seven nullable/defaulted columns to the existing,
actively written ``dev_runs`` table plus seven new tables FK'd to it. The
two new ``dev_runs`` CHECK constraints are installed ``NOT VALID`` on
PostgreSQL (a metadata-only operation that does not scan the table) and
validated in 0075, so this migration never holds an ``ACCESS EXCLUSIVE``
lock for the duration of a full-table scan on a hot table. The new tables
are created empty, so their own CHECK constraints need no such split.

``plan_step_partition``/``relationship_closure_verified`` fold what an
earlier revision of this branch modeled as a dedicated
``dev_run_investigation_results`` table: observations are already
persisted 1:N in ``dev_run_source_observations`` and ``dev_answer_frames``
is the replay source of truth, so the only post-terminal
``dev_investigation_result.v1`` facts nothing else reconstructs -- which
plan steps ran, and the closure bit -- are persisted directly on
``dev_runs`` instead of a ninth table.

Downgrade exists for isolated pre-release rehearsal only, matching 0068's
posture: it refuses (raises) if any v2 data is present, rather than silently
dropping it.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0074"
down_revision: str | None = "0073"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_UUID = postgresql.UUID(as_uuid=True)
_JSON = sa.JSON().with_variant(postgresql.JSONB(), "postgresql")

_CONTRACT_GENERATION_CK = "ck_dev_runs_contract_generation"
_PUBLIC_OUTCOME_CK = "ck_dev_runs_public_outcome"

_PUBLIC_OUTCOME_VALUES = (
    "'answered', 'answered_with_gaps', 'needs_clarification', 'not_found', "
    "'temporarily_unavailable', 'unsupported', 'denied', 'failed'"
)


def _owner_fk(table: str) -> sa.ForeignKeyConstraint:
    return sa.ForeignKeyConstraint(
        ["run_id", "org_id", "user_id"],
        ["dev_runs.id", "dev_runs.org_id", "dev_runs.user_id"],
        name=f"fk_{table}_run_owner",
        ondelete="CASCADE",
    )


def _owner_columns() -> list[sa.Column]:
    return [
        sa.Column("id", _UUID, nullable=False),
        sa.Column("run_id", _UUID, nullable=False),
        sa.Column("org_id", _UUID, nullable=False),
        sa.Column("user_id", _UUID, nullable=False),
    ]


def _created_at_column() -> sa.Column:
    return sa.Column(
        "created_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.text("CURRENT_TIMESTAMP"),
    )


def upgrade() -> None:
    bind = op.get_bind()
    is_postgres = bind.dialect.name == "postgresql"

    # -- dev_runs additive columns -------------------------------------
    op.add_column(
        "dev_runs",
        sa.Column(
            "contract_generation",
            sa.String(length=4),
            nullable=False,
            server_default="v1",
        ),
    )
    op.add_column(
        "dev_runs", sa.Column("public_outcome", sa.String(length=32), nullable=True)
    )
    op.add_column(
        "dev_runs",
        sa.Column(
            "compatibility_projection_version", sa.String(length=128), nullable=True
        ),
    )
    op.add_column(
        "dev_runs", sa.Column("plan_id", sa.String(length=128), nullable=True)
    )
    op.add_column(
        "dev_runs", sa.Column("plan_version", sa.String(length=128), nullable=True)
    )
    op.add_column("dev_runs", sa.Column("plan_step_partition", _JSON, nullable=True))
    op.add_column(
        "dev_runs",
        sa.Column("relationship_closure_verified", sa.Boolean(), nullable=True),
    )

    if is_postgres:
        op.execute(
            f"ALTER TABLE dev_runs ADD CONSTRAINT {_CONTRACT_GENERATION_CK} "
            "CHECK (contract_generation IN ('v1', 'v2')) NOT VALID"
        )
        op.execute(
            f"ALTER TABLE dev_runs ADD CONSTRAINT {_PUBLIC_OUTCOME_CK} "
            f"CHECK (public_outcome IS NULL OR public_outcome IN "
            f"({_PUBLIC_OUTCOME_VALUES})) NOT VALID"
        )
    else:
        with op.batch_alter_table("dev_runs") as batch:
            batch.create_check_constraint(
                _CONTRACT_GENERATION_CK, "contract_generation IN ('v1', 'v2')"
            )
            batch.create_check_constraint(
                _PUBLIC_OUTCOME_CK,
                f"public_outcome IS NULL OR public_outcome IN "
                f"({_PUBLIC_OUTCOME_VALUES})",
            )

    # -- dev_run_intents -------------------------------------------------
    op.create_table(
        "dev_run_intents",
        *_owner_columns(),
        sa.Column("intent_id", sa.String(length=48), nullable=False),
        sa.Column("cardinality", sa.String(length=24), nullable=False),
        sa.Column("requires_clarification", sa.Boolean(), nullable=False),
        sa.Column("interpreter_version", sa.String(length=128), nullable=False),
        sa.Column("payload", _JSON, nullable=False),
        _created_at_column(),
        sa.CheckConstraint(
            "intent_id IN ('entity_status', 'portfolio_status', 'remaining_work', "
            "'observed_change', 'registered_statistics', 'metric_comparison', "
            "'data_trust', 'project_health', 'team_health', "
            "'team_workload_balance', 'operational_deficiency_inventory', "
            "'bounded_investigation')",
            name="ck_dev_run_intents_intent_id",
        ),
        sa.CheckConstraint(
            "cardinality IN ('singular', 'plural_cohort', 'organization_wide')",
            name="ck_dev_run_intents_cardinality",
        ),
        _owner_fk("dev_run_intents"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("run_id", name="uq_dev_run_intents_run"),
    )
    op.create_index(
        "ix_dev_run_intents_owner_run",
        "dev_run_intents",
        ["org_id", "user_id", "run_id"],
    )

    # -- dev_run_resolutions (append-only ledger entries) -----------------
    op.create_table(
        "dev_run_resolutions",
        *_owner_columns(),
        sa.Column("entry_ordinal", sa.SmallInteger(), nullable=False),
        sa.Column("mention_id", _UUID, nullable=False),
        sa.Column("outcome", sa.String(length=32), nullable=False),
        sa.Column("payload", _JSON, nullable=False),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=False),
        _created_at_column(),
        sa.CheckConstraint(
            "entry_ordinal >= 0 AND entry_ordinal <= 99",
            name="ck_dev_run_resolutions_entry_ordinal",
        ),
        sa.CheckConstraint(
            "outcome IN ('exact_match', 'ambiguous_candidates', "
            "'no_authorized_match', 'catalog_unavailable', 'unsupported_kind')",
            name="ck_dev_run_resolutions_outcome",
        ),
        _owner_fk("dev_run_resolutions"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "run_id", "entry_ordinal", name="uq_dev_run_resolutions_run_ordinal"
        ),
    )
    op.create_index(
        "ix_dev_run_resolutions_owner_run",
        "dev_run_resolutions",
        ["org_id", "user_id", "run_id"],
    )

    # -- dev_run_subject_sets ---------------------------------------------
    op.create_table(
        "dev_run_subject_sets",
        *_owner_columns(),
        sa.Column("set_id", _UUID, nullable=False),
        sa.Column("entity_kind", sa.String(length=32), nullable=False),
        sa.Column("cohort_complete", sa.Boolean(), nullable=False),
        sa.Column("fingerprint", sa.String(length=128), nullable=False),
        sa.Column("payload", _JSON, nullable=False),
        _created_at_column(),
        sa.CheckConstraint(
            "entity_kind IN ('repository', 'project', 'work_unit', 'issue', "
            "'pull_request', 'team')",
            name="ck_dev_run_subject_sets_entity_kind",
        ),
        _owner_fk("dev_run_subject_sets"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("run_id", name="uq_dev_run_subject_sets_run"),
    )
    op.create_index(
        "ix_dev_run_subject_sets_owner_run",
        "dev_run_subject_sets",
        ["org_id", "user_id", "run_id"],
    )

    # -- dev_run_source_observations ---------------------------------------
    op.create_table(
        "dev_run_source_observations",
        *_owner_columns(),
        sa.Column("ordinal", sa.SmallInteger(), nullable=False),
        sa.Column("observation_id", _UUID, nullable=False),
        sa.Column("source_class", sa.String(length=32), nullable=False),
        sa.Column("requirement_level", sa.String(length=16), nullable=False),
        sa.Column("observed_state", sa.String(length=32), nullable=False),
        sa.Column("data_semantics", sa.String(length=16), nullable=False),
        sa.Column("usable_fact_count", sa.Integer(), nullable=False),
        sa.Column("sample_count", sa.Integer(), nullable=True),
        sa.Column("subject_coverage", sa.Float(), nullable=False),
        sa.Column("payload", _JSON, nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        _created_at_column(),
        sa.CheckConstraint(
            "ordinal >= 0 AND ordinal <= 24",
            name="ck_dev_run_source_observations_ordinal",
        ),
        sa.CheckConstraint(
            "source_class IN ('status_change', 'work_item', 'work_graph', "
            "'pull_request', 'code_change', 'review', 'ci_run', 'test_report', "
            "'deployment', 'incident', 'operational_control', 'source_health')",
            name="ck_dev_run_source_observations_source_class",
        ),
        sa.CheckConstraint(
            "requirement_level IN ('mandatory', 'conditional', 'optional', "
            "'not_applicable')",
            name="ck_dev_run_source_observations_requirement_level",
        ),
        sa.CheckConstraint(
            "observed_state IN ('available_current', 'available_stale', "
            "'available_unknown', 'unconfigured', 'unavailable', "
            "'unauthorized_or_not_visible', 'not_applicable', 'truncated')",
            name="ck_dev_run_source_observations_observed_state",
        ),
        sa.CheckConstraint(
            "data_semantics IN ('measured_zero', 'no_data', 'not_measured')",
            name="ck_dev_run_source_observations_data_semantics",
        ),
        sa.CheckConstraint(
            "usable_fact_count >= 0 AND (sample_count IS NULL OR sample_count >= 0)",
            name="ck_dev_run_source_observations_counts_nonnegative",
        ),
        sa.CheckConstraint(
            "subject_coverage >= 0 AND subject_coverage <= 1",
            name="ck_dev_run_source_observations_coverage_range",
        ),
        _owner_fk("dev_run_source_observations"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "run_id", "ordinal", name="uq_dev_run_source_observations_run_ordinal"
        ),
    )
    op.create_index(
        "ix_dev_run_source_observations_owner_run",
        "dev_run_source_observations",
        ["org_id", "user_id", "run_id"],
    )

    # -- dev_answer_frames --------------------------------------------------
    op.create_table(
        "dev_answer_frames",
        *_owner_columns(),
        sa.Column("frame_id", _UUID, nullable=False),
        sa.Column("public_outcome", sa.String(length=32), nullable=False),
        sa.Column("payload", _JSON, nullable=False),
        _created_at_column(),
        sa.CheckConstraint(
            f"public_outcome IN ({_PUBLIC_OUTCOME_VALUES})",
            name="ck_dev_answer_frames_public_outcome",
        ),
        _owner_fk("dev_answer_frames"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("run_id", name="uq_dev_answer_frames_run"),
    )
    op.create_index(
        "ix_dev_answer_frames_owner_run",
        "dev_answer_frames",
        ["org_id", "user_id", "run_id"],
    )

    # -- dev_run_narratives ---------------------------------------------
    op.create_table(
        "dev_run_narratives",
        *_owner_columns(),
        sa.Column("narrative_id", _UUID, nullable=False),
        sa.Column("frame_id", _UUID, nullable=False),
        sa.Column("mode", sa.String(length=24), nullable=False),
        sa.Column("provider_fingerprint", sa.String(length=71), nullable=True),
        sa.Column("narrative_text", sa.Text(), nullable=False),
        sa.Column("payload", _JSON, nullable=False),
        _created_at_column(),
        sa.CheckConstraint(
            "mode IN ('provider', 'deterministic_fallback')",
            name="ck_dev_run_narratives_mode",
        ),
        sa.CheckConstraint(
            "provider_fingerprint IS NULL OR "
            "(length(provider_fingerprint) = 71 "
            "AND provider_fingerprint LIKE 'sha256:%')",
            name="ck_dev_run_narratives_provider_fingerprint",
        ),
        _owner_fk("dev_run_narratives"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("run_id", name="uq_dev_run_narratives_run"),
    )
    op.create_index(
        "ix_dev_run_narratives_owner_run",
        "dev_run_narratives",
        ["org_id", "user_id", "run_id"],
    )

    # -- dev_run_stage_diagnostics -----------------------------------------
    op.create_table(
        "dev_run_stage_diagnostics",
        *_owner_columns(),
        sa.Column("ordinal", sa.SmallInteger(), nullable=False),
        sa.Column("stage_id", sa.String(length=32), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("latency_ms", sa.Integer(), nullable=True),
        sa.Column("counts", _JSON, nullable=False),
        _created_at_column(),
        sa.CheckConstraint(
            "ordinal >= 0 AND ordinal <= 9",
            name="ck_dev_run_stage_diagnostics_ordinal",
        ),
        sa.CheckConstraint(
            "stage_id IN ('interpreting', 'resolving_subjects', 'planning', "
            "'collecting', 'synthesizing_frame', 'narrating_optional', "
            "'projecting_answer')",
            name="ck_dev_run_stage_diagnostics_stage_id",
        ),
        sa.CheckConstraint(
            "status IN ('started', 'completed', 'failed', 'skipped')",
            name="ck_dev_run_stage_diagnostics_status",
        ),
        sa.CheckConstraint(
            "latency_ms IS NULL OR latency_ms >= 0",
            name="ck_dev_run_stage_diagnostics_latency_nonnegative",
        ),
        _owner_fk("dev_run_stage_diagnostics"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "run_id", "ordinal", name="uq_dev_run_stage_diagnostics_run_ordinal"
        ),
    )
    op.create_index(
        "ix_dev_run_stage_diagnostics_owner_run",
        "dev_run_stage_diagnostics",
        ["org_id", "user_id", "run_id"],
    )


def downgrade() -> None:
    bind = op.get_bind()
    # Pre-release rehearsal only (matching 0068's posture): refuse rather
    # than silently discard v2 data. Checked before dropping anything.
    if bind.dialect.has_table(bind, "dev_runs"):
        v2_rows = bind.execute(
            sa.text("SELECT count(*) FROM dev_runs WHERE contract_generation = 'v2'")
        ).scalar()
        if v2_rows:
            raise RuntimeError(
                "refusing to downgrade 0074: dev_runs has "
                f"{v2_rows} row(s) with contract_generation = 'v2'; this "
                "downgrade is for pre-release rehearsal only"
            )
        # Independent sweep, not just belt-and-suspenders with the tagging
        # check above: DevPersistenceService.record_investigation_result
        # can populate these two folded columns (Codex-review CHAOS-3299
        # fold) before a run ever reaches record_frame -- the call that
        # tags contract_generation = 'v2' -- e.g. a failure between the
        # investigation and frame-synthesis stages. A run in that state is
        # still tagged 'v1' and would silently pass the check above while
        # carrying real v2 data.
        folded_column_rows = bind.execute(
            sa.text(
                "SELECT count(*) FROM dev_runs WHERE "
                "plan_step_partition IS NOT NULL "
                "OR relationship_closure_verified IS NOT NULL"
            )
        ).scalar()
        if folded_column_rows:
            raise RuntimeError(
                "refusing to downgrade 0074: dev_runs has "
                f"{folded_column_rows} row(s) with plan_step_partition or "
                "relationship_closure_verified populated (independent of "
                "contract_generation tagging); this downgrade is for "
                "pre-release rehearsal only"
            )
        for table in (
            "dev_run_intents",
            "dev_run_resolutions",
            "dev_run_subject_sets",
            "dev_run_source_observations",
            "dev_answer_frames",
            "dev_run_narratives",
            "dev_run_stage_diagnostics",
        ):
            if bind.dialect.has_table(bind, table):
                for_v2_rows = bind.execute(
                    sa.select(sa.func.count()).select_from(sa.table(table))
                ).scalar()
                if for_v2_rows:
                    raise RuntimeError(
                        f"refusing to downgrade 0074: {table} has "
                        f"{for_v2_rows} row(s); this downgrade is for "
                        "pre-release rehearsal only"
                    )

    op.drop_table("dev_run_stage_diagnostics")
    op.drop_table("dev_run_narratives")
    op.drop_table("dev_answer_frames")
    op.drop_table("dev_run_source_observations")
    op.drop_table("dev_run_subject_sets")
    op.drop_table("dev_run_resolutions")
    op.drop_table("dev_run_intents")

    if bind.dialect.name == "postgresql":
        # Constraint name is a module-level literal; DDL identifiers cannot be bound parameters.
        op.execute(  # nosemgrep: python.lang.security.audit.formatted-sql-query.formatted-sql-query, python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query
            f"ALTER TABLE dev_runs DROP CONSTRAINT {_PUBLIC_OUTCOME_CK}"
        )
        op.execute(  # nosemgrep: python.lang.security.audit.formatted-sql-query.formatted-sql-query, python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query
            f"ALTER TABLE dev_runs DROP CONSTRAINT {_CONTRACT_GENERATION_CK}"
        )
        op.drop_column("dev_runs", "relationship_closure_verified")
        op.drop_column("dev_runs", "plan_step_partition")
        op.drop_column("dev_runs", "plan_version")
        op.drop_column("dev_runs", "plan_id")
        op.drop_column("dev_runs", "compatibility_projection_version")
        op.drop_column("dev_runs", "public_outcome")
        op.drop_column("dev_runs", "contract_generation")
    else:
        with op.batch_alter_table("dev_runs") as batch:
            batch.drop_constraint(_PUBLIC_OUTCOME_CK, type_="check")
            batch.drop_constraint(_CONTRACT_GENERATION_CK, type_="check")
            batch.drop_column("relationship_closure_verified")
            batch.drop_column("plan_step_partition")
            batch.drop_column("plan_version")
            batch.drop_column("plan_id")
            batch.drop_column("compatibility_projection_version")
            batch.drop_column("public_outcome")
            batch.drop_column("contract_generation")
