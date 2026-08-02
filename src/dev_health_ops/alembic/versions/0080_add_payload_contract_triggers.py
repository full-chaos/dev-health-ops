"""DB-enforced payload contract + row-binding triggers (CHAOS-3297 Codex review round 9).

Revision ID: 0080
Revises: 0079
Create Date: 2026-08-02 00:00:00

Three rounds of session-level guards (an AST scanner over
``DevPersistenceService``'s own source, then SQLAlchemy
``before_insert``/``before_update`` mapper events, then a
``do_orm_execute`` session-level hook covering every Core DML shape
SQLAlchemy 2.x has) were each defeated by a write shape the previous one
could not see -- most recently a Core-table ``UPDATE`` issued through the
``Session`` (whose ``bind_mapper`` is ``None``, since the statement
targets a bare ``Table``, not a mapped class the listener can look up)
and an ``INSERT ... ON CONFLICT DO UPDATE`` whose conflict ``SET`` clause
is never inspected by anything that validates the INSERT's own values.

The invariant moves to the one boundary every write path -- ORM, Core,
executemany, upsert, even a raw connection -- must cross to become a row:
the database itself. A ``BEFORE INSERT OR UPDATE`` trigger on each
payload-bearing table (``dev_answer_frames``, ``dev_run_narratives``)
validates the FINAL row about to be written, whatever produced it:

* ``payload`` is not null
* ``payload->>'schema_version'`` equals the table's one contract version
* the payload's own identity fields equal this row's own columns --
  ``frame_id``/``run_id``/``public_outcome`` for ``dev_answer_frames``,
  ``narrative_id``/``run_id``/``frame_id``/``mode`` for
  ``dev_run_narratives`` -- mirroring ``record_frame``'s/
  ``record_narrative``'s own cross-checks.

Scope note: ``provider_fingerprint`` is NOT cross-checked here.
``record_narrative``'s own cross-check compares a SHA-256 digest of
``payload.provider_metadata.model_fingerprint`` against the column;
computing that digest inside a trigger needs ``pgcrypto``, a new
production extension dependency this revision does not introduce. The
session listener (``persistence/service.py``) still performs that one
check on every session-mediated write -- every write this application's
own code issues.

This does not replace full contract validation (every field
``DevAnswerFrameContract``/``DevNarrativeContract`` enforce beyond
identity) -- that remains the session listener's job, correctly
described now as fast-fail UX, not the load-bearing guard.

The SQLite arm of this same trigger pair is installed automatically by
``models/dev_persistence.py``'s ``event.listen(<table>, "after_create",
...)`` registrations, which ``Base.metadata.create_all`` (every unit
test's in-memory fixture) already calls. Only the PostgreSQL arm needs an
explicit migration, since production databases are never built via
``create_all``.

Additive; the downgrade drops both triggers and functions, leaving the
tables exactly as 0079 left them.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op
from sqlalchemy import text

revision: str = "0080"
down_revision: str | None = "0079"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

__all__ = ["revision", "down_revision", "branch_labels", "depends_on"]

_DEV_ANSWER_FRAME_SCHEMA_VERSION = "dev_answer_frame.v1"
_DEV_NARRATIVE_SCHEMA_VERSION = "dev_narrative.v1"


def upgrade() -> None:
    # DDL identifiers/bodies are module-level literals; DDL cannot take bound parameters.
    op.execute(
        text(  # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
            f"""
        CREATE OR REPLACE FUNCTION dev_answer_frames_validate_payload() RETURNS trigger AS $$
        BEGIN
            IF NEW.payload IS NULL THEN
                RAISE EXCEPTION 'dev_answer_frames.payload must not be null' USING ERRCODE = '23514';
            END IF;
            IF NEW.payload->>'schema_version' IS DISTINCT FROM '{_DEV_ANSWER_FRAME_SCHEMA_VERSION}' THEN
                RAISE EXCEPTION 'dev_answer_frames.payload.schema_version does not equal {_DEV_ANSWER_FRAME_SCHEMA_VERSION}' USING ERRCODE = '23514';
            END IF;
            IF (NEW.payload->>'frame_id')::uuid IS DISTINCT FROM NEW.frame_id THEN
                RAISE EXCEPTION 'dev_answer_frames.payload.frame_id does not match the row''s frame_id' USING ERRCODE = '23514';
            END IF;
            IF (NEW.payload->>'run_id')::uuid IS DISTINCT FROM NEW.run_id THEN
                RAISE EXCEPTION 'dev_answer_frames.payload.run_id does not match the row''s run_id' USING ERRCODE = '23514';
            END IF;
            IF NEW.payload->>'public_outcome' IS DISTINCT FROM NEW.public_outcome THEN
                RAISE EXCEPTION 'dev_answer_frames.payload.public_outcome does not match the row''s public_outcome' USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
        )
    )
    # Two separate op.execute() calls, not one multi-statement string:
    # asyncpg (this project's async Postgres driver) rejects multiple SQL
    # commands in a single prepared statement -- confirmed empirically.
    op.execute(
        text(
            "DROP TRIGGER IF EXISTS dev_answer_frames_validate_payload_trigger "
            "ON dev_answer_frames;"
        )
    )
    op.execute(
        text("""
        CREATE TRIGGER dev_answer_frames_validate_payload_trigger
            BEFORE INSERT OR UPDATE ON dev_answer_frames
            FOR EACH ROW EXECUTE FUNCTION dev_answer_frames_validate_payload();
        """)
    )
    # DDL identifiers/bodies are module-level literals; DDL cannot take bound parameters.
    op.execute(
        text(  # nosemgrep: python.sqlalchemy.security.audit.avoid-sqlalchemy-text.avoid-sqlalchemy-text
            f"""
        CREATE OR REPLACE FUNCTION dev_run_narratives_validate_payload() RETURNS trigger AS $$
        BEGIN
            IF NEW.payload IS NULL THEN
                RAISE EXCEPTION 'dev_run_narratives.payload must not be null' USING ERRCODE = '23514';
            END IF;
            IF NEW.payload->>'schema_version' IS DISTINCT FROM '{_DEV_NARRATIVE_SCHEMA_VERSION}' THEN
                RAISE EXCEPTION 'dev_run_narratives.payload.schema_version does not equal {_DEV_NARRATIVE_SCHEMA_VERSION}' USING ERRCODE = '23514';
            END IF;
            IF (NEW.payload->>'narrative_id')::uuid IS DISTINCT FROM NEW.narrative_id THEN
                RAISE EXCEPTION 'dev_run_narratives.payload.narrative_id does not match the row''s narrative_id' USING ERRCODE = '23514';
            END IF;
            IF (NEW.payload->>'run_id')::uuid IS DISTINCT FROM NEW.run_id THEN
                RAISE EXCEPTION 'dev_run_narratives.payload.run_id does not match the row''s run_id' USING ERRCODE = '23514';
            END IF;
            IF (NEW.payload->>'frame_id')::uuid IS DISTINCT FROM NEW.frame_id THEN
                RAISE EXCEPTION 'dev_run_narratives.payload.frame_id does not match the row''s frame_id' USING ERRCODE = '23514';
            END IF;
            IF NEW.payload->>'mode' IS DISTINCT FROM NEW.mode THEN
                RAISE EXCEPTION 'dev_run_narratives.payload.mode does not match the row''s mode' USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
        )
    )
    op.execute(
        text(
            "DROP TRIGGER IF EXISTS dev_run_narratives_validate_payload_trigger "
            "ON dev_run_narratives;"
        )
    )
    op.execute(
        text("""
        CREATE TRIGGER dev_run_narratives_validate_payload_trigger
            BEFORE INSERT OR UPDATE ON dev_run_narratives
            FOR EACH ROW EXECUTE FUNCTION dev_run_narratives_validate_payload();
        """)
    )


def downgrade() -> None:
    op.execute(
        text(
            "DROP TRIGGER IF EXISTS dev_answer_frames_validate_payload_trigger "
            "ON dev_answer_frames;"
        )
    )
    op.execute(text("DROP FUNCTION IF EXISTS dev_answer_frames_validate_payload();"))
    op.execute(
        text(
            "DROP TRIGGER IF EXISTS dev_run_narratives_validate_payload_trigger "
            "ON dev_run_narratives;"
        )
    )
    op.execute(text("DROP FUNCTION IF EXISTS dev_run_narratives_validate_payload();"))
