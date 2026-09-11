"""PostgreSQL proof for alembic 0130 -- ``go_api_routing_audits``.

Three verbs (``enable``, ``disable``, ``repoint``) change which plane
serves production traffic, and none of them left a durable record: the
only trace was ``recorded_by`` + ``review_evidence`` on the MUTABLE
routing row, which the next write overwrites.

The first draft of this migration widened ``worker_operator_audits``.
chris ruled against it (2026-09-09): "this column is definitely for syncs
to pass to workers". So this is a dedicated table, and the properties that
make it worth having are asserted here against a real Postgres rather than
read off the migration source:

1. The **action** and **credential_class** vocabularies are closed. A verb
   or a credential class nobody authorized is refused by the database, not
   merely by the writer. ``status`` is absent on purpose -- it is a
   read-only diagnostic, and an audit row for a read would make "this
   operation was touched on the 9th" untrue.
2. The **pairing** CHECK is an equivalence, so "a verified credential
   named this subject" and "no credential was presented" cannot be
   recorded as each other. That distinction is the entire reason both
   classes exist.
3. ``mode_before`` is NULLABLE and ``mode_after`` is not: an operation
   that had no row and one whose row said ``python`` are different facts,
   and a NOT NULL default would render them alike.
4. The mode vocabulary MATCHES ``ck_go_api_routing_state_mode`` exactly. A
   mode this table can record but that table cannot hold would let an
   audit row describe a state the system cannot be in.
5. There is **no foreign key** to ``go_api_routing_state``: the table is
   append-only and must outlive the row it describes.

And ``downgrade`` must actually remove it -- a migration whose downgrade is
untested is one you cannot roll back.
"""

from __future__ import annotations

import importlib
import os
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from sqlalchemy.engine import Engine, make_url

from tests._alembic_heads import application_schema_head

_POSTGRES_URI_ENV = "DEV_HEALTH_POSTGRES_TEST_URI"
_ALEMBIC_DIR = Path(__file__).parents[1] / "src" / "dev_health_ops" / "alembic"
_TABLE = "go_api_routing_audits"
_GO_AUDIT_TEST = Path(__file__).parents[1] / (
    "internal/testsupport/routingauditschema/schema.go"
)

_MIGRATION = importlib.import_module(
    "dev_health_ops.alembic.versions.0130_add_go_api_routing_audits"
)

_ROUTING_STATE_MODES = ("python", "shadow", "canary", "primary", "disabled")


def _config() -> Config:
    config = Config()
    config.set_main_option("script_location", str(_ALEMBIC_DIR))
    return config


def _columns(engine: Engine, table: str) -> dict[str, bool]:
    with engine.connect() as c:
        rows = c.execute(
            sa.text(
                "SELECT column_name, is_nullable FROM information_schema.columns "
                "WHERE table_name = :t"
            ),
            {"t": table},
        ).all()
    return {name: nullable == "YES" for name, nullable in rows}


def _constraints(engine: Engine, table: str) -> dict[str, str]:
    with engine.connect() as c:
        rows = c.execute(
            sa.text(
                "SELECT conname, contype FROM pg_constraint "
                "WHERE conrelid = to_regclass(:t)"
            ),
            {"t": table},
        ).all()
    return {name: contype for name, contype in rows}


def _insert(engine: Engine, **overrides: object) -> None:
    values: dict[str, object] = {
        "action": "enable",
        "credential_class": "effective_principal_envelope",
        "principal_id": "b0a1c2d3-0000-4000-8000-000000000001",
        "recorded_by": "lane-routing-verbs",
        "review_evidence": "CHAOS-5505",
        "schema_digest": "sha256:29d509cd",
        "document_digest": "06ca28a0",
        "selected_operation": "featureFlags",
        "candidate_build_before": None,
        "candidate_build_after": "ffd9e5d5",
        "mode_before": None,
        "mode_after": "canary",
    }
    values.update(overrides)
    with engine.connect() as c:
        c.execute(
            sa.text(
                "INSERT INTO go_api_routing_audits (correlation_id, action,"
                " credential_class, principal_id, recorded_by, review_evidence,"
                " schema_digest, document_digest, selected_operation,"
                " candidate_build_before, candidate_build_after, mode_before,"
                " mode_after, recorded_at) VALUES (gen_random_uuid(), :action,"
                " :credential_class, :principal_id, :recorded_by, :review_evidence,"
                " :schema_digest, :document_digest, :selected_operation,"
                " :candidate_build_before, :candidate_build_after, :mode_before,"
                " :mode_after, now())"
            ),
            values,
        )
        c.commit()


@pytest.fixture
def migrated(monkeypatch: pytest.MonkeyPatch) -> Iterator[Engine]:
    uri = os.environ.get(_POSTGRES_URI_ENV)
    if uri is None:
        if os.getenv("CI") or os.getenv("GITHUB_ACTIONS"):
            pytest.fail(f"{_POSTGRES_URI_ENV} must be configured for migration tests")
        pytest.skip(f"requires {_POSTGRES_URI_ENV}")
    url = make_url(uri)
    if url.get_backend_name() != "postgresql":
        pytest.fail(f"{_POSTGRES_URI_ENV} must use PostgreSQL")

    name = f"test_chaos_5505_{uuid.uuid4().hex}"
    admin = sa.create_engine(
        url.set(drivername="postgresql+psycopg2", database="postgres"),
        isolation_level="AUTOCOMMIT",
    )
    engine: Engine | None = None
    try:
        with admin.connect() as c:
            c.exec_driver_sql(f'CREATE DATABASE "{name}"')
        monkeypatch.setenv(
            "POSTGRES_URI",
            url.set(drivername="postgresql+asyncpg", database=name).render_as_string(
                hide_password=False
            ),
        )
        monkeypatch.delenv("MIGRATION_DATABASE_URI", raising=False)
        monkeypatch.delenv("MIGRATION_DATABASE_URI_FILE", raising=False)
        # The revision BELOW this one, DERIVED rather than typed: a literal
        # here is the class of pin CHAOS-5425's 0128 broke twelve times over
        # (tests/_alembic_heads.py's docstring).
        command.upgrade(_config(), _MIGRATION.down_revision)
        engine = sa.create_engine(
            url.set(drivername="postgresql+psycopg2", database=name)
        )
        yield engine
    finally:
        if engine is not None:
            engine.dispose()
        with admin.connect() as c:
            c.exec_driver_sql(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        admin.dispose()


def test_0130_is_the_application_schema_head_and_chains_after_0129() -> None:
    """Derived, not typed. A renumber moves this in one place."""
    assert _MIGRATION.revision == application_schema_head(), (
        "0130 must be the application_schema head; if another migration landed "
        "first, renumber this one and re-run"
    )
    assert _MIGRATION.down_revision == "0129"


def test_0130_creates_the_table_with_the_right_nullability(
    migrated: Engine,
) -> None:
    assert not _columns(migrated, _TABLE), f"{_TABLE} exists before 0130"

    command.upgrade(_config(), _MIGRATION.revision)

    columns = _columns(migrated, _TABLE)
    assert set(columns) == {
        "id",
        "correlation_id",
        "action",
        "credential_class",
        "principal_id",
        "recorded_by",
        "review_evidence",
        "schema_digest",
        "document_digest",
        "selected_operation",
        "candidate_build_before",
        "candidate_build_after",
        "mode_before",
        "mode_after",
        "recorded_at",
    }
    # Nullable exactly where a value may genuinely not exist.
    assert columns["principal_id"] is True, (
        "operator_direct rows have no subject to name"
    )
    assert columns["mode_before"] is True, (
        "an operation with NO prior row must record NULL, not an invented default -- "
        "'there was no row' and 'the row said python' are different facts"
    )
    assert columns["candidate_build_before"] is True
    # NOT NULL everywhere the row would otherwise say that something
    # happened without saying what.
    for column in (
        "correlation_id",
        "action",
        "credential_class",
        "recorded_by",
        "review_evidence",
        "schema_digest",
        "document_digest",
        "selected_operation",
        "candidate_build_after",
        "mode_after",
        "recorded_at",
    ):
        assert columns[column] is False, f"{column} must be NOT NULL"


def test_0130_action_vocabulary_is_closed_and_excludes_status(
    migrated: Engine,
) -> None:
    command.upgrade(_config(), _MIGRATION.revision)
    for action in ("enable", "disable", "repoint"):
        _insert(migrated, action=action)
    for action in ("status", "delete", "Enable", ""):
        with pytest.raises(sa.exc.IntegrityError):
            _insert(migrated, action=action)


def test_0130_credential_class_vocabulary_is_closed(migrated: Engine) -> None:
    command.upgrade(_config(), _MIGRATION.revision)
    _insert(migrated, credential_class="effective_principal_envelope")
    _insert(migrated, credential_class="operator_direct", principal_id=None)
    for credential_class in ("service_credential", "human", ""):
        with pytest.raises(sa.exc.IntegrityError):
            _insert(migrated, credential_class=credential_class, principal_id=None)


def test_0130_pairing_binds_principal_id_to_the_credential_class(
    migrated: Engine,
) -> None:
    """The distinction both classes exist for: 'a verified credential named
    this subject' must not be recordable as 'no credential was presented',
    or the reverse."""
    command.upgrade(_config(), _MIGRATION.revision)

    with pytest.raises(sa.exc.IntegrityError):
        _insert(
            migrated,
            credential_class="effective_principal_envelope",
            principal_id=None,
        )
    with pytest.raises(sa.exc.IntegrityError):
        _insert(
            migrated,
            credential_class="operator_direct",
            principal_id="someone",
        )
    _insert(migrated, credential_class="effective_principal_envelope")
    _insert(migrated, credential_class="operator_direct", principal_id=None)


def test_0130_mode_vocabulary_matches_the_routing_table_exactly(
    migrated: Engine,
) -> None:
    """A mode this table can record but go_api_routing_state cannot hold
    would let an audit row describe a state the system cannot be in."""
    command.upgrade(_config(), _MIGRATION.revision)
    assert tuple(_MIGRATION.MODES) == _ROUTING_STATE_MODES

    for mode in _ROUTING_STATE_MODES:
        _insert(migrated, mode_after=mode, mode_before=mode)
    with pytest.raises(sa.exc.IntegrityError):
        _insert(migrated, mode_after="sideways")
    with pytest.raises(sa.exc.IntegrityError):
        _insert(migrated, mode_before="sideways")
    # NULL stays legal for mode_before, and only for it.
    _insert(migrated, mode_before=None)


def test_0130_bounds_the_operator_written_text(migrated: Engine) -> None:
    """Append-only: an accidental paste of a whole log has no later remedy,
    and an empty reason is the same failure as no reason."""
    command.upgrade(_config(), _MIGRATION.revision)
    limit = _MIGRATION.REVIEW_EVIDENCE_MAX

    _insert(migrated, review_evidence="x" * limit)
    with pytest.raises(sa.exc.IntegrityError):
        _insert(migrated, review_evidence="x" * (limit + 1))
    with pytest.raises(sa.exc.IntegrityError):
        _insert(migrated, review_evidence="")
    with pytest.raises(sa.exc.IntegrityError):
        _insert(migrated, recorded_by="")
    with pytest.raises(sa.exc.IntegrityError):
        _insert(migrated, recorded_by="x" * 129)


def test_0130_has_no_foreign_key_to_the_routing_table(migrated: Engine) -> None:
    """Deliberate. The table is append-only and must outlive the row it
    describes; an FK would make deleting a routing row either impossible or
    silently destructive to the record of what happened to it."""
    command.upgrade(_config(), _MIGRATION.revision)
    contypes = _constraints(migrated, _TABLE)
    assert "f" not in contypes.values(), (
        f"{_TABLE} must carry no foreign key: {contypes}"
    )
    # And the CHECKs that ARE expected are all present.
    for name in (
        "ck_go_api_routing_audits_action",
        "ck_go_api_routing_audits_credential_class",
        "ck_go_api_routing_audits_principal_pairing",
        "ck_go_api_routing_audits_mode_before",
        "ck_go_api_routing_audits_mode_after",
        "ck_go_api_routing_audits_review_evidence_bounded",
        "ck_go_api_routing_audits_recorded_by_bounded",
    ):
        assert contypes.get(name) == "c", f"missing CHECK {name}"


def test_0130_leaves_worker_operator_audits_untouched(migrated: Engine) -> None:
    """chris's ruling, pinned: worker_operator_audits belongs to the
    sync -> worker operator plane. The first draft of this migration widened
    its CHECKs; this asserts the shipped one does not, so a future edit that
    reaches for that table again fails here."""
    before = _constraints(migrated, "worker_operator_audits")
    command.upgrade(_config(), _MIGRATION.revision)
    assert _constraints(migrated, "worker_operator_audits") == before

    with pytest.raises(sa.exc.IntegrityError):
        with migrated.connect() as c:
            c.execute(
                sa.text(
                    "INSERT INTO worker_operator_audits (credential_id,"
                    " principal_type, principal_id, action, resource_type,"
                    " resource_id, reason_code, correlation_id, status, created_at)"
                    " VALUES (NULL, 'service_credential', 'who',"
                    " 'go_api_routing.enable', 'r', 'r', 'c', 'corr', 'started',"
                    " now())"
                )
            )
            c.commit()


def test_0130_downgrade_removes_the_table(migrated: Engine) -> None:
    command.upgrade(_config(), _MIGRATION.revision)
    assert _columns(migrated, _TABLE)

    command.downgrade(_config(), _MIGRATION.down_revision)

    assert not _columns(migrated, _TABLE), "downgrade left the table behind"


def _named_go_raw_string(source: str, name: str) -> str:
    """The body of ``const <name> = ` ... ` `` from Go source.

    Raises rather than returning "" when the constant is gone: a mirror
    check whose input silently became empty passes every ``in`` assertion it
    makes, which is the vacuous-green shape these files keep tripping over.
    """
    opener = f"const {name} = `"
    start = source.find(opener)
    if start < 0:
        raise AssertionError(f"{name} is no longer declared as a Go raw string")
    start += len(opener)
    end = source.find("`", start)
    if end < 0:
        raise AssertionError(f"{name} has no closing backtick")
    body = source[start:end]
    if not body.strip():
        raise AssertionError(f"{name} is empty")
    return body


def test_audit_ddl_mirror_matches_the_migration() -> None:
    """The Go integration suite builds this table from a hand-kept DDL
    string (``DDL`` in ``internal/testsupport/routingauditschema/schema.go``,
    shared by internal/goapiproof's own suite and cmd/go-api-routing's
    end-to-end verb tests) so the verbs' audit writes meet the REAL CHECKs.
    A mirror that fell behind would let those tests pass against a schema
    Postgres does not have.

    This check lives on the PYTHON side deliberately: reading the alembic
    files FROM a Go test makes them inputs to the Go workflow, and
    ``go.yml``'s path filters do not cover ``alembic/versions`` -- a
    migration-only PR would then satisfy ``go-quality`` vacuously.
    """
    ddl = _named_go_raw_string(_GO_AUDIT_TEST.read_text(encoding="utf-8"), "DDL")

    for action in _MIGRATION.ACTIONS:
        assert f"'{action}'" in ddl, f"auditDDL does not admit action {action!r}"
    for credential_class in _MIGRATION.CREDENTIAL_CLASSES:
        assert f"'{credential_class}'" in ddl, (
            f"auditDDL does not admit credential class {credential_class!r}"
        )
    for mode in _MIGRATION.MODES:
        assert f"'{mode}'" in ddl
    assert (
        "(credential_class = 'effective_principal_envelope') = "
        "(principal_id IS NOT NULL)" in ddl
    ), "auditDDL is missing the credential-pairing CHECK"
    assert f"BETWEEN 1 AND {_MIGRATION.REVIEW_EVIDENCE_MAX}" in ddl, (
        "auditDDL's review_evidence bound does not match the migration's"
    )

    # Non-vacuity: the loops above pass trivially over empty tuples.
    assert len(_MIGRATION.ACTIONS) == 3
    assert len(_MIGRATION.CREDENTIAL_CLASSES) == 2
    assert len(_MIGRATION.MODES) == 5


def test_go_constants_match_the_migrated_vocabulary() -> None:
    """The Go writer's constants and the migration's CHECKs are one
    vocabulary in two files. If they drift, every audit write fails at
    RUNTIME -- during a rollout operation, the worst possible moment."""
    source = (
        Path(__file__).parents[1] / "internal/goapiproof/routing_audit.go"
    ).read_text(encoding="utf-8")
    for action in _MIGRATION.ACTIONS:
        assert f'"{action}"' in source, f"routing_audit.go does not define {action!r}"
    for credential_class in _MIGRATION.CREDENTIAL_CLASSES:
        assert f'"{credential_class}"' in source
    assert f"auditReviewEvidenceMax = {_MIGRATION.REVIEW_EVIDENCE_MAX}" in source
