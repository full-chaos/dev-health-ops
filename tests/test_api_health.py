import pytest
from sqlalchemy.engine import make_url

from dev_health_ops.api import _health

_FAKE_ASYNC_URI = (
    "postgresql+asyncpg://u:p@pooler.example.com/db"
    "?sslmode=require&channel_binding=require"
)


def _stub_connectivity(monkeypatch, *, captured: dict[str, str]):
    """Stub the connectivity ping to always succeed and record the DSN it saw."""

    async def check(dsn: str) -> bool:
        captured["dsn"] = dsn
        return True

    monkeypatch.setenv("POSTGRES_URI", _FAKE_ASYNC_URI)
    monkeypatch.delenv("DATABASE_URI", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setattr(_health, "_check_sqlalchemy_health_async", check)


def _stub_schema_status(monkeypatch, *, satisfied: bool, captured: dict[str, str]):
    """Stub the schema-revision check (CHAOS-3299) without touching a real DB.

    ``_check_postgres_health`` imports ``application_schema_status`` from
    ``dev_health_ops.migrate`` inside its own body on every call, so the
    patch target is the source module's attribute, not ``_health``'s.
    """

    async def status(dsn: str | None = None) -> tuple[bool, tuple[str, ...]]:
        captured["schema_status_dsn"] = dsn or ""
        return satisfied, ()

    monkeypatch.setattr("dev_health_ops.migrate.application_schema_status", status)


@pytest.mark.asyncio
async def test_postgres_health_uses_normalized_async_uri(monkeypatch):
    """URI normalization only -- the schema-revision check (CHAOS-3299) is
    stubbed satisfied so this stays decoupled from that separate concern
    (see ``test_postgres_health_reports_down_when_schema_revision_behind``
    for the fail-closed case, and the live-PostgreSQL-gated
    ``test_health_postgres_check_reports_down_below_required_revision`` in
    ``tests/test_ask_dev_v2_persistence_startup_gate.py`` for the real
    ancestor-walk proof)."""

    captured: dict[str, str] = {}
    _stub_connectivity(monkeypatch, captured=captured)
    _stub_schema_status(monkeypatch, satisfied=True, captured=captured)

    service, status = await _health._check_postgres_health()

    assert service == "postgres"
    assert status == "ok"
    url = make_url(captured["dsn"])
    assert url.drivername == "postgresql+asyncpg"
    assert url.query["ssl"] == "require"
    assert "sslmode" not in url.query
    assert "channel_binding" not in url.query
    # The schema-revision check reuses the exact same normalized DSN as the
    # connectivity ping -- not a second, independently-resolved URI.
    assert captured["schema_status_dsn"] == captured["dsn"]


@pytest.mark.asyncio
async def test_postgres_health_reports_down_when_schema_revision_behind(monkeypatch):
    """Fail-closed by design (CHAOS-3299): a reachable database missing the
    required application schema revision is ``"down"``, not ``"ok"`` --
    closing the rolling-deploy window a one-time startup check can't. Fast
    mock-level companion to the live-PostgreSQL-gated ancestor-walk proof in
    ``tests/test_ask_dev_v2_persistence_startup_gate.py``."""

    captured: dict[str, str] = {}
    _stub_connectivity(monkeypatch, captured=captured)
    _stub_schema_status(monkeypatch, satisfied=False, captured=captured)

    service, status = await _health._check_postgres_health()

    assert service == "postgres"
    assert status == "down"
