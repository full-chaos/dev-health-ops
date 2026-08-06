"""``tests._helpers.sync_postgres_test_url`` must not be a second normalizer.

CHAOS-3450 (#1523) centralized the async->blocking coercion every
Postgres-gated test needs. The first version swapped ``drivername`` and
nothing else, which is correct for CI's bare
``postgresql+asyncpg://user:pass@host:5432/db`` and wrong for every managed
or TLS URI: ``asyncpg`` accepts ``?ssl=`` and ``?channel_binding=``, psycopg2
accepts neither and fails with ``invalid connection option "ssl"``.

Production already owned that translation in
``dev_health_ops.db.normalize_sync_postgres_uri``. Several Postgres-gated
tests called it directly until the helper replaced them, so the swap silently
traded a working normalizer for a partial one -- invisible in CI, where the
URI carries no query string at all.

These tests pin the QUERY-PARAMETER half specifically. The driver half is
already exercised by every Postgres-gated test that connects; the parameter
half is not reachable from CI's URI, so without an explicit case it has no
coverage and the divergence can return unobserved.
"""

from __future__ import annotations

import pytest
from sqlalchemy.engine import make_url

from dev_health_ops.db import normalize_sync_postgres_uri
from tests._helpers import sync_postgres_test_url

_ENV = "DEV_HEALTH_POSTGRES_TEST_URI"

_MANAGED_URI = (
    "postgresql+asyncpg://someuser:s3cr3t@db.example.com:5432/appdb"
    "?ssl=require&channel_binding=require"
)


def test_managed_uri_query_parameters_are_translated_for_psycopg2(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``ssl`` becomes ``sslmode``; ``channel_binding`` is dropped entirely.

    Both are asyncpg-only spellings. Leaving either in place is not a cosmetic
    difference -- psycopg2 rejects the connection outright.
    """
    monkeypatch.setenv(_ENV, _MANAGED_URI)

    url = sync_postgres_test_url()

    assert url.drivername == "postgresql+psycopg2"
    assert url.query.get("sslmode") == "require", (
        f"asyncpg's ssl= was not translated to psycopg2's sslmode=: {url.query}"
    )
    assert "ssl" not in url.query, (
        f'psycopg2 rejects the connection with invalid connection option "ssl"; '
        f"the parameter must not survive: {url.query}"
    )
    assert "channel_binding" not in url.query, (
        f"channel_binding is asyncpg-only and must be dropped: {url.query}"
    )


def test_credentials_survive_the_normalization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The password must not be masked on the way through.

    ``normalize_sync_postgres_uri`` renders the URL back to a string, and
    SQLAlchemy's default rendering replaces the password with ``***``. Passing
    that through would turn a driver mismatch into a "password authentication
    failed" that reads like broken CI credentials -- the exact failure the
    helper's own docstring exists to prevent.
    """
    monkeypatch.setenv(_ENV, _MANAGED_URI)

    url = sync_postgres_test_url()

    assert url.password == "s3cr3t"
    assert url.username == "someuser"
    assert url.host == "db.example.com"
    assert url.port == 5432
    assert url.database == "appdb"


def test_helper_agrees_with_the_production_normalizer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One normalizer, not two.

    Stated as a differential rather than a restatement of the expected query
    string: whatever ``normalize_sync_postgres_uri`` decides about parameters
    is what the helper must produce, so a future production change cannot
    leave the test suite connecting differently from production.
    """
    monkeypatch.setenv(_ENV, _MANAGED_URI)

    expected = make_url(normalize_sync_postgres_uri(_MANAGED_URI))
    actual = sync_postgres_test_url()

    assert dict(actual.query) == dict(expected.query), (
        "the test helper and the production normalizer disagree about "
        f"connection parameters: {dict(actual.query)} != {dict(expected.query)}"
    )


def test_bare_ci_uri_is_unchanged_apart_from_the_driver(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The CI shape keeps working, and keeps carrying no parameters.

    This is the case that made the divergence invisible: it passes under both
    the old and the new helper. It is here so that a future normalizer change
    which starts injecting parameters into a bare URI is caught too.
    """
    monkeypatch.setenv(
        _ENV, "postgresql+asyncpg://postgres:postgres@localhost:5432/test_db"
    )

    url = sync_postgres_test_url()

    assert url.drivername == "postgresql+psycopg2"
    assert dict(url.query) == {}
    assert url.database == "test_db"
