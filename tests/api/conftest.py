"""Shared fixtures for ``tests/api/`` (cascades into ``tests/api/**``).

CI's "test" job provides a real, reachable PostgreSQL service container
(``POSTGRES_URI`` set at the job level) that is deliberately left
unmigrated for the general unit-test tier -- almost every test uses an
ephemeral in-memory SQLite fixture instead, and only opt-in
``DEV_HEALTH_POSTGRES_TEST_URI``-gated modules apply migrations to a real
database. ``ci/local_validate.sh`` never sets ``POSTGRES_URI`` at all, so
this gap is invisible locally.

Any test under here that boots the real app via
``with TestClient(main.app) as client:`` runs the real ``lifespan()``
startup sequence, which (CHAOS-3299) hard-aborts when PostgreSQL is
reachable but missing the required application schema revision -- exactly
CI's situation. That's a false negative unrelated to whatever the test is
actually checking (health-endpoint shape, webhook wiring, work-unit-explain
validation, ...), so the schema-revision check is stubbed satisfied here by
default. See ``test_main_app_integration.py``'s
``test_lifespan_aborts_startup_when_schema_status_is_unsatisfied`` for the
companion proof that overriding this stub still aborts startup -- this
fixture narrows *what* is exercised, not *whether* the guard still works.
"""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _stub_application_schema_status_satisfied(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _satisfied(dsn: str | None = None) -> tuple[bool, tuple[str, ...]]:
        return True, ()

    monkeypatch.setattr(
        "dev_health_ops.migrate.application_schema_status",
        _satisfied,
    )
